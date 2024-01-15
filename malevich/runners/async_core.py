from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count
from typing import Optional, TypeVar
from uuid import uuid4 as u

import malevich_coretools as mct
import pandas as pd

from .._core.ops import batch_upload_collections
from ..constants import DEFAULT_CORE_HOST
from ..interpreter.core import CoreInjectable, CoreInterpreter
from ..models.collection import Collection
from ..models.flow_function import FlowFunction
from ..models.task.interpreted import InterpretedTask
from .base import BaseRunner

AsyncCoreInjections = TypeVar("AsyncCoreInjections", bound=pd.DataFrame)
Callback = TypeVar("Callback", bound=list[pd.DataFrame])
Executor = ThreadPoolExecutor


class AsyncCoreRunner(BaseRunner[AsyncCoreInjections]):
    def __init__(
        self,
        task: FlowFunction,
        interpreter: CoreInterpreter = None,
        core_auth: mct.AUTH = None,
        core_host: Optional[str] = DEFAULT_CORE_HOST,
        workers: int = max(cpu_count() >> 1, 1),
    ) -> None:
        super().__init__(task)
        if not interpreter:
            interpreter = CoreInterpreter(
                core_auth=core_auth, core_host=core_host
            )
        self._interpreter = interpreter
        self._core_auth = core_auth
        self._core_host = core_host
        self._interpreted_task: InterpretedTask = None
        self._workers = workers
        self._executor: Executor = None
        self._prepared = False

    def _boot(self, workers: Optional[int] = None) -> None:
        workers = workers or self._workers
        if self._executor:
            self._executor.shutdown()
        self._executor = Executor(
            max_workers=workers
        )

    def is_ready(self) -> bool:
        try:
            # mct.ping()
            mct.check_auth(
                # login=self._core_auth[0],
                conn_url=self._core_host,
                auth=self._core_auth
            )
        except Exception as e:
            return False, e
        return True, None

    def _prepare(self) -> None:
        if not self._interpreted_task:
            raise Exception("Task was not interpreted")
        elif self._prepared:
            return
        self._interpreted_task.prepare(with_logs=True, debug_mode=True)
        self._prepared = True

    def _intepret(self, **func_kwargs) -> None:
        print("Interpreting task")
        status, e_ = self.is_ready()
        self._boot()
        if not status:
            raise Exception(
                "Could not connect to Core. Check your credentials and host."
            ) from e_
        task_ = self._base_task_f(**func_kwargs)
        task_.interpret(self._interpreter)
        self._interpreted_task = task_.get_interpreted_task()

    def run_options(self) -> list[str]:
        if not self._interpreted_task:
            self._intepret()

        return [i.get_inject_key() for i in self._interpreted_task.get_injectables()]

    def run(self, *, callback: Callback, **injections: AsyncCoreInjections) -> None:
        assert all([isinstance(i, pd.DataFrame) for i in injections.values()]), \
            "All injections must be pandas DataFrames"

        if not self._interpreted_task:
            self._intepret()
        self._prepare()
        print('operation_id', self._interpreted_task.state.params.operation_id)

        injectables: list[CoreInjectable] = self._interpreted_task.get_injectables()
        run_id = u().hex

        collections = [
            Collection(
                collection_id=f'async-core-runner-override-{k}-{run_id}',
                collection_data=v,
                persistent=False
            ) for k, v in injections.items()
        ]
        core_ids = batch_upload_collections(
            collections,
            auth=self._core_auth,
            conn_url=self._core_host
        )

        key_to_core_id = {
            k: core_id
            for k, core_id in zip(injections.keys(), core_ids)
        }

        real_injections = {
            injectable.get_inject_data(): key_to_core_id[injectable.get_inject_key()]
            for injectable in injectables
        }

        fut_ = self._executor.submit(
            self._interpreted_task.run,
            # task=self._interpreted_task,
            overrides=real_injections,
            run_id=run_id,
            with_logs=True,
            debug_mode=True
        )

        fut_.add_done_callback(
            lambda _: callback(
                self._interpreted_task.results(run_id=run_id)
            )
        )

    def wait(self) -> None:
        # Gracefully shutdown the executor
        self._executor.shutdown()
        if self._interpreted_task:
            try:
                self._interpreted_task.stop()
            except Exception:
                pass

    def stop(self) -> None:
        if self._interpreted_task:
            try:
                self._interpreted_task.stop()
            except Exception:
                pass
