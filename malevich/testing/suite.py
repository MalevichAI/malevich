import os
import sys
from typing import Any, Type

from pydantic import BaseModel
from pytest import fixture

from malevich.constants import TEST_DIR
from malevich.interpreter.abstract import Interpreter
from malevich.manifest import OverrideManifest
from malevich.models.flow_function import FlowFunction
from malevich.models.installers.image import ImageDependency
from malevich.models.installers.space import SpaceDependency
from malevich.models.results.base import BaseResult
from malevich.models.task.interpreted.space import SpaceTask
from malevich.models.task.promised import PromisedTask

from .env import EnvManager

env_manager = EnvManager()
manifest_override = OverrideManifest(TEST_DIR)

@fixture(autouse=True, scope='session')
def clean_env() -> None:
    global env_manager
    # env_manager.clean_env()

class FlowTestEnv(BaseModel):
    dependencies: dict[str, ImageDependency | SpaceDependency] = []
    env_vars: dict[str, str] = {}


class FlowTestSuite:
    environment: FlowTestEnv = None
    func_args: list[Any] = []
    func_kwargs: dict[str, Any] = {}
    prepare_args: list[Any] = []
    prepare_kwargs: dict[str, Any] = {}
    run_args: list[Any] = []
    run_kwargs: dict[str, Any] = {}
    result_kwargs: dict[str, Any] = {}
    interpreter: Interpreter = None

    @staticmethod
    def __offload_modules(stubs) -> None:
        packages = ['malevich.' + x[0] for x in stubs]
        for package in packages:
            for module in [*sys.modules.keys()]:
                if package in module:
                    sys.modules.pop(module)


    @staticmethod
    def on_interpretation(task: PromisedTask) -> None:
        pass

    @staticmethod
    def on_interpretation_error(task: PromisedTask, error: Exception) -> Exception | None:  # noqa: E501
        return error

    @staticmethod
    def on_prepare_start(flow: FlowFunction) -> None:
        pass

    @staticmethod
    def on_prepare_end(flow: FlowFunction, output: Any) -> None:  # noqa: ANN401
        pass

    @staticmethod
    def on_prepare_error(flow: FlowFunction, error: Exception) -> Exception | None:
        return error

    @staticmethod
    def on_run_start(flow: FlowFunction) -> None:
        pass

    @staticmethod
    def on_run_end(flow: FlowFunction, output: Any) -> None:  # noqa: ANN401
        pass

    @staticmethod
    def on_run_error(flow: FlowFunction, error: Exception) -> Exception | None:
        return error

    @staticmethod
    def on_result(flow: FlowFunction, results: list[BaseResult]) -> None:
        pass

    @staticmethod
    def on_result_error(flow: FlowFunction, error: Exception) -> Exception | None:
        return error

    @classmethod
    def test_flow(cls: Type['FlowTestSuite']) -> None:
        old_stubs, _ = env_manager.current_env()
        cls.__offload_modules(old_stubs)
        env_manager.request_env([*cls.environment.dependencies.values()])
        stubs, _ = env_manager.current_env()
        for k, v in cls.environment.env_vars.items():
            os.environ[k] = v
        cls.__offload_modules(stubs)
        for attr in dir(cls):
            f = getattr(cls, attr)
            if isinstance(f, FlowFunction):
                with manifest_override:
                    task: PromisedTask = f(
                        *cls.func_args,
                        **cls.func_kwargs
                    )
                    try:
                        task.interpret(cls.interpreter)
                        if isinstance(t := task.get_interpreted_task(), SpaceTask):
                            # FIXME: local fix for upload, remove later
                            print("UPLOADING")
                            t.upload()
                        cls.on_interpretation(task)
                    except Exception as e:
                        if e_ := cls.on_interpretation_error(task, e):
                            raise e_
                    if not isinstance(task, PromisedTask):
                        raise TypeError(
                            f"FlowFunction {f} did not return a PromisedTask"
                        )

                    try:
                        cls.on_prepare_start(f)
                        cls.on_prepare_end(
                            f,
                            task.prepare(
                                *cls.prepare_args,
                                **cls.prepare_kwargs
                            )
                        )
                    except Exception as e:
                        if e_ := cls.on_prepare_error(f, e):
                            raise e_


                    try:
                        cls.on_run_start(f)
                        cls.on_run_end(
                            f,
                            task.run(
                                *cls.run_args,
                                **cls.run_kwargs
                            )
                        )
                    except Exception as e:
                        if e_ := cls.on_run_error(f, e):
                            raise e_

                    # try:
                    #     task
                    # except Exception:
                    #     pass

                    try:
                        results = task.results(
                            **cls.result_kwargs
                        )
                        cls.on_result(f, results)
                    except Exception as e:
                        if e_ := cls.on_result_error(f, e):
                            raise e_

        for k in cls.environment.env_vars:
            os.environ.pop(k)
