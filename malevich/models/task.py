import warnings
from typing import Callable, Generic, Optional, TypeVar, Union

from malevich_coretools import AppLogs, task_prepare, task_run, task_stop
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from malevich.constants import DEFAULT_CORE_HOST

T = TypeVar("T")
Callback = TypeVar("Callback", bound=Callable[["Task"], None])


class Task(Generic[T]):
    def __init__(
        self,
        alias: str,
        task_id: str,
        cfg_id: str,
        payload: Optional[T] = None,
        host: Optional[tuple[str, str]] = DEFAULT_CORE_HOST,
    ) -> None:
        self.__task_id = task_id
        self.__cfg = cfg_id
        self.__host = host
        self.__operation_id = None
        self.__payload = payload
        self.__prepared = False
        self.__finished = False
        self.__alias = alias

        self.__callbacks: dict[str, list[Callback]] = {
            "onerr": [lambda t: t.stop()],
        }
        self.__progress = Progress(
            SpinnerColumn(), TextColumn(
                "{task.description} | "), TimeElapsedColumn()
        )

    def onerror(self, callback: Callback) -> None:
        self.__callbacks["onerr"].append(callback)

    def prepare(
        self,
        no_callbacks: bool = False,
        quiet: bool = False,
        *args,
        **kwargs,
    ) -> tuple[bool, Union[AppLogs, Exception]]:
        """Prepares the task to be executed at Malevich API.

        Suppresses all errors and returns an Exception in case of failure.

        Args:
            no_callbacks (bool, optional):
                If True, callbacks will not be called on error. Defaults to False.
            quiet (bool, optional):
                If True, no logs will be printed. Defaults to False.
            *args:
                Positional arguments to be passed to the `task_run` function of
                Malevich API.
            **kwargs:
                Keyword arguments to be passed to the `task_run` function of
                Malevich API.

        Returns:
            tuple[bool, Union[AppLogs, Exception]]:
                A tuple of success status and either AppLogs
                (in case of success) or Exception (in case of failure)
        """
        try:
            # Starting task to display progress in
            # CLI interface
            if not quiet:
                __t = self.__progress.add_task(
                    f"Preparing operation [i b yellow]{self.__alias}[/i b yellow]",
                    total=1
                )
                self.__progress.start()

            if 'conn_url' in kwargs:
                warnings.warn(
                    "Keyword argument `conn_url` for `task_prepare` is overriden "
                    "by `host` argument of `Task` class.")
                kwargs.pop('conn_url')

            # Preparing task, if no errors, updating
            # progress bar
            __out = (
                True,
                task_prepare(
                    self.__task_id,
                    cfg_id=self.__cfg,
                    conn_url=self.__host,
                    *args,
                    **kwargs
                ),
            )  # saving output to return it later

            self.__operation_id = __out[1].operationId
            if not quiet:

                self.__progress.update(
                    __t, completed=1,
                    refresh=True,
                    description=(
                        f"[green]✔[/green] Preparing task [i b green]{self.__alias}[/i b green] done. "  # noqa: E501
                        f"Operation ID: [i bright_black]{self.__operation_id}[/ i bright_black]"  # noqa: E501
                    )
                )

            self.__prepared = True
            return __out

        except Exception as e:
            # If error occured, calling callbacks
            if not no_callbacks:
                for func in self.__callbacks['onerr']:
                    func(self)
            # Stopping progress bar to prevent
            # further updates
            try:
                self.__progress.update(__t, description="[red]✘[/red] Error occured. Stopping task preparation.")  # noqa: E501
            except Exception:
                pass
            self.__progress.stop()
            return False, e

    def run(
        self,
        no_callbacks: bool = False,
        quiet: bool = False,
        *args,
        **kwargs,
    ) -> tuple[bool, Union[AppLogs, Exception]]:
        """Runs the task at Malevich API.

        Suppresses all errors and returns an Exception in case of failure.

        Args:
            no_callbacks (bool, optional):
                If True, callbacks will not be called on error. Defaults to False.
            quiet (bool, optional):
                If True, no logs will be printed. Defaults to False.
            *args:
                Positional arguments to be passed to the `task_run` function of
                Malevich API.
            **kwargs:
                Keyword arguments to be passed to the `task_run` function of
                Malevich API.

        Returns:
            tuple[bool, Union[AppLogs, Exception]]:
                A tuple of success status and either AppLogs
                (in case of success) or Exception (in case of failure)
        """
        try:
            # Starting task to display progress in
            # CLI interface
            if not quiet:
                __t = self.__progress.add_task(
                    f"Running operation [i bright_black]{self.__operation_id}"
                    "[/i bright_black]",
                    total=1
                )
                self.__progress.start()

            # Preparing task, if no errors, updating
            # progress bar
            __out = (
                True,
                task_run(
                    self.__operation_id,
                    cfg_id=self.__cfg,
                    *args,
                    **kwargs
                ),
            )  # saving output to return it later

            if not quiet:
                self.__progress.update(
                    __t, completed=1, refresh=True,
                    description=f"[green]✔[/green] Task run finished. Operation ID: [i bright_black]{self.__operation_id}[/i bright_black]"  # noqa: E501
                )
            self.__prepared = True
            return __out

        except Exception as e:
            # If error occured, calling callbacks
            if not no_callbacks:
                for func in self.__callbacks['onerr']:
                    func(self)
            # Stopping progress bar to prevent
            # further updates
            try:
                self.__progress.update(__t, description="[red]✘[/red] Error occured. Stopping task running.")  # noqa: E501
            except Exception:
                pass
            self.__progress.stop()
            return False, e

    def stop(
        self, *args, **kwargs
    ) -> tuple[bool, Union[AppLogs, Exception]]:
        """Stops the task at Malevich API.

        Args:
            *args:
                Positional arguments to be passed to the `task_run` function of
                Malevich API.
            **kwargs:
                Keyword arguments to be passed to the `task_run` function of
                Malevich API.

        Returns:
            tuple[bool, Union[AppLogs, Exception]]:
                A tuple of success status and either AppLogs
                (in case of success) or Exception (in case of failure)
        """
        if 'conn_url' in kwargs:
            warnings.warn(
                "Keyword argument `conn_url` for `task_stop` is overriden "
                "by `host` argument of `Task` class.")
            kwargs.pop('conn_url')

        try:
            __out = True, task_stop(
                self.__operation_id,
                conn_url=self.__host,
                *args,
                **kwargs
            )

            self.__progress.stop()
            return __out
        except Exception as e:
            return False, e

    @property
    def finished(self) -> bool:
        return self.__finished

    @property
    def prepared(self) -> bool:
        return self.__prepared

    @property
    def task_id(self) -> str:
        return "" + self.__task_id

    @property
    def payload(self) -> str:
        return self.__payload
