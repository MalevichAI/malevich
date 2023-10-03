from jls_utils import task_prepare, task_run, task_stop


class Task:
    def __init__(self, task_id: str, cfg: str) -> None:
        self.__task_id = task_id
        self.__cfg = cfg
        self.__operation_id = None

    @property
    def task_id(self) -> str:
        return "" + self.__task_id

    def prepare(self, *args, **kwargs):  # noqa: ANN002, ANN003, ANN201
        self.__operation_id = task_prepare(
            self.__task_id, self.__cfg, *args, **kwargs
        ).operationId

    def run(self, *args, **kwargs):  # noqa: ANN201, ANN002, ANN003
        task_run(self.__operation_id, *args, **kwargs)

    def stop(self, *args, **kwargs):  # noqa: ANN201, ANN002, ANN003
        task_stop(self.__operation_id, *args, **kwargs)

