from jls_utils import task_run, task_stop


class Task:
    def __init__(self, task_id: str) -> None:
        self.__task_id = task_id

    @property
    def task_id(self) -> str:
        return "" + self.__task_id

    def run(self, *args, **kwargs):
        task_run(self.__task_id, *args, **kwargs)

    def stop(self, *args, **kwargs):
        task_stop(self.__task_id, *args, **kwargs)

