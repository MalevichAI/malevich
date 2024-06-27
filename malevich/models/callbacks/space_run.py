from pydantic import BaseModel

from malevich.core_api import ResultDoc


class SpaceCallbackResult(BaseModel):
    in_flow_id: str
    core_id: str
    docs: list[ResultDoc]
    in_flow_alias: str | None = None
    core_alias: str | None = None
    core_username: str | None = None
    core_password: str | None = None


class SpaceCallbackBody(BaseModel):
    run_id: str | None = None
    task_id: str | None = None
    in_flow_id: str | None = None
    results: list[SpaceCallbackResult]


