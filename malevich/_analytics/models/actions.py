from pydantic import BaseModel


class BaseAction(BaseModel):
    pass


class CLIAction(BaseAction):
    full_command: str

class CoreRequestAction(BaseAction):
    query: str
    response_status: int
    response_raw: bytes
    response_media_type: str
