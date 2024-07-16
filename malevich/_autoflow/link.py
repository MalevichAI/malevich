from pydantic import BaseModel


class AutoflowLink(BaseModel):
    index: int
    name: str
    in_sink: bool = False
