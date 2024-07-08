from pydantic import BaseModel


class AutoflowLink(BaseModel):
    index: int
    name: str
