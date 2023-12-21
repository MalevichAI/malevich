from malevich.square.jls import scheme
from pydantic import BaseModel


@scheme()
class default_scheme(BaseModel):  # noqa: N801
    data: object
