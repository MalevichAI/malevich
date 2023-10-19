from pydantic import BaseModel


class CanInterpretVerdict(BaseModel):
    interpreter_name: str
    verdict: bool
    reason: str
