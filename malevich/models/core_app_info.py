# WARN: Models can be not up to date with the ones returned by the API

from typing import Dict, List, Optional, Tuple

from pydantic import BaseModel


class FunctionInfo(BaseModel):
    id: str
    name: str
    arguments: List[Tuple[str, Optional[str]]]
    finishMsg: Optional[str]  # noqa: N815
    doc: Optional[str]


class InputFunctionInfo(FunctionInfo):
    collectionsNames: Optional[List[str]]  # noqa: N815
    extraCollectionsNames: Optional[List[str]]  # noqa: N815
    query: Optional[str]
    mode: str


class ProcessorFunctionInfo(FunctionInfo):
    pass


class OutputFunctionInfo(FunctionInfo):
    collectionOutNames: Optional[List[str]]  # noqa: N815


class InitInfo(BaseModel):
    id: str
    enable: bool
    tl: Optional[int]
    prepare: bool
    argname: Optional[str]
    doc: Optional[str]


class AppFunctionsInfo(BaseModel):
    inputs: Dict[str, InputFunctionInfo] = dict()
    processors: Dict[str, ProcessorFunctionInfo] = dict()
    outputs: Dict[str, OutputFunctionInfo] = dict()
    schemes: Dict[str, str] = dict()
    inits: Dict[str, InitInfo] = dict()
    logs: Optional[str] = None
