from .base import BaseCoreState


class LocalInterpreterState(BaseCoreState):
    """State of the LocalInterpreter"""
    import_paths: set[str] = set()

