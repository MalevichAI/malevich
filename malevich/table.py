import os

_backend = os.environ.get("MALEVICH_DF_BACKEND", "pandas")

match _backend:
    case "pandas":
        try:
            import pandas as pd
            _table_cls = pd.DataFrame
        except ImportError:
            pass
    case _:
        _table_cls = None

if not _table_cls:
    raise ImportError(
        "No backend found for meta table"
    )

class table(_table_cls):  # noqa: N801
    pass

__all__ = ["table"]
