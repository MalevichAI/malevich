try:
    from malevich_app.export.secondary.jls_imported import imported3, set_imported3

    if not imported3:
        from malevich_app.jls_lib.utils import *
        from malevich_app.export.jls.jls import input_doc, input_df, input_true, processor, output, condition, scheme, init
        from malevich_app.export.jls.df import M, DF, DFS, Sink, OBJ, Doc, Docs
        from malevich_app.export.defaults.schemes import *
        set_imported3(True)
    from malevich_app.docker_export.schemes import *
except ImportError:
    from .jls import *
    from .utils import *
    from .defaults import *
    from .df import DF, DFS, M, Sink, OBJ, Doc, Docs
