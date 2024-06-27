from .tracer import traced, tracedLike


class notrace:  # noqa: N801
    def __init__(self) -> None:
        self.__traced_new = None
        self.__traced_like_new = None

    def __enter__(self) -> None:
        self.__traced_new = traced.__new__
        self.__traced_like_new = tracedLike.__new__

        def passthrough(cls, obj):
            return obj

        passthrough.__name__ = '__NoTrace__'

        setattr(traced, "__new__", passthrough)
        setattr(tracedLike, "__new__", passthrough)

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        setattr(traced, "__new__", self.__traced_new)
        setattr(tracedLike, "__new__", self.__traced_like_new)
