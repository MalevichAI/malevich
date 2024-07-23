from typing import Callable, List, Optional


def input_doc(id: Optional[str] = None, collection_from: Optional[str] = None, collections_from: Optional[List[str]] = None, extra_collection_from: Optional[str] = None, extra_collections_from: Optional[List[str]] = None, query: Optional[str] = None, finish_msg: Optional[str] = None):  # noqa: ANN201, E501
    def wrapper(fun: Callable) -> Callable:
        return fun
    return wrapper


def input_df(id: Optional[str] = None, collection_from: Optional[str] = None, collections_from: Optional[List[str]] = None, extra_collection_from: Optional[str] = None, extra_collections_from: Optional[List[str]] = None, by_args: bool = False, query: Optional[str] = None, finish_msg: Optional[str] = None):  # noqa: ANN201, E501
    def wrapper(fun: Callable) -> Callable:
        return fun
    return wrapper


def input_true(id: Optional[str] = None, collection_from: Optional[str] = None, collections_from: Optional[List[str]] = None, extra_collection_from: Optional[str] = None, extra_collections_from: Optional[List[str]] = None, query: Optional[str] = None, finish_msg: Optional[str] = None):  # noqa: ANN201, E501
    def wrapper(fun: Callable) -> Callable:
        return fun
    return wrapper


def processor(id: Optional[str] = None, finish_msg: Optional[str] = None, drop_internal: bool = True, get_scale_part_all: bool = False):    # noqa: ANN201, E501
    """Denotes a processor of the app.

    Processors are core logical units of the app. To
    understand processors more, see :doc:`What is Processor? </SDK/Apps/Processors>`
    
    Args:
        id: The id of the processor. If not provided, the name of the function will be used.
        finish_msg: The message to be sent to e-mail (if configured) when the processor finishes.
        drop_internal: Whether to drop the internal collections after the processor finishes.
        get_scale_part_all: Whether to get the scale part of the input collection.

    Returns:
        The decorated function.
    """ # noqa: E501
    def wrapper(fun: Callable) -> Callable:
        return fun
    return wrapper


def output(id: Optional[str] = None, collection_name: Optional[str] = None, collection_names: Optional[List[str]] = None, finish_msg: Optional[str] = None, drop_internal: bool = True):    # noqa: ANN201, E501
    def wrapper(fun: Callable) -> Callable:
        return fun
    return wrapper


def condition(id: Optional[str] = None, finish_msg: Optional[str] = None, drop_internal: bool = True):    # noqa: ANN201, E501
    """Denotes a condition of the app.

    Conditions are core logical units of the app.

    Args:
        id: The id of the condition. If not provided, the name of the function will be used.
        finish_msg: The message to be sent to e-mail (if configured) when the condition finishes.
        drop_internal: Whether to drop the internal collections after the condition finishes.
        get_scale_part_all: Whether to get the scale part of the input collection.

    Returns:
        The decorated function.
    """ # noqa: E501
    def wrapper(fun: Callable) -> Callable:
        return fun
    return wrapper


def scheme():  # noqa: ANN201
    """Denotes a class as a scheme that might be used in DF/DFS/M/Sink annotations.

    The class automatically derives from :class:`pydantic.BaseModel`.

    Example:

    .. code-block:: python

        @scheme()
        class Users:
            name: str
            age: int

        @processor()
        def process_users(users: DF[Users]):
    """
    def wrapper(cl):  # noqa: ANN202
        return cl
    return wrapper


def init(id: Optional[str] = None, enable: bool = True, tl: Optional[int] = None, prepare: bool = False):   # noqa: ANN201, E501
    """Denotes an initialization function.

    Initialization functions are called before the app starts.
    Depending on the configuration, they might be called on
    task preparation stage or before each task run.

    Args:
        id: The id of the initialization function. If not provided, the name of the function will be used.
        enable: Whether to enable the initialization function. Defaults to True.
        tl: The time limit of the initialization function (in seconds).
        prepare: Whether to call the initialization function on task preparation stage.
    """ # noqa: E501
    def wrapper(fun: Callable) -> Callable:
        return fun
    return wrapper
