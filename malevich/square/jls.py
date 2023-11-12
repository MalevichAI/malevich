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
    def wrapper(fun: Callable) -> Callable:
        return fun
    return wrapper


def output(id: Optional[str] = None, collection_name: Optional[str] = None, collection_names: Optional[List[str]] = None, finish_msg: Optional[str] = None, drop_internal: bool = True):    # noqa: ANN201, E501
    def wrapper(fun: Callable) -> Callable:
        return fun
    return wrapper


def scheme():  # noqa: ANN201
    def wrapper(cl):  # noqa: ANN202
        return cl
    return wrapper


def init(id: Optional[str] = None, enable: bool = True, tl: Optional[int] = None, prepare: bool = False):   # noqa: ANN201, E501
    def wrapper(fun: Callable) -> Callable:
        return fun
    return wrapper
