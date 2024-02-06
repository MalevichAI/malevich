import os
from functools import cached_property
from typing import (
    Any,
    ForwardRef,
    Generic,
    Iterator,
    List,
    Optional,
    TypeVar,
    Union,
)

import pandas as pd
from typing_extensions import TypeVarTuple, Unpack

Schemes = TypeVarTuple('Schemes')
Scheme = TypeVar('Scheme')
SchemeM = TypeVar('SchemeM')


def _is_M(type: Any) -> bool:  # noqa: N802, ANN401
    return hasattr(type, "__origin__") and type.__origin__.__module__ == __name__ and type.__origin__.__name__ == "M"  # noqa: E501


class M(Generic[SchemeM]):
    """Special indicator to be used in :class:`DFS` to denote variable number of inputs.
    
    See :class:`DFS` for more details.
    """
    pass


class DF(Generic[Scheme], pd.DataFrame):
    """Wrapper class for tabular data.
    
    DF (and DFS) classes are used to denote tabular data.
    They can specify the scheme of the data. The scheme can be a reference 
    tp class adecorated with :func:`malevich.square.jls.scheme`
    or simply a string containing the name of the scheme.
    
    DF may follow an interface of different data frames implementation.
    But, most of the time, it is just a wrapper of :class:`pandas.DataFrame`.
    So, you can use all the methods of :class:`pandas.DataFrame` directly on DF object.
    
    .. warning::
    
        You should not construct DF directly. Instead, when
        returning results from processors, you should use
        any of supported data frames classes directly.
        
        For example, you may return a :class:`pandas.DataFrame`:
        
        .. code-block:: python
    
            import pandas as pd
        
            @processor()
            def my_processor(df: DF):
                return pd.DataFrame(...)

    """
    def __init__(self, df: pd.DataFrame) -> None:
        super().__init__(df)

    def cast(self, scheme: str) -> None:
        raise NotImplementedError("scheme cast not yet implemented")    # TODO

    def scheme(self) -> None:
        raise NotImplementedError("scheme not yet implemented")         # TODO

    @cached_property
    def scheme_name(self) -> Optional[str]:
        """Returns the name of the scheme of the data frame."""
        scheme = self._scheme_cls()
        if scheme is None:
            return None
        if hasattr(scheme, "__name__"):
            return scheme.__name__
        if hasattr(scheme, "_name"):
            return scheme._name
        if isinstance(scheme, str):
            return scheme
        if isinstance(scheme, ForwardRef):
            return scheme.__forward_arg__
        return scheme

    @cached_property
    def _scheme_cls(self) -> Optional[Any]:  # noqa: ANN401
        if hasattr(self, "__orig_class__"):
            return self.__orig_class__.__args__[0]
        return None


class DFS(Generic[Unpack[Schemes]]):
    """Wrapper class for tabular data.
    
    DFS is a container for multiple DFs. It is used to denote an
    output of processors that return multiple data frames. 
    
    Each of the elements of DFS is also a :class:`DF` or :class:`DFS`.
    """
    def __init__(self) -> None:
        """set dfs with init"""
        self.__dfs: List[Union[DF, DFS, OBJ, None]] = []
        self.__inited = False

    def init(self, *dfs: pd.DataFrame, nested: bool = False) -> 'DFS':
        """must be called after __init__, nested should be False"""
        assert not self.__inited, "DFS already inited"
        self.__inited = True
        self.__init(list(dfs), nested)
        return self

    def __add_jdf(self, df: Union[str, pd.DataFrame], type) -> None:
        if isinstance(df, str):
            self.__dfs.append(OBJ(df))
        else:
            self.__dfs.append(DF[type](df))

    def __init(self, dfs: List[pd.DataFrame], nested: bool = False) -> None:
        types = self.__orig_class__.__args__ if hasattr(self, "__orig_class__") else [Any for _ in dfs]  # noqa: E501
        many_df_index = None
        for i, type in enumerate(types):
            if _is_M(type):
                assert not nested, "nested M in DFS"
                if many_df_index is not None:
                    raise Exception("more than one M in DFS")
                else:
                    many_df_index = i
        if many_df_index is None:
            assert len(types) == len(dfs), f"wrong arguments size: expected {len(types)}, found {len(dfs)}"  # noqa: E501
            for df, type in zip(dfs, types):
                self.__add_jdf(df, type)
        else:
            assert len(types) - 1 <= len(dfs), f"wrong arguments size: expected at least {len(types) - 1}, found {len(dfs)}"    # noqa: E501
            for df, type in zip(dfs[:many_df_index], types[:many_df_index]):
                self.__add_jdf(df, type)
            count = len(dfs) + 1 - len(types)
            if count != 0:
                type_many = types[many_df_index].__args__[0]
                temp = DFS[tuple([type_many] * count)]().init(*dfs[many_df_index:many_df_index + count], nested=True)  # noqa: E501
                self.__dfs.append(temp)
            else:
                self.__dfs.append(None)
            for df, type in zip(dfs[many_df_index + count:], types[many_df_index + 1:]):
                self.__add_jdf(df, type)

    def __len__(self) -> int:
        """Returns the number of elements in the DFS"""
        return len(self.__dfs)

    def __getitem__(self, key: int) -> Union[DF, 'DFS', 'OBJ', None]:
        """Returns the i-th element of the DFS"""
        return self.__dfs[key]

    def __iter__(self) -> Iterator[Union[DF, 'DFS', 'OBJ', None]]:
        """Returns an iterator over the elements of the DFS"""
        return iter(self.__dfs)


class Sink(Generic[Unpack[Schemes]]):
    """Wrapper class to denote a specific type inputs to processor
    
    Normally, each argument in processor function signature corresponds to
    exactly one output of the previous processor or exactly one collection.
    
    To denote a processor that is able to accept a variable number of inputs,
    you should use this class.
        
    Argument of type `Sink` should be the only argument
    of the processor function besides Context.
    
    ```python
    
        from typing import Any
        from malevich.square import Sink, DFS, M, processor
        
        @processor()
        def merge_tables_sink(dfs: Sink["sql_tables"]):
            pass
            
        @processor()
        def merge_tables_dfs(dfs: DFS[M["sql_tables"]]):
            pass
    ```
            
    Here, we have two processors. `Sink[schema]` is
    equivalent to `List[DFS[M[schema]]]`. 
    
    The difference between two processors lies in the fact that
    the first one can be connected to any number of processors
    that return data frames with scheme "sql_tables", while the
    second one can be connected to exactly one processor that
    returns any number of data frames with scheme "sql_tables".
    
    """
    def __init__(self) -> None:
        """set sink with init"""
        self.__data: List[DFS] = []
        self.__inited = False

    def init(self, *list_dfs: List[pd.DataFrame]) -> 'Sink':
        """must be called after __init__"""
        assert not self.__inited, "Sink already inited"
        self.__inited = True
        self.__init(list(list_dfs))
        return self

    def __init(self, list_dfs: List[List[pd.DataFrame]]) -> None:
        types = self.__orig_class__.__args__ if hasattr(self, "__orig_class__") else None   # noqa: E501
        for dfs in list_dfs:
            cur_types = [Any for _ in dfs] if types is None else types
            self.__data.append(DFS[cur_types]().init(*dfs))

    def __len__(self) -> int:
        return len(self.__data)

    def __getitem__(self, key: int) -> DFS:
        return self.__data[key]

    def __iter__(self) -> Iterator[DFS]:
        return iter(self.__data)


class OBJ:
    """Wrapper class that represents files (or folders)
    
    Used in the same way as :class:`DF`, but provides
    additional functionality to work with files and folders.
    """
    def __init__(self, path: str) -> None:
        self.__path: Any = path

    @property
    def path(self) -> str:
        """A real path to the binary object (or a folder)"""
        return self.__path

    @cached_property
    def raw(self) -> bytes:
        """Reads the binary object from the path"""
        with open(self.__path, 'rb') as f:
            data = f.read()
        return data

    @cached_property
    def as_df(self) -> DF['obj']:   # noqa: F821
        """df, file paths"""
        paths = []
        if os.path.isfile(self.__path):
            paths.append(self.__path)
        else:
            for address, _, files in os.walk(self.__path):
                for name in files:
                    paths.append(os.path.join(address, name))
        return DF['obj'](pd.DataFrame.from_dict({"path": paths}))

    @cached_property
    def df(self) -> pd.DataFrame:
        """Reads the asset as a data frame as .csv file
        
        Raises:
            Exception: If asset is not pointed to a .csv file 
        """
        return pd.read_csv(self.__path)
