import json
import os
from functools import cache, cached_property
from typing import (
    Any,
    Dict,
    ForwardRef,
    Generic,
    Iterator,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
)

import pandas as pd
from pydantic import BaseModel
from typing_extensions import TypeVarTuple, Unpack

Schemes = TypeVarTuple('Schemes')
Scheme = TypeVar('Scheme')

_docs_first_k_show = 3
DELIMITER = "-" * 50


def _is_M(type: Any) -> bool:  # noqa: N802, ANN401
    return hasattr(type, "__origin__") and type.__origin__.__module__ == __name__ and type.__origin__.__name__ == "M"  # noqa: E501


class M(Generic[Scheme]):
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
    def __init__(self, df: Union[pd.DataFrame, Type[BaseModel], Dict, List, 'Doc', 'Docs']) -> None:    # noqa: E501
        if df is None:
            super().__init__(df)
            return

        if isinstance(df, Docs):
            df = df.parse(recursive=True)
        elif isinstance(df, Doc):
            df = [df.dict()]
        elif isinstance(df, Dict):  # Doc
            df = [df]
        elif issubclass(df.__class__, BaseModel):
            df = [df.model_dump()]
        else:
            assert isinstance(df, List) or isinstance(df, pd.DataFrame), f"DF create: expected pd.DataFrame (or Docs, Doc, BaseModel, List, Dict), found {type(df)}"    # noqa: E501
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
        self.__dfs: List[Union[DF, DFS, OBJ, Doc, Docs, None]] = []
        self.__inited = False

    def init(self, *dfs: Union[str, pd.DataFrame, Type[BaseModel], Dict, List], nested: bool = False) -> 'DFS': # noqa: E501
        """must be called after __init__, nested should be False"""
        assert not self.__inited, "DFS already inited"
        self.__inited = True
        self.__init(list(dfs), nested)
        return self

    def __add_jdf(self, df: Union[str, pd.DataFrame, Type[BaseModel], Dict, List], type) -> None: # noqa: E501
        if isinstance(df, str):
            self.__dfs.append(OBJ(df))
        elif (hasattr(type, "__origin__") and type.__origin__ is Doc) or type is Doc:
            self.__dfs.append(type(df).init())
        elif (type is Any or type is None) and (isinstance(df, Dict) or issubclass(df.__class__, BaseModel)):   # noqa: E501
            self.__dfs.append(Doc[type](df))
        else:
            self.__dfs.append(DF[type](df))

    def __init(self, dfs: List[Union[str, pd.DataFrame, Type[BaseModel], Dict, List]], nested: bool = False) -> None:   # noqa: E501
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

    def __getitem__(self, key: int) -> Union[DF, 'DFS', 'OBJ', 'Doc', 'Docs', None]:
        """Returns the i-th element of the DFS"""
        return self.__dfs[key]

    def __iter__(self) -> Iterator[Union[DF, 'DFS', 'OBJ', 'Doc', 'Docs', None]]:
        """Returns an iterator over the elements of the DFS"""
        return iter(self.__dfs)

    def __repr__(self) -> str:
        return f"\n{DELIMITER}\n".join(map(lambda x: str(x), self))


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

    def init(self, *list_dfs: List[Union[str, pd.DataFrame, Type[BaseModel], Dict, List]]) -> 'Sink':   # noqa: E501
        """must be called after __init__"""
        assert not self.__inited, "Sink already inited"
        self.__inited = True
        self.__init(list(list_dfs))
        return self

    def __init(self, list_dfs: List[List[Union[str, pd.DataFrame, Type[BaseModel], Dict, List]]]) -> None:  # noqa: E501
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

    def __repr__(self) -> str:
        return f"\n{DELIMITER * 2}\n".join(map(lambda x: str(x), self))


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
    def as_df(self) -> DF['obj']:
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

    def __repr__(self) -> str:
        return f"OBJ(path={self.__path})"


class Doc(Generic[Scheme]):
    def __init__(self, data: Union[Scheme, Dict, 'Doc', pd.DataFrame, List]) -> None:
        if isinstance(data, List):
            assert len(data) == 1, f"Doc create: too big List, expected size=1, found={len(data)}"  # noqa: E501
            data = data[0]
        if isinstance(data, Doc):
            data = data.parse()
        elif isinstance(data, pd.DataFrame):
            assert data.shape[0] == 1, f"Doc create: too big pd.DataFrame, expected size=1, found={data.shape[0]}"  # noqa: E501
            data = data.to_dict(orient="records")[0]
        assert data is None or isinstance(data, Dict) or issubclass(data.__class__, BaseModel), f"wrong Doc data type: expected Dict, pd.DataFrame or subclass of BaseModel, found {type(data)}"    # noqa: E501
        self.__data: Union[Scheme, Type[BaseModel], Dict] = data

    def parse(self) -> Union[Scheme, Type[BaseModel], Dict]:
        return self.__data

    def init(self) -> 'Doc':
        """must be called after __init__"""
        if self.__data is None:
            return self

        scheme = self._scheme_cls
        if scheme is Any:
            assert isinstance(self.__data, Dict) or issubclass(self.__data.__class__, BaseModel), f"wrong Doc data type: expected Dict or subclass of BaseModel, found {type(self.__data)}" # noqa: E501
            return self
        if scheme is None or scheme.__name__ == "NoneType":
            return self
        if issubclass(scheme, BaseModel):
            if isinstance(self.__data, Dict):
                self.__data = scheme(**self.__data)
            elif isinstance(self.__data, scheme):
                pass
            else:
                self.__data = scheme(**self.__data.model_dump())
        elif isinstance(scheme, str):
            # json_scheme = schemes[scheme]
            raise Exception(f"Doc not yet work with user json schemes: {scheme}")
        else:
            raise Exception(f"Unknown Doc type: {scheme}")
        return self

    def __repr__(self) -> str:
        return f"Doc(__data={{{self.__data}}})"

    @cached_property
    def _scheme_cls(self) -> Optional[Any]: # noqa: ANN401
        if hasattr(self, "__orig_class__"):
            return self.__orig_class__.__args__[0]
        return None

    def dict(self) -> Dict[str, Any]:
        if isinstance(self.__data, Dict):
            return self.__data
        return self.__data.model_dump()

    def json(self) -> str:
        return json.dumps(self.dict())


class Docs(Generic[Scheme]):
    def __init__(self, data: Union[List[Scheme], List[Dict], List[Doc], 'Docs', Scheme, Dict, 'Doc', pd.DataFrame]) -> None:    # noqa: E501
        if data is not None:
            if isinstance(data, pd.DataFrame):
                data = data.to_dict(orient="records")
            if isinstance(data, List):
                if len(data) > 0:
                    first = data[0]
                    assert isinstance(first, Dict) or issubclass(first.__class__, BaseModel) or isinstance(first, Doc),  f"wrong Docs data type: expected List[Dict] or List[Doc] or List[subclass of BaseModel] or Docs, Scheme, Dict, Doc, pd.DataFrame; found List[{type(first)}]"   # noqa: E501
            elif isinstance(data, Docs):
                data = data.parse(recursive=True)
            else:
                assert isinstance(data, Doc) or isinstance(data, Dict) or issubclass(data.__class__, BaseModel), f"wrong Docs data type: expected List[Dict] or List[Doc] or List[subclass of BaseModel] or Docs, Scheme, Dict, Doc, pd.DataFrame; found {type(data)}"  # noqa: E501
        self.__data: List[Doc[Scheme]] = data

    @cache
    def parse(self, *, recursive: bool = False) -> Union[List[Doc[Scheme]], List[Union[Scheme, Type[BaseModel], Dict]]]:    # noqa: E501
        if recursive:
            return [doc.parse() for doc in self.__data]
        return self.__data

    def init(self) -> 'Docs':
        """must be called after __init__"""
        if self.__data is None:
            return self

        scheme = self._scheme_cls
        if isinstance(self.__data, List):
            self.__data = list(map(lambda doc: Doc[scheme](doc).init(), self.__data))
        else:
            self.__data = [Doc[scheme](self.__data).init()]
        return self

    def __len__(self) -> int:
        return len(self.__data)

    def __getitem__(self, key: int) -> Optional[Doc]:
        return self.__data[key]

    def __iter__(self) -> Iterator[Doc]:
        return iter(self.__data)

    def __repr__(self) -> str:
        if len(self.__data) == 0:
            return "Docs(__data=[])"
        if len(self.__data) <= _docs_first_k_show:
            return f"Docs(__data={self.__data[:_docs_first_k_show]}, len={len(self.__data)})"   # noqa: E501
        return f"Docs(__data=[{', '.join(map(str, self.__data[:_docs_first_k_show]))}, ...], len={len(self.__data)})"   # noqa: E501

    @cached_property
    def _scheme_cls(self) -> Optional[Any]: # noqa: ANN401
        if hasattr(self, "__orig_class__"):
            return self.__orig_class__.__args__[0]
        return None

    def json(self) -> str:
        return json.dumps([doc.dict() for doc in self.__data])
