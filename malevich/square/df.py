from typing import Any, ForwardRef, Generic, List, Optional, TypeVar, Union, _tp_cache

import pandas as pd
from typing_extensions import TypeVarTuple, Unpack

Schemes = TypeVarTuple('Schemes')
Scheme = TypeVar('Scheme')
SchemeM = TypeVar('SchemeM')


def _is_M(type: Any) -> bool:  # noqa: N802, ANN401
    return hasattr(type, "__origin__") and type.__origin__.__module__ == __name__ and type.__origin__.__name__ == "M"  # noqa: E501


class M(Generic[SchemeM]):
    pass


class DF(Generic[Scheme], pd.DataFrame):
    def __init__(self, df: pd.DataFrame) -> None:
        super().__init__(df)

    def cast(self, scheme: str) -> None:
        raise NotImplementedError("scheme cast not yet implemented")    # TODO

    def scheme(self) -> None:
        raise NotImplementedError("scheme not yet implemented")         # TODO

    @_tp_cache
    def scheme_name(self) -> Optional[str]:
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

    @_tp_cache
    def _scheme_cls(self) -> Optional[Any]:  # noqa: ANN401
        if hasattr(self, "__orig_class__"):
            return self.__orig_class__.__args__[0]
        return None


class DFS(Generic[Unpack[Schemes]]):
    def __init__(self) -> None:
        """set dfs with init"""
        self.__dfs: List[Union[DF, DFS, None]] = []
        self.__inited = False

    def init(self, *dfs: pd.DataFrame, nested: bool = False) -> 'DFS':
        """must be called after __init__, nested should be False"""
        assert not self.__inited, "DFS already inited"
        self.__inited = True
        self.__init(list(dfs), nested)
        return self

    def __init(self, dfs: List[pd.DataFrame], nested: bool = False) -> None:
        types = self.__orig_class__.__args__ if hasattr(self, "__orig_class__") else [Any for _ in dfs] # noqa: E501
        many_df_index = None
        for i, type in enumerate(types):
            if _is_M(type):
                assert not nested, "nested M in DFS"
                if many_df_index is not None:
                    raise Exception("more than one M in DFS")
                else:
                    many_df_index = i
        if many_df_index is None:
            assert len(types) == len(dfs), f"wrong arguments size: expected {len(types)}, found {len(dfs)}" # noqa: E501
            for df, type in zip(dfs, types):
                self.__dfs.append(DF[type](df))
        else:
            assert len(types) - 1 <= len(dfs), f"wrong arguments size: expected at least {len(types) - 1}, found {len(dfs)}"    # noqa: E501
            for df, type in zip(dfs[:many_df_index], types[:many_df_index]):
                self.__dfs.append(DF[type](df))
            count = len(dfs) + 1 - len(types)
            if count != 0:
                type_many = types[many_df_index].__args__[0]
                temp = DFS[tuple([type_many] * count)]().init(*dfs[many_df_index:many_df_index + count], nested=True)  # noqa: E501
                self.__dfs.append(temp)
            else:
                self.__dfs.append(None)
            for df, type in zip(dfs[many_df_index + count:], types[many_df_index + 1:]):
                self.__dfs.append(DF[type](df))

    def __len__(self) -> int:
        return len(self.__dfs)

    def __getitem__(self, key: int) -> Union[DF, 'DFS', None]:
        return self.__dfs[key]

    def __iter__(self) -> Union[DF, 'DFS', None]:
        return iter(self.__dfs)
