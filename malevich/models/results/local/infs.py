import json
import os
from pathlib import Path
from typing import Literal, Type, TypeVar, overload

import pandas as pd
from pydantic import BaseModel

from malevich.models.results.base import BaseResult
from malevich.path import Paths

Types = Literal['df', 'doc',  'obj']
DocumentType = TypeVar('DocumentType', bound=BaseModel)

class LocalResult(BaseResult):
    def __init__(
        self,
        operation_id: str,
        run_id: str,
        name: str,
        login: str,
    ) -> None:
        self.__path = Path(Paths.local_results(
            operation_id,
            run_id,
            name
        ))
        self.__login = login
        self.__type = (
            'df' if str(next(self.__path.iterdir())).endswith('.csv') else 'doc'
        ) if not self._is_asset() else 'obj'

    def __get_mnt_path(self, path) -> str:
        return Paths.local(
            'mnt_obj',
            self.__login,
            path
        )

    def _is_asset(self) -> bool:
        return (
            str(self.__path).endswith('.csv')
            and (df := pd.read_csv(self.__path)).columns == ['path']
            and all(
                os.path.exists(self.__get_mnt_path(path))
                for path in df['path']
            )
        )

    def get_df(self) -> pd.DataFrame:
        if self.__type != 'df':
            raise TypeError("Result is not a dataframe")
        return pd.read_csv(
            self.__path / '0.csv'
        )

    def get_dfs(self) -> list[pd.DataFrame]:
        return [
            pd.read_csv(file)
            for file in self.__path.glob('*.csv')
        ]

    @overload
    def get_document(self, index: int = 0) -> dict:
        """Returns dumped document from the result if possible

        Returns:
            dict: The dumped document
        """
        pass

    @overload
    def get_document(
        self,
        index: int = 0,
        *,
        model: Type[DocumentType] | None = None
    ) -> DocumentType:
        """Returns document from the result if possible

        Returns:
            DocumentType: The document
        """
        pass

    def get_document(
        self,
        index: int = 0,
        model: Type[DocumentType] | None = None
    ) -> dict | DocumentType:
        if self.__type != 'doc':
            raise TypeError("Result is not a document")

        if model is not None:
            return model.model_validate_json(
                open(self.__path / f'{index}.json').read()
            )
        return json.loads(open(self.__path / f'{index}.json').read())

    def get_documents(self) -> list[dict]:
        return [
            json.loads(file.read_text())
            for file in self.__path.glob('*.json')
        ]

    def get_binary(self) -> bytes:
        if not self._is_asset():
            raise TypeError(f"Result is not an asset, but {self.__type}")
        return open(self.__get_mnt_path(
            self.get_df().iloc[0]['path']
        ), 'rb').read()

    def get_binary_dir(self) -> dict[str, bytes]:
        if not self._is_asset():
            raise TypeError(f"Result is not an asset, but {self.__type}")
        return {
            file.name: open(self.__get_mnt_path(file.name), 'rb').read()
            for file in self.__path.glob('*')
        }

    def get(self) -> list[pd.DataFrame]:
        return self.get_dfs()

    @property
    def num_elements(self) -> int:
        return len(os.listdir(self.__path))
