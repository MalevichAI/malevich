import hashlib
import inspect
import os
import pickle
from typing import Iterable, overload
import io
import dill
import tempfile

import malevich.annotations
from malevich._autoflow import tracer as gn  # engine
from malevich.models import AssetNode, AssetOverride, PythonString


class AssetFactory:
    """Creates binary collections (assets) from files or folders"""

    @staticmethod
    def override(
        path: str | None,
        file: os.PathLike | None = None,
        folder: os.PathLike | None = None,
        files: Iterable[os.PathLike] | None = None,
    ) -> AssetOverride:
        _ = (
            file is not None,
            folder is not None,
            files is not None,
        ).count(True)
        if _ != 1:
            raise ValueError(
                'Exactly one of `file`, `folder`, or `files` must be provided'
            )

        return AssetOverride(
            path=path,
            file=file,
            folder=folder,
            files=files,
        )

    @staticmethod
    def capture_function(
        function: callable,
    ) -> malevich.annotations.Asset:
        tfile_ = tempfile.NamedTemporaryFile(delete=False)
        tfile_.write(dill.dumps((function, inspect.getsource(function)), recurse=True))

        return gn.traced(AssetNode(
            name=function.__name__,
            core_path='__functions__/' +
            function.__name__ + '_' + str(hash(function)),
            real_path=tfile_.name,
            alias='captureof_' + function.__name__,
        ))

    @staticmethod
    def from_remote_path(
        reverse_id: PythonString,
        remote_path: str,
        alias: str | None = None,
    ) -> malevich.annotations.Asset:
        return gn.traced(AssetNode(
            name=reverse_id,
            alias=alias,
            core_path=remote_path,
        ))

    @staticmethod
    @overload
    def from_files(
        reverse_id: PythonString,
        *,
        file: os.PathLike = ...,
        remote_path: str | None = ...,
        alias: str | None = ...,
    ) -> malevich.annotations.Asset:
        pass

    @staticmethod
    @overload
    def from_files(
        reverse_id: PythonString,
        *,
        folder: os.PathLike = ...,
        remote_path: str | None = ...,
        alias: str | None = ...,
    ) -> malevich.annotations.Asset:
        pass

    @staticmethod
    @overload
    def from_files(
        reverse_id: PythonString,
        *,
        files: Iterable[os.PathLike] = ...,
        remote_path: str | None = ...,
        alias: str | None = ...,
    ) -> malevich.annotations.Asset:
        pass

    @staticmethod
    def from_files(
        reverse_id: str,
        *,
        file: os.PathLike | None = None,
        folder: os.PathLike | None = None,
        files: Iterable[os.PathLike] | None = None,
        remote_path: str | None = None,
        alias: str | None = None,
    ) -> malevich.annotations.Asset:
        _ = (
            file is not None,
            folder is not None,
            files is not None,
        ).count(True)
        if _ != 1:
            raise ValueError(
                'Exactly one of `file`, `folder`, or `files` must be provided'
            )

        if file is not None:
            if not os.path.isfile(file):
                raise FileNotFoundError(f'File not found: {file}')

            if not remote_path:
                remote_path = os.path.basename(file)
            return gn.traced(AssetNode(
                name=reverse_id,
                real_path=file,
                alias=alias,
                core_path=remote_path,
            ))

        if folder is not None:
            if not os.path.isdir(folder):
                raise FileNotFoundError(f'Folder not found: {folder}')

            files = []
            for root, _, filenames in os.walk(folder):
                for filename in filenames:
                    files.append(os.path.join(root, filename))

            if not remote_path:
                remote_path = os.path.basename(folder)

            return gn.traced(AssetNode(
                name=reverse_id,
                real_path=files,
                alias=alias,
                core_path=remote_path,
            ))

        if files is not None:
            for file in files:
                if not os.path.isfile(file):
                    raise FileNotFoundError(f'File not found: {file}')

            if not remote_path:
                raise ValueError(
                    '`remote_path` must be provided '
                    'when using `files=` argument'
                )

            return gn.traced(AssetNode(
                name=reverse_id,
                real_path=files,
                alias=alias,
                core_path=remote_path,
            ))

        raise ValueError(
            'Exactly one of `file`, `folder`, or `files` must be provided'
        )
