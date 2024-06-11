import os
from typing import Optional, overload

from .._autoflow import tracer as gn  # engine
from ..annotations import Asset
from ..models.nodes.asset import AssetNode


class AssetFactory:
    """Creates binary collections (assets) from files or folders"""



    @staticmethod
    def on_core(
        name: str,
        core_path: str,
    ) -> Asset:
        """Specifies an asset from a core path

        Args:
            name (str): Name of the asset
            core_path (str): Path to the core file

        Returns:
            Asset object that is used within @flow function.
        """
        return gn.traced(AssetNode(
            name=name,
            core_path=core_path,
        ))


    @overload
    @staticmethod
    def on_space(
        reverse_id: str,
        /,
    ) -> Asset:
        """
        Attaches to an asset in the space

        Args:
            reverse_id (str): Reverse ID of the space

        Returns:
            Asset object that is used within @flow function.
        """

    @overload
    @staticmethod
    def on_space(
        reverse_id: str,
        *,
        file: str,
    ) -> Asset:
        """
        Creates an asset in the space from a single file

        Args:
            reverse_id (str): Reverse ID of the space
            file (str): Path to the file

        Returns:
            Asset object that is used within @flow function.
        """

    @overload
    @staticmethod
    def on_space(
        reverse_id: str,
        *,
        files: list[str],
    ) -> Asset:
        """
        Creates an asset in the space from multiple files

        Args:
            reverse_id (str): Reverse ID of the space
            files (list[str]): List of file paths

        Returns:
            Asset object that is used within @flow function.
        """

    @overload
    @staticmethod
    def on_space(
        reverse_id: str,
        *,
        folder_path: str,
    ) -> Asset:
        """
        Creates an asset in the space from a folder

        Args:
            reverse_id (str): Reverse ID of the space
            folder_path (str): Path to the folder

        Returns:
            Asset object that is used within @flow function.
        """

    @staticmethod
    def on_space(
        reverse_id: str,
        *,
        file: Optional[str] = None,
        files: Optional[list[str]] = None,
        folder_path: Optional[str] = None,
        alias: Optional[str] = None
    ) -> Asset:
        """Creates or attaches to an asset in the space

        Args:
            reverse_id (str): Reverse ID of the space
            file (Optional[str], optional): Path to a single file. Defaults to None.
            files (Optional[list[str]], optional): List of file paths. Defaults to None.
            folder_path (Optional[str], optional): Path to a folder. Defaults to None.
            alias (Optional[str], optional): Alias of the asset. Defaults to None.

        Returns:
            Asset object that is used within @flow function.
        """
        assert (file is not None) + (files is not None) + (folder_path is not None) < 2, \
            "Exactly one of file, files, or folder must be provided."

        if folder_path is not None:
            folder_path = folder_path.rstrip('/')

        if file is not None:
            assert os.path.exists(file), f"File {file} does not exist."
            assert os.path.isfile(file), f"Path {file} is not a file."
            assert os.path.getsize(file) > 0, f"File {file} is empty."

            return gn.traced(AssetNode(
                name=reverse_id,
                real_path=file,
                alias=alias,
            ))

        if files is not None:
            assert len(files) > 0, "Files must not be empty."
            _1 = [os.path.exists(file) for file in files if not os.path.exists(file)]
            _2 = [os.path.isfile(file) for file in files if not os.path.isfile(file)]
            _3 = [
                os.path.getsize(file) > 0
                for file in files
                if os.path.getsize(file) == 0
            ]

            assert len(_1) == 0,f"Some files do not exist: {_1}"
            assert len(_2) == 0,f"Some files are not files: {_2}"
            assert len(_3) == 0,f"Some files are empty: {_3}"

            return gn.traced(AssetNode(
                name=reverse_id,
                real_path=files,
                alias=alias,
                is_composite=True,
            ))

        if folder_path is not None:

            assert os.path.exists(folder_path), f"Folder {folder_path} does not exist."
            assert os.path.isdir(folder_path), f"Path {folder_path} is not a folder."
            assert os.path.getsize(folder_path) > 0, f"Folder {folder_path} is empty."

            return gn.traced(AssetNode(
                name=reverse_id,
                real_path=[
                    os.path.join(root, file)
                    for root, _, files in os.walk(folder_path)
                    for file in files
                ],
                alias=alias,
                is_composite=True,
            ))

        return gn.traced(AssetNode(
            name=reverse_id,
            alias=alias,
        ))

    @staticmethod
    def from_file(
        #self,
        path: str,
        name: Optional[str] = None,
        alias: Optional[str] = None
    ) -> Asset:
        """Creates an asset with a single file inside

        The argument `name` is used as a name of the asset, so it
        should be unique to avoid collisions. If not provided, the
        name of the file will be used. The name must be a valid
        Python identifier.

        Args:
            path (str): Path to the file
            name (Optional[str], optional): Name of the asset. Defaults to None.
            alias (Optional[str], optional): Alias of the asset. Defaults to None.

        Returns:
            Asset object that is used within @flow function.
        """
        assert os.path.exists(path), f"File {path} does not exist."
        assert os.path.isfile(path), f"Path {path} is not a file."
        assert os.path.getsize(path) > 0, f"File {path} is empty."

        if not name:
            name, _ = os.path.splitext(os.path.basename(path))

        if not name.isidentifier():
            raise ValueError(
                "When creating an asset as `asset.from_file(...)` "
                "you must ensure that the name is a valid Python identifier. "
                "Rename the file or provide a name explicitly: "
                "`asset.from_file(..., name='...')`."
            )

        return gn.traced(AssetNode(
            name=name,
            real_path=path,
            alias=alias,
            core_path=name
        ))

    @staticmethod
    @overload
    def from_files(
        name: str,
        *,
        files: list[str],
        alias: Optional[str] = None
    ) -> Asset:
        """
        Creates an asset with multiple files inside. The asset will be
        saved as `name/file1`, `name/file2`, etc.

        The name must be a valid Python identifier.

        Args:
            name (str): Name of the asset
            files (list[str]): List of file paths
            alias (Optional[str], optional): Alias of the asset. Defaults to None.

        Returns:
            Asset object that is used within @flow function.
        """

    @staticmethod
    @overload
    def from_files(
        name: Optional[str] = None,
        *,
        folder_path: str,
        alias: Optional[str] = None
    ) -> Asset:
        """
        Creates an asset with multiple files inside. The asset will be
        saved as `folder_path/` and all files from the folder will be
        included.

        The name must be a valid Python identifier. If not provided, the
        name of the folder will be used.

        Args:
            folder_path (str): Path to the folder
            name (Optional[str], optional): Name of the asset. Defaults to None.
            alias (Optional[str], optional): Alias of the asset. Defaults to None.

        Returns:
            Asset object that is used within @flow function.
        """

    @staticmethod
    def from_files(
        name: Optional[str] = None,
        *,
        files: Optional[list[str]] = None,
        folder_path: Optional[str] = None,
        alias: Optional[str] = None,
    ) -> Asset:
        """Creates an asset with multiple files inside

        Files are read from specified folder path, or using
        given files (which are not required to be stored together).

        Either folder_path or name with files must be provided.

        The argument `name` is used as a name of the asset, so it
        should be unique to avoid collisions. If not provided, the
        name of the folder will be used.

        Args:
            name (Optional[str], optional): Name of the asset. Defaults to None.
            files (Optional[list[str]], optional): List of files. Defaults to None.
            folder_path (Optional[str], optional): Path to the folder. Defaults to None.
            alias (Optional[str], optional): Alias of the asset. Defaults to None.

        Returns:
            Asset object that is used within @flow function.
        """
        assert folder_path is not None or (name is not None and files is not None), \
            "Either folder_path or name with files must be provided."

        if folder_path is not None:
            folder_path = folder_path.rstrip('/')

        if name is None:
            if folder_path is not None:
                name = os.path.basename(folder_path)
            else:
                raise ValueError(
                    "The `name` must be provided "
                    "if files not folder are given."
                )

        if not name.isidentifier():
            raise ValueError(
                "When creating an asset as `asset.from_files(...)` "
                "you must ensure that the name is a valid Python identifier. "
                "Rename the file or provide a name explicitly: "
                f"`asset.from_files(..., name='...')`. Inferred name: {name}"
            )

        if folder_path is not None:
            assert os.path.exists(folder_path), f"Folder {folder_path} does not exist."
            assert os.path.isdir(folder_path), f"Path {folder_path} is not a folder."
            assert os.path.getsize(folder_path) > 0, f"Folder {folder_path} is empty."

            files = [os.path.join(folder_path, file)
                     for file in os.listdir(folder_path)]
        else:
            assert files is not None, "Files must be provided."
            assert len(files) > 0, "Files must not be empty."
            _1 = [os.path.exists(file) for file in files if not os.path.exists(file)]
            _2 = [os.path.isfile(file) for file in files if not os.path.isfile(file)]
            _3 = [
                os.path.getsize(file) > 0
                for file in files
                if os.path.getsize(file) == 0
            ]
            assert len(_1) == 0,f"Some files do not exist: {_1}"
            assert len(_2) == 0,f"Some files are not files: {_2}"
            assert len(_3) == 0,f"Some files are empty: {_3}"


        return gn.traced(AssetNode(
            name=name,
            real_path=files,
            alias=alias,
            is_composite=True,
            core_path=name
        ))

