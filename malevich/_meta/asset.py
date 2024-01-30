import os
from typing import Optional

from .._autoflow import tracer as gn  # engine
from ..models.nodes.asset import AssetNode


class AssetFactory:
    """Creates binary collections (assets) from files or folders"""

    @staticmethod
    def file(
        #self,
        path: str,
        name: Optional[str] = None,
        alias: Optional[str] = None
    ) -> gn.traced[AssetNode]:
        """Creates an asset with a single file inside

        The argument `name` is used as a name of the asset, so it
        should be unique to avoid collisions. If not provided, the
        name of the file will be used.

        Args:
            path (str): Path to the file
            name (Optional[str], optional): Name of the asset. Defaults to None.
            alias (Optional[str], optional): Alias of the asset. Defaults to None.

        Returns:
            gn.traced[AssetNode]: Asset object that can be used in the flow
        """
        assert os.path.exists(path), f"File {path} does not exist."
        assert os.path.isfile(path), f"Path {path} is not a file."
        assert os.path.getsize(path) > 0, f"File {path} is empty."

        if not name:
            name = os.path.basename(path)
        return gn.traced(AssetNode(real_path=path, alias=alias, core_path=name))

    @staticmethod
    def multifile(
        #self,
        name: Optional[str] = None,
        files: Optional[list[str]] = None,
        folder_path: Optional[str] = None,
        alias: Optional[str] = None,
    ) -> gn.traced[AssetNode]:
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
            gn.traced[AssetNode]: Asset object that can be used in the flow
        """
        assert folder_path is not None or (name is not None and files is not None), \
            "Either folder_path or name with files must be provided."

        if folder_path is not None and name is None:
            name = os.path.basename(folder_path)

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
            real_path=files,
            alias=alias,
            is_composite=True,
            core_path=name
        ))

