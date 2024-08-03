import os
import tempfile
import zipfile

import requests


def upload_zip_asset(
    url: str,
    file: os.PathLike | None = None,
    files: list[os.PathLike] | None = None,
    folder: os.PathLike | None = None,
):
    """Uploads a zip asset to the platform

    Args:
        file (os.PathLike, optional): Path to the file to upload. Defaults to None.
        files (list[os.PathLike], optional): List of paths to the files to upload. Defaults to None.
        folder (os.PathLike, optional): Path to the folder to upload. Defaults to None.
    """
    if file is not None:
        response = requests.post(url, data=open(file, 'rb').read())
    elif folder is not None:
        files = [os.path.join(folder, f) for f in os.listdir(folder)]
        with tempfile.TemporaryFile(mode='+rb') as temp_file:
            with zipfile.ZipFile(temp_file, 'w') as zip_file:
                for f in files:
                    zip_file.write(f)
            temp_file.seek(0)
            response: requests.Response = requests.post(url, data=temp_file.read(), params={
                'zip': True
            })

    response.raise_for_status()
    return response.raw
