import os
import tempfile
import zipfile
from functools import partial

import malevich_coretools as api

from ..refs import AssetRef
from .service import BaseCoreService


class AssetService(BaseCoreService):
    def __init__(self, auth: api.AUTH, conn_url: str) -> None:
        super().__init__(auth, conn_url)

    @staticmethod
    def _upload(
        path: str,
        file: str | None = None,
        files: list[str] | None = None,
        folder: str | None = None,
        data: bytes | None = None,
        zip: None = None,
        *args,
        **kwargs
    ) -> api.Alias.Info:
        if file is not None:
            with open(file, "rb") as f:
                data = f.read()
        if folder is not None:
            files = files or []
            for root, _, fs in os.walk(folder):
               files.extend(
                   os.path.join(root, f) for f in fs
                )
        if files is not None:
            temp_file = tempfile.NamedTemporaryFile(mode="+rb", delete=False)
            with zipfile.ZipFile(temp_file, "w") as zip_file:
                for f in files:
                    zip_file.write(f, f)
            temp_file.seek(0)
            data = temp_file.read()

        return api.update_collection_object(
            path,
            data=data,
            zip=files is not None,
            *args,
            **kwargs
        )

    def path(
        self,
        path: str,
        /
    ):
        return AssetRef(
            name=f'AssetRef{path}',
            create=partial(
                AssetService._upload,
                path=path,
                auth=self.auth,
                conn_url=self.conn_url
            ),
            update=partial(
                AssetService._upload,
                path=path,
                auth=self.auth,
                conn_url=self.conn_url
            ),
            delete=partial(
                api.delete_collection_object,
                path, auth=self.auth, conn_url=self.conn_url
            ),
            get=partial(
                api.get_collection_object,
                path, auth=self.auth, conn_url=self.conn_url
            ),
            get_link=partial(
                api.get_collection_object_presigned_url,
                path, auth=self.auth, conn_url=self.conn_url
            ),
            post_link=partial(
                api.post_collection_object_presigned_url,
                path, auth=self.auth, conn_url=self.conn_url
            ),
            list=partial(
                api.get_collection_objects,
                path, auth=self.auth, conn_url=self.conn_url
            ),
        )

