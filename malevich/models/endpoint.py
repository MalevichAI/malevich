import json
import logging
import os
import uuid

from malevich_coretools import (
    AUTH,
    AppLogsWithResults,
    Cfg,
    Endpoint,
    EndpointOverride,
    FlattenAppLogsWithResults,
    UserConfig,
    create_collection_from_df,
    create_doc,
    get_cfg,
    get_endpoint,
    post_collection_object_presigned_url,
    run_endpoint,
    update_collection_object,
)
from malevich_coretools.secondary.const import ENDPOINTS_RUN

from malevich._utility import upload_zip_asset

from .overrides import AssetOverride, CollectionOverride, DocumentOverride, Override

endpoint_logger = logging.getLogger('malevich.run_endpoint')

class MetaEndpoint(Endpoint):
    conn_url: str
    auth: AUTH

    @staticmethod
    def connect_by_hash(hash: str, *, conn_url: str, auth: AUTH) -> "MetaEndpoint":
        endpoint = get_endpoint(
            hash=hash,
            conn_url=conn_url,
            auth=auth
        )
        return MetaEndpoint(
            **endpoint.model_dump(),
            conn_url=conn_url,
            auth=auth
        )

    def get_url(self) -> str:
        return f'{self.conn_url.rstrip("/")}/{ENDPOINTS_RUN(self.hash)}'

    def run(
        self,
        overrides: dict[str, Override] = {},
        endpoint_override: EndpointOverride | None = None,
        with_auth: bool = False,
        *args,
        **kwargs
    ) -> AppLogsWithResults | FlattenAppLogsWithResults:
        """Runs the endpoint with the given override.

        Overrides are tables

        Args:
            endpoint_override (EndpointOverride): Override for the endpoint.
            with_auth (bool, optional): Whether to authenticate the author of the run.
                Defaults to False.

        Returns:
            AppLogsWithResults | FlattenAppLogsWithResults
        """
        assert endpoint_override or overrides, (
            'Either `overrides` or `endpoint_override` should be provided'
        )
        config = Cfg(**json.loads(get_cfg(
            self.taskId,
            conn_url=self.conn_url,
            auth=self.auth,
        )))

        override_keys = list(config.collections.keys())
        diff = set(overrides.keys()) - set(override_keys)

        if diff:
            endpoint_logger.warning(
                'Overrides %s are not in the endpoint config', diff
            )

        overrides = {
            k: v
            for k, v in overrides.items()
            if k in override_keys
        }

        real_overrides = {}
        for key, override in overrides.items():
            if isinstance(override, CollectionOverride):
                real_overrides[key] = create_collection_from_df(
                    override.data,
                    conn_url=self.conn_url,
                    auth=self.auth,
                )

                endpoint_logger.info(
                    'Override collection %s: %s', key, real_overrides[key]
                )

            elif isinstance(override, DocumentOverride):
                real_overrides[key] = '#' + create_doc(
                    override.data.model_dump_json(),
                    conn_url=self.conn_url,
                    auth=self.auth,
                )

                endpoint_logger.info(
                    'Override document %s: %s', key, real_overrides[key]
                )
            elif isinstance(override, AssetOverride):
                if override.file:
                    if not override.path:
                        override.path = (
                            'overrides/' +
                            uuid.uuid4().hex +
                            '/' +
                            os.path.basename(override.file)
                        )

                    update_collection_object(
                        override.path,
                        override.file,
                        conn_url=self.conn_url,
                        auth=self.auth,
                    )
                else:
                    if not override.path:
                        override.path = 'overrides/' + uuid.uuid4().hex

                    url = post_collection_object_presigned_url(
                        override.path,
                        expires_in=600,
                        conn_url=self.conn_url,
                        auth=self.auth,
                    )

                    upload_zip_asset(
                        url,
                        files=override.files,
                        folder=override.folder,
                    )
                real_overrides[key] = '$' + override.path

                endpoint_logger.info(
                    'Override asset %s: %s', key, real_overrides[key]
                )


        if endpoint_override is None:
            endpoint_override = EndpointOverride(
                cfg=UserConfig(collections=real_overrides),
                withResult=True,
                formatLogs=True,
            )

        return run_endpoint(
            self.hash,
            endpoint_override,
            conn_url=self.conn_url,
            with_auth=with_auth,
            *args,
            **kwargs
        )


