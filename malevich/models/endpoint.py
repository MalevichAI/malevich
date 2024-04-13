from malevich_coretools import (
    AUTH,
    AppLogsWithResults,
    Endpoint,
    EndpointOverride,
    FlattenAppLogsWithResults,
    UserConfig,
    get_endpoint,
    run_endpoint,
)
from malevich_coretools.secondary.const import ENDPOINTS_RUN

from malevich._meta import table


class MetaEndpoint(Endpoint):
    conn_url: str

    @staticmethod
    def connect_by_hash(hash: str, *, conn_url: str, auth: AUTH) -> "MetaEndpoint":
        endpoint = get_endpoint(
            hash=hash,
            conn_url=conn_url,
            auth=auth
        )

        return MetaEndpoint(**endpoint.model_dump(), conn_url=conn_url)

    def get_url(self) -> str:
        return f'{self.conn_url.rstrip("/")}/{ENDPOINTS_RUN(self.hash)}'

    def run(
        self,
        overrides: dict[str, table] = {},
        endpoint_override: EndpointOverride = None,
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

        override_keys = list(self.expectedCollectionsWithSchemes.keys())
        overrides = {
            k: v
            for k, v in overrides.items()
            if k in override_keys
        }

        if endpoint_override is not None:
            endpoint_override = EndpointOverride(
                cfg=UserConfig(rawMapCollections={
                    k: v.to_dict(orient='records', index=False)
                    for k, v in overrides.items()
                }),
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


