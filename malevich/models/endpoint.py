from malevich_coretools import (
    AppLogsWithResults,
    Endpoint,
    EndpointOverride,
    FlattenAppLogsWithResults,
    run_endpoint,
)
from malevich_coretools.secondary.const import ENDPOINTS_RUN


class MetaEndpoint(Endpoint):
    conn_url: str

    def get_url(self) -> str:
        return f'{self.conn_url.rstrip("/")}/{ENDPOINTS_RUN(self.hash)}'

    def run(
        self,
        endpoint_override: EndpointOverride,
        with_auth: bool = False,
        *args,
        **kwargs
    ) -> AppLogsWithResults | FlattenAppLogsWithResults:
        """Runs the endpoint with the given override.

        Args:
            endpoint_override (EndpointOverride): Override for the endpoint.
            with_auth (bool, optional): Whether to authenticate the author of the run.
                Defaults to False.

        Returns:
            AppLogsWithResults | FlattenAppLogsWithResults
        """
        return run_endpoint(
            self.hash,
            endpoint_override,
            conn_url=self.conn_url,
            with_auth=with_auth,
            *args,
            **kwargs
        )


