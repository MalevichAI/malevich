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
        return f'{self.conn_url}{ENDPOINTS_RUN}/{self.hash}'

    def run(
        self,
        endpoint_override: EndpointOverride,
        with_auth: bool = False,
        *args,
        **kwargs
    ) -> AppLogsWithResults | FlattenAppLogsWithResults:
        return run_endpoint(
            self.hash,
            endpoint_override,
            conn_url=self.conn_url,
            with_auth=with_auth,
            *args,
            **kwargs
        )


