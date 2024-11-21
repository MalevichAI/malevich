import malevich_coretools as api


class BaseCoreService:
    """Provides API access to the Malevich Core service."""

    def __init__(self, auth: api.AUTH, conn_url: str) -> None:
        """Provides API access to the Malevich Core service.

        The service captures the user's authentication credentials
        and the URL of the Malevich Core service and provides functions
        to interact with the service.

        Args:
            auth (malevich_coretools.AUTH): The user's authentication credentials.
            conn_url (str): The URL of the Malevich Core service.
        """
        self.auth = auth
        self.conn_url = conn_url
