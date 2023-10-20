from abc import ABC, abstractmethod


class CIOps(ABC):
    @abstractmethod
    def setup_ci(
        self,
        user: str,
        token: str,
        repository: str,
        space_user: str,
        space_token: str,
        space_url: str,
        branch: str,
        registry_url: str,
        registry_id: str,
        image_user: str = None,
        image_token: str = None,
        verbose: bool = False,
    ) -> None:
        pass
