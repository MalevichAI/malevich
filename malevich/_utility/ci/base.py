from abc import ABC, abstractmethod
from typing import Optional


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
        image_user: Optional[str] = None,
        image_token: Optional[str] = None,
        verbose: bool = False,
    ) -> None:
        pass
