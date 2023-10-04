"""Installer interface."""

from abc import ABC, abstractmethod


class Installer(ABC):
    """Abstract installer interface."""
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def install(self, *args, **kwargs) -> None: # noqa: ANN003, ANN002, ANN204
        """Installation function. Called by CLI commands"""
        pass
