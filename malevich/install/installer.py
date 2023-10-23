"""Installer interface."""

from abc import ABC, abstractmethod

from ..models.manifest import Dependency


class Installer(ABC):
    name: str

    """Abstract installer interface."""
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def install(self, *args, **kwargs) -> Dependency: # noqa: ANN003, ANN002, ANN204
        """Installation function. Called by CLI commands"""
        pass

    @abstractmethod
    def restore(self, dependency: Dependency) -> None:
        """Restore function. Called by CLI commands with the Dependency object"""
        pass

    @abstractmethod
    def construct_dependency(self, object: dict) -> Dependency:
        """Construct a Dependency object from a dictionary"""
        pass
