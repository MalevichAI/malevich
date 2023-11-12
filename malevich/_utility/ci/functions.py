import os
import tempfile
from typing import Optional

from malevich_space.ops.component_provider.base import BaseComponentProvider
from malevich_space.parser import YAMLParser
from malevich_space.schema import ComponentSchema

from ..git.clone import clone


class CIFunctions(BaseComponentProvider):
    @staticmethod
    def has_component_in_folder(
        folder: str
    ) -> tuple[ComponentSchema | None, str]:
        """Check if a folder contains a valid component

        Args:
            folder (str): Folder to check

        Returns:
            component (ComponentSchema): Component if found, None otherwise
            path (str): Path to the component
        """
        # Scan for YAML files in the folder
        component = None
        path = os.path.abspath(folder)
        parser = YAMLParser()
        for file in os.listdir(folder):
            if file.endswith(".yaml"):
                try:
                    __component = parser._parse_comp_file(
                        os.path.join(folder, file), folder
                    )
                    if component is not None:
                        raise ValueError(
                            "Multiple valid YAML files found in folder"
                        )
                    component = __component[0]
                    path = os.path.join(folder, file)
                except Exception:
                    continue

        return component, path

    @staticmethod
    def get_available_git_components(
        github_repository: str,
        github_user: Optional[str] = None,
        github_token: Optional[str] = None,
        branch: str = 'main'
    ) -> list[tuple[ComponentSchema, str]]:
        components = []

        with tempfile.TemporaryDirectory() as tmpdir:
            clone(
                github_repository,
                (github_user, github_token)
                if github_user and github_token else None,
                branch,
                folder=tmpdir,
            )

            # Iterate over folder within the repository
            # with depth of one
            for folder in os.listdir(tmpdir):
                if os.path.isdir(os.path.join(tmpdir, folder)):
                    component, path = CIFunctions.has_component_in_folder(
                        os.path.join(tmpdir, folder)
                    )
                    if component is not None:
                        components.append((component, path))
        return components

    def __init__(
        self,
        git_link: str,
        git_user: Optional[str] = None,
        git_token: Optional[str] = None,
        branch: str = 'main'
    ) -> None:
        self.comp_paths = CIFunctions.get_available_git_components(
            github_repository=git_link,
            github_user=git_user,
            github_token=git_token,
            branch=branch
        )

    def get_by_reverse_id(self, reverse_id: str) -> ComponentSchema | None:
        for component, _ in self.comp_paths:
            if component.reverse_id == reverse_id:
                return component
        return None

    def get_all(self) -> dict[str, ComponentSchema]:
        return {
            component.reverse_id: component
            for component, _ in self.comp_paths
        }
