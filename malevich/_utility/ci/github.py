import enum
import os
import tempfile

import requests
from git import Repo
from github import Github

from .base import CIOps


class DockerRegistry(enum.Enum):
    OTHER = "other"
    PUBLIC_AWS_ECR = "ecr"
    PRIVATE_AWS_ECR = "ecr-private"
    YANDEX = "yandex"
    GCR = "gcr"


class GithubCIOps(CIOps):
    def __has_malevich_actions(self, repo_dir: str) -> bool:
        return os.path.exists(
            os.path.join(repo_dir, ".github", "workflows", "malevich-ci.yml")
        )

    def __create_action(self, repo_dir: str, branch: str) -> str:
        os.makedirs(os.path.join(repo_dir, ".github",
                    "workflows"), exist_ok=True)

        action_file_path = os.path.join(
            repo_dir, ".github", "workflows", f"malevich-ci_{branch}.yml"
        )

        template_file_path = os.path.join(
            os.path.dirname(__file__), 'templates', 'malevich-ci.yml'
        )

        with open(action_file_path, "w") as action, \
                open(template_file_path) as template:

            action.write(template.read().format(
                BRANCH_NAME=branch
            ))

        return action_file_path

    def __create_manual_action(self, repo_dir: str, branch: str) -> str:
        os.makedirs(os.path.join(repo_dir, ".github",
                    "workflows"), exist_ok=True)

        action_file_path = os.path.join(
            repo_dir, ".github", "workflows", f"malevich-ci-manual_{branch}.yml"
        )

        template_file_path = os.path.join(
            os.path.dirname(__file__), 'templates', 'malevich-ci-manual.yml'
        )

        with open(action_file_path, "w") as action, \
                open(template_file_path) as template:

            action.write(template.read().format(
                BRANCH_NAME=branch
            ))

        return action_file_path

    def _log(self, verbose: bool, msg: str) -> None:
        if verbose:
            print(msg)

    def setup_ci(
        self,
        token: str,
        repository: str,
        space_user: str,
        space_token: str,
        space_url: str,
        branch: str,
        registry_url: str,
        registry_id: str,
        image_user: str,
        image_token: str,
        org_id: str,
        verbose: bool = False,
    ) -> None:
        """Setup CI for a Github repository

        Args:
            token (str): Github token
            repository (str): Github repository in the form of `user/repo`
            space_user (str): Malevich Space username
            space_token (str): Malevich Space token (password)
            space_url (str): Malevich Space API URL
            branch (str): Branch to setup CI in
            registry_url (str):
                Docker Image Registry URL, for example `public.ecr.aws` or 'cr.yandex'
            registry_id (str): Docker Registry ID
            registry_type (DockerRegistry): Docker Registry type
            image_user (str): Username to access the Docker Image Registry
            image_token (str): TOKEN to access the Docker Image Registry
            org_id (str): ORGANIZATION_ID
            verbose (bool, optional): Verbose mode. Defaults to False.
        """
        g = Github(token)

        repo = g.get_repo(repository)

        repo.create_environment(branch)
        pub_key = repo.get_public_key()
        repo_id = repo.id

        keys = [
            "SPACE_USERNAME",
            "SPACE_PASSWORD",
            "API_URL",
            "IMAGE_USERNAME",
            "IMAGE_PASSWORD",
            "REGISTRY_URL",
            "REGISTRY_ID",
            "REGISTRY_TYPE",
            "SPACE_ORGANIZATION_ID"
        ]

        values = [
            space_user,
            space_token,
            space_url,
            image_user,
            image_token,
            registry_url,
            registry_id,
            'ecr' if 'ecr' in registry_url else 'other',
            org_id
        ]

        for key, value in zip(keys, values):
            payload = pub_key.encrypt(value)
            url = f'https://api.github.com/repositories/{repo_id}/environments/{branch}/secrets/{key}'
            requests.put(
                url,
                headers={
                    "Accept": "application/vnd.github+json",
                    "Authorization": f"Bearer {token}",
                    "X-GitHub-Api-Version": "2022-11-28"
                },
                json={
                    'encrypted_value': f'{payload}',
                    'key_id': f'{pub_key.key_id}'
                }
            )
            self._log(verbose, f"Updated secret {key}")

        with tempfile.TemporaryDirectory() as tmpdir:
            git_repo = Repo.clone_from(repo.clone_url, tmpdir)
            self._log(verbose, f"Cloned repository {repository} into {tmpdir}")
            head = git_repo.create_head(branch)
            head.checkout()
            self._log(verbose, f"Checkouted to branch {branch}")
            action_file = self.__create_action(tmpdir, branch)
            manual_action_file = self.__create_manual_action(tmpdir, branch)
            self._log(verbose, f"Created action file {action_file}")
            git_repo.index.add([action_file])
            git_repo.index.add([manual_action_file])
            git_repo.index.commit(
                "add: malevich-ci.yml && malevich-ci-manual.yml")
            self._log(verbose,
                      f"Committed actions file {action_file}, {manual_action_file}"
                      )
            origin = git_repo.remote(name="origin")
            self._log(verbose, "Pushing to origin")
            origin.push(git_repo.active_branch)
            self._log(verbose, "Pushed to origin")
