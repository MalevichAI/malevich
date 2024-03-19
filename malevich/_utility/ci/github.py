import enum
import os
import tempfile

import requests
from git import Repo
from github import PublicKey

from .base import CIOps


class DockerRegistry(enum.Enum):
    OTHER = "other"
    PUBLIC_AWS_ECR = "ecr"
    PRIVATE_AWS_ECR = "ecr-private"
    YANDEX = "yandex"
    GCR = "ghcr"

class _NotSet:
    pass

class GithubCIOps(CIOps):
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
        registry_url: str = 'ghcr.io',
        registry_id: str = _NotSet,
        image_user: str = 'USERNAME',
        image_token: str = _NotSet,
        org_id: str = 'empty',
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

        pub_key = requests.get(
            f"https://api.github.com/repos/{repository}/actions/secrets/public-key",
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "X-GitHub-Api-Version": "2022-11-28"
            }
        )
        if pub_key.status_code >= 400:
            raise Exception(
                "We couldn't get the public key from Github. "
                "Most probably, given credentials are invalid or "
                "do not have enough permissions. "
                "Below is the response from Github:\n"
                f"{pub_key.json()}"
            )
        pub_key = pub_key.json()

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

        registry_type = DockerRegistry.GCR
        if 'ecr' in registry_url:
            registry_type = (
                DockerRegistry.PUBLIC_AWS_ECR if 'public' in registry_url
                else DockerRegistry.PRIVATE_AWS_ECR
            )
        elif 'yandex' in registry_url:
            registry_type = DockerRegistry.YANDEX

        values = [
            space_user,
            space_token,
            space_url,
            image_user,
            token if image_token == _NotSet else image_token,
            registry_url,
            repository.split('/')[0].lower() if registry_id == _NotSet else registry_id,
            registry_type,
            org_id
        ]

        for key, value in zip(keys, values):
            payload = PublicKey.encrypt(pub_key['key'], value)
            url = f'https://api.github.com/repos/{repository}/actions/secrets/{key}'

            requests.put(
                url,
                headers={
                    "Accept": "application/vnd.github+json",
                    "Authorization": f"Bearer {token}",
                    "X-GitHub-Api-Version": "2022-11-28"
                },
                json={
                    'encrypted_value': f'{payload}',
                    'key_id': f'{pub_key["key_id"]}'
                }
            )
            self._log(verbose, f"Updated secret {key}")

        with tempfile.TemporaryDirectory() as tmpdir:
            git_repo = Repo.clone_from(
                f'https://{token}@github.com/{repository}', tmpdir
            )
            self._log(verbose, f"Cloned repository {repository} into {tmpdir}")
            git_repo.git.checkout(branch)
            self._log(verbose, f"Checkouted to branch {branch}")
            action_file = self.__create_action(tmpdir, branch)
            manual_action_file = self.__create_manual_action(tmpdir, branch)
            self._log(verbose, f"Created action file {action_file}")
            git_repo.git.add(action_file)
            git_repo.git.add(manual_action_file)
            try:
                git_repo.git.commit(
                    "-m",
                    "add: malevich-ci.yml && malevich-ci-manual.yml"
                )
                self._log(
                    verbose,
                    f"Committed actions file {action_file}, {manual_action_file}"
                )
                git_repo.git.push()
            except Exception as e:
                self._log(
                    verbose,
                    f"{action_file} and {manual_action_file} already exist"
                )

                print(e)
