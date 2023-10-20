import os
import tempfile

from git import Repo
from github import Github

from .base import CIOps


class GithubCIOps(CIOps):
    def __has_malevich_actions(self, repo_dir: str) -> bool:
        return os.path.exists(
            os.path.join(repo_dir, ".github", "workflows", "malevich-ci.yml")
        )

    def __create_action(self, repo_dir: str, branch: str) -> str:
        os.makedirs(os.path.join(repo_dir, ".github", "workflows"), exist_ok=True)

        action_file_path = os.path.join(
            repo_dir, ".github", "workflows", "malevich-ci.yml"
        )
        template_file_path = os.path.join(
            os.path.dirname(__file__), 'templates', 'malevich-ci.yml')
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
            image_user (str): Username to access the Docker Image Registry
            image_token (str): TOKEN to access the Docker Image Registry
            verbose (bool, optional): Verbose mode. Defaults to False.
        """
        g = Github(token)

        repo = g.get_repo(repository)

        repo.create_secret("SPACE_USERNAME",space_user)
        self._log(verbose, "Updated secret SPACE_USERNAME")
        repo.create_secret("SPACE_PASSWORD", space_token)
        self._log(verbose, "Updated secret SPACE_PASSWORD")
        repo.create_secret("API_URL", space_url)
        self._log(verbose, "Updated secret API_URL")
        repo.create_secret("IMAGE_USERNAME", image_user)
        self._log(verbose, "Updated secret IMAGE_USERNAME")
        repo.create_secret("IMAGE_PASSWORD", image_token)
        self._log(verbose, "Updated secret IMAGE_PASSWORD")
        repo.create_secret("REGISTRY_URL", registry_url)
        self._log(verbose, "Updated secret REGISTRY_URL")
        repo.create_secret("REGISTRY_ID", registry_id)
        self._log(verbose, "Updated secret REGISTRY_ID")
        repo.create_secret('REGISTRY_TYPE', 'ecr' if 'ecr' in registry_url else 'other')
        self._log(verbose, "Updated secret REGISTRY_TYPE")

        with tempfile.TemporaryDirectory() as tmpdir:
            git_repo = Repo.clone_from(repo.clone_url, tmpdir)
            self._log(verbose, f"Cloned repository {repository} into {tmpdir}")
            head = git_repo.create_head(branch)
            head.checkout()
            self._log(verbose, f"Checkouted to branch {branch}")
            action_file = self.__create_action(tmpdir, branch)
            self._log(verbose, f"Created action file {action_file}")
            git_repo.index.add([action_file])
            git_repo.index.commit("add: malevich-ci.yml")
            self._log(verbose, f"Committed action file {action_file}")
            origin = git_repo.remote(name="origin")
            self._log(verbose, "Pushing to origin")
            origin.push(git_repo.active_branch)
            self._log(verbose, "Pushed to origin")


