import os
import tempfile
from subprocess import run
from typing import Optional


def clone_python_files(
    link: str,
    auth: Optional[tuple[str, str]] = None,
    branch: Optional[str] = None,
    folder: os.PathLike = tempfile.TemporaryDirectory(),
) -> list[str]:
    """Clone a Git repository and return a list of Python files.

    The function is optimized to clone only the latest commit and only the
    files that are needed.


    Args:
        link: Link to Git repository or Docker image
        auth: Username and password or token for Git repository
        branch: Branch to clone. Defaults to the default branch.
        folder: Folder to clone into. Defaults to a temporary directory.

    Returns:
        - file_paths: list[str] - paths to Python files

    Raises:
        CalledProcessError: If the `git clone` command fails.
        AssertionError: If the link is not a Git repository
    """
    # Must be a Git repository, so
    # it must start with `git` or `http`
    assert any(
        [link.startswith("git"), link.startswith("http")]
    ), "Link must be a Git repository or Docker image"

    # Base command for cloning
    # --depth 1: Only clone the latest commit
    # --no-tags: Do not clone tags
    # --filter=blob:none: Do not clone blobs (large files)
    command = ["git", "clone", "--depth", "1",
               "--no-tags", "--filter=blob:none"]

    # Add branch if specified
    if branch:
        command += ["--branch", branch]

    # Add authentication if specified
    if auth and auth[0] and auth[1]:
        command += [f"https://{auth[0]}:{auth[1]}@{link}"]
    else:
        command += [link]

    # Add folder to clone into
    command += [os.path.abspath(
        os.path.join(
            os.getcwd(),
            folder,
        )
    )]

    # Run the command
    run(command, check=True)

    # Traverse the directory and subdirectories to find all Python files
    return [
        os.path.join(root, file)
        for root, dirs, files in os.walk(folder)
        for file in files
        if file.endswith(".py")
    ]


def clone(
    link: str,
    auth: Optional[tuple[str, str]] = None,
    branch: Optional[str] = None,
    folder: os.PathLike = tempfile.TemporaryDirectory().name,
) -> None:
    command = ["git", "clone",  "--filter=blob:none"]

    # Add authentication if specified
    if auth and auth[0] and auth[1]:
        command += [f"https://{auth[0]}:{auth[1]}@{link}"]
    else:
        command += [link]

    # Add branch if specified
    if branch:
        command += ["--branch", branch]

    # Add folder to clone into
    command += [os.path.abspath(
        os.path.join(
            os.getcwd(),
            folder,
        )
    )]

    # Run the command
    run(command, check=True)

    # Return list of all files
    return [
        os.path.join(root, file)
        for root, dirs, files in os.walk(folder)
        for file in files
    ]
