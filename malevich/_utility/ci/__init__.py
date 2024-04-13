from .base import CIOps
from .functions import CIFunctions

try:
    from .github import GithubCIOps, DockerRegistry
except Exception:
    import warnings
    warnings.warn(
        "Could not import GitHub operations submodule. Check whether `git` "
        "executable is available in your system."
    )
