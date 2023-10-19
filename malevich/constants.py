from .models.platform import Platform

DEFAULT_CORE_HOST = "https://core.malevich.ai"

APP_HELP = """
Welcome to [bold magenta]Malevich Metascript[/bold magenta]! This is a CLI tool that
allows you to create and run multitask pipelines using [yellow]Malevich API[/yellow].
\n _____ \n
The tool includes[i] autoflow [/i]engine, that analyzes your code and turns it into a human-readable
pipeline;[i] installers [/i]that enable you to populate your flow with a huge variety of pre-built
components; and finally[i] interpreters [/i]that run your code on Malevich backend freeing you
from installing any dependency.

How you start:
\t1. Run [code] malevich use [/code] to install apps and get access to their functionality
\t2. Write a pipeline using simple Python scripts and [code] @flow [/code] decorators
\t3. Connect to Malevich API and run your flow effortlessly

Check out [link=https://docs.malevich.ai/malevich]Malevich documentation[/link] for more details
"""  # noqa: E501

USE_HELP = """
Install apps to get access to the processors they provide
"""

USE_IMAGE_HELP = """
Install apps using published Docker images. The information about
the functionality provided by them will be pulled and parsed accordingly.
"""

IMAGE_BASE = "public.ecr.aws/u6e1k0c7/{app}:latest"


CorePlatform = Platform.CORE
SpacePlatofrm = Platform.SPACE
