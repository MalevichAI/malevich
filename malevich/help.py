main = {
    '--help': """Welcome to [bold magenta]Malevich Metascript[/bold magenta]! This is a CLI tool that
allows you to create and run multitask pipelines using [yellow]Malevich API[/yellow].
\n _____ \n
The tool includes[i] autoflow [/i]engine, that analyzes your code and turns it into a human-readable
pipeline;[i] installers [/i]that enable you to populate your flow with a huge variety of pre-built
components; and finally[i] interpreters [/i]that run your code on Malevich backend freeing you
from installing any dependency.

How you start:
\t1. Run  malevich use to install apps and get access to their functionality
\t2. Write a pipeline using simple Python scripts and @flow decorators
\t3. Connect to Malevich API and run your flow effortlessly

Check out [link=https://docs.malevich.ai/malevich]Malevich documentation[/link] for more details"""
}

use = {
    "--help": """Install apps to get access to the processors they provide""",
    'image --help': """Install apps using published Docker images.

    Usage examples:

    1. Install an app from a Dockerhub image user/malevich:app and expose it as [b]app[b] in the current environment

            malevich use image app user/malevich:app

    2. Install the same app, but from a privately hosted Malevich Core

            malevich use image app user/malevich:app --core-host https://core.malevich.ai --core-user admin --core-token kd923fn
    """,
    'space --help': """Install apps available at Malevich Space.

    Usage examples:

    1. Install [code i]my.openai.app[/code i] and expose it as [b]openai[b] in the current environment

            malevich use space openai my.openai.app
    """,
    'local --help': """Install apps from a local code.

    Usage examples:

    1. Install an app from a local code

            malevich use local app ./path/to/app
    """,
}

install =  {
    '--help': """The simplest way to install Malevich apps

    Usage examples:

        1. Install utility

            malevich install utility

        2. Install utility and openai

            malevich install utility openai

        3. Install utility and openai using the image installer

            malevich install utility openai --using image

        4. Install utility using the image installer and specify the image reference

            malevich install utility --using image --with-args image_ref=user/malevich:utility
    """
}


restore = {
    '--help': """Restore all installed apps manifested in the current environment"""
}

space = {
    '--help': """Communicate with Malevich Space - a public provider of Malevich Core""",
    'init --help': """Imports Malevich Space configuration into the current environment""",
    'login --help': """Interactive login to Malevich Space""",
    'whoami --help': """Get information about connected Space user""",
    'upload-flow --help': """Upload a new version of flow declared with @flow decorator""",
}

remove = {
    '--help': """Remove apps from the current environment""",
}

ci = {
    '--help': """Setup CI/CD pipeline for your repository""",
}

list_ = {
    '--help': """List all installed package stubs."""
}

new = {
    '--help': """Create a new app from a template""",
}

core = {
    '--help': """Manage your Core account"""
}
