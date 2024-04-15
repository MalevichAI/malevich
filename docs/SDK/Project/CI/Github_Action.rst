=====
CI/CD
=====

Malevich has its own `Github Actions Workflow <https://github.com/features/actions>`_ pipeline for building and delivering apps to space.

------------
Requirements
------------

In order to build your apps and upload them to Space:

1. App folders should be inside the root directory.

2. Every app should contain ``Dockerfile`` and ``space.yaml`` files.

For example:

.. code-block::

    your_repository/
    ├─ app1/
       ├─ apps/
       ├─ Dockerfile
       ├─ space.yaml
    ├─ app2/
       ├─ apps/
       ├─ Dockerfile
       ├─ space.yaml 
    ...

3. You will need the following credentials:
    - `Space credentials <https://space.malevich.ai/>`_: Username, Token/Password and Organization slug (optional).
    - `Github PAT <https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens>`_.
    - Docker registry info: Registry Host URL, Repository ID/Name, Credentials (username and password).
      
      Full list:
.. code-block:: console

    └─$ malevich ci github init --help
                                                                                                                                                                                                                    
    Usage: malevich ci github init [OPTIONS]      

    Initialize the Github CI/CD pipeline

    ╭─ Options ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
    │ --interactive     --no-interactive          Run the initialization wizard                                                                                                                                              │
    │ --repo-name                           TEXT  Github repository name in the format <username>/<repo-name>                                                                                                                │
    │ --github-token                        TEXT  Github token with access to the repository                                                                                                                                 │
    │ --space-user                          TEXT  Malevich Space username                                                                                                                                                    │
    │ --space-token                         TEXT  Malevich Space token (password)                                                                                                                                            │
    │ --space-url                           TEXT  Malevich Space API URL                                                                                                                                                     │
    │ --branch                              TEXT  Branch to setup CI in                                                                                                                                                      │
    │ --registry-url                        TEXT  Docker Image Registry URL, for example `public.ecr.aws` or 'cr.yandex'                                                                                                     │
    │ --registry-id                         TEXT  Docker Registry ID                                                                                                                                                         │
    │ --image-user                          TEXT  Username to access the Docker Image Registry                                                                                                                               │
    │ --image-token                         TEXT  Password to access the Docker Image Registry                                                                                                                               │
    │ --org-id                              TEXT  Malevich space organization ID                                                                                                                                             │
    │ --help                                      Show this message and exit.                                                                                                                                                │
    ╰────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯

---
Run
---

When all requirements are satisfied, use `malevich ci github init` command to launch initialization.

.. code-block:: console

    └─$ malevich ci github init --interactive
    Welcome to the Github CI/CD pipeline initialization wizard!
    Enter the Github repository name in the format <username>/<repo-name>: MalevichAI/malevich-example
    Enter the Github token with access to the repository (ghp_...): ghp_*******  
    Enter the username to access Malevich Space (leave empty to use access token instead): user@example.com
    Enter the password to access Malevich Space: ***
    Enter the host of Malevich Space  (https://dev.api.malevich.ai): https://dev.api.malevich.ai
    Enter the branch to run the pipeline on (main): main
    Enter the Docker Container Registry type [other/ecr/ecr-private/yandex/ghcr]: ghcr
    Enter the Docker Container Registry URL: ghcr.io
    Enter the Docker Container Registry ID: repo_id
    Enter the username to access the Docker Container Registry: username
    Enter the password to access the Docker Container Registry: ****

During the run, the command will install 2 files inside ``.github/workflows`` directory:

1. ``malevich_ci__<branch>.yml``: Auto CI, which will build and push all apps that were changed in the commit which triggered the workflow.
2. ``malevich_ci__manual.yml``: CI, which can be triggered manually. It will build all the apps in the repository.

----------
References
----------

- Action Repo: https://github.com/MalevichAI/auto-ci