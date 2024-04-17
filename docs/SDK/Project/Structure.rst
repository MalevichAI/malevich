=================
Managing Projects
=================

A folder with a file ``malevich.yaml`` is considered to be a Malevich project. Each project can have:

* Its own set of dependencies 
* A set of apps 
  
Projects can be shared and their dependencies can be easily restored with Malevich CLI.

----------------------------------
Project Initialization
----------------------------------

Starting a new project on Malevich is done with running the following command:

.. code-block:: console

    malevich init


It will create two files in the your working directory:

* ``malevich.yaml`` -- a list of installed dependencies and user preferences called *manifest*.
* ``malevich.secrets.yaml`` -- a list of secrets - passwords, access keys and etc. **Make sure that you are not sharing this file or making it public.**


-----------
Space Login
-----------

Usually, project is connected to a particular Space user. To connect a project, run the following command:

.. code-block:: console

    malevich space login


You will be prompted to type your username, password and organization slug. If you do not have password, you should type one of your access tokens.


-----------------
Managing Manifest
-----------------

You can manualy manage your manifest

.. code-block:: console 
                                                                                                      
    Usage: malevich manifest [OPTIONS] COMMAND [ARGS]...                                                 
                                                                                                        
    Manage the manifest file (malevich.yaml)                                                             
                                                                                                        
    ╭─ Options ──────────────────────────────────────────────────────────────────────────────────────────╮
    │ --help          Show this message and exit.                                                        │
    ╰────────────────────────────────────────────────────────────────────────────────────────────────────╯
    ╭─ Commands ─────────────────────────────────────────────────────────────────────────────────────────╮
    │ query               Query the path in manifest                                                     │
    │ secrets             Manage secrets stored in manifest                                              │
    │ show                Show manifest file                                                             │
    ╰────────────────────────────────────────────────────────────────────────────────────────────────────╯


Manifest works as a multi-level YAML document. Each value in the manifest can 
be retrieved by a path. For example, if the manifest is the following:

.. code-block:: yaml

    dependencies:
    - utility:
        installer: image
        options:
            core_auth_token: null
            core_auth_user: null
            core_host: https://core.malevich.ai/
            image_auth_pass: null
            image_auth_user: null
            image_ref: public.ecr.aws/o1z1g3t0/utility:latest
        package_id: utility
        version: ''
    preferences:
    log_format: RICH
    log_level: INFO
    verbosity:
        Installation: 0
        Intrepretation: 0
        Preparation: 1
        Removal: 0
        Results: 1
        Run: 1
        Stop: 0
    project_id: null
    space: null
    version: null

You can retrieve information about ``utility`` by using the following command:

.. code-block:: console

    malevich manifest query dependencies utility

or you may get deeper values 

.. code-block:: console

    malevich manifest query dependencies utility options image_ref


.. note::

    As you may notice, ``dependencies`` is the list, but there is no numerical
    index in the query. This is due to the structure of the manifest: all lists 
    contain dictionaries that consist of exactly one key. Thus, each item can be queried by this only key.
    This invariant is preserved for the whole manifest.


----------------
Managing Secrets
----------------

Secrets are automatically added each time a sensitive piece of information such as passwords appear in the manifest.
Secrets are referenced by their ID as text literals in the form: ``secret#000000``. When a slice of manifest is requested
you may pass ``--resolve-secrets`` flag to substitute all secret literals with actual values. Do this with a caution.

Restoring secrets
+++++++++++++++++

You may want to share a project with other people. However, they may not have access to the secret values you have
used in the project. Once they receive a manifest, they may run the following command to restore missing secrets:

.. code-block:: console

    malevich secrets restore

By running the command, the manifest is scanned for secrets and then user is prompted to restore their values.


System Project
++++++++++++++

If no project is initialized, Malevich uses a global manifest located in ``~/.malevich`` directory.