============
Contributing
============

We promote a number of packages to open-source, and welcome contributions from the community. Explore 
the page to see what we have available, and feel free to fork and submit pull requests for any
changes you think would be useful.


Documentation
-------------

If you were reading the documentation, and spotted a mistake, or something that could be improved,
please fork the `docs <https://github.com/malevichAI/malevich/tree/docs>`_ branch of the repository 
and create a pull request with your changes to the :code:`dev/docs` branch. We will review your
changes and merge them into the documentation.

As part of documentation is generated from docstrings, we also welcome improvements to the docstrings
and interfaces that improve developer experience. 

The documentation is built using `Sphinx <https://www.sphinx-doc.org/en/master/>`_. To build the
documentation locally, you will need to install the dependencies from :code:`requirements.dev.txt`,
and then run 

.. code-block:: console

    rm -rf build-docs && sphinx-build docs build-docs

.. warning::

   The branch :code:`dev/docs` is not meant to be merged to any of upstream
   branches. It is used only for documentation purposes.

Git Workflow
------------

We use a :code:`dev/unstable` branch for development. Or changes merge here first, and are then
reviewed and merged into the :code:`dev/stable` branch. After a period of working with dev version 
we conclude that release is indeed stable, we merge it into the :code:`main` branch. 

If you wish to contribute to the project, please fork the repository and create a pull request
with your changes to the :code:`dev/unstable` branch. We will review your changes and merge them.

We perform a couple of procedures to ensure that the code is of high quality:

1. Before merging into the :code:`dev/unstable` branch, check your code with :code:`ruff`
   linter. This will ensure that your code is formatted correctly, and that you have not
   introduced any errors. After installing dependencies from :code:`requirements.dev.txt`, you can
   run :code:`ruff check malevich/` from the root of the repository.

2. When dealing with branches, we follow the following convention:

   * If you are working on a new feature, create a branch with the name :code:`feature/<name>`. These
     branches are merged into the :code:`dev/unstable` branch. If someone else is working on the same
     feature, you can create a branch with the name :code:`feature/<name>-<username>`. 
    
   * If you are fixing a bug, create a branch with the name :code:`fix/<name>`. These branches are
     are merged into the :code:`dev/unstable` branch. If someone else is working on the same bug, you
     can create a branch with the name :code:`fix/<name>-<username>`. *We hope that there were no
     bugs that will require multiple people to work on them at the same time, but stil...*

   * If you spot an issue that preventing you from using an important feature of Malevich package
     you can create a branch with the name :code:`hotfix/<name>`. These branches are merged into the
     :code:`main` branch directly after review yielding a new version immediately. 

3. When dealing with commits, we follow the following convention for commit messages:

.. code-block:: 

    <type>: <message>

    <longer message>

where ``type`` is one of:

* `add`: A new feature
* `fix`: A bug fix 
* `docs`: Documentation only changes
* `style`: Changes that do not affect the meaning of the code (white-space, formatting, missing semi-colons, etc)
* `upd`: A code change that neither fixes a bug nor adds a feature
* `misc`: Other changes

If commit serve several purposes, you can use multiple types in the commit message. For example,
if you are adding a new feature, but also fixing a bug, you can use the commit message:

.. code-block::

    add, fix: <message>

    <longer message>

or 

.. code-block::

    add: <message>; fix: <message>

    <longer message>


Posting an issue
----------------

If you have found a bug, or have a feature request, please post an issue on `Github <https://github.com/MalevichAI/malevich/issues>`_.

When posting an issue, it is helpful to include:

* A description of the bug or feature request
* A version of Malevich you are using
* A minimal working example that reproduces the bug or describes the feature request
* The content of ``~/.malevich/logs`` folder (it will contain useful anonymous information about actions you have performed with Malevich)
* The content of ``malevich.yaml`` file from the root of your project (if you have one)


.. toctree:: 

    self