==============
Dependencies
==============

Malevich provides a package manager service just like ``pip`` or ``npm``. Each :doc:`app </SDK/Apps/Introduction>` that can be run
with Malevich can be installed into the current environment and got manifested in the project.

Once a package is installed, Malevich creates a *stub module* in the ``malevich`` module. The stub
contains Python objects that can be imported and used within :doc:`Flows </SDK/Flows/Introduction>`.

----------
Installers
----------

A Malevich depedency can be installed in different ways. Currently, two ways are provided:

Install from Image
++++++++++++++++++

An app can be installed directly from Docker image. The image should be pushed to any of 
image repositories including private ones. Image-based apps can be only used when deployed directly on Core.

To install with image use the following command:

.. code-block:: console

    malevich use image <PACKAGE_NAME> <IMAGE_REF> [IMAGE_AUTH_USER] [IMAGE_AUTH_PASSWORD]  

Image installer does not require any authorization besides credentials required to access Docker image (in case the image is pushed to a private repository).

Install from Space
+++++++++++++++++++

.. important::

    Installation using Space requires the project to be connected to a `Space <https://space.malevich.ai>`_ account.
    To connect use the following command:

    .. code-block:: console

        malevich space login

    Apps installed with Space can be deployed both on Core directly or via Space

To install with Space, use the following command:

.. code-block:: console

     malevich use space [OPTIONS] PACKAGE_NAME REVERSE_ID [BRANCH] [VERSION]


Automatic Installation
++++++++++++++++++++++

Malevich provides an extensive library of public components that can be installed in your environment. The easiest
way to install a public component is to use ``malevich install <APP_NAMES...>`` command:

.. code-block:: console

    malevich install utility openai 

By default, Space installer is utilized. To use the image installer, use:

.. code-block:: console

    malevich install utility openai --using image

Python Environment
++++++++++++++++++

Malevich is not managing Python environment. It installs apps into the activated Python environment. Once the environment is changed, you may restore dependencies stated in manifest with ``malevich restore``. To show a list of installed apps, you may use
``malevich list``.