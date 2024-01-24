============
What is App?
============

.. _pogrebnoijak/julius_export_python: https://hub.docker.com/r/pogrebnoijak/julius_export_python
.. _pogrebnoijak/julius_export_python11: https://hub.docker.com/r/pogrebnoijak/julius_export_python11
.. _pogrebnoijak/julius_export_python_torch: https://hub.docker.com/r/pogrebnoijak/julius_export_python_torch
.. _pogrebnoijak/julius_export_python_ubuntu: https://hub.docker.com/r/pogrebnoijak/julius_export_python_ubuntu

Apps can be understood as an environment consisting of the following objects:

* **Processors**: logical units of computation that transform data or do useful work.
* **Inputs**: filter units that pre-process inputs before handing them off to the processors.
* **Outputs**: post-processors that perform actions on the outputs of the processors.
* **Inits**: initializers that prepare the environment for the execution of the processors.

By interconnecting these objects, you can create a pipeline that solves particular problems. 

Currently, apps resides within Docker images. To ensure seamless integration with Malevich, 
an image app must meet the following criteria:

1. The image must be based on one of the following **base images**:

    * `pogrebnoijak/julius_export_python`_ - Optimized for Python 3.9 projects.
    * `pogrebnoijak/julius_export_python11`_ - Geared towards Python 3.11 projects.
    * `pogrebnoijak/julius_export_python_torch`_ - Customized for Python 3.11 projects requiring PyTorch and CUDA.
    * `pogrebnoijak/julius_export_python_ubuntu`_ - Perfect for projects based on Ubuntu 20.04.

2. The app's **codebase must** reside within the `./apps` directory **inside the Docker image**. The inner structure of the `./apps` directory does not matter, all Python files will be parsed and all processor functions will be registered automatically regardless of their location.

Here is a Dockerfile template illustrating how to assemble the `Utility <https://github.com/MalevichAI/malevich-library/tree/main/lib/src/utility>`_ app, a component of our library:

.. code-block:: docker

    # Starting from a Python 3.9 base image
    FROM pogrebnoijak/julius_export_python:0.1

    # Transfer the requirements.txt file
    COPY requirements.txt requirements.txt

    # Process the installation of dependencies
    RUN if test -e requirements.txt; then pip install --no-cache-dir -r requirements.txt; fi

    # Populate the image with our application's code
    COPY ./apps ./apps


Library
=======

The Malevich development team curates a comprehensive library of apps tailored for use in your endeavors. Our repository on `GitHub <https://github.com/MalevichAI/malevich-library>`_ houses this collection.

To add an app from the library to your project, execute:

.. code-block:: bash

    malevich install <APP_NAME>

This will integrate the app and its relevant dependencies into your project, making it ready for use within your pipeline. As an illustration, let's install the `utility <https://github.com/MalevichAI/malevich-library/tree/main/lib/src/utility>`_ app:

.. code-block:: bash

    malevich install utility

Following installation, the app is primed for pipeline deployment:

.. code-block:: python

    from malevich.utility import add_column
    from malevich import flow, collection

    @flow()
    def foo():
        data = collection("data", df=pd.DataFrame({"a": [1, 2, 3]}))
        enriched_data = add_column(data, config={"column": "b", "value": 1})
        return enriched_data

Remember, the library is actively expanding with new apps designed to further streamline and enhance your project workflows.