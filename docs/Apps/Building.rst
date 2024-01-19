=======================
Building Your First App
=======================

Prerequisites
-------------

To successfully build and run your first app, please make sure you have the following:

* `Docker <https://www.docker.com/>`_ installed and running. You will use it to build an environment for your app.
* `Malevich Package <https://github.com/MalevichAI/malevich>`_ installed. All the magic happens here. You can install it with:

.. code-block:: bash

    pip install malevich


Apps and Processors
-------------------

App is a collection of special objects: inputs, outputs, inits and processors. Processors are the most crucial
part of the app. They are responsible for the actual logic. They receive data, process it and return the result.
The data can come in two forms: as an asset or as a collection. Think about asset as a group of files and about
collection as tabular data. The result can be either an asset or a collection.

