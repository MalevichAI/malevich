========
Data
========

Malevich allows you to upload and manage data. There are two ways of storing data in Malevich:

1. **Collection**: We refer to collection when talking about tabular data. Collections consist of documents: key-value pairs.
   Each document have the same set of keys. One can think about collections as about tables in relational databases or pandas dataframes. Another
   way to think about collections is collections on MongoDB, Firestore or other document-oriented databases.

2. **Assets**: To represent binary objects we use the term asset or object. They are implemented as a file storage and asset in this case is a 
   path within the storage. If asset is a path to a folder, we call such assets "composite" and they are stored and retrieved as a set of files.

These entities are used to represent both the data you use within flows, and data produced by each of processors during flow execution.


Collections
-----------

Collections are useful when your data is structured and easily representable. For example, list of users, text attributes
of products on marketplace, links to websites you wish to crawl, etc. It is the most common way to store data in Malevich.

Create a collection
+++++++++++++++++++

To create a collection to be used in the flow, you have to use :func:`collection <malevich._meta.collection>` function. The
function allows you to upload tabular data using :class:`pandas.DataFrame` object or :code:`csv` file. The function returns
a `traced` object - a special placeholder that dictates the flow execution engine which operations should be performed on
the data. The data is uploaded (or updated) only on :doc:`interpretation </Flows/Lifecycle>` stage (or even later).

.. warning::

    The :func:`collection <malevich._meta.collection>` function can only be used inside the :func:`flow <malevich._meta.flow>` function.

Example:

.. code-block:: python

    import pandas as pd

    from malevich import collection, flow

    data = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['John', 'Jane', 'Bob'],
        'age': [30, 25, 40]
    })


    @flow
    def my_flow():
        users = collection('Users Collection', df=data)
        # or
        users = collection('Users Collection', file='users.csv')

        # Operation on users
        ...

        return ...

Assets
------

Assets (or objects) are useful when working with files or multiple files that are not easily representable as a table.
For example, images, videos, text files, machine learning model weights, etc. Assets are stored in the cloud storage
and mounted to apps filesystem on demand.

Create an asset
+++++++++++++++

There two ways to create an asset. Both of them utilize :class:`AssetFactory`. The factory
can be imported and used in the following way:

.. code-block:: python

    from malevich import asset, flow


    @flow
    def my_flow():
        file_asset = asset.file('path/to/file')
        folder_asset = asset.multifile(folder='path/to/folder')
        multiple_files = asset.multifile(files=['path/to/file1', 'path/to/file2'])

        # Operation on assets
        ...

        return ...

    
.. toctree::
    :hidden:
    :maxdepth: 3

    self