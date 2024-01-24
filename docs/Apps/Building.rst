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


Getting Started
---------------

If you are not yet familar with `apps <./What_is_App.html>`_ or `processors <./What_is_Processor.html>`_, please read the corresponding pages first.
Let us create an app. For it, run the following command:

.. code-block:: bash

    malevich new example-app

This will create a new directory ``example-app`` with the following structure:

.. code-block::
    
    example-app/
    ├─ apps/
    │  ├─ processors.py
    ├─ Dockerfile
    ├─ flow.py
    ├─ README.md
    ├─ requirements.txt


The file :code:`processors.py` contains processors that will become available 
once you push and build your app. The file :code:`flow.py` contains the example flow that
can be run with your apps. For additional information you might refer to the
:code:`README.md` file that was generated together with the app.


Building an app
---------------

To make your app available for use, you need to build it. For this, you should have
`Docker <https://www.docker.com/>`_ installed and running. To build your app, run the following command:

.. code-block:: bash

    cd example-app
    docker build -t example-app .


Publishing an app
-----------------

After you have built your app, you need to push the image to the registry. Once you
run a flow with your app, the image will be pulled from the registry and run in a container
on Malevich cloud. 

To prototype your app, it is enough to create a public repository on `Docker Hub <https://hub.docker.com/>`_.
Pushing to such a repository is free and simple.

To push your app to the registry, firstly login to the registry:

.. code-block:: bash

    docker login

Then tag your app:

.. code-block:: bash

    docker tag example-app <your-docker-hub-username>/example-app   

And, finally, push it:

.. code-block:: bash

    docker push <your-docker-hub-username>/example-app  


Installing an app
-----------------

Now your app is available for Malevich cloud to run. You need to install it to provide 
the cloud sufficient information. To install your app, run the following command:

.. code-block:: bash

    malevich install example-app


Running a flow
--------------

Now, your app is accessible in flows! Import and observe available processors:

.. code-block:: python

    from malevich.example_app import check_malevich
    from malevich import CoreInterpreter, flow
    
    @flow()
    def example_flow():
        return check_malevich()

    
    task = example_flow()
    task.interpret(CoreInterpreter(core_auth=('example', 'Welcome to Malevich!')))

    print(task())
   

The flow will run the processor :code:`check_malevich` and print the result. Explore the
section Flows to understand what are they and how to use them.


Inputs and Outputs
------------------

Each processor dictates what data it receives and what it produces. We refer to it
as inputs and outputs of the processor. The inputs are defined through function arguments,
Each argument can annoated with one of the following types: `DF <../API/square/df.html#malevich.square.df.DF>`_, 
`DFS <../API/square/dfs.html#malevich.square.df.DFS>`_, `Sink <../API/square/sink.html#malevich.square.df.Sink>`_, and 
`OBJ <../API/square/obj.html#malevich.square.df.OBJ>`_. Explore each of them to understand their purpose.

Processors are meant to be linked together in flows. The outputs of one processor become the inputs of another.
Each processor input refers to only a single output of another processor. When processor returns multiple outputs,
they are grouped into `DFS <../API/square/dfs.html#malevich.square.df.DFS>`_ object and assigned to a single
particular input. Processors can also be connected to a data sources - collecttions or assets. In this case,
each collection and asset is assigned to a separate input of the processor.

There, however, is a special case when a processor has only one input and it is annotated with `Sink <../API/square/dfs.html#malevich.square.df.Sink>`_.
Such processors cannot be receive information from data sources, but can receive inputs from unlimited number of other processors.

Here is some example of a processors:

.. code-block:: python

    from malevich import processor, DF, DFS, Sink, OBJ

    @processor()
    def only_df(df: DF):
        """
        This processor can be connected to processors that 
        produce only a single data frame, or to a data source.
        """
        return df

    @processor()
    def only_dfs(dfs: DFS):
        """
        This processor can be connected to processors that
        return any number of data frames, and still can be
        connected to a data source.
        """
        return dfs

    @processor()
    def two_dfs(dfs1: DFS, dfs2: DFS):
        """
        This processor can be connected to processors that
        return any number of data frames. Each of the inputs
        can be connected to a data source.
        """
        return dfs1, dfs2

    @processor()
    def df_and_dfs(df: DF, dfs: DFS):
        """
        This processor can be connected to two processors.
        The first one should return a single data frame, the
        second one should return any number of data frames.

        Still, each of the inputs can be connected to a data source.
        """
        return df, dfs

    @processor()
    def df_and_sink(df: DF, sink: Sink):
        """
        This processor can be connected to any amount of processors.
        The first processor connected to it should return a single
        data frame, the rest of the processors can return any.

        A data source can be only to `df` input.
        """
        return df, sink

    @processor()
    def sink_df(sink: Sink, df: DF, dfs: DFS):
        """
        This processor can be connected to any amount of processors.
        The last processor connected can return any number of data frames,
        while the one before it should return a single data frame. The
        rest of the processors can return any. The minimum amount of
        processors connected to this one is 3.

        A data source can be only to `df` and `dfs` inputs.
        """
        return dfs, sink


    @processor()
    def asset_and_df(asset: OBJ, df: DF):
        """
        This processor can be connected to two processors
        or data sources. The first one should return an asset,
        while the second one should return a single data frame (or asset, see below).

        The first data source should be a file or a folder, while
        the second one can be any.
        """
        return asset, df


.. note::

    An argument of type :code:`DF` can also receive an asset (a :code:`OBJ` object)
    which will be converted to a data frame with a single column :code:`path` containing
    paths to files in the asset. The corresponding schema which is called :code:`obj` can
    be used to denote the expected conversion.


App Configuration
-----------------

Applications can accept configuration - a set of parameters that can be set by the user
when running a flow. To make the app configurable, you need to accept an extra argument which
is explicitly annotated with `Context <../API/square/utils.html#malevich.square.utils.Context>`_.
The configuration is stored in `app_cfg <../API/square/utils.html#malevich.square.utils.Context.app_cfg>`_ attribute of the context.

Example:

.. code-block:: python

    from malevich import processor, DF, Context

    @processor()
    def get_slice(df: DF, context: Context):
        """
        Context is a special argument that can be used to access
        the configuration of the app. Also, it contains 
        useful information about the environment and utilities
        to interact with it. See the API reference for more details.
        """
        slice_start = context.app_cfg.get('slice_start', 0)
        slice_end = context.app_cfg.get('slice_end', 10)
        return df.iloc[slice_start:slice_end]


Then, when running a flow, you can set the configuration:

.. code-block:: python

    from malevich.example_app import get_slice
    from malevich import collection, flow

    @flow()
    def example_flow():
        data = collection('Example data', file='data.csv')
        return get_slice(data, config={'slice_start': 10, 'slice_end': 20})

        