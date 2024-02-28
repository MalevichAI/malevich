=======================
Building Apps
=======================

Prerequisites
-------------

To successfully build and run your first app, ensure you have the following:

* `Docker <https://www.docker.com/>`_ installed and operational. You will use Docker to create an environment for your app.
* The `Malevich Package <https://github.com/MalevichAI/malevich>`_ installed. This is where all the magic happens. Install it using:

.. code-block:: console

    pip install malevich

Getting Started
---------------

If you are not yet familiar with :doc:`apps <What_is_App>` or :doc:`processors <What_is_Processor>`, please review the corresponding pages first.
Let's create an app by running the following command:

.. code-block:: console

    malevich new example_app

This command generates a new directory named ``example_app`` with the following structure:

.. code-block::

    example_app/
    ├─ apps/
    │  ├─ processors.py
    ├─ Dockerfile
    ├─ flow.py
    ├─ README.md
    ├─ requirements.txt

The :code:`processors.py` file includes processors that become available once you build and push your app. The :code:`flow.py` file showcases an example flow that can be executed using your apps. For more information, refer to the :code:`README.md` file generated alongside the app.

Building an App
---------------

To make your app available for use, it must be built. Ensure you have `Docker <https://www.docker.com/>`_ installed and running. Build your app with this command:

.. code-block:: console

    cd example_app
    docker build -t example_app .

Publishing an App
-----------------

After building your app, it needs to be pushed to a registry. When you run a flow including your app, Malevich's cloud service will pull the image from this registry and execute it in a container.

To prototype your app, create a public repository on `Docker Hub <https://hub.docker.com/>`_. Pushing to Docker Hub is free and straightforward.

Push your app to the registry by first logging in:

.. code-block:: console

    docker login

Next, tag your image:

.. code-block:: console

    docker tag example_app <your-docker-hub-username>/example_app   

Finally, push it:

.. code-block:: console

    docker push <your-docker-hub-username>/example_app  

Installing an App
-----------------

Your app is now ready for execution in Malevich's cloud. Install it to provide the cloud with necessary information by running this command:

.. code-block:: console

    malevich use image example_app <your-docker-hub-username>/example_app

In case you have a private registry, you have to provide your credentials, so that Malevich's cloud can pull the image from the registry. To do so, run the following command:

.. code-block:: console

    malevich use image example_app <your-docker-hub-username>/example_app <your-docker-hub-username> <your-docker-hub-password>


Running a Flow
--------------

Your app is now accessible within flows! To use the provided processors, import them and set up a flow as follows:

.. code-block:: python

    import pandas as pd

    from malevich import CoreInterpreter, collection, flow
    from malevich.example_app import find_pattern


    @flow()
    def find_direct_speech():
        data = collection(
            name='Example Text', df=pd.DataFrame(
                {'text': ["This is a regular text", "'Hi!', said Alice"]}
            ))

        return find_pattern(data, config={'pattern': r"'.+'"})


    task = find_direct_speech()
    task.interpret(CoreInterpreter(core_auth=('example', 'Welcome to Malevich!')))

    print(task()[0].get_df().head())



Executing this flow will run the :code:`check_malevich` processor and output the result. Visit the Flows section for more details on their usage and capabilities.

Inputs and Outputs
------------------

Each processor specifies its expected inputs and outputs. Inputs are defined through function arguments, which can be annotated with types such as :class:`DF <malevich.square.df.DF>`, :class:`DFS <malevich.square.df.DFS>`, :class:`OBJ <malevich.square.df.OBJ>`, and :class:`Sink <malevich.square.df.Sink>`. These types help define how data should be handled within flows.

Processors are designed to be linked together in flows, where one's outputs feed into another's inputs. Each processor input corresponds to precisely one output of another processor. When a processor returns multiple outputs, they are bundled into a :class:`DFS <malevich.square.df.DFS>` object associated with a single input. Processors may also connect to data sources such as collections or assets; each source must link to a distinct processor input.

An exception occurs when a processor has only one input annotated as :class:`Sink <malevich.square.df.Sink>`. These processors cannot receive data directly from sources but can accept inputs from an unlimited number of other processors.

Below are examples of processors with various configurations of inputs and outputs:
    
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

    An argument of type :class:`DF <malevich.square.df.DF>` can also accept an asset (a :class:`OBJ <malevich.square.df.OBJ>` object), which will be converted into a dataframe with a single column named :attr:`path <malevich.square.df.OBJ.path>` containing file paths from the asset. The relevant schema is known as :class:`obj <malevich.square.df.obj>`, which indicates the expected conversion.

App Configuration
-----------------

Applications may accept user-defined configurations when running a flow by including an argument explicitly annotated with :class:`Context <malevich.square.utils.Context>`. This configuration resides within the context's :attr:`app_cfg` attribute.

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


Then configure your app when executing a flow like this:

.. code-block:: python

    from malevich.example_app import get_slice
    from malevich import collection, flow

    @flow()
    def example_flow():
        data = collection('Example data', file='data.csv')
        return get_slice(data, config={'slice_start': 10, 'slice_end': 20})
