=================
Execution 
=================

Flow captures the semantic of the execution tree and can be used as a standalone function. As 
any function, it can be run with input data and produce the output. Collections and assets serve
as flow inputs, while the output of last :doc:`processors </SDK/Apps/Processors>` is the output of the flow.

.. mermaid::
    :align: center

    graph LR
        C1(Collection #1) --> P1[Processor #1]
        C2(Collection #2) --> P2[Processor #2]
        A1(Asset #1) --> P2
        P1 --> P2
        P2 --> P3{{Processor #3}}
        P1 --> P4{{Processor #4}}


Here, collections (Collections #1 and #2) and assets (Asset #1) are the inputs of the flow. Processors #1 and #2
transform the input data, while Processor #3 and Processor #4 are the final processors that produce the output of the flow.
In Python, it would look like this:

.. code-block:: python

    from malevich import collection, asset

    def my_flow(collection_1: collection, collection_2: collection, asset_1: asset):
        # ... Flow logic here ...
        return processor_3_output, processor_4_output


Overriding Data
---------------

When defining a flow, you specify the initial data in collections

.. code-block:: python

    from malevich import collection, flow, table

    @flow
    def my_flow():
        data = collection(
            'name_of_my_collection',
            df=table(...),
            alias='my_data'
        )

        # ... Flow logic here ...

But on run, you can supply new data to the flow. It is done using the ``alias`` of the collection. The process
of supplying new data is called `overriding`.

.. code-block:: python

    from malevich import Space, table

    interpreted_on_space = Space(my_flow)
    interpreted_on_space.prepare()
    interpreted_on_space.run(overrides={'my_data': table(...)})

If you :doc:`install the flow </SDK/Flows/Integration>`, you may supply new data using 
the keyword argument of the flow stub:

.. code-block:: python

    from malevich.flows import my_flow

    my_flow(my_data=table(...))


If no data is supplied, the flow will run with the default data defined in the flow.

.. warning::

    The `alias` of each component in the flow should be unique. 

Configuration Extension
-----------------------

In addition to the data, you may supply configuration extensions to the processors in the flow. It is
also done by aliases.

.. code-block:: python

    from malevich.utility import rename, add_column   # installed processors
    from malevich import Core, collection, flow, table

    @flow
    def my_flow():
        data = collection(
            'marketplace_cards',
            df=table(...),
            alias='cards'
        )

        title = locs(data, column='Card Title')
        description = locs(data, column='Card Description')

        return (
            rename(title, config={"Card Title": "title"}, alias='to_title'),
            rename(description, config={"Card Description": "description", alias='to_description'}
        )


    interpreted_on_core = Core(my_flow)
    interpreted_on_core.prepare()
    interpreted_on_core.run(config_extensions={'to_title': {'column': 'Title'}})

In this example, we supply a new configuration to the ``to_title`` processor. The configuration is a dictionary
that will be merged with the default configuration of the processor. 

The extension does not replace the default configuration, but extends it. Assume the
following logic behind it:

.. code-block:: python

    default_config = {"column": "Card Title"}
    new_config = {"column": "Title"}

    final_config = {**default_config, **new_config}

    # final_config = {"column": "Title"}


