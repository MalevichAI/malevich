==================
Processors
==================

Processors are the core logic units of apps. They are responsible for processing 
data and generating output. 

Processors receive input in the form of assets, collections and their combinations. Assets can be understood
as files or folders, while the term collection refers to tabular data. Processor can have 
multiple inputs and can output multiple objects. 

How to define a processor?
++++++++++++++++++++++++++

To define a processor, you have to decorate a function with the `@processor` decorator.

.. code-block:: python

    from malevich.square import processor, Context

    @processor()
    def my_processor(input1, input2, context: Context):
        # do something with input1 and input2
        return output1, output2

The function have to follow the following conventions:

1. Each of argument but the one annotated with :code:`context: Context` has to be a reference to a particular input. The input
can be either an output of a previous processor, a collection, or an asset.
2. The function has to return either a single dataframe or a tuple of dataframes. 


Each of the input references to the output of exactly one previous processor or 
collection. Assume, the following pipeline:

.. mermaid::

    graph LR
        A[train_model] --> |"| model, metrics |"| B[predict]
        C[prediction_data] --> B[predict]
    
and the following code:

.. code-block:: python

    from malevich.square import processor


    @processor()
    def train_model(data):
        ...
        return model, metrics


    @processor()
    def predict(train_outcome, data_for_prediction):
        model, metrics = train_outcome
        ...
        return predictions

In this case, :code:`train_outcome` refers to the output of :code:`train_model` and :code:`data_for_prediction` refers to data in :code:`prediction_data` collection.
To access model and metrics, you have to unpack the :code:`train_outcome` variable.

DF, DFS, Sink and OBJ
+++++++++++++++++++++

Malevich makes use of specific data types when passing data between processors. Each
of these types denote a specific entity that processor can receive as an input or return as an output.

* :class:`DF <malevich.square.df.DF>` - a single instance of tabular data. The table can follow a specific schema. 
* :class:`DFS <malevich.square.df.DFS>` - a collection of tabular data. The collection can be bound by a specific number of tables or be unlimited. Also, it can impose a schema on each table.
* :class:`Sink <malevich.square.df.Sink>` - a collection of DFS that allows you to denote a processor capable of being link to unbounded number of processors.
* :class:`OBJ <malevich.square.df.OBJ>` - a collection of files that can hold arbitrary binary data.

See, how they are applied in the following example:

.. code-block:: python

    from malevich.square import processor, DF, Sink, OBJ, obj


        @processor()
        def train_model(data: DF['TrainData']) -> tuple[OBJ, DF['Metrics']]:
            ...
            return model, metrics


        @processor()
        def predict(
            train_outcome: DFS['obj', 'Metrics'], 
            data_for_prediction: DF["ValidationData"]
        ) -> DF["Predictions"]:
            model, metrics = train_outcome
            ...
            return predictions

