Flows
======

Apps can be composed into complex flows that solve real-world problems. A flow dictates the data's path through the system and the order in which apps are executed.

To simplify the definition of flows, we leverage the Python language. As processors are Python functions, your data becomes arguments, and the flow is determined by the sequence of operations in your code.

Consider the following flow:

.. mermaid::

    graph LR
        A{{Training Data}} --> P1[Preprocess]
        P1 --> P2[Training]
        B{{Training Labels}} --> P2
        P2 --> P3[Evaluation]
        P2 --> P4[Training Report Generation]
        P3 --> P5[Final Report Generation]
        P4 --> P5

To compose and execute this flow, you would write code as follows:

.. code-block:: python

    from malevich.app import preprocess, train, evaluate, training_report, final_report
    from malevich import flow, collection

    @flow()
    def train_and_evaluate():
        # Define the start of the flow with collections.
        training_data = collection('training_data')
        training_labels = collection('training_labels')

        # Passing a collection to a processor function will
        # automatically register the dependency (Training Data --> Preprocess).
        preprocessed_data = preprocess(training_data)

        # Passing results of previous processors will also register
        # dependencies (Preprocess --> Training).
        trained_model = train(preprocessed_data, training_labels)

        # Processors for generating reports and evaluation results.
        training_report_result = training_report(trained_model)
        evaluation_result = evaluate(trained_model)

        # The final report is generated based on previous outcomes.
        return final_report(training_report_result, evaluation_result)


.. note::

    This flow function captures the order and details about operations and data. **The important detail
    here is that flow definition contains no logic.** All the processors are just stubs (empty functions)
    that mimics interface of real functions and only used to define the flow. When flow is **run**
    the functions are replaced with real ones on cloud and executed in the order defined by the flow function.

    In other words, calling :code:`preprocess` will not preprocess any data on your computer and
    :code:`train` will not train any models. Instead, the will define an order, that will be sent
    to Malevich together with all necessary information about operations and data 
    in the flow for it to be executed remotely.

    So, the flow definition is totally independent from its execution.


Explore subsequent sections to learn how it all works and how you can manage flows in Malevich:

.. toctree::
    :maxdepth: 2

    Autoflow
    Lifecycle
    Results