====================
Working with Results
====================

Once flow is interpreted and executed, you may obtain results of the execution.

Let's consider the following example:

.. mermaid::

    graph LR
        A{{Training Data}} --> P1[Preprocess]
        P1 --> P2([Training])
        B{{Training Labels}} --> P2
        P2 -->|trained_model| P3[Evaluation]
        P2 -->|trained_model| P4[Training Report Generation]
        P2 -->|metrics| P5
        P3 --> P5([Final Report Generation])
        P4 --> P5

To compose and execute this flow, you would write code as follows:

.. code-block:: python

    from malevich.app import preprocess, train, evaluate, training_report, final_report
    from malevich import flow, collection, CoreInterpreter

    @flow
    def train_and_evaluate():
        # Define the start of the flow with collections.
        training_data = collection('training_data')
        training_labels = collection('training_labels')

        # Passing a collection to a processor function will
        # automatically register the dependency (Training Data --> Preprocess).
        preprocessed_data = preprocess(training_data)

        # Passing results of previous processors will also register
        # dependencies (Preprocess --> Training).
        trained_model, metrics = train(preprocessed_data, training_labels)

        # Processors for generating reports and evaluation results.
        training_report_result = training_report(trained_model)
        evaluation_result = evaluate(trained_model)

        # The final report is generated based on previous outcomes.
        return trained_model, final_report(training_report_result, evaluation_result, metrics)

    # Deploy and execute the flow.
    task = train_and_evaluate()
    task.interpret(CoreInterpreter(core_auth=('example', 'Welcome to Malevich!')))
    task.prepare()
    task.run()
    task.stop()

    results = task.results()

.. note::

    Now, the object :code:`results` will contain results of flow execution. We fetch outputs
    of operations you have returned from the flow function (ones in rounded rectangles). In this case, the output of
    :code:`train` processor is assumed to be an asset and a collection. The output of
    :code:`final_report` processor is assumed to be a collection. So, the variable
    :code:`results` will be a list of two objects of type :class:`malevich.models.results.base.BaseResult`.
    

Different interpreters address results in different ways. We have run task on Malevich Core, so 
we actually get a list of :class:`malevich.models.results.core.CoreResult` objects. Here is a way to 
extract actual results:

.. code-block:: python

    # CoreResultPayload
    train_results = results[0].get()
    # pandas.DataFrame
    report = results[1].get_df()

    # train returns a 
    model, metrics = train_results[0].data, train_results[1].data
