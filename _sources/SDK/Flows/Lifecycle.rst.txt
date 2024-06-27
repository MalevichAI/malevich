=============
Lifecycle
=============

Once you define a flow, it can be in any of the following states:

1. **Defined**: The initial state of a flow just after you defined a function decorated with :code:`@flow`.
2. **Interpreted**: When flow is attached to any of the platform and ready to be converted to an actual task on it.
3. **Deployed (Prepared)**. When flow has successfully been converted to a task on the platform and ready to accept and proceed with the input data.
4. **Stopped (Suspended)**. When underlying task is stopped and cannot be executed anymore.

Also, once task is deployed, it can be run multiple times with different input data.

Definition
----------

To define a flow, you shold import a :func:`meta <malevich._meta.flow>` decorator and use it on an ordinary function:

.. code-block:: python

    from malevich import flow

    @flow
    def my_flow():
        pass
    
    task = my_flow()

After it, calling :code:`my_flow` will return a :class:`malevich.models.PromisedTask` instance. You can
attach the flow to any of the platforms and run it there.


Interpretation
--------------

Interpretation is a process of attaching a flow to a specific platform you
wish to run it on. The flow is automatically interpreted when passed to a 
deploy assistant such as :class:`Core <malevich._deploy.Core>` or :class:`Space <malevich._deploy.Space>`.

.. code-block:: python

    from malevich import Core, Space

    @flow
    def my_flow():
        # Your flow logic here...
    
    interpreted_on_core = Core(my_flow) 
    # or Core(my_flow(...)) if my_flow accepts args

    interpreted_on_space = Space(my_flow)

Here, :code:`interpreted_on_core` and :code:`interpreted_on_space` are instances of interpreted tasks. They
preserve the flow logic and inner state specific to the platform they are attached to. Running :meth:`prepare` method
will create an instance of a the task on a platform. A ready-to-run task can be invoked with :meth:`run` method and
stopped with :meth:`stop` method. The full list of methods and their signatures is described in :class:`CoreTask <malevich.models.task.interpreted.core.CoreTask>` and :class:`SpaceTask <malevich.models.task.interpreted.space.SpaceTask>` classes.


Deployment
----------

We refer to deployment as a process of acquiring resources on the platform and
preparing the task to be run. To deploy a task, you should call :meth:`prepare`

.. code-block:: python

    interpreted_on_core = Core(my_flow)
    interpreted_on_core.prepare() # Creates a task on the platform

Once the task is deployed, you may run :meth:`run`, :meth:`stop`.

Running
-------

Each task can be considered as a standalone function that can be run on the platform. To run a task, you should call
:meth:`run` method on it. The method can accept input data for :doc:`collections </SDK/Data/index>`, :doc:`assets </SDK/Data/index>` and configurations.
.. TODO: write about configuration overrides

.. code-block:: python

    from malevich import table

    interpreted_on_core.run()                                   # Run with default input data defined in the flow
    interpreted_on_core.run(overrides={'my_data': table(...)})  # Run with new data in `my_data` collection
    interpreted_on_core.run(config_extensions={'preprocess': {...}})  # Run with configuration extensions
    
    # .run method returns run_id for future reference
    run_id = interpreted_on_core.run(
        overrides={'my_data': table(...)},
        config_extensions={'preprocess': {...}}
    )  

    # Also, you may run the task with a run_id
    interpreted_on_core.run(run_id=run_id)



Signature and logic of :meth:`run` method is different for different interpreters. Check the documentation for
specific interpreters for more information.

Stopping
--------

It is important to release resources on the platform when you don't need them anymore. To stop a task, you should call
:meth:`stop` method on it.

.. code-block:: python

    task.prepare()  # Prepare the task
    task.run()      # Do the job
    task.stop()     # Release resources

.. warning:: 

    Tasks that are not stopped manually and not active for a certain period are subjects to be suspended 
    automatically. 


Results
-------

After the task is run, you may want to get the results of its execution. To do so, you should call :meth:`results` method
on it. The flow fetches results for objects you have returned in :code:`flow` decorated function. Results are a list
of :class:`malevich.models.results.base.BaseResult` instances. See `Results`_ section to learn more about how to work
with results.

.. code-block:: python

    interpreted_on_core.run()
    # list of CoreResult instances
    results = interpreted_on_core.results()

    interpreted_on_space.run()
    # list of SpaceResult instances
    results = interpreted_on_space.results()



