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

To define a flow, you shold import a :func:`malevich._meta.flow` decorator and use it on an ordinary function:

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
wish to run it on. To interpret a flow, you should call :meth:`interpret` method
on a :class:`malevich.models.PromisedTask` instance with a certain interpreter.

.. code-block:: python

    from malevich import CoreInterpreter, SpaceInterpreter, flow

    @flow
    def my_flow():
        pass

    task = my_flow()
    task.interpret(CoreInterpreter(core_auth=('example', 'Welcome to Malevich!')))

    space_task = my_flow()
    space_task.interpret(SpaceInterpreter())


Once the task is interpreted, you may run :meth:`prepare`, :meth:`run`, :meth:`stop` and :meth:`results` methods on it.
The arguments and logic of these methods are different for different interpreters. Check the documentation for
specific interpreters for more information.

Deployment
----------

We refer to deployment as a process of acquiring resources on the platform and
preparing the task to be run. To deploy a task, you should call :meth:`prepare`

.. code-block:: python

    task = my_flow()
    task.interpret(CoreInterpreter(core_auth=('example', 'Welcome to Malevich!')))
    task.prepare()

Once the task is deployed, you may run :meth:`run`, :meth:`stop`.

Running
-------

To run a task, you should call :meth:`run` method on it. 

.. code-block:: python

    task = my_flow()
    task.interpret(CoreInterpreter(core_auth=('example', 'Welcome to Malevich!')))
    task.prepare()
    task.run()

Signature and logic of :meth:`run` method is different for different interpreters. Check the documentation for
specific interpreters for more information.


Stopping
--------

It is important to release resources on the platform when you don't need them anymore. To stop a task, you should call
:meth:`stop` method on it.

.. code-block:: python

    task = my_flow()
    task.interpret(CoreInterpreter(core_auth=('example', 'Welcome to Malevich!')))
    task.prepare()
    task.run()
    task.stop()


Results
-------

After the task is run, you may want to get the results of its execution. To do so, you should call :meth:`results` method
on it. The flow fetches results for objects you have returned in :code:`flow` decorated function. Results are a list
of :class:`malevich.models.results.base.BaseResult` instances. See `Results`_ section to learn more about how to work
with results.

.. code-block:: python

    task = my_flow()
    task.interpret(CoreInterpreter(core_auth=('example', 'Welcome to Malevich!')))
    task.prepare()
    task.run()
    results = task.results()


