==================
malevich._autoflow
==================

Malevich Autoflow is a dependency tracking engine allowing us to automatically
detect flows and build execution graphs from the order of function calls.

Autoflow is built on following ideas:

1. The part of code that is graph is built for is called within :class:`Flow <malevich._autoflow.flow.Flow>` context manager.
2. The functions that represent processor nodes are decorated with :func:`autotrace <malevich._autoflow.function.autotrace>` or :func:`autotrace <malevich._autoflow.function.sinktrace>` decorators.
3. Objects that are passed to the functions are wrapped with :class:`traced  <malevich._autoflow.function.traced>` class.

Usually, the engine is working under the hood and you don't need to interact with it directly. For example,
:func:`flow` function that is used to decorated flow definitions, is already doing all the magic using Autoflow engine.

However, if you want to build your own flow definition, you can use Autoflow engine directly.

.. automodule:: malevich._autoflow
    :members:

    .. automodule:: malevich._autoflow.function
        :members:

    .. automodule:: malevich._autoflow.flow
        :members:

    .. automodule:: malevich._autoflow.tracer
        :members:

    .. automodule:: malevich._autoflow.tree
        :members: