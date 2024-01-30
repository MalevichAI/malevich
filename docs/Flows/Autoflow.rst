=========
Autoflow
=========

As apps are defined as regular Python functions, we decided to use Python also 
to define flows. The main goal was to alleviate users from manually defining
dependencies between flow components. 

The idea is to turn processors of apps into *functions stubs* - empty functions
that copies the signature of the processor. This enables two important things:

1. The function stub gives the user a hint of what the processor expects as
   input and what it returns as output. It also copies docstrings. So function
   is self-contained and her purpose is clear.

2. As stub does not have any implementation, you are not required to install any dependencies
   that are necessary to run the processor. Once you installed :code:`malevich` package you
   can use a processor of any complexity (API calls, model inference, etc.) without installing
   extra packages.


To better understand what happens to apps when they are used in flows, consider the following processor:

.. code-block:: python

    from transformers import AutoTokenizer

    @processor()
    def tokenize(text: DF, ctx: Context) -> DF:
        """Tokenize texts using BERT tokenizer."""

        tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")
        # Text pre-processing
        text = ...
        # Actual tokenization
        tokens = ...
        # Return a dataframe with tokens
        return pd.DataFrame({
            "tokens": ...
        })


Once you install app, a function stub is created in the :code:`malevich` package. The stub
is a regular Python function that copies the signature of the processor and has no implementation:

.. code-block:: python

    from malevich._autoflow.function import autotrace,
   

    @autotrace
     def tokenize(text, config: dict = {}):
        """Tokenize texts using BERT tokenizer."""
        
        return OperationNode(...)


So, to define a flow with this processor, you do not need to install `transformers` package. Simply,
import the function stub and use it as a regular Python function:

.. code-block:: python

    from malevich.my_app import tokenize

    @flow()
    def tokenize_flow():
        texts = collection(...)
        # Calling tokenize function stub
        # will create an operation node
        tokens = tokenize(texts)

        return tokens

    task = tokenize_flow()
    task.interpret()

    # Remote execution
    print(task())


.. note::

    Such a simple design achieved by **Autoflow** - a special engine that enables dependency tracking. Whenever you
    call one of special Malevich functions: :code:`collection`, :code:`asset.file`, :code:`asset.mutltifile`, or
    a processor stub, you produce a special kind of entities - tracers. They are wrappers around arbitrary objects, which
    hold a reference to current plan of execution and advance it when you passed to other stubs.

To be precise, in the example above, :code:`texts` is a traced object, and when it is passed to function stub :code:`tokenize`,
a new dependency (texts → tokenize) is registred within the flow.


.. warning::

    Beware that :code:`tokens` has nothing to do with the actual result of the :code:`tokenize` processor. It is just a
    placeholder, that is used to define a dependency between :code:`texts` and :code:`tokenize`. The actual result of the
    flow retrieved by returning :code:`tokens` from the flow function, running the flow, and requesting results. See `Working with Results <Results.html>`_ for more details.
    