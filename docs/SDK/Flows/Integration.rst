=================
Flows Integration
=================

A flow can be easily installed in Malevich project and then be used as a
standalone function.

To install the flow, run the installation command:

.. code-block:: bash

    malevich install <reverse_id>

It will fetch all available versions of the flow and create flow stubs. The stub can be imported
from ``flows`` subpackage. Assuming the flow has been installed with the reverse ID ``my-flow``,

.. code-block:: python

    from malevich.flows import my_flow

The flow can be run as a standalone function:

.. code-block:: python

    my_flow.run()

The flow can also be run with arguments to supply the input data:

.. code-block:: python

    from malevich import table

    my_flow.run(input_data=table(...))

The arguments reflects the names of collections in the flow. Arguments for each
combination of branch and version can vary. The flow can be run with a specific
branch and version:

.. code-block:: python

    # A version on active branch
    my_flow.run(version='1.0.0', data=table(...))
    
    # An active version on a specific branch
    my_flow.run(branch='dev', data=table(...))

    # A specific version on a specific branch
    my_flow.run(branch='dev', version='1.0.0', data=table(...))



