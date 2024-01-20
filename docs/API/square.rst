================
Malevich Square
================

**Malevich Square** is a submodule of **Malevich** that provides tools for building 
your own `apps <../Apps/What_is_App.html>`_. Explore the submodule API below and check out
the `tutorial <../Apps/Building.html>`_ to learn how to build your own app on `Malevich <https://malevich.ai>`_


..  automodule:: malevich.square

   .. autoclass:: Context
      :members: 
      :undoc-members:
       
   .. autoclass:: malevich.square::Context._ObjectStorage
      :members:
      :undoc-members:

   .. autoclass:: malevich.square::Context._DagKeyValue
      :members:
      :undoc-members:

   .. autofunction:: malevich.square::to_df
   
   .. autofunction:: malevich.square::from_df

   .. autoattribute:: malevich.square::APP_DIR

      Working directory from which the app is run.
      Equivalent to :code:`os.getcwd()` from within the app.

   .. autoattribute:: malevich.square::WORKDIR

      Directory into which the user code is copied during app construction.
