=====================
malevich.square.utils
=====================

..  automodule:: malevich.square.utils

   .. autoclass:: malevich.square.utils::Context
      :exclude-members: app_cfg
      :members: 
      :undoc-members:

      Context contains all the necessary information about the run 
      and the environment in which it is executed. Also, context provides 
      a number of auxiliary functions  for interacting with the environment, such as
      working with shared storage (:meth:`share`, :meth:`get_share_path`, :meth:`delete_share`), 
      dealing with common objects (:attr:`common`), 
      access to the key-value storage (:attr:`dag_key_value`), 
      and object storage (:attr:`object_storage`).


      Usage
      =====

      Context object is used implicitly in apps. It may
      be requested by including an argument with explicit
      type annotation in the function signature:

      .. code-block:: python

         from malevich.square import Context, processor

         @processor()
         def my_app(ctx: Context):
               pass


      =============  
      Example usage 
      =============

      Here is some frequently used examples of context usage.

      -------------
      Sharing files
      -------------

      During the run, each of apps in the run has its own isolated file system.
      To share files between apps, you can use :meth:`share` and :meth:`get_share_path` methods.

      .. code-block:: python

         from malevich.square import Context, processor, DF, APP_DIR

         @processor()
         def download_from_google_drive(
               links: DF['GoogleDriveLink'], 
               context: Context
         ):
               outputs = []
               for link in links.link.to_list():
                  # Dowload file from google drive
                  output_file = gdown.download(
                     link,
                     fuzzy=True,
                     quiet=True
                  )

                  # Get a file name
                  # (e.g. files/my_file.txt -> my_file.txt)
                  basename = os.path.basename(output_file)

                  # Copy file to shared directory
                  # (default is APP_DIR)
                  shutil.copyfile(
                     output_file,
                     os.path.join(
                           APP_DIR,
                           basename
                     )
                  )

                  # Save file name to
                  # pass it to the next app
                  outputs.append(
                     basename
                  )

                  # Ensure the file is shared
                  # and the next app can access it
                  # by the name, included into outputs
                  context.share(basename)

               return pd.DataFrame({
                  'filename': outputs
               })

      See the :meth:`share` method for more details. Explore the code
      at `GitHub <https://github.com/MalevichAI/malevich-library/tree/main/lib/src/drives/apps/download.py>`_.

      -----------------------                    
      Accessing shared files
      -----------------------

      To access shared files, you can use :meth:`get_share_path` method.

      .. code-block:: python

         from malevich.square import Context, processor, DF, APP_DIR

         import pandas as pd
         import os
         from rembg import remove

         def remove_background(
               images: DF['ImagePaths'], 
               context: Context
         ):
               outputs = []
               for img in images.path_to_image.to_list():
                  # A shared path (the one in data frame)
                  # is only a key to a real file. The real
                  # file is stored in a shared directory
                  # and can be accessed using `get_share_path`
                  img_file = context.get_share_path(
                     img, not_exist_ok=True
                  )

                  img_array = cv2.imread(img_file)
                  nobg = remove(
                     img_array
                  )

                  # add _nobg before extention
                  base, _ = os.path.splitext(img)
                  base += "_nobg" + '.png'
                  path = os.path.join(APP_DIR, _base)
                  cv2.imwrite(
                     path,
                     nobg
                  )

                  # Sharing the file to pass it to the next app
                  context.share(_base)
                  outputs.append(_base)

               return pd.DataFrame(outputs, columns=['no_background_image'])

      See the :meth:`share` method for more details. Explore the code
      at `GitHub <https://github.com/MalevichAI/malevich-library/tree/main/lib/src/media/apps/image/remove_background.py>`_.


      .. autoattribute:: malevich.square.utils::Context.app_cfg
         :no-index:

         Example:
         --------
         Assume, you have a processor that adds a column to a dataframe.
         You can configure the name of the column and its value using
         the app configuration:
         
         .. code-block:: python
         
            from malevich.square import DF, Any, Context, processor


            @processor()
            def add_column(df: DF[Any], context: Context):
                  # Using .get() method to have default values
                  column_name = context.app_cfg.get('column', 'new_column')
                  value = context.app_cfg.get('value', 'new_value')
                  position = context.app_cfg.get('position', 0)

                  # After configuration is parsed, we can add a column
                  # to the dataframe according to it

                  if position < 0:
                     position = len(df.columns) + position + 1

                  df.insert(position, column_name, value)

                  return df
         
         Source: `GitHub <https://github.com/MalevichAI/malevich-library/tree/main/lib/src/utility/apps/add/processor.py>`_.
         
         Metascript
         ----------
         When developing a flow in Metascript, you can pass the configuration
         using the :code:`config` parameter:
         
         .. code-block:: python
         
            from malevich import flow, collection
            from malevich.utility import add_column
            
            @flow()
            def my_flow():
                  data = collection('data.csv')
                  add_column(data, config={
                     'column': 'A',
                     'value': '10',
                     'position': -1
                  })
               
    
       
   .. autoclass:: malevich.square.utils::Context._ObjectStorage
      :members:
      :undoc-members:

   .. autoclass:: malevich.square.utils::Context._DagKeyValue
      :members:
      :undoc-members:

   .. autoclass:: malevich.square.utils::S3Helper
      :members:
      :undoc-members:
   
   .. autoclass:: malevich.square.utils::SmtpSender
      :members:
      :undoc-members:

   .. autofunction:: malevich.square.utils::to_df
   
   .. autofunction:: malevich.square.utils::from_df

   .. autofunction:: malevich.square.utils::to_binary

   .. autofunction:: malevich.square.utils::from_binary

   .. autoattribute:: malevich.square.utils::APP_DIR

      Working directory from which the app is run.
      Equivalent to :code:`os.getcwd()` from within the app.

   .. autoattribute:: malevich.square.utils::WORKDIR

      Directory into which the user code is copied during app construction.
