===================
malevich.square.df
===================

:class:`DF`, :class:`DFS`, :class:`OBJ`, :class:`M` and :class:`Sink` are special types
used to denote a specification of units like processors in your apps. 

.. automodule:: malevich.square.df

    .. autoclass:: malevich.square.df.DF
        
        .. automethod:: malevich.square.df.DF.scheme_name

    .. class:: malevich.square.df.DFS

        Wrapper class for tabular data.
        
        DFS is a container for multiple DFs. It is used to denote an
        output of processors that return multiple data frames. 
        
        Each of the elements of DFS is also a :class:`DF` or :class:`DFS`.

        Usage
        =====
        
        DFS is primarily used to denote types of arguments of processors. There
        are couple of cases to consider:
        
        1. Explicit number of inputs
        -----------------------------
        
        Once you know the number of inputs, and their schemes, you can use DFS
        in the following way:
        
        .. code-block:: python
        
            from typing import Any
            from malevich.square import DFS, processor
            
            @processor()
            def my_processor(dfs: DFS["users", Any]):
                ...
                
        Here, we have one input argument (from either collection or previous app) that
        consists of two data frames. The first data frame has scheme "users",
        and the second data frame has an arbitrary scheme.
        
        2. Variable number of inputs
        -----------------------------
        
        You may also assume an unbouded number of inputs. In this case, you
        should use :class:`malevich.square.jls.M` together with DFS:
        
        .. code-block:: python
        
            from typing import Any
            from malevich.square import DFS, M, processor
            
            @processor()
            def process_tables(dfs: DFS[M["sql_tables"]]):
                ...
                
            @processor()
            def process_user_data(dfs: DFS["users", M[Any]]):
                ...
                
        Here, we have two processors. The first one accepts any number of data
        frames with scheme "sql_tables". The second one accepts one data frame
        with scheme "users", and any number of data frames with arbitrary schemes
        as one argument.
        
        .. note::
        
            When iterating over argument of type :code:`DFS[M["sql_tables"]]`, it will
            contain exactly one element of type DFS, which will consist of a number
            of data frames with scheme "sql_tables". 
            
            When iterating over argument of type :code:`DFS["users", M[Any]]`,
            the first element will be of type DF, and the second element will be
            of type DFS, consisting of data frames with arbitrary schemes.

        .. automethod:: malevich.square.df.DFS.__getitem__
        .. automethod:: malevich.square.df.DFS.__iter__
        .. automethod:: malevich.square.df.DFS.__len__

    .. autoclass:: malevich.square.df.OBJ
        :members:

    .. autoclass:: malevich.square.df.M
        :members:
        
   .. class:: Sink
    
        Wrapper class to denote a specific type inputs to processor
        
        Normally, each argument in processor function signature corresponds to
        exactly one output of the previous processor or exactly one collection.
        
        To denote a processor that is able to accept a variable number of inputs,
        you should use this class.
        
        .. code-block:: python
        
            from typing import Any
            from malevich.square import Sink, DFS, M, processor
            
            @processor()
            def merge_tables_sink(dfs: Sink["sql_tables"]):
                pass
                
            @processor()
            def merge_tables_dfs(dfs: DFS[M["sql_tables"]]):
                pass
                
        Here, we have two processors. :code:`Sink[schema]` is
        equivalent to :code:`List[DFS[M[schema]]]`. 
        
        The difference between two processors lies in the fact that
        the first one can be connected to any number of processors
        that return data frames with scheme "sql_tables", while the
        second one can be connected to exactly one processor that
        returns any number of data frames with scheme "sql_tables".
        
        See the difference visually:
        
        .. mermaid::
        
            graph TD
                C1[Prev. processor] -->|table_1, table_2| B[merge_tables_dfs]
                C2[Collection 1] -->|table_1| A[merge_tables_sink]
                C3[Collection 2] -->|table_2| A[merge_tables_sink]
                C4[Prev. processor] -->|table_3, table_4| A[merge_tables_sink]
                
        In this case, in function :code:`merge_tables_sink`, accessing
        :code:`dfs[0]` will return a :class:`DFS` object consisting of
        a single data frame with scheme "sql_tables", but accessing
        :code:`dfs[2]` will return a :class:`DFS` with two data frames
        with scheme "sql_tables" inside. 
        
        In case of :code:`merge_tables_dfs`, accessing :code:`dfs[0]`
        will return a :class:`DFS` object consisting of a two data frames
        with scheme "sql_tables". There is no way to connect more than
        one processor to :code:`merge_tables_dfs`. 
        
        .. note::
        
            In case there are other arguments in processor
            with :code:`Sink` argument, they will be mapped
            to non-sink arguments and Sink will greedily collect the rest. 

            In other words, first and last inputs will be connected to non-Sink arguments 
            (if there are any), and the rest will be included into Sink.

            .. code-block:: python3

                @processor()
                def merge_tables_sink(
                    table_1: DF, 
                    dfs: Sink["sql_tables"]
                    table_2: DF,
                ):
                    pass

            .. mermaid::

                graph TD
                    C1[App 1] -->|table_1| A[merge_tables_sink]
                    C2[Collection 1] -->|table_2| A[merge_tables_sink]
                    C4[App 2] -->|table_4, table_5| A[merge_tables_sink]
                    C3[Collection 2] -->|table_3| A[merge_tables_sink]
                
            Consider the example above. In this case, :code:`table_1` will come
            from :code:`App 1`, :code:`table_3` will come from :code:`Collection 2`,
            and the rest of input data frames (:code:`table_2`, :code:`table_4` and :code:`table_5`)
            will be included into :code:`dfs` argument. Accessing :code:`dfs[0]` will
            return a :class:`DFS` object consisting of a single data frame (:code:`table_2`),
            but accessing :code:`dfs[1]` will return a :class:`DFS` with two data frames (:code:`table_4` and :code:`table_5`).


