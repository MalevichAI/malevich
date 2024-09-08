from malevich.square import Sink, M, DFS, Context, processor, Doc, DF
from typing import Any, List, Union

@processor()
def group(dfs: Sink[DFS[M[Any]]], ctx: Context)-> DFS:
    """The processor gets all tables and dictionaries and return them as one structure.
    
    dfs type Sink[DFS[M[Any]]] means, that any number of processors may pass any number of tables and dictionaries to this processor.
    
    For example, assume processors `kafka_reader` and `rabbit_reader` return different number of tables and dictionaries.
    
    Then the following code is valid:
    
    ```python
    tables_k = kafka_reader(...)
    tables_r = rabbit_reader(...)
    
    files_r = group(tables_k, tables_r)

    ```
    
    The processor returns a list of tabels and dictionaries, so its return type is DFS.
    """
    res = []
    for m in dfs:
        for df_s in m:
            for df in df_s:
                res.append(df)
    return res