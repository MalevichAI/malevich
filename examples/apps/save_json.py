import json
import os

import pandas as pd
from malevich.square import processor, scheme, DFS, DF, Doc, Context, M, APP_DIR
from typing import Any, Literal, List

@scheme()
class SaveJSONConfig:
    """A scheme which defines in which format to save the table and other save options"""
    orient: Literal['columns', 'index', 'records', 'split', 'table', 'values'] = 'columns' # How the table should be transformed to a dict
    prefix: str = '' # Save path prefix

@processor()
def save_to_json(dfs: DFS[M[DF]], ctx: Context[SaveJSONConfig]) -> tuple[DF, Doc]:
    """The processor gets the tables and saves them to JSON files.
    
    DFS[M[DF]] type means, that only one processor can connect to the current, but the number of tables it can pass is unlimited.
    
    For example, assume processors `kafka_reader` and `rabbit_reader` return different number of tables.
    
    Then the following code is valid:
    
    ```python
    tables_k = kafka_reader(...)
    
    files_k = save_to_json(tables_k)
    
    tables_r = rabbit_reader(...)
    
    files_r = save_to_json(tables_r)

    ```
    
    However, the next code block is invalid as 2 processors are trying to pass tables into the current:
    
    ```python
    tables_k = kafka_reader(...)
    
    tables_r = kafka_reader(...)
    
    files = save_to_json(tables_k, tables_r)
    ```
    
    The processor takes all tables from the input and saves to JSON files using app config's 'orient' field.
    The processor uses Context.share() method to make these files accessible for other processors.
    
    Thre processor returns result filepaths in both table and dictionary formats, so its return type is tuple[DF, Doc]
    """
    
    data:list[DF] = []

    for m in dfs:
        for df in m:
            data.append(df)
            
    result = []
    
    for i, df in enumerate(data):
        if ctx.app_cfg.get('prefix'):
            path = os.path.join(ctx.app_cfg.get('prefix'), f'{i}.json')
        else:
            path = f'{i}.json'
        
        with open(
            os.path.join(
                APP_DIR,
                path
            ),
            'w'
        ) as f:
            f.write(
                json.dumps(
                    df.to_dict(
                        orient=ctx.app_cfg.get['orient']
                    )
                )
            )
        ctx.share(path)
        result.append(path)
    
    return pd.DataFrame(result, columns=['path']), result