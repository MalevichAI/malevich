import json

import pandas as pd
from malevich.square import processor, scheme, Context, OBJ, DFS, Docs
from typing import Literal

@scheme()
class ReadJSONConfig:
    """A scheme which defines processor output formats"""
    output_format: Literal['table', 'dict', 'both'] = 'table'

@processor()
def read_json(paths: OBJ, ctx: Context[ReadJSONConfig])-> DFS|Docs|tuple[DFS, Docs]:
    """The processor reads .json files and returns data as tables/dictionaries.
    
    The processor input type is OBJ, which means that only filepaths might be passed to this processor.
    
    Depends on the app configuration, the processor may return tables or dictionaries or both of them, so the return processor type can be DFS, Docs or tuple(DFS, Docs). 
    """
    
    output_type = ctx.app_cfg.get('output_format')
    
    result = []
    
    if output_type == 'both':
        result = [[], []]
    
    for path in paths.as_df['path'].to_list():
        data = json.load(open(ctx.get_share_path(path, 'rb')))

        if output_type == 'both':
            result[0].append(pd.DataFrame(data))
            result[1].append(data)
        
        else:
            if output_type == 'table':
                data = pd.DataFrame(data)
            
            result.append(data)
            
    return result