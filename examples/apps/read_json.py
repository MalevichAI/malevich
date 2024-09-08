import json

import pandas as pd
from malevich.square import processor, scheme, Context, OBJ, DFS, Docs
from typing import Literal

@scheme()
class ReadJSONConfig:
    """A scheme """
    output_format: Literal['table', 'dict'] = 'table'

@processor()
def read_json(paths: OBJ, ctx: Context)-> DFS|Docs:
    """T
    """
    
    output_type = ctx.app_cfg.get('output_format')
    
    result = []
    
    for path in paths.as_df['path'].to_list():
        data = json.load(open(ctx.get_share_path(path, 'rb')))
        
        if output_type == 'table':
            data = pd.DataFrame(data)
        
        result.append(data)
            
    return result