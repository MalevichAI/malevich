import pandas as pd
from malevich.square import processor, scheme, Context, DF
from typing import List

@scheme()
class CreateColumns:
    """A scheme which defines columns for the table"""
    columns: List[str] # Table columns 


@processor()
def create_table(ctx: Context[CreateColumns])-> DF:
    """The processor creates a table with columns defined in Context app config.
    
    No processor can be connected to this one as it doesn't require any arguments except Context.
    
    The processor takes column names from 'columns' field of app config, creates a table with 1 row and these columns, and fill it with 'default value' string. 
    
    The pocessor returns a DF as a response, so its return type is 'DF'.
    """
    
    columns = ctx.app_cfg.get('columns')
    
    data = {}
    
    for c in columns:
        data[c] = ['default value']
    
    return pd.DataFrame(data)