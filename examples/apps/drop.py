from malevich.square import DF, Context, processor, scheme
from typing import List

@scheme()
class DropColumns:
    """Defines which columns to delete"""
    columns: List[str] # Columns to delete

@processor()
def drop_columns(df: DF, ctx: Context[DropColumns]) -> DF:
    """The processor deletes columns from the table.
    
    The input table may contain any number of different columns with different values, so the input type is just 'DF'
    
    The processor takes the column names from Context app config and removes columns with these names from input table.
    
    The processor returns the table with removed columns, so its return type is 'DF'.  
    """
    return df.drop(ctx.app_cfg.get('columns') , axis=1)