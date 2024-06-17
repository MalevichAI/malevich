from malevich.square import DF, processor, Context
import pandas as pd

@processor()
def simple_proc(df: DF, ctx: Context):
    res = []
    for d in df['data'].tolist():
        res.append(str(d) + " is simple!")
    return pd.DataFrame({'data': res})
