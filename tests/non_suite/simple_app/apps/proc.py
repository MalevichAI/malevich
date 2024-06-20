from malevich.square import DF, processor, Context
import pandas as pd

@processor()
def simple_proc(df: DF, ctx: Context):
    res = []
    for d in df['data'].tolist():
        res.append(str(d) + " is simple!")
    return pd.DataFrame({'data': res})

@processor()
def split_in_two(df: DF, ctx: Context):
    lst = df['data'].to_list()
    if len(lst) == 1:
        return pd.DataFrame({'data': lst}), pd.DataFrame({'data': lst})
    else:
        return pd.DataFrame({'data': lst[:len(lst)//2]}), pd.DataFrame({'data': lst[:len(lst)//2:]})