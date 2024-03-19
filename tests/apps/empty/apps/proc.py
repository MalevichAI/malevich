from malevich.square import DF, processor, Context
import pandas as pd


@processor()
def get_empty(df: DF, ctx: Context):
    return pd.DataFrame([], columns=['empty'])