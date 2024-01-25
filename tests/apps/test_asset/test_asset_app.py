import os

import pandas as pd
from malevich.square import processor, OBJ, obj, DF

@processor()
def print_asset(asset: OBJ):
    if os.path.isfile(asset.path):
        print(f"File {asset.path} exists")
        return pd.DataFrame({"file": [open(asset.path).read()]})
    elif os.path.isdir(asset.path):
        print(f"Folder {asset.path} exists")
        return pd.DataFrame({"files": os.listdir(asset.path)})
    else:
        raise Exception(f"Path {asset.path} is neither file nor folder")


@processor()
def return_asset(asset: OBJ):
    return asset


@processor()
def df_asset(asset: DF[obj]):
    path = str(asset.path.iloc[0])
    if os.path.isfile(path):
        print(f"File {path} exists")
        return pd.DataFrame({"file": [open(path).read()]})
    elif os.path.isdir(path):
        print(f"Folder {path} exists")
        return pd.DataFrame({"files": os.listdir(path)})
    else:
        raise Exception(f"Path {path} is neither file nor folder")
    
