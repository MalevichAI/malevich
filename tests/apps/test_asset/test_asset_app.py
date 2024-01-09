import os

import pandas as pd
from malevich.square import processor, OBJ

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
