import pandas as pd
from pandas.io.json import build_table_schema


def pd_to_json_schema(df: pd.DataFrame, format=False) -> dict:
    df = df.infer_objects()
    table_schema = build_table_schema(df)

    schema = {
        'type': 'object',
        'properties': {},
        'required': [],
    }

    for t in table_schema['fields']:
        if t['name'] == 'index':
            continue

        schema['properties'][t['name']] = {
            'type': t['type']
        }


    return schema
