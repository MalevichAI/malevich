import pandas as pd


def pd_to_json_schema(df: pd.DataFrame, format=False) -> dict:
    mappings = {
        'integer': 'number',
        'float': 'number',
        'boolean': 'boolean',
        'string': 'string',
        'object': 'string', # we will regret
        'datetime': 'string',
        'date': 'string',
        'datetime64[ns]': 'string',
        'category': 'string',
        'timedelta[ns]': 'string',
    }

    schema = {
        'type': 'object',
        'properties': {},
        'required': [],
    }

    for col in df.columns:
        col_type = df[col].dtype.name
        schema['properties'][col] = {
            'type': mappings.get(col_type, 'string'),
        }
        if format:
            if col_type == 'object':
                schema['properties'][col]['format'] = 'date-time'
            if col_type == 'datetime64[ns]':
                schema['properties'][col]['format'] = 'date-time'
            if col_type == 'date':
                schema['properties'][col]['format'] = 'date'
            if col_type == 'boolean':
                schema['properties'][col]['format'] = 'boolean'
            if col_type == 'integer':
                schema['properties'][col]['format'] = 'int64'
            if col_type == 'float':
                schema['properties'][col]['format'] = 'float64'

    return schema
