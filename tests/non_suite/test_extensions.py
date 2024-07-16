import os

import pytest
from malevich import Core, flow, table, collection
from malevich.utility import locs, get_links_to_files

@flow
def extension_flow_test():
    col1 = collection(
        'test_extension_col',
        df=table(
            {
                'data': ['123', '321'],
                'value': ['111', '000']
            }
        )
    )

    return locs(col1, alias='get_data', column='data')

def test_extension():
    task = Core(
        extension_flow_test,
        user=os.environ.get('CORE_USER'),
        access_key=os.environ.get('CORE_PASS')
    )
    task.prepare()
    
    with pytest.raises(ValueError):
        task.run(
            config_extension={
                'get_data': ['hello', 'world']
            }
        )
    
    with pytest.raises(ValueError):
        task.run(
            config_extension={
                'get_data': get_links_to_files.config(expiration=120)
            }
        )

    task.run(
        config_extension={
            'get_data': locs.config(column='value')
        }
    )
    res = task.results()[0].get_df()
    assert len(res.columns) == 1 and 'value' in res.columns, "Invalid column"
    assert res['value'].tolist() == [111, 000]

    task.stop()