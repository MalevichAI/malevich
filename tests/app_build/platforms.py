import pandas as pd
from malevich import Space, Core, flow, collection, table
from malevich.simple_app import simple_proc
from malevich.utility import rename

import os

@flow(reverse_id='malevich_platform_test_space')
def platform_flow():
    col = collection(
        'platform_test_col',
        df=pd.DataFrame({'data': ['Space', 'Core']})
    )
    return rename(col, config={'data': 'result'})

@flow(reverse_id='malevich_platform_test_core')
def platform_core_flow():
    col = collection(
        'platform_test_core_col',
        df=pd.DataFrame({'data': ['Space', 'Core']})
    )
    return simple_proc(col)

def test_space_platform():
    task = Space(platform_flow)
    task.prepare()
    task.run()
    res = task.results()[0].get_df()
    assert len(res.columns) == 1 and res.columns[0] == 'result'
    task1 = Space(
        reverse_id='malevich_platform_test_space',
        force_attach=True,
        deployment_id=task.state.aux.task_id
    )
    assert task.state.aux.task_id == task1.state.aux.task_id
    task1.run()
    res = task1.results()[0].get_df()
    assert len(res.columns) == 1 and res.columns[0] == 'result'
    task2 = Space(
        reverse_id='malevich_platform_test_space',
        force_attach=True,
        attach_to_last=True
    )
    assert task.state.aux.task_id == task2.state.aux.task_id
    task2.run()
    res = task2.results()[0].get_df()
    assert len(res.columns) == 1 and res.columns[0] == 'result'
    task1.stop()
    task2.stop()

def test_core_platform():
    task = Core(platform_core_flow, os.environ.get('CORE_USER'), access_key=os.environ.get('CORE_PASS'))
    task.prepare()
    task.run()
    res = task.results()[0].get_df()
    assert res['data'].tolist() == ['Space is simple!', 'Core is simple!']
    task.stop()