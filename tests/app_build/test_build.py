import os
from malevich import flow, collection, table, Core
from malevich.simple_app import simple_proc
import pandas as pd

@flow()
def build_simple_flow():
    col = collection(name='build.test.collection', df=table({'data': ['Malevich', 'C++']}))
    return simple_proc(col)

def test_proc():
    task = Core(build_simple_flow, user=os.environ.get('CORE_USER'), access_key=os.environ.get('CORE_PASS'))
    task.prepare()
    task.run(with_logs=True, profile_mode='all')
    res = task.results()
    for r in res:
        assert (r.get_df() == pd.DataFrame({'data': ["Malevich is simple!", "C++ is simple!"]}))['data'].all(), "Incorrect proc execution"