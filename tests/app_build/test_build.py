import os
from malevich import flow, collection, table, Core
from malevich.simple_app import simple_proc
import malevich_coretools as c
import pandas as pd

@flow()
def build_simple_flow():
    col = collection(name='buildTestCollection', alias='build1', df=table({'data': ['Malevich', 'C++']},), persistent=False)
    return simple_proc(col)

@flow()
def rebuild_simple_flow():
    col = collection(name='test_second_proc', alias='build2', df=table({'data': ['Java', 'Python']}), persistent=False)
    return simple_proc(col) 

def in_progress_proc(): # TODO: Finish the test
    task = Core(build_simple_flow, user=os.environ.get('CORE_USER'), access_key=os.environ.get('CORE_PASS'))
    task.prepare()
    task.run(with_logs=True, profile_mode='all')
    res = task.results()
    for r in res:
        assert (r.get_df() == pd.DataFrame({'data': ["Malevich is simple!", "C++ is simple!"]}))['data'].all(), "Incorrect proc execution"

def test_endpoint():
    task = Core(build_simple_flow, user=os.environ.get('CORE_USER'), access_key=os.environ.get('CORE_PASS'))
    endpoint = task.publish()
    first_hash = endpoint.hash
    first_tid = endpoint.taskId
    first_cid = endpoint.cfgId
    results = endpoint.run(overrides={
        'buildTestCollection': pd.DataFrame(
            {
                'data': ["Rule", "Test"]
            }
        )
    })

    for _, r in results.results.items():
        assert r == [[{'data': "Rule is simple!"}, {'data': "Test is simple!"}]], "Incorrect proc execution"

    task1 = Core(rebuild_simple_flow, user=os.environ.get('CORE_USER'), access_key=os.environ.get('CORE_PASS'))
    endpoint1 = task1.publish(hash=first_hash)
    assert endpoint1.hash == first_hash, "Endpoint Update failed" 
    assert endpoint1.taskId != first_tid
    
    results = endpoint1.run(overrides={'buildTestCollection': pd.DataFrame({'data': ['test', 'test2']})})

    for _, r in results.results.items():
        assert r == [[{'data': "test is simple!"}, {'data': "test2 is simple!"}]], "Incorrect proc execution"
    
    task1.stop()
    task.stop()
    c.delete_endpoint(hash=endpoint1.hash)

   # collections={'build.test.collection': 3caa8867-81d4-4039-b36f-c8c36d39a17e} different={} schemes_aliases={} msg_url=None init_apps_update={} app_settings=[AppSettings(taskId=02f497426c4e4950a24a7aee539aaa98-simple_proc-simple_proc-1, appId=02f497426c4e4950a24a7aee539aaa98-simple_proc-simple_proc-1, saveCollectionsName=['result-02f497426c4e4950a24a7aee539aaa98-simple_proc-1'])] app_cfg_extension={} email=None
   # collections={'build.test.collection1': 180be765-c4ec-471b-a175-3c8724d442e3} different={} schemes_aliases={} msg_url=None init_apps_update={} app_settings=[AppSettings(taskId=e1e38bf5d62341b79d2db9c71991ce74-simple_proc-simple_proc-2, appId=e1e38bf5d62341b79d2db9c71991ce74-simple_proc-simple_proc-2, saveCollectionsName=['result-e1e38bf5d62341b79d2db9c71991ce74-simple_proc-2'])] app_cfg_extension={} email=None