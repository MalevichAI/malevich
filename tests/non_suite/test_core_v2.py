from malevich import Core, collection, table, flow
from malevich.simple_app import simple_proc, split_in_two
from malevich.utility import rename, merge
import os

COREV2_HOST = os.environ.get("COREV2_HOST")
CORE_USER = os.environ.get("CORE_USER")
CORE_PASS = os.environ.get("CORE_PASS")


@flow
def my_flow():
    col1 = collection(
        name='v2_test_collection',
        df=table(
            {
                'data': [
                    'Hello', 'Malevich'
                ]
            }
        ),
        alias='core_v2_col'
    )
    return rename(
        simple_proc(col1),
        data='result',
        alias='rename_column'
    )


def test_default():
    task = Core(
        my_flow,
        core_host=COREV2_HOST,
        user=CORE_USER,
        access_key=CORE_PASS,
        use_v2=True
    )
    task.prepare()
    task.run(
        run_id='test010101',
        override={
            'core_v2_col': table(
                {
                    'data': ['Malevich', 'Bye']
                }
            )
        }
    )
    runid1 = 'test010101'
    runid2 = task.run(
        config_extension={
            'rename_column': {
                'data': 'data1'
            }
        }
    )
    assert runid1 != runid2
    
    res1 = task.results(runid1)[0].get_df()
    assert len(res1.columns) == 1 and 'result' in res1.columns
    assert res1['result'].to_list() == ['Malevich is simple!', 'Bye is simple!']

    res2 = task.results(runid2)[0].get_df()
    assert len(res2.columns) == 1 and 'data1' in res2.columns
    assert res2['data1'].to_list() == ['Hello is simple!', 'Malevich is simple!']

    task.stop()


@flow
def multileaf_test():
    col1 = collection(
        name='v2_test_collection',
        df=table(
            {
                'data': [
                    'Hello', 'Malevich'
                ]
            }
        ),
        alias='core_v2_col'
    )
    return simple_proc(col1), rename(col1, data='result')


def test_multileaf():
    task = Core(
        multileaf_test,
        core_host=COREV2_HOST,
        user=CORE_USER,
        access_key=CORE_PASS,
        use_v2=True
    )
    task.prepare()
    task.run()
    res = task.results()
    res1 = res[0].get_df()
    res2 = res[1].get_df()

    assert res1['data'].to_list() == ['Hello is simple!', 'Malevich is simple!']
    assert res2['result'].to_list() == ['Hello', 'Malevich']

    task.stop()


@flow
def split_test():
    col1 = collection(
        name='v2_test_collection',
        df=table(
            {
                'data': [
                    'Hello', 'Malevich'
                ]
            }
        ),
        alias='core_v2_col'
    )
    split = split_in_two(col1)
    return merge(rename(split[0], data='result1'), rename(split[1], data='result2'))

def test_split():
    task = Core(
        split_test,
        core_host=COREV2_HOST,
        user=CORE_USER,
        access_key=CORE_PASS,
        use_v2=True
    )
    task.prepare()
    task.run()
    res = task.results()[0].get_df()
    assert res.columns.to_list() == ['result1', 'result2']
    assert res['result1'].to_list() == ['Hello']
    assert res['result2'].to_list() == ['Malevich']
    