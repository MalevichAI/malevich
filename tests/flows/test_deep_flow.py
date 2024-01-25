from malevich.cli import app
from malevich import flow, collection, config

from ..fixtures.ghcr_package import ghcr_package
from ..fixtures.package import package

import pandas as pd

from .env import FlowTestEnv, TestingScope

@flow()
def subflow_1(x):
    from malevich.utility import rename
    return rename(x, config={'A': 'AA', 'B': 'BB', 'C': 'CC'})

@flow()
def subflow_2(x):
    from malevich.utility import rename
    return rename(x)

@flow()
def subflow_3(x):
    from malevich.utility import merge, add_column
    return add_column(
        merge(subflow_2(x), subflow_1(subflow_2(x)), config={'how': 'inner', 'on': 'index'}),
        config={'column': 'D', 'value': 9, 'position': -1}
    )

@flow()
def deep_flow():
    data = collection(
        name='data', 
        df=pd.DataFrame({
            'A': [0, 1, 2],
            'B': [1, 2, 3],
            'C': [2, 3, 4]
        }))
    
    return subflow_3(data)
    
    
def test_deep_flow(
    package: str,
    ghcr_package: tuple[str, tuple[str, str]]
):
    tag, (user, password) = ghcr_package

    with FlowTestEnv(
        dependencies=[package], 
        scope=TestingScope.CORE, 
        install_args={
            'image_ref': tag,
            'image_auth_user': user,
            'image_auth_password': password,
        }
    ) as runner:
        result = runner.full_test(deep_flow)[0].get()

        assert 'D' in result[0].data.columns, "Column 'D' is not in the DataFrame"
        assert 'AA' in result[0].data.columns, "Column 'AA' is not in the DataFrame"
        assert 9 in result[0].data['D'].to_list()
        
    # with FlowTestEnv(
    #     dependencies=[package], 
    #     scope=TestingScope.SPACE, 
    # ) as runner:
    #     result = runner.full_test(deep_flow)[0].get()

    #     assert 'D' in result[0].columns, "Column 'D' is not in the DataFrame"
    #     assert 'AA' in result[0].columns, "Column 'AA' is not in the DataFrame"
    #     assert 9 in result[0]['D'].to_list()
        