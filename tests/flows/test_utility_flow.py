from malevich.cli import app
from malevich import flow, collection, config

from ..fixtures.ghcr_package import ghcr_package
from ..fixtures.package import package

import pandas as pd

from .env import FlowTestEnv, TestingScope

@flow()
def utility_flow():
    from malevich.utility import add_column
    
    data = collection(
        name='data', 
        df=pd.DataFrame({
            'A': [0, 1, 2],
            'B': [1, 2, 3],
            'C': [2, 3, 4]
        }))
    
    return add_column(data, config=config(column='D', value=9, position=-1))
    
    
def test_utility(
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
        result: list[pd.DataFrame] = runner.full_test(utility_flow)[0].get()
        assert 'D' in result[0].data.columns, "Column 'D' is not in the DataFrame"
        assert 9 in result[0].data['D'].to_list()
        
    
    with FlowTestEnv(
        dependencies=[package], 
        scope=TestingScope.SPACE, 
    ) as runner:
        result: list[pd.DataFrame] = runner.full_test(utility_flow)[0].get()

        assert 'D' in result[0].columns, "Column 'D' is not in the DataFrame"
        assert 9 in result[0]['D'].to_list()
