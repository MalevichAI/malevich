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
def return_op():
    from malevich.utility import add_column, rename
    
    data = collection(
        name='data', 
        df=pd.DataFrame({
            'A': [0, 1, 2],
            'B': [1, 2, 3],
            'C': [2, 3, 4]
        }))
    
    x = add_column(data, config=config(column='D', value=9, position=-1))
    y = subflow_1(x)
    return rename(y)

@flow()
def return_subflow():
    from malevich.utility import add_column, rename
    
    data = collection(
        name='data', 
        df=pd.DataFrame({
            'A': [0, 1, 2],
            'B': [1, 2, 3],
            'C': [2, 3, 4]
        }))
    
    x = add_column(data, config=config(column='D', value=9, position=-1))
    return subflow_1(x)
    
    
    
def test_simple_subflow_core(
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
        op_result = runner.full_test(return_op)
        subflow_result = runner.full_test(return_subflow)
        
        assert 'D' in op_result[0].columns, "Column 'D' is not in the DataFrame"
        assert 'AA' in op_result[0].columns, "Column 'AA' is not in the DataFrame"
        assert 9 in op_result[0]['D'].to_list()
        
        assert 'D' in subflow_result[0].columns, "Column 'D' is not in the DataFrame"
        assert 'AA' in subflow_result[0].columns, "Column 'AA' is not in the DataFrame"
        assert 9 in subflow_result[0]['D'].to_list()
        
        assert all(op_result[0].compare(subflow_result[0]).all().to_list()), "Results are not equal"


def test_simple_subflow_space(
    package: str,
    ghcr_package: tuple[str, tuple[str, str]]
):
    tag, (user, password) = ghcr_package

    with FlowTestEnv(
        dependencies=[package], 
        scope=TestingScope.SPACE, 
    ) as runner:
        op_result = runner.full_test(return_op)
        subflow_result = runner.full_test(return_subflow)
        
        assert 'D' in op_result[0].columns, "Column 'D' is not in the DataFrame"
        assert 'AA' in op_result[0].columns, "Column 'AA' is not in the DataFrame"
        assert 9 in op_result[0]['D'].to_list()
        
        assert 'D' in subflow_result[0].columns, "Column 'D' is not in the DataFrame"
        assert 'AA' in subflow_result[0].columns, "Column 'AA' is not in the DataFrame"
        assert 9 in subflow_result[0]['D'].to_list()
        
        assert all(op_result[0].compare(subflow_result[0]).all().to_list()), "Results are not equal"
    