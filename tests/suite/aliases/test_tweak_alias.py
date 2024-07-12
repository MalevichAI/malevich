from malevich.models.task.promised import PromisedTask
from malevich.models.flow_function import FlowFunction
from malevich.models.results.base import BaseResult
from malevich.testing import FlowTestEnv, FlowTestSuite, ImageDependency
from malevich import CoreInterpreter, flow
import os

class TestAlias(FlowTestSuite):
    environment = FlowTestEnv(
        dependencies={
            "utility": ImageDependency(package_id="utility")
        }
    )
    interpreter = CoreInterpreter((os.environ.get('CORE_USER'), os.environ.get('CORE_PASS')))

    @flow
    def tweak_aliases():
        from malevich import collection, table
        from malevich.utility import rename, merge, add_column

        data = collection(
            name='test_col1',
            alias='test1_collection',
            df=table(
                [[1,2,3,4,5]],
                columns=['A', 'B', 'C', 'D', 'E']
            )
        )

        data1 = collection(
            name='test_col2',
            alias='test2_collection',
            df=table(
                [[1,2,3,4,5]],
                columns=['X', 'B', 'C', 'D', 'Y']
            )
        )

        data2 = rename(data, A='F', alias='rename1')

        data3 = add_column(data1, column='Z', value=10, alias='add_column1')

        return merge(data2, data3, alias='merge1')
    
    @staticmethod
    def on_interpretation(task: PromisedTask) -> None:
        ops = task.get_operations()
        assert 'rename1' in ops
        assert 'add_column1' in ops
        assert 'merge1' in ops

        injs = task.get_injectables()
        inj_assert = []
        inj_values = [('test2_collection', 'test_col2'), ('test1_collection', 'test_col1')]
        
        for inj in injs:
            inj_assert.append((inj.get_inject_key(), inj.get_inject_data()))
        
        for val in inj_values:
            assert val in inj_assert