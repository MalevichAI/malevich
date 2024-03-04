from malevich.models.flow_function import FlowFunction
from malevich.models.results.base import BaseResult
from malevich.testing import FlowTestEnv, FlowTestSuite, ImageDependency
from malevich import CoreInterpreter, flow

class TestAlias(FlowTestSuite):
    environment = FlowTestEnv(
        dependencies={
            "utility": ImageDependency(package_id="utility")
        }
    )
    interpreter = CoreInterpreter(('leo', 'pak'))

    @flow
    def tweak_aliases():
        from malevich import collection, table, run, config
        from malevich.utility import rename, merge, add_column

        data = collection(
            name='test_col1',
            df=table(
                [1,2,3,4,5],
                columns=['A', 'B', 'C', 'D', 'E']
            )
        )

        data1 = run(rename(data, config(A='F')), alias='rename1')

        data2 = run(add_column(data, config(column='Z', value=10)), alias='add_column1')

        return run(merge(data, data1, data2), alias='merge1')
    
    @staticmethod
    def on_result(flow: PromisedTask, results) -> None:
        flow.get_operations()