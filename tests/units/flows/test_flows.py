from malevich import CoreInterpreter, SpaceInterpreter,flow
from malevich.models.flow_function import FlowFunction
from malevich.models.results.base import BaseResult
from malevich.testing import FlowTestEnv, FlowTestSuite, ImageDependency, SpaceDependency, SpaceDependencyOptions
import os

class TestUtility(FlowTestSuite):
  environment = FlowTestEnv(dependencies={
    'utility': ImageDependency(package_id='utility')
  })
  interpreter = CoreInterpreter(core_auth=('leo', 'pak'))

  @flow
  def my_flow():
      from malevich import collection, table
      from malevich.utility import add_column

      x = collection('test_collection', df=table([1, 2, 3], columns=['x']))
      return add_column(x)

  @staticmethod
  def on_result(flow, results):
      assert 'new_column' in results[0].get_df()
       

class TestManyToOne(FlowTestSuite):
    environment = FlowTestEnv(
        dependencies={
            "utility": ImageDependency(package_id="utility"),
            "scrape": ImageDependency(package_id="scrape")
        }
    )
    interpreter = CoreInterpreter(core_auth=('leo', 'pak'))

    @flow
    def many_to_one():
        from malevich import collection, config, table
        from malevich.utility import merge, rename

        col1 = collection(name="col1", df=table([[1, 2, 3, 4]], columns=['A', 'B', 'C', 'D']))
        col2 = collection(name="col2", df=table([[5, 6, 7, 8]], columns=['E', 'F', 'G', 'H']))
        col3 = collection(name="col3", df=table([[9, 10, 11, 12]], columns=['I', 'J', 'K', 'L']))
        return merge(rename(col1), rename(col2), rename(col3), config(how='cross'))

    @staticmethod
    def on_result(flow, results):
        data = results[0].get_df()
        assert len(data.columns) == 12
        assert len(data.index) == 1

        


class TestOneToMany(FlowTestSuite):
   environment = FlowTestEnv(dependencies={
      "utility": ImageDependency(package_id='utility'),
      "scrape": ImageDependency(package_id="scrape")
   })
   interpreter = CoreInterpreter(core_auth=('leo', 'pak'))

   @flow
   def test():
        from malevich import config, collection, table
        from malevich.utility import merge, add_column, rename
        
        col = collection(
            name='Test many to one',
            df=table(
                [[1, 2, 3, 4], [5, 6, 7, 8]],
                columns=['A', 'B', 'C', 'D']
            )
        )
        
        add = add_column(col, config(column='E', value=10))
        renamed = rename(col, config(D='Q')) 

        result = merge(add, renamed)

        return col, add, renamed, result
   
   @staticmethod
   def on_result(flow, results) -> None:
        col = results[0].get()
        add = results[1].get_df()
        renamed = results[2].get_df()
        result = results[3].get_df()
        
        assert len(col.index) == len(result.index)
        assert 'E' in add.columns
        assert 'Q' in renamed.columns
        for _, row in result.iterrows():
            assert row['E'] == 10
            assert row['Q'] == row['D']

class TestSpaceFlow(FlowTestSuite):
    environment = FlowTestEnv(
        dependencies={
            "utility": SpaceDependency(package_id="utility", options=SpaceDependencyOptions(reverse_id='utility')),
        }
    )
    interpreter = SpaceInterpreter(version_mode='default')

    @flow
    def space_flow():
        from malevich import collection, table, config, run
        from malevich.utility import rename, merge

        data = collection(name='test_data', df=table([1, 2, 3, 4, 5], columns=['test1']))
        return run(merge(run(rename(data), alias='pass'), run(rename(data, config(test1='test2')), alias='change')), alias="combine")

    @staticmethod
    def on_result(flow, results) -> None:
        data = results[0].get_df()
        for _, row in data.iterrows():
            assert row['test1'] == row['test2']

    