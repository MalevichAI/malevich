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
        from malevich import config, collection, table
        from malevich.scrape import scrape_aliexpress
        from malevich.utility import merge, subset
        
        ali = collection(
            name='ali_link',
            df=table(
                ['https://aliexpress.ru/item/1005006097314769.html'],
                columns=['aliexpress_link']
            )
        )

        scrape = scrape_aliexpress(ali)

        text = subset(scrape, config(expr='0'))
        images = subset(scrape, config(expr='1'))
        props = subset(scrape, config(expr='2')) 
        cards = subset(scrape, config(expr='3'))
        errors = subset(scrape, config(expr='4'))
        

        text_errors = merge(text, props, config(how='cross'))

        meta = merge(images, cards, config(how='cross'))

        mext = merge(text_errors, meta, errors, config(how='cross'))

        return text, images, props, cards, errors

    @staticmethod
    def on_result(flow, results):
        assert len(results) == 5

        text = results[0].get_df()
        images = results[1].get_df()
        props = results[2].get_df()
        cards = results[3].get_df()
        errors = results[4].get_df()

        if errors.empty:
            assert 'text' in text.columns
            assert 'name' in props.columns
            assert 'value' in props.columns

            assert 'image' in images.columns
            assert 'name' in cards.columns
            assert 'value' in cards.columns
        else:
            assert 'error' in errors.columns

class TestOneToMany(FlowTestSuite):
   environment = FlowTestEnv(dependencies={
      "utility": ImageDependency(package_id='utility'),
      "scrape": ImageDependency(package_id="scrape")
   })
   interpreter = CoreInterpreter(core_auth=('leo', 'pak'))

   @flow
   def test():
        from malevich import config, collection, table
        from malevich.scrape import scrape_aliexpress
        from malevich.utility import s3_download_files_auto, get_links_to_files

        collect = collection(
            name='link',
            df = table([
                'testfile.txt'
            ], columns=['s3_key'])
        )
        
        ali = collection(
            name='ali_link',
            df=table(
                ['https://aliexpress.ru/item/1005003680742556.html'],
                columns=['aliexpress_link']
            )
        )

        files = s3_download_files_auto(
            collect,
            config(
                aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
                bucket_name='malevich-pytest'
            )
        )

        return get_links_to_files(files), files, scrape_aliexpress(ali)
   
   @staticmethod
   def on_result(flow, results) -> None:
       assert len(results) == 3
       assert len(results[2].get_dfs()) == 5




class TestSpaceFlow(FlowTestSuite):
    environment = FlowTestEnv(
        dependencies={
            "utility": SpaceDependency(package_id="utility", options=SpaceDependencyOptions(reverse_id='utility')),
        }
    )
    interpreter = SpaceInterpreter()

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
