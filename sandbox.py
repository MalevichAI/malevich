# # from malevich.core_api import EndpointOverride, UserConfig
# # from malevich.utility import add_column, rename


# # @flow
# # def f():
# #     """My test flow"""
# #     x = collection('x', df=table([1, 2, 3], columns=['x']))
# #     return add_column(run(rename(x), alias='rename_1'))


# # x = f()
# # x.interpret(CoreInterpreter(core_auth=('leo', 'pak')))
# # y= x.publish(capture_results='all')
# # # print(x.publish().get_url())
# # # print(x.publish(capture_results=['rename_1']).get_url())
# # # # endpoint.get_url()
# # y.run(endpoint_override=EndpointOverride(cfg=UserConfig(rawMapCollections={'x': [{'x': 10}])))


# # from malevich import CoreInterpreter, flow
# # from malevich.testing import FlowTestEnv, FlowTestSuite, ImageDependency


# # class TestSuite(FlowTestSuite):
# #   environment = FlowTestEnv(dependencies={
# #     'utility': ImageDependency(package_id='utility')
# #   })
# #   interpreter = CoreInterpreter(core_auth=('leo', 'pak'))

# #   @flow
# #   def my_flow():
# #       from malevich import collection, table
# #       from malevich.utility import add_column

# #       x = collection('test_collection', df=table([1, 2, 3], columns=['x']))
# #       return add_column(x)


# from malevich import CoreInterpreter, flow
# # from malevich.testing import FlowTestEnv, FlowTestSuite, ImageDependency
# # 0

# # class TestSuite(FlowTestSuite):
# #   environment = FlowTestEnv(dependencies={
# #     'utility': ImageDependency(package_id='utility')
# #   })
# #   interpreter = CoreInterpreter(core_auth=('leo', 'pak'))

# #   @flow
# #   def my_flow():
# #       from malevich import collection, table
# #       from malevich.utility import add_column

# #       x = collection('test_collection', df=table([1, 2, 3], columns=['x']))
# #       return add_column(x)

# #   @staticmethod
# #   def on_result(flow, results):
# #       assert 'new_column' in results[0].get_df()
# #       print(results[0].get_df())


# @flow 
# def my_flow():
#     from malevich import table, asset
#     from malevich.utility import  get_links_to_files, s3_save_files_auto

#     file = asset.file('/home/teexone/malevich/meta/setup.py')

#     return get_links_to_files(file)


#   # def on_result(flow, results):
#   #     assert 'new_column' in results[0].get_df()


# # from malevich import CoreInterpreter, collection, flow, run, table
# # from malevich.utility import add_column


# # @flow
# # def my_flow():
# #     x = collection('test_collection', df=table([1, 2, 3], columns=['x']))
# #     return add_column(x)


# f = my_flow()
# f.interpret(CoreInterpreter(core_auth=('leo', 'pak')))
# f.prepare()
# f.run()
# f.stop()
# print(f.results()[0].get())

from malevich import flow, CoreInterpreter, VersionMode, collection
from malevich.utility import add_column
from malevich.core_api import create_endpoint

# create_endpoint()
@flow(reverse_id='fafaf')
def x():
  y = collection('y')
  return add_column(y)

interpreter = CoreInterpreter(core_auth=('leo', 'pak'))
y = x()
y.interpret(interpreter)
# print(y.results(prepare=True))
print(y.publish().get_url())
# y.run()
# y.stop()