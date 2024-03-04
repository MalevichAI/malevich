from malevich import CoreInterpreter, flow
from malevich.models.flow_function import FlowFunction
from malevich.models.results.base import BaseResult
from malevich.testing import FlowTestEnv, FlowTestSuite, ImageDependency

class TestAsset(FlowTestSuite):
    environment = FlowTestEnv(
        dependencies={
            "utility": ImageDependency(package_id="utility")
        }
    )
    interpreter = CoreInterpreter(core_auth=('leo', 'pak'))

    @flow
    def assets_one_file():
        from malevich import asset
        from malevich.utility import  get_links_to_files

        file = asset.file('tests/units/assets/file.txt')

        return get_links_to_files(file)
    
    @staticmethod
    def on_result(flow, results) -> None:
        import wget
        data = results[0].get_df()

        key =  data['path'].to_list()[0]

        wget.download(key, 'file_result.txt')
        file = open('tests/units/assets/file.txt').read()
        file1 = open('file_result.txt').read()

        assert file == file1
            

class TestMultipleAssets(FlowTestSuite):
    environment = FlowTestEnv(
        dependencies={
            "utility": ImageDependency(package_id="utility")
        }
    )
    interpreter = CoreInterpreter(core_auth=('leo', 'pak'))

    @flow
    def test_multiasset():
        from malevich import asset
        from malevich.utility import  get_links_to_files

        file = asset.multifile(name='malevich-test', files=['tests/units/assets/file.txt', 'tests/units/assets/file1.txt'])

        return get_links_to_files(file)
    
    @staticmethod
    def on_result(flow, results):
        import wget
        import hashlib

        df = results[0].get_df()
        assert len(df.index) == 2
        files = ['tests/units/assets/file.txt', 'tests/units/assets/file1.txt']
        for file, link in zip(files, df['path'].to_list()):
            path = wget.download(
                link,
                f"{hashlib.sha256(link.encode()).hexdigest()}.txt"
            )
            assert open(path).read() == open(file).read()
