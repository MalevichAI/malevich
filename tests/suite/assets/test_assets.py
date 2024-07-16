from malevich import CoreInterpreter, flow
from malevich.models.flow_function import FlowFunction
from malevich.models.results.base import BaseResult
from malevich.testing import FlowTestEnv, FlowTestSuite, ImageDependency
import os

class TestAsset(FlowTestSuite):
    environment = FlowTestEnv(
        dependencies={
            "utility": ImageDependency(package_id="utility")
        }
    )
    interpreter = CoreInterpreter(core_auth=(os.environ.get('CORE_USER'), os.environ.get('CORE_PASS')))

    @flow
    def assets_one_file():
        from malevich import asset
        from malevich.utility import get_links_to_files

        file = asset.from_files(reverse_id='test_asset', file='tests/suite/assets/file.txt')

        return get_links_to_files(file)
    
    @staticmethod
    def on_result(flow, results) -> None:
        import wget
        data = results[0].get_df()

        key =  data['path'].to_list()[0]

        wget.download(key, 'file_result.txt')
        file = open('tests/suite/assets/file.txt').read()
        file1 = open('file_result.txt').read()

        assert file == file1
            

class TestMultipleAssets(FlowTestSuite):
    environment = FlowTestEnv(
        dependencies={
            "utility": ImageDependency(package_id="utility")
        }
    )
    interpreter = CoreInterpreter(core_auth=(os.environ.get('CORE_USER'), os.environ.get('CORE_PASS')))

    @flow
    def test_multiasset():
        from malevich import asset
        from malevich.utility import  get_links_to_files

        file = asset.from_files(reverse_id='malevich_test_assets', remote_path='test/a', files=['tests/suite/assets/file.txt', 'tests/suite/assets/file1.txt'])

        return get_links_to_files(file)
    
    @staticmethod
    def on_result(flow, results):
        import wget
        import hashlib

        df = results[0].get_df()
        assert len(df.index) == 2
        files = ['tests/suite/assets/file.txt', 'tests/suite/assets/file1.txt']
        for file, link in zip(files, df['path'].to_list()):
            path = wget.download(
                link,
                f"{hashlib.sha256(link.encode()).hexdigest()}.txt"
            )
            assert open(path).read() == open(file).read()
