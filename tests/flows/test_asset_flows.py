import tempfile, os, shutil
from malevich import flow, asset

from ..fixtures.test_ghcr_package import test_asset_package, test_utility_package
from .env import FlowTestEnv, TestingScope

t_file = tempfile.NamedTemporaryFile()
t_dir = tempfile.TemporaryDirectory()

@flow()
def test_asset_flow_file():
    from malevich.utility import merge
    from malevich.test_asset import print_asset, return_asset, df_asset
    file = asset.file(t_file.name)
    
    asset_app = print_asset(return_asset(file))
    df_app = df_asset(return_asset(file))
    
    return asset_app, df_app, merge(asset_app, df_app, config={'how': 'inner', 'on': 'index'})


def test_asset_flow(
    test_utility_package: tuple[str, tuple[str, str]],
    test_asset_package: tuple[str, tuple[str, str]]
):
    utility_tag, (utility_user, utility_password) = test_utility_package
    tag, (user, password) = test_asset_package
    with FlowTestEnv(
        dependencies=['test_asset', 'utility'],
        install_args=[
            {
                'image_ref': tag,
                'image_auth_user': user,
                'image_auth_password': password,
            },
            {
                'image_ref': utility_tag,
                'image_auth_user': utility_user,
                'image_auth_password': utility_password,
            }
        ],
        scope=TestingScope.CORE
    ) as runner:
        with open(t_file.name, 'w') as f:
            f.write('Hello from Malevich!')

        df1, df2, _ = runner.full_test(test_asset_flow_file)
        assert df1.equals(df2), 'DataFrames are not equal'
        
        os.remove(t_file.name)
        shutil.rmtree(t_dir.name)