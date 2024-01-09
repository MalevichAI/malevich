from malevich.cli import app
from malevich import flow, collection, config
from typer.testing import CliRunner

import pandas as pd

from ..fixtures.core_provider import CoreProvider, core_provider
from ..fixtures.runner import runner
from ..fixtures.package import package
from ..fixtures.ghcr_package import ghcr_package
from ..utils import assert_installed_package, wipe_installed_package

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
    runner: CliRunner,
    core_provider: CoreProvider,
    package: str,
    ghcr_package: tuple[str, tuple[str, str]]
):
    url = core_provider.get_endpoint()
    tag, (user, password) = ghcr_package
    cmd_ = [
        'install',
        package,
        '--using',
        'image',
        '--with-args',
        f'core_host={url},image_ref={tag},image_auth_user={user},image_auth_password={password}',
    ]
    result = runner.invoke( app, cmd_)
    
    assert result.exit_code == 0, "Installation failed"

    m_path, s_path = assert_installed_package(package, 'image')

    task = utility_flow()
    task.interpret(core_provider.get_interpreter())
    task.prepare(with_logs=True, profile_mode='all')
    task.run(with_logs=True, profile_mode='all')
    result: pd.DataFrame = task.results()[0]
    assert 'D' in result.columns, "Column 'D' is not in the DataFrame"
    assert 9 in result['D'].to_list()
    
    wipe_installed_package(m_path, s_path, package)