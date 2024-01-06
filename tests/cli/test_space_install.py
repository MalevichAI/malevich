from typer.testing import CliRunner
from malevich.cli import app
from malevich.manifest import manf
# Importing fixtures
from ..fixtures.package import package
from ..fixtures.space_provider import SpaceProvider, space_provider
from ..fixtures.runner import runner

from ..utils import assert_installed_package, wipe_installed_package


def _test_install(package) -> None:
    module_path, stub_path = assert_installed_package(package, 'space')

    assert 'reverse_id' in manf.query('dependencies', package, 'options')

    wipe_installed_package(module_path, stub_path, package)


def test_default_install(runner: CliRunner, package: str, space_provider: SpaceProvider) -> None:
    runner = CliRunner()
    result = runner.invoke(app, ['install', package])
    print(f'malevich install {package}')

    assert result.exit_code == 0

    _test_install(package)


def test_default_install(runner: CliRunner, package: str, space_provider: SpaceProvider) -> None:
    result = runner.invoke(app, ['install', package, '--using', 'space'])
    print(f'malevich install {package} --using space')

    assert result.exit_code == 0

    _test_install(package)
