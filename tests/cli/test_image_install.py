from typer.testing import CliRunner
from malevich.cli import app
from malevich.manifest import manf
# Importing fixtures
from ..fixtures.package import package
from ..fixtures.core_provider import CoreProvider, core_provider
from ..fixtures.ghcr_package import ghcr_package
from ..fixtures.runner import runner
from ..utils import assert_installed_package, wipe_installed_package


def test_default_image_install(runner: CliRunner, package: str) -> None:
    result = runner.invoke(app, ['install', package, '--using', 'image'])
    print(f'malevich install {package} --using image')

    assert result.exit_code == 0

    module_path, stub_path = assert_installed_package(package, 'image')

    wipe_installed_package(module_path, stub_path, package)


def test_host_image_install(runner: CliRunner, core_provider: CoreProvider, package: str) -> None:
    url = core_provider.get_endpoint()
    result = runner.invoke(
        app, ['install', package, '--using', 'image', '--with-args', f'core_host={url}'])
    print(
        f'malevich install {package} --using image --with-args core_host={url}')
    assert result.exit_code == 0

    module_path, stub_path = assert_installed_package(package, 'image')

    manf.query('dependencies', package, 'options', 'core_host') == url

    wipe_installed_package(module_path, stub_path, package)


def test_host_ghcr_image_install(
    runner: CliRunner,
    core_provider: CoreProvider,
    package: str,
    ghcr_package: tuple[str, tuple[str, str]]
) -> None:
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
    safe_cmd_ = [
        'install',
        package,
        '--using',
        'image',
        '--with-args',
        f'core_host={url},image_ref={tag},image_auth_user={user},image_auth_password=********',
    ]
    result = runner.invoke(
        app,
        cmd_,
    )
    print('malevich ' + ' '.join(safe_cmd_))
    assert result.exit_code == 0

    module_path, stub_path = assert_installed_package(package, 'image')

    manf.query('dependencies', package, 'options', 'core_host') == url
    manf.query('dependencies', package, 'options', 'image_ref') == tag
    manf.query('dependencies', package, 'options', 'image_auth_user') == user
    manf.query(
        'dependencies',
        package,
        'options',
        'image_auth_pass',
        resolve_secrets=True
    ) == password

    wipe_installed_package(module_path, stub_path, package)
