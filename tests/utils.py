import importlib
import os
import shutil

from malevich._utility.registry import Registry
from malevich.manifest import manf


def assert_installed_package(package_name: str, installer: str) -> tuple[str, str]:
    try:
        module_path = importlib.import_module(
            f'malevich.{package_name}'
        ).__file__

        stub_path = os.path.join(
            os.path.dirname(module_path), '__init__.py'
        )

    except ImportError:
        raise AssertionError(
            f'{package_name} is not importable'
        ) from ImportError

    assert os.path.exists(stub_path)
    assert manf.query(
        'dependencies',
        package_name
    ), 'Utility not registered in manifest'

    assert manf.query(
        'dependencies',
        package_name,
        'installer'
    ) == installer, 'Wrong installer'

    with open(stub_path) as stub_file:
        stub = stub_file.read()

        assert '__checksum' in stub, 'Checksum not found in stub'
        assert len(
            Registry().registry
        ) > 0, 'Package has not registered any operations'

    return module_path, stub_path


def wipe_installed_package(module_path: str, stub_path: str, package_name: str) -> None:
    shutil.rmtree(os.path.dirname(module_path))
    manf.remove('dependencies', package_name)

    assert not os.path.exists(stub_path), 'Stub not removed'
    assert not manf.query(
        'dependencies',
        package_name
    ), f'{package_name} still registered in manifest'


