import importlib
import os
import shutil

from malevich._utility.registry import Registry
from malevich.manifest import manf


def assert_installed_package(package_name: str, installer: str) -> tuple[str, str]:
    try:
        module_path = os.path.dirname(
            importlib.import_module(
                'malevich'
            ).__file__
        )
        for _, dirs, _ in os.walk(module_path):
            if package_name in dirs:
                module_path = os.path.join(module_path, package_name)
                for _, _, files in os.walk(module_path):
                    if 'F.py' in files:
                        stub_path = os.path.join(module_path, 'F.py')
                        break
                    else:
                        raise ImportError
                break
            else:
                raise ImportError
        importlib.import_module('malevich.' + package_name + ".F")
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

    assert len(
        Registry().registry
    ) > 0, 'Package has not registered any operations'

    return module_path, stub_path


def wipe_installed_package(module_path: str, stub_path: str, package_name: str) -> None:
    shutil.rmtree(os.path.dirname(stub_path))
    manf.remove('dependencies', package_name)

    assert not os.path.exists(stub_path), 'Stub not removed'
    assert not manf.query(
        'dependencies',
        package_name
    ), f'{package_name} still registered in manifest'


