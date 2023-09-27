import os
from pathlib import Path


def _write(file_path: Path, content: str) -> None:
    with open(file_path, 'w') as f:
        f.write(content)


def mimic_package(package_name: str, functions: list[str]) -> None:
    import malevich

    malevich_path = Path(malevich.__file__).parent
    os.makedirs(malevich_path / package_name, exist_ok=True)
    content = '\n\n'.join(functions)
    _write(malevich_path / package_name /\
        '__init__.py', f'from malevich.{package_name}._f import *')
    _write(malevich_path / package_name / '_f.py', content)


