import os
from hashlib import sha256
from pathlib import Path


def mimic_package(package: str, metascript: str) -> tuple[bool, str]:
    # Creates a package with the given name and metascript
    # to be available for import as malevic.<package>
    import importlib

    import malevich

    checksum = sha256(metascript.encode()).hexdigest()


    package_path = Path(malevich.__file__).parent / package
    os.makedirs(package_path, exist_ok=True)

    with open(package_path / "__init__.py", "w") as f:
        f.write(f"__checksum = {checksum}\n")
        f.write(metascript)

    try:
        __pkg = importlib.import_module(f"malevich.{package}")
        assert getattr(__pkg, "__checksum") == checksum
    except (ImportError, AssertionError):
        return False, checksum
    return True, checksum
