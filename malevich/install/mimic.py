"""
Provides functionality for creating a package with the given name and contents.
"""


import importlib
import os
from hashlib import sha256
from pathlib import Path

import malevich


def mimic_package(package: str, metascript: str) -> tuple[bool, str]:
    """
    Create a package with the given name and contents
    Args:
        package (str): The name of the package to create.
        metascript (str): The contents of the package.

    Returns:
        A tuple containing a boolean indicating whether the package was
        created successfully, and the checksum of the package.
    """
    # Calculate the checksum of the package contents
    checksum = sha256(metascript.encode()).hexdigest()
    package = package

    # Create the package directory
    # The package will be located at <python>/malevich/<package>
    package_path = Path(malevich.__file__).parent / package
    # Create the package directory if it doesn't exist
    os.makedirs(package_path, exist_ok=True)

    # Create the package __init__.py file
    with open(package_path / "__init__.py", "w") as f:
        # Write the metascript to the file
        f.write(metascript)
        # Write the checksum to the file
        f.write(f"\n__checksum = '{checksum}'\n")

    # Post-hoc check to make sure the package was created successfully
    try:
        # Import the package
        __pkg = importlib.import_module(f"malevich.{package}")
        # Assert that the checksum of the package is the same as the checksum
        assert getattr(__pkg, "__checksum") == checksum, \
            "Package checksum does not match metascript checksum. "
    except (ImportError, AssertionError):
        return False, checksum
    return True, checksum
