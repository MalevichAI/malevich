import os

import pytest

DEFAULT_TEST_PACKAGE = 'utility'


@pytest.fixture
def package() -> str:
    return os.getenv('TEST_PACKAGE_NAME', DEFAULT_TEST_PACKAGE)
