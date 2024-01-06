import os

import pytest

DEFAULT_PACKAGE_TAG = 'ghcr.io/malevichai/utility-test'


@pytest.fixture
def ghcr_package() -> tuple[str, tuple[str, str]]:  # (tag, (user, password)
    tag_ = os.getenv('TEST_GHCR_PACKAGE_TAG', DEFAULT_PACKAGE_TAG)
    pass_ = os.getenv('TEST_GHCR_PACKAGE_PASSWORD', None)

    if not pass_:
        raise ValueError('TEST_PACKAGE_PASSWORD is not set')

    return tag_, ('USERNAME', pass_)
