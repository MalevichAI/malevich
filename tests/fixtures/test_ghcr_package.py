import os

import pytest

DEFAULT_TEST_REPO = 'ghcr.io/malevichai/test'


@pytest.fixture
def test_asset_package() -> tuple[str, tuple[str, str]]:  # (tag, (user, password)
    tag_ = os.getenv('TEST_GHCR_PACKAGE_TAG', DEFAULT_TEST_REPO)
    pass_ = os.getenv('TEST_GHCR_PACKAGE_PASSWORD', None)

    if not pass_:
        raise ValueError('TEST_GHCR_PACKAGE_PASSWORD is not set')

    return f'{tag_}:test_asset', ('USERNAME', pass_)



@pytest.fixture
def test_utility_package() -> tuple[str, tuple[str, str]]:  # (tag, (user, password)
    pass_ = os.getenv('TEST_GHCR_PACKAGE_PASSWORD', None)

    if not pass_:
        raise ValueError('TEST_GHCR_PACKAGE_PASSWORD is not set')

    return 'ghcr.io/malevichai/utility-test:latest', ('USERNAME', pass_)
