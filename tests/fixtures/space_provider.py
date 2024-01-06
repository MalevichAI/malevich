import os

from malevich_space.constants import DEV_SPACE_API_URL
from malevich_space.ops import SpaceOps
from malevich_space.schema import SpaceSetup
import pytest

from malevich.manifest import manf

from ..models import NotAvailableError


class SpaceProvider:
    def __init__(self, endpoint=None):
        if endpoint is None:
            endpoint = DEV_SPACE_API_URL

        self.endpoint = endpoint

    def is_available(self) -> tuple[bool, str]:
        user_ = os.environ.get('TESTS_SPACE_USER', None)
        password_ = os.environ.get('TESTS_SPACE_PASSWORD', None)

        if user_ is None or password_ is None:
            return False, None

        self.setup = SpaceSetup(
            api_url=self.endpoint,
            username=user_,
            password=password_,
        )

        try:
            self.ops = SpaceOps(self.setup)
        except Exception as e:
            print(f'Space is not available at {self.endpoint}: {e}')
            return False, None

        return True, self.ops

    def get_ops(self):
        available, ops = self.is_available()

        if not available:
            raise NotAvailableError('Space is not available')

        return ops

    def assert_manifest(self):
        available, _ = self.is_available()

        if not available:
            raise NotAvailableError('Space is not available')

        assert hasattr(self, 'setup'), 'Space setup not available'
        assert hasattr(self, 'ops'), 'Space Operations not available'

        setup_ = self.setup.model_copy()
        setup_.password = manf.put_secret('test_space_password', self.setup.password)

        manf.put('space', value=setup_.model_dump())

        assert manf.query('space'), 'Failed to put space setup in manifest'
        assert manf.query('space', 'api_url') == self.setup.api_url, 'Failed to put space api url in manifest'
        assert manf.query('space', 'username') == self.setup.username, 'Failed to put space username in manifest'
        assert manf.query('space', 'password', resolve_secrets=True) == self.setup.password, \
            'Failed to put space password in manifest'

        return setup_


@pytest.fixture
def space_provider() -> SpaceProvider:
    prov = SpaceProvider()
    prov.assert_manifest()
    yield prov


