import os
import signal
import uuid

import malevich_coretools as mct
import pytest

from malevich.constants import DEFAULT_CORE_HOST
from malevich.interpreter.core import CoreInterpreter

from ..models import NotAvailableError


class CoreProvider:
    def __init__(self, endpoints=None) -> None:
        if endpoints is None:
            endpoints = [DEFAULT_CORE_HOST]

        self.endpoints = endpoints

    def is_available(self) -> tuple[bool, str]:
        excps_ = []
        for endpoint in self.endpoints:
            try:
                mct.update_core_credentials(None, None)
                mct.set_host_port(endpoint)
                mct.ping()
                return True, endpoint
            except Exception as e:
                excps_.append((endpoint, e,))
                pass
        for url_, e_ in excps_:
            print(f'Core is not available at {url_}: {e_}')
        return False, None

    def assert_sa(self) -> tuple[str | None, str | None, str]:
        available, url = self.is_available()

        if not available:
            raise NotAvailableError('Core is not available')

        user_ = os.environ.get('TESTS_CORE_USER', None)
        password_ = os.environ.get('TESTS_CORE_PASSWORD', None)

        if user_ is None:
            if hasattr(self, '__user'):
                user_ = self.__user
                password_ = self.__password
            else:
                user_ = str(uuid.uuid4().hex) + '-malevich-tests'
                password_ = str(uuid.uuid4().hex)

        try:
            mct.create_user(auth=(user_, password_), conn_url=url)
        except Exception:
            pass

        self.__user = user_
        self.__password = password_
        self.__host = url

        return user_, password_, url

    def get_interpreter(self) -> CoreInterpreter:
        user_, password_, url = self.assert_sa()

        return CoreInterpreter(
            (user_, password_), url
        )

    def get_endpoint(self) -> str:
        _, _, url = self.assert_sa()

        return url

    def prune(self):
        if hasattr(self, '__user'):
            try:
                mct.delete_user(
                    auth=(self.__user, self.__password),
                    conn_url=self.__host
                )
            except:
                pass

            self.__user = None
            self.__password = None
            self.__host = None

    def __enter__(self) -> tuple[str | None, str | None, str]:
        self.intr_ = self.assert_sa()

        signal.signal(signal.SIGINT, self.prune)
        signal.signal(signal.SIGTERM, self.prune)

        return self.intr_

    def __exit__(self, exc_type, exc_value, traceback):
        self.prune()

        signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

        return False


@pytest.fixture(scope='session')
def core_provider() -> CoreProvider:
    cp_ = CoreProvider()
    yield cp_
    cp_.prune()
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
