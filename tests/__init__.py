import pytest
from dotenv import load_dotenv

@pytest.fixture(scope='session', autouse=True)
def load_env() -> None:
    load_dotenv()
