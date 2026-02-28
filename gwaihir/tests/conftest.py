import os
from pathlib import Path

import pytest
from dotenv import load_dotenv
from loguru import logger

# pytest plugins
pytest_plugins = ['plugins.init_logger']


@pytest.fixture(scope='session', autouse=True)
def test_path() -> Path:
    """Return path to tests directory."""
    path = Path(__file__).parent
    logger.info(f'Local test path: {path!r}')
    return path


@pytest.fixture(scope='session', autouse=True)
def test_outputs_path(test_path: Path) -> Path:
    """Return the path to test outputs directory."""
    test_outputs_path = test_path / 'outputs'
    test_outputs_path.mkdir(exist_ok=True)
    return test_outputs_path


@pytest.fixture(scope='session', autouse=True)
def setup_environment(test_path: Path) -> bool:
    env_file = test_path / '.env'
    loaded = load_dotenv(dotenv_path=env_file)
    # If .env is absent but required vars are already in the environment, don't force a skip.
    return loaded or ('ADO_PAT' in os.environ)


@pytest.fixture
def skip_if_missing_env_vars(setup_environment: bool) -> None:
    if not setup_environment:
        pytest.skip('Skipping test because the environment variables could not be loaded from the .env file.')
