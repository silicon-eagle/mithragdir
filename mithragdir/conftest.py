import os
from pathlib import Path

import pytest
from dotenv import load_dotenv
from loguru import logger

pytest_plugins = ['pytest_plugins.init_logger']


@pytest.fixture(scope='session', autouse=True)
def root_path() -> Path:
    """Return path to project root directory."""
    root_path = Path(__file__).parent
    logger.info(f'Project root path: {root_path!r}')
    return root_path


@pytest.fixture(scope='session', autouse=True)
def setup_environment(root_path: Path) -> bool:
    root_env_file = root_path / '.env'
    loaded_root = load_dotenv(dotenv_path=root_env_file)
    loaded = loaded_root or ('HF_TOKEN' in os.environ)
    logger.info(f'Environment variables loaded from {root_env_file!r}: {loaded_root}')
    return loaded


@pytest.fixture
def skip_if_missing_env_vars(setup_environment: bool) -> None:
    if not setup_environment:
        pytest.skip('Skipping test because the environment variables could not be loaded from the .env file.')


@pytest.fixture(autouse=True)
def test_env(setup_environment: bool, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest) -> None:
    """Set test environment using DEV variations."""
    if 'prod_env' in request.fixturenames:
        logger.info('Skipping test_env because prod_env is active for this test.')
        return

    logger.info('Setting test environment variables for database and Qdrant URLs.')
    if not setup_environment:
        pytest.skip('Skipping test because the environment variables could not be loaded from the .env file.')
    dev_db = os.environ.get('DEV_DATABASE_URL')
    dev_qd = os.environ.get('DEV_QDRANT_URL')
    if not dev_db or not dev_qd:
        pytest.skip('Skipping test because DEV_DATABASE_URL or DEV_QDRANT_URL is missing.')

    monkeypatch.setenv('DATABASE_URL', dev_db)
    logger.info(f'Overwriting DATABASE_URL for testing: {dev_db}')
    monkeypatch.setenv('QDRANT_URL', dev_qd)
    logger.info(f'Overwriting QDRANT_URL for testing: {dev_qd}')


@pytest.fixture
def prod_env(setup_environment: bool) -> None:
    """Set environment using production DATABASE_URL and QDRANT_URL."""
    if not setup_environment:
        pytest.skip('Skipping test because the environment variables could not be loaded from the .env file.')
