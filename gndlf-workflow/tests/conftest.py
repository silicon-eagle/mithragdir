import os

import pytest
from loguru import logger


@pytest.fixture(autouse=True)
def set_ollama_model() -> None:
    current_model = os.getenv('OLLAMA_MODEL')
    logger.info(f'Setting OLLAMA_MODEL environment variable for tests from {current_model} to "gemma3:1b"')
    os.environ['OLLAMA_MODEL'] = 'gemma3:1b'
