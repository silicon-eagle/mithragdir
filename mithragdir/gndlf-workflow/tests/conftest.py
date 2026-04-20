import os

import pytest


@pytest.fixture(autouse=True)
def set_ollama_model() -> None:
    os.environ['OLLAMA_MODEL'] = 'gemma3:1b'
