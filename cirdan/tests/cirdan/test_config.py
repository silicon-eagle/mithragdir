import pytest
from cirdan.config import Settings
from pydantic import ValidationError


def test_settings_defaults() -> None:
    settings = Settings()

    assert settings.sqlite_connection_string.startswith('sqlite:///')
    assert settings.qdrant_collection == 'gwaihir_chunks'
    assert settings.top_k == 5


def test_settings_validates_top_k() -> None:
    with pytest.raises(ValidationError, match='top_k'):
        Settings(top_k=0)
