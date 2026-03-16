from cirdan.main import app
from fastapi.testclient import TestClient


def test_health_endpoint() -> None:
    client = TestClient(app)

    response = client.get('/health')

    assert response.status_code == 200
    assert response.json() == {'status': 'ok'}


def test_ready_returns_503_when_dependencies_fail() -> None:
    client = TestClient(app)

    response = client.get('/ready')

    assert response.status_code == 503
    assert response.json()['error']['code'] == 'dependency_offline'
