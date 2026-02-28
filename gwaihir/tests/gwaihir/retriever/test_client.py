from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest
from gwaihir.db.db import RedbookDatabase
from gwaihir.retriever.client import Page, TolkienGatewayClient

BASE_URL = 'https://tolkiengateway.net'


@pytest.fixture
def db(tmp_path: Path) -> RedbookDatabase:
    database = RedbookDatabase(db_path=tmp_path / 'test.db')
    database._create_documents_table()
    return database


@pytest.fixture
async def client(db: RedbookDatabase) -> AsyncIterator[TolkienGatewayClient]:
    c = TolkienGatewayClient(base_url=BASE_URL, db=db, batch_size=5)
    yield c
    await c.close()


@pytest.fixture(scope='module')
def _api_reachable() -> None:
    """Skip the entire module when the Tolkien Gateway API is behind Cloudflare or unreachable."""
    try:
        resp = httpx.get(
            f'{BASE_URL}/w/api.php',
            params={'action': 'query', 'meta': 'siteinfo', 'format': 'json'},
            headers=TolkienGatewayClient.DEFAULT_HEADERS,
            timeout=10,
        )
        if resp.status_code == 403:
            pytest.skip('Tolkien Gateway blocked by Cloudflare (403)')
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        pytest.skip(f'Tolkien Gateway unreachable: {exc}')


pytestmark = pytest.mark.usefixtures('_api_reachable')


class TestGetIndex:
    async def test_returns_pages(self, client: TolkienGatewayClient) -> None:
        pages = await client.get_index(limit=5)
        assert len(pages) > 0
        assert len(pages) <= 5

    async def test_page_stubs_have_expected_keys(self, client: TolkienGatewayClient) -> None:
        pages = await client.get_index(limit=3)
        first = pages[0]
        assert 'pageid' in first
        assert 'title' in first


class TestGetPage:
    async def test_existing_page(self, client: TolkienGatewayClient) -> None:
        page = await client.get_page('Gandalf')
        assert page is not None
        assert isinstance(page, Page)
        assert page.title == 'Gandalf'
        assert page.pageid > 0
        assert BASE_URL in page.url
        assert len(page.content) > 0

    async def test_missing_page_returns_none(self, client: TolkienGatewayClient) -> None:
        page = await client.get_page('ThisPageShouldDefinitelyNotExist12345')
        assert page is None


class TestBatchedStorage:
    async def test_flush_writes_to_db(self, client: TolkienGatewayClient, db: RedbookDatabase) -> None:
        page = await client.get_page('Gandalf')
        assert page is not None

        client._enqueue(page)
        client._flush()

        with db.connect() as conn:
            row = conn.execute(
                'SELECT title, url, raw_content FROM documents WHERE title = ?',
                (page.title,),
            ).fetchone()

        assert row is not None
        assert row[0] == page.title
        assert row[1] == page.url
        assert len(row[2]) > 0

    async def test_auto_flush_at_batch_size(self, client: TolkienGatewayClient, db: RedbookDatabase) -> None:
        page = await client.get_page('Gandalf')
        assert page is not None

        for _ in range(5):
            client._enqueue(page)

        assert len(client._buffer) == 0

        with db.connect() as conn:
            count = conn.execute('SELECT COUNT(*) FROM documents').fetchone()[0]
        assert count == 5
