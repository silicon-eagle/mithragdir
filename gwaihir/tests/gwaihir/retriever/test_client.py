import sqlite3
from pathlib import Path

import pytest
from gwaihir.db.db import RedbookDatabase
from gwaihir.retriever.client import Page, TolkienGatewayClient


@pytest.fixture
def db(tmp_path: Path) -> RedbookDatabase:
    database = RedbookDatabase(db_path=tmp_path / 'test_client.db')
    database._create_documents_table()
    return database


@pytest.fixture
def client(db: RedbookDatabase) -> TolkienGatewayClient:
    return TolkienGatewayClient(
        base_url='https://tolkiengateway.net',
        db=db,
        batch_size=2,
        timeout_seconds=20,
    )


def _document_count(db: RedbookDatabase) -> int:
    with sqlite3.connect(db.db_path) as connection:
        result = connection.execute('SELECT COUNT(*) FROM documents').fetchone()
        return int(result[0]) if result is not None else 0


class TestTolkienGatewayClient:
    def test_get_index_with_small_limit(self, client: TolkienGatewayClient) -> None:
        pages = client.get_index(limit=2)
        assert len(pages) == 2
        assert all('title' in page and page['title'] for page in pages)
        assert all('pageid' in page and int(page['pageid']) > 0 for page in pages)
        assert all('url' in page and str(page['url']).startswith('https://tolkiengateway.net/wiki/') for page in pages)

    def test_get_page_returns_content(self, client: TolkienGatewayClient) -> None:
        page = client.get_page('Gandalf')
        assert page.title
        assert page.pageid > 0
        assert page.url.startswith('https://tolkiengateway.net/wiki/')
        assert page.content

    def test_store_page_flushes_at_batch_size(self, client: TolkienGatewayClient, db: RedbookDatabase) -> None:
        first = Page(title='One', pageid=1, url='http://example/one', content='alpha')
        second = Page(title='Two', pageid=2, url='http://example/two', content='beta')

        client.store_page(first)
        assert _document_count(db) == 0

        client.store_page(second)
        assert _document_count(db) == 2

    @pytest.mark.asyncio
    async def test_crawl_limited(self, client: TolkienGatewayClient, db: RedbookDatabase) -> None:
        await client.crawl(limit=10, max_workers=2, pause_seconds=2)
        assert _document_count(db) == 10
