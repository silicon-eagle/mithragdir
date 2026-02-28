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
        assert db.document_count() == 0

        client.store_page(second)
        assert db.document_count() == 2

    def test_crawl_limited(self, client: TolkienGatewayClient, db: RedbookDatabase) -> None:
        client.crawl(limit=3, pause_seconds=0.5)
        assert db.document_count() == 3

    def test_crawl_retries_after_error(
        self, client: TolkienGatewayClient, db: RedbookDatabase, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def fake_get_index(limit: int | None = None) -> list[dict[str, int | str]]:
            _ = limit
            return [{'title': 'RetryPage', 'pageid': 1, 'url': 'u'}]

        monkeypatch.setattr(client, 'get_index', fake_get_index)

        calls = {'count': 0}

        def fake_get_page(title: str) -> Page:
            calls['count'] += 1
            if calls['count'] == 1:
                raise RuntimeError('temporary failure')
            return Page(title=title, pageid=1, url='http://example/retry', content='ok')

        sleeps: list[float] = []

        monkeypatch.setattr(client, 'get_page', fake_get_page)
        monkeypatch.setattr('gwaihir.retriever.client.time.sleep', lambda seconds: sleeps.append(seconds))

        client.crawl(limit=1, pause_seconds=0.0, nr_attemps=2, retry_sleep_seconds=120.0)

        assert calls['count'] == 2
        assert 120.0 in sleeps
        assert db.document_count() == 1
