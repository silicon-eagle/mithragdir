from pathlib import Path

import pytest
from gwaihir.db.db import RedbookDatabase
from gwaihir.db.models import Index, Page
from gwaihir.retriever.client import TolkienGatewayClient
from numpy import ceil


@pytest.fixture
def db(tmp_path: Path) -> RedbookDatabase:
    database = RedbookDatabase(db_path=tmp_path / 'test_client.db')
    database._create_index_table()
    database._create_page_table()
    return database


@pytest.fixture
def client(db: RedbookDatabase) -> TolkienGatewayClient:
    return TolkienGatewayClient(
        base_url='https://tolkiengateway.net',
        db=db,
        batch_size=25,
        timeout_seconds=20,
    )


class TestTolkienGatewayClient:
    def test_get_index_with_small_limit(self, client: TolkienGatewayClient) -> None:
        pages = client.get_index(limit=2)
        assert len(pages) == 2
        assert all(isinstance(page, Index) for page in pages)
        assert all(page.title for page in pages)
        assert all(page.pageid > 0 for page in pages)
        assert all(page.url.startswith('https://tolkiengateway.net/wiki/') for page in pages)

    def test_get_page_returns_content(self, tmp_path: Path, client: TolkienGatewayClient) -> None:
        page = client.get_page('Gandalf')
        assert page.title
        assert page.pageid > 0
        assert page.url.startswith('https://tolkiengateway.net/wiki/')
        assert page.content
        path = page.dump_to_json(base_path=tmp_path)
        assert path.exists()

    def test_store_page_flushes_at_batch_size(self, client: TolkienGatewayClient, db: RedbookDatabase) -> None:
        pages = [Page(title=f'Page {i}', pageid=i, url=f'http://example/{i}', content=f'content {i}') for i in range(1, 26)]
        db.insert_indexes([Index(title=page.title, pageid=page.pageid, url=page.url) for page in pages])

        for page in pages[:-1]:
            client.store_page(page)
        assert db.page_count() == 0

        client.store_page(pages[-1])
        assert db.page_count() == 25

    def test_crawl_limited(self, client: TolkienGatewayClient, db: RedbookDatabase) -> None:
        nr_pages = 10
        index = client.get_index(limit=nr_pages, batch_size=int(ceil(nr_pages / 2)), pause_seconds=0.5)
        client.crawl(index=index, pause_seconds=0.5)
        assert db.page_count() == nr_pages

    def test_crawl_retries_after_error(self, client: TolkienGatewayClient, db: RedbookDatabase, monkeypatch: pytest.MonkeyPatch) -> None:
        test_index = Index(title='RetryPage', pageid=1, url='u')
        db.insert_index(test_index)

        calls = {'count': 0}

        def fake_get_page(title: str) -> Page:
            calls['count'] += 1
            if calls['count'] == 1:
                raise RuntimeError('temporary failure')
            return Page(title=title, pageid=1, url='http://example/retry', content='ok')

        sleeps: list[float] = []

        monkeypatch.setattr(client, 'get_page', fake_get_page)
        monkeypatch.setattr('gwaihir.retriever.client.time.sleep', sleeps.append)

        client.crawl(index=test_index, pause_seconds=0.0, nr_attempts=2, retry_sleep_seconds=120.0)

        assert calls['count'] == 2
        assert 120.0 in sleeps
        assert db.page_count() == 1

    def test_crawl_skips_existing_page(self, client: TolkienGatewayClient, db: RedbookDatabase, monkeypatch: pytest.MonkeyPatch) -> None:
        existing_index = Index(title='Existing', pageid=10, url='http://example/existing')
        existing_page = Page(title='Existing', pageid=10, url='http://example/existing', content='already there')
        db.insert_index(existing_index)
        db.insert_page(existing_page)

        def fail_if_called(_title: str) -> Page:
            raise AssertionError('get_page should not be called for existing page')

        monkeypatch.setattr(client, 'get_page', fail_if_called)

        client.crawl(index=existing_index, pause_seconds=0.0, nr_attempts=0)

        assert db.page_count() == 1
