from __future__ import annotations

import asyncio
from dataclasses import dataclass

import httpx
from loguru import logger

from gwaihir.db.db import RedbookDatabase


@dataclass
class Page:
    """A page fetched from the Tolkien Gateway."""

    title: str
    pageid: int
    url: str
    content: str


class TolkienGatewayClient:
    """Client for crawling pages from the Tolkien Gateway MediaWiki API.

    Args:
        base_url: Base URL for the wiki API (e.g. "https://tolkiengateway.net").
        db: RedbookDatabase instance for storing crawled pages.
        batch_size: Number of pages to accumulate before flushing to the database.
    """

    API_PATH: str = '/w/api.php'

    def __init__(
        self,
        base_url: str,
        db: RedbookDatabase,
        batch_size: int = 50,
    ) -> None:
        self.base_url = base_url
        self.db = db
        self.batch_size = batch_size
        self._buffer: list[Page] = []
        default_headers = {
            'User-Agent': 'gwaihir-client/0.1 (https://github.com/lembasaservice/gwaihir; polite-bot)',
        }
        self._client = httpx.AsyncClient(base_url=base_url, headers=default_headers, timeout=30.0)

    async def close(self) -> None:
        await self._client.aclose()

    # -- public API -----------------------------------------------------------

    async def get_index(
        self,
        namespace: int = 0,
        filter_redirects: str = 'nonredirects',
        limit: int | str = 'max',
    ) -> list[dict]:
        """Return a list of *all* page stubs (pageid + title) in the given namespace.

        Uses the ``list=allpages`` query module with automatic continuation.

        Args:
            namespace: MediaWiki namespace id (0 = main articles).
            filter_redirects: Filter for redirects (all, nonredirects, redirects).
            limit: Max pages per request batch, or ``'max'`` for the server maximum.
        """
        pages: list[dict] = []
        params: dict = {
            'action': 'query',
            'list': 'allpages',
            'apnamespace': namespace,
            'apfilterredir': filter_redirects,
            'aplimit': limit,
            'format': 'json',
        }

        while True:
            resp = await self._client.get(self.API_PATH, params=params)
            resp.raise_for_status()
            data = resp.json()

            pages.extend(data.get('query', {}).get('allpages', []))

            if 'continue' not in data:
                break
            params.update(data['continue'])

        logger.info('Fetched index: {} pages', len(pages))
        return pages

    async def get_page(self, title: str) -> Page | None:
        """Fetch the parsed wikitext content of a single page by *title*.

        Returns ``None`` when the page cannot be parsed (e.g. missing page).
        """
        params = {
            'action': 'parse',
            'page': title,
            'prop': 'wikitext',
            'format': 'json',
        }

        resp = await self._client.get(self.API_PATH, params=params)
        resp.raise_for_status()
        data = resp.json()

        if 'error' in data:
            logger.warning('Failed to fetch page "{}": {}', title, data['error'].get('info', ''))
            return None

        parse = data.get('parse', {})
        page_title = parse.get('title', title)
        return Page(
            title=page_title,
            pageid=parse.get('pageid', -1),
            url=f'{self.base_url}/wiki/{page_title}',
            content=parse.get('wikitext', {}).get('*', ''),
        )

    # -- batched storage ------------------------------------------------------

    def _enqueue(self, page: Page) -> None:
        """Add a page to the internal buffer; flush when ``batch_size`` is reached."""
        self._buffer.append(page)
        if len(self._buffer) >= self.batch_size:
            self._flush()

    def _flush(self) -> None:
        """Write all buffered pages to the database in a single connection."""
        if not self._buffer:
            return

        with self.db.connect() as conn:
            for page in self._buffer:
                conn.execute(
                    'INSERT INTO documents (title, url, raw_content) VALUES (?, ?, ?);',
                    (page.title, page.url, page.content),
                )
        logger.info('Flushed {} pages to database', len(self._buffer))
        self._buffer.clear()

    # -- crawl ----------------------------------------------------------------

    async def crawl(
        self,
        max_concurrent: int = 5,
        delay: float = 2.0,
        namespace: int = 0,
    ) -> None:
        """Crawl all pages in *namespace* and store them to the database.

        Args:
            max_concurrent: Maximum number of concurrent fetch tasks.
            delay: Seconds to wait between launching each request (rate-limit).
            namespace: MediaWiki namespace id (0 = main articles).
        """
        index = await self.get_index(namespace=namespace)
        semaphore = asyncio.Semaphore(max_concurrent)

        async def _fetch_and_store(title: str) -> None:
            async with semaphore:
                page = await self.get_page(title)
                if page is not None:
                    self._enqueue(page)

        tasks: list[asyncio.Task] = []
        for entry in index:
            title = entry['title']
            task = asyncio.create_task(_fetch_and_store(title))
            tasks.append(task)
            await asyncio.sleep(delay)

        await asyncio.gather(*tasks)

        # flush remaining buffered pages
        self._flush()

        logger.info('Crawl complete - processed {} pages', len(index))
