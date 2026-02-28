from __future__ import annotations

import time
from collections.abc import Sequence
from dataclasses import dataclass
from urllib.parse import quote

from curl_cffi import requests as curl_requests
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

    def __init__(
        self,
        base_url: str,
        db: RedbookDatabase,
        batch_size: int = 25,
        timeout_seconds: float = 30,
    ) -> None:
        self.base_url = base_url.rstrip('/')
        self.api_url = f'{self.base_url}/w/api.php'
        self.db = db
        self.batch_size = batch_size
        self.timeout_seconds = timeout_seconds
        self._pending_pages: list[Page] = []
        logger.info(
            f'Initialized TolkienGatewayClient(base_url={self.base_url}, '
            f'batch_size={self.batch_size}, timeout_seconds={self.timeout_seconds})'
        )

    def _request_json(self, params: dict[str, str]) -> dict:
        logger.debug(f'Requesting MediaWiki API: {self.api_url} params={params}')
        response = curl_requests.get(
            self.api_url,
            params=params,
            impersonate='chrome',
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if 'error' in payload:
            error = payload['error']
            raise RuntimeError(f'MediaWiki API error: {error}')
        return payload

    def _build_page_url(self, title: str) -> str:
        return f'{self.base_url}/wiki/{quote(title.replace(" ", "_"))}'

    def get_index(self, limit: int | None = None) -> list[dict[str, int | str]]:
        logger.info(f'Fetching page index with limit={limit}')
        pages: list[dict[str, int | str]] = []
        next_continue: str | None = None

        while True:
            remaining = None if limit is None else limit - len(pages)
            if remaining is not None and remaining <= 0:
                break

            params: dict[str, str] = {
                'action': 'query',
                'format': 'json',
                'list': 'allpages',
                'aplimit': str(min(500, remaining) if remaining is not None else 500),
            }
            if next_continue is not None:
                params['apcontinue'] = next_continue

            payload = self._request_json(params)
            allpages = payload.get('query', {}).get('allpages', [])
            logger.debug(f'Fetched {len(allpages)} index entries in current page')
            for item in allpages:
                title = item['title']
                pages.append(
                    {
                        'title': title,
                        'pageid': int(item['pageid']),
                        'url': self._build_page_url(title),
                    }
                )
                if limit is not None and len(pages) >= limit:
                    break

            next_continue = payload.get('continue', {}).get('apcontinue')
            if not next_continue:
                break

        logger.info(f'Fetched {len(pages)} total index entries')
        return pages

    def get_page(self, title: str) -> Page:
        logger.debug(f'Fetching page content for title={title}')
        payload = self._request_json(
            {
                'action': 'parse',
                'format': 'json',
                'page': title,
                'prop': 'text',
                'disablelimitreport': '1',
                'disableeditsection': '1',
                'disablestylededuplication': '1',
            }
        )
        parsed = payload.get('parse', {})
        if not parsed:
            raise RuntimeError(f'Could not parse page: {title}')

        pageid = int(parsed.get('pageid', -1))
        parsed_title = str(parsed.get('title', title))
        content = parsed.get('text', {}).get('*', '')
        logger.debug(f'Fetched page title={parsed_title} pageid={pageid} content_len={len(content)}')
        return Page(
            title=parsed_title,
            pageid=pageid,
            url=self._build_page_url(parsed_title),
            content=content,
        )

    def store_page(self, page: Page) -> None:
        self._pending_pages.append(page)
        logger.debug(f'Buffered page {page.title} (pending={len(self._pending_pages)}/{self.batch_size})')
        if len(self._pending_pages) >= self.batch_size:
            logger.info('Batch size reached; flushing pending pages')
            self.flush()

    def flush(self) -> int:
        if not self._pending_pages:
            logger.debug('Flush called with empty page buffer')
            return 0

        stored = 0
        for page in self._pending_pages:
            self.db.insert_document(page.title, page.url, page.content)
            stored += 1

        logger.info(f'Flushed {stored} pages to database')
        self._pending_pages.clear()
        return stored

    def crawl(
        self,
        limit: int | None = None,
        pause_seconds: float = 2.0,
        nr_attemps: int = 2,
        retry_sleep_seconds: float = 120.0,
    ) -> int:
        if nr_attemps < 0:
            raise ValueError('nr_attemps must be >= 0')

        logger.info(f'Starting crawl(limit={limit}, pause_seconds={pause_seconds}, nr_attemps={nr_attemps})')

        index = self.get_index(limit=limit)
        logger.info(f'Crawl index contains {len(index)} pages')

        for item in index:
            title = str(item['title'])
            max_attempts = nr_attemps + 1
            stored = False
            for attempt_number in range(1, max_attempts + 1):
                try:
                    page = self.get_page(title)
                    self.store_page(page)
                    logger.debug(f'Stored crawled page {title} after attempt {attempt_number}')
                    stored = True
                    break
                except Exception as exc:  # noqa: BLE001
                    if attempt_number >= max_attempts:
                        logger.warning(f'Failed to fetch page {title} after {max_attempts} attempts: {exc}')
                        break

                    logger.warning(
                        f'Error while crawling {title} (attempt {attempt_number}/{max_attempts}): {exc}. '
                        f'Pausing for {retry_sleep_seconds} seconds before retrying.'
                    )
                    time.sleep(retry_sleep_seconds)

            if not stored:
                logger.debug(f'Skipping page {title} after retry exhaustion')

            time.sleep(pause_seconds)

        flushed = self.flush()
        logger.info(f'Crawl completed; flushed {flushed} remaining pages')
        return flushed

    def store_pages(self, pages: Sequence[Page]) -> int:
        logger.info(f'Storing {len(pages)} pages via buffered store_pages')
        for page in pages:
            self.store_page(page)
        return self.flush()

    def close(self) -> None:
        logger.info('Closing client and flushing pending pages')
        self.flush()
