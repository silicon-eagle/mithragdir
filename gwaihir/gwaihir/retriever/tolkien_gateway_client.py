from __future__ import annotations

import time
from collections.abc import Sequence
from urllib.parse import quote

from curl_cffi import requests as curl_requests
from loguru import logger

from gwaihir.db.db import RedbookDatabase
from gwaihir.db.models import Index, Page


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
            f'Initialized TolkienGatewayClient(base_url={self.base_url}, batch_size={self.batch_size}, timeout_seconds={self.timeout_seconds})'
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

    def _fetch_index_payload_with_retry(
        self,
        params: dict[str, str],
        nr_attempts: int,
        retry_sleep_seconds: float,
    ) -> dict:
        max_attempts = nr_attempts + 1
        payload: dict | None = None
        for attempt_number in range(1, max_attempts + 1):
            try:
                payload = self._request_json(params)
                break
            except Exception as exc:  # noqa: BLE001
                if attempt_number >= max_attempts:
                    logger.warning(f'Failed to fetch index batch after {max_attempts} attempts: {exc}')
                    raise

                logger.warning(
                    f'Error while fetching index batch (attempt {attempt_number}/{max_attempts}): {exc}. '
                    f'Pausing for {retry_sleep_seconds} seconds before retrying.'
                )
                time.sleep(retry_sleep_seconds)

        if payload is None:
            raise RuntimeError('Index payload is unexpectedly None after retries')
        return payload

    def _build_index_batch(self, allpages: list[dict[str, object]], remaining: int | None = None) -> list[Index]:
        batch_indexes: list[Index] = []
        for item in allpages:
            if remaining is not None and len(batch_indexes) >= remaining:
                break

            title = str(item.get('title', ''))
            pageid_raw = item.get('pageid', -1)
            pageid = int(pageid_raw) if isinstance(pageid_raw, int | str) and str(pageid_raw).isdigit() else -1
            url = self._build_page_url(title)
            batch_indexes.append(Index(title=title, pageid=pageid, url=url))

        return batch_indexes

    def _store_index_batch(self, indexes: Sequence[Index]) -> None:
        if not indexes:
            return
        self.db.insert_indexes(indexes)

    def _extract_apcontinue(self, payload: dict) -> str | None:
        continue_payload = payload.get('continue')
        if isinstance(continue_payload, dict):
            apcontinue = continue_payload.get('apcontinue')
            return apcontinue if isinstance(apcontinue, str) else None
        return None

    def get_index(
        self,
        limit: int | None = None,
        batch_size: int = 100,
        pause_seconds: float = 1.0,
        nr_attempts: int = 2,
        retry_sleep_seconds: float = 5.0,
    ) -> list[Index]:
        logger.info(f'Fetching page index with limit={limit}, batch_size={batch_size}, pause_seconds={pause_seconds}, nr_attempts={nr_attempts}')
        pages: list[Index] = []
        next_continue: str | None = None

        loop_counter = 0
        while True:
            loop_counter += 1
            time.sleep(pause_seconds)
            remaining = None if limit is None else limit - len(pages)
            if remaining is not None and remaining <= 0:
                break
            batch_size = min(batch_size, remaining, 500) if remaining is not None else min(batch_size, 500)

            logger.info(f'loop {loop_counter}: Fetching index batch with batch_size={batch_size}.')
            params: dict[str, str] = {
                'action': 'query',
                'format': 'json',
                'list': 'allpages',
                'aplimit': str(batch_size),
                **({'apcontinue': next_continue} if next_continue else {}),
            }

            payload = self._fetch_index_payload_with_retry(
                params=params,
                nr_attempts=nr_attempts,
                retry_sleep_seconds=retry_sleep_seconds,
            )

            allpages = payload.get('query', {}).get('allpages', [])
            total = len(allpages) + len(pages)
            logger.info(f'Fetched {len(allpages)} (total={total}) index entries in current page')

            batch_indexes = self._build_index_batch(allpages=allpages, remaining=remaining)
            pages.extend(batch_indexes)
            self._store_index_batch(batch_indexes)

            next_continue = self._extract_apcontinue(payload)
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
                'prop': 'text|categories|links|images|externallinks|sections|revid|displaytitle|properties',
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

        categories = parsed.get('categories')
        images = parsed.get('images')
        links = parsed.get('links')
        external_links = parsed.get('externallinks')
        sections = parsed.get('sections')
        revid = parsed.get('revid')
        displaytitle = parsed.get('displaytitle')
        properties = parsed.get('properties')

        logger.debug(f'Fetched page title={parsed_title} pageid={pageid} content_len={len(content)}')
        return Page(
            title=parsed_title,
            pageid=pageid,
            url=self._build_page_url(parsed_title),
            content=content,
            categories=categories,
            images=images,
            links=links,
            external_links=external_links,
            sections=sections,
            revid=revid,
            displaytitle=displaytitle,
            properties=properties,
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
            self.db.insert_document(page)
            stored += 1

        logger.info(f'Flushed {stored} pages to database')
        self._pending_pages.clear()
        return stored

    def crawl(
        self,
        index: Index | list[Index] | None = None,
        limit: int | None = None,
        pause_seconds: float = 2.0,
        nr_attempts: int = 2,
        retry_sleep_seconds: float = 120.0,
    ) -> int:
        if nr_attempts < 0:
            raise ValueError('nr_attemps must be >= 0')

        logger.info(f'Starting crawl(limit={limit}, pause_seconds={pause_seconds}, nr_attemps={nr_attempts})')

        # Resolve crawl input: use provided index or fetch one.
        if index is None:
            crawl_index = self.get_index(limit=limit)
        elif isinstance(index, Index):
            crawl_index = [index]
        else:
            crawl_index = index

        total_pages = len(crawl_index)
        logger.info(f'Crawl index contains {total_pages} pages')

        processed_count = 0
        stored_count = 0
        failed_count = 0
        try:
            for item in crawl_index:
                title = item.title

                # Skip network work if this page is already stored.
                if self.db.document_exists(title):
                    processed_count += 1
                    logger.info(
                        f'Crawl progress: {processed_count}/{total_pages} (stored={stored_count}, failed={failed_count}) - skipped existing {title}'
                    )
                    continue

                max_attempts = nr_attempts + 1
                stored = False
                # Retry failed page retrieval a limited number of times.
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
                    failed_count += 1
                else:
                    stored_count += 1

                processed_count += 1
                logger.info(f'Crawl progress: {processed_count}/{total_pages} (stored={stored_count}, failed={failed_count})')

                # Polite pause between pages.
                time.sleep(pause_seconds)
        finally:
            # Always flush buffered pages, even on interruption/error.
            flushed = self.flush()

        logger.info(f'Crawl completed: processed={processed_count}, stored={stored_count}, failed={failed_count}, flushed_remaining={flushed}')
        return flushed

    def store_pages(self, pages: Sequence[Page]) -> int:
        logger.info(f'Storing {len(pages)} pages via buffered store_pages')
        for page in pages:
            self.store_page(page)
        return self.flush()

    def close(self) -> None:
        logger.info('Closing client and flushing pending pages')
        self.flush()
