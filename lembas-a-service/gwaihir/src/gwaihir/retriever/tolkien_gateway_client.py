from __future__ import annotations

import random
import time
from collections.abc import Iterable, Sequence
from urllib.parse import quote

import click
from curl_cffi import requests as curl_requests
from lembas_core.db import RedbookDatabase
from lembas_core.schemas import Index, Page
from loguru import logger

DEFAULT_USER_AGENT = 'gwaihir-bot/1.0 (+https://github.com/silicon-eagle/; mailto:silicon.eagle@pm.me)'


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
        pause_seconds: float = 1.0,
        jitter_pause: bool = True,
        user_agent: str = DEFAULT_USER_AGENT,
    ) -> None:
        """Initialize API client state and buffering.

        Args:
            base_url: Tolkien Gateway base URL.
            db: Database used for persistence.
            batch_size: Number of pages buffered before flush.
            timeout_seconds: Request timeout for API calls.
            pause_seconds: Base pause duration between API calls.
            jitter_pause: Whether to apply random jitter to pause durations.
            user_agent: User-Agent sent on all MediaWiki API requests.
        """
        self.base_url = base_url.rstrip('/')
        self.api_url = f'{self.base_url}/w/api.php'
        self.db = db
        self.batch_size = batch_size
        self.timeout_seconds = timeout_seconds
        self.pause_seconds = pause_seconds
        self.jitter_pause = jitter_pause
        self.user_agent = user_agent
        self._session = curl_requests.Session(impersonate='chrome')
        self._session.headers.update({'User-Agent': self.user_agent})
        self._pending_pages: list[Page] = []
        logger.info(
            f'Initialized TolkienGatewayClient('
            f'base_url={self.base_url}, '
            f'batch_size={self.batch_size}, '
            f'timeout_seconds={self.timeout_seconds}, '
            f'pause_seconds={self.pause_seconds}, '
            f'jitter_pause={self.jitter_pause}, '
            f'user_agent={self.user_agent}'
            f')'
        )

    def _sleep_with_pause_jitter(self, pause_seconds: float | None = None, apply_jitter: bool = True) -> None:
        """Sleep with optional jitter based on configured pacing.

        Args:
            pause_seconds: Base pause duration. If None, uses ``self.pause_seconds``.
            apply_jitter: Whether jitter should be applied to this sleep call.
        """
        base_pause = self.pause_seconds if pause_seconds is None else pause_seconds
        if base_pause <= 0:
            return

        jitter = random.uniform(0.0, 1.5) if self.jitter_pause and apply_jitter else 0.0
        time.sleep(base_pause + jitter)

    def _request_json(self, params: dict[str, str]) -> dict:
        """Execute a MediaWiki API request and return JSON payload.

        Args:
            params: Query string parameters for the API call.

        Returns:
            Parsed JSON payload.
        """
        logger.debug(f'Requesting MediaWiki API: {self.api_url} params={params}')
        response = self._session.get(
            self.api_url,
            params=params,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if 'error' in payload:
            error = payload['error']
            raise RuntimeError(f'MediaWiki API error: {error}')
        return payload

    def _build_page_url(self, title: str) -> str:
        """Build a canonical wiki URL for a page title.

        Args:
            title: Page title.

        Returns:
            URL-safe page URL.
        """
        return f'{self.base_url}/wiki/{quote(title.replace(" ", "_"))}'

    def _fetch_index_payload_with_retry(
        self,
        params: dict[str, str],
        nr_attempts: int,
        retry_sleep_seconds: float,
    ) -> dict:
        """Fetch one index payload with retry/backoff.

        Args:
            params: API query parameters.
            nr_attempts: Number of retry attempts after first call.
            retry_sleep_seconds: Delay between retries.

        Returns:
            Successful API payload.
        """
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
                self._sleep_with_pause_jitter(retry_sleep_seconds, apply_jitter=False)

        if payload is None:
            raise RuntimeError('Index payload is unexpectedly None after retries')
        return payload

    def _build_index_batch(self, allpages: list[dict[str, object]], remaining: int | None = None) -> list[Index]:
        """Transform MediaWiki allpages entries into Index models.

        Args:
            allpages: Raw allpages items.
            remaining: Optional remaining item count to include.

        Returns:
            Parsed index entries for this batch.
        """
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
        """Persist an index batch to the database.

        Args:
            indexes: Index rows to persist.
        """
        if not indexes:
            return
        self.db.insert_indexes(indexes)

    def _extract_apcontinue(self, payload: dict) -> str | None:
        """Extract continuation token from an index response.

        Args:
            payload: API payload that may contain continue block.

        Returns:
            Next apcontinue token or None.
        """
        continue_payload = payload.get('continue')
        if isinstance(continue_payload, dict):
            apcontinue = continue_payload.get('apcontinue')
            return apcontinue if isinstance(apcontinue, str) else None
        return None

    def get_index(
        self,
        limit: int | None = None,
        batch_size: int = 100,
        pause_seconds: float | None = None,
        nr_attempts: int = 2,
        retry_sleep_seconds: float = 5.0,
        show_progress: bool = True,
    ) -> list[Index]:
        """Fetch and store index entries from MediaWiki allpages.

        Args:
            limit: Optional max number of pages to fetch.
            batch_size: Requested allpages batch size.
            pause_seconds: Delay between index requests.
            nr_attempts: Number of retries per request.
            retry_sleep_seconds: Delay between retry attempts.
            show_progress: Whether to render progress output in the terminal.

        Returns:
            Collected index entries.
        """
        logger.info(f'Fetching page index with limit={limit}, batch_size={batch_size}, pause_seconds={pause_seconds}, nr_attempts={nr_attempts}')
        pages: list[Index] = []
        next_continue: str | None = None
        spinner_frames = ('|', '/', '-', '\\')
        spinner_step = 0

        progress_bar = None
        if show_progress and limit is not None:
            progress_bar = click.progressbar(
                length=limit,
                label='Getting index',
                show_pos=True,
                show_percent=True,
                show_eta=True,
            )
            progress_bar.__enter__()

        try:
            loop_counter = 0
            while True:
                loop_counter += 1
                self._sleep_with_pause_jitter(pause_seconds)
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

                if progress_bar is not None:
                    progress_bar.update(len(batch_indexes))
                elif show_progress:
                    spinner = spinner_frames[spinner_step % len(spinner_frames)]
                    spinner_step += 1
                    click.echo(f'\rGetting index {spinner} pages fetched={len(pages)}', nl=False)

                next_continue = self._extract_apcontinue(payload)
                if not next_continue:
                    break
        finally:
            if progress_bar is not None:
                progress_bar.__exit__(None, None, None)
            elif show_progress:
                click.echo('')

        logger.info(f'Fetched {len(pages)} total index entries')
        return pages

    def get_page(self, title: str) -> Page:
        """Fetch and parse one wiki page.

        Args:
            title: Page title to fetch.

        Returns:
            Parsed page model.
        """
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
        """Buffer one crawled page and flush at batch size.

        Args:
            page: Page payload to store.
        """
        self._pending_pages.append(page)
        logger.debug(f'Buffered page {page.title} (pending={len(self._pending_pages)}/{self.batch_size})')
        if len(self._pending_pages) >= self.batch_size:
            logger.info('Batch size reached; flushing pending pages')
            self.flush()

    def flush(self) -> int:
        """Persist buffered pages to the database.

        Returns:
            Number of flushed pages.
        """
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

    def crawl(  # noqa: C901
        self,
        index: Index | list[Index] | None = None,
        limit: int | None = None,
        pause_seconds: float | None = None,
        nr_attempts: int = 2,
        retry_sleep_seconds: float = 120.0,
        show_progress: bool = True,
    ) -> int:
        """Crawl pages from an index and persist them with retries.

        Args:
            index: Optional pre-fetched index input.
            limit: Optional limit used when index is fetched internally.
            pause_seconds: Delay between page requests.
            nr_attempts: Number of retries per page.
            retry_sleep_seconds: Delay between page retries.
            show_progress: Whether to render progress output in the terminal.

        Returns:
            Number of pages flushed during final cleanup flush.
        """
        if nr_attempts < 0:
            raise ValueError('nr_attemps must be >= 0')
        if limit is not None and limit < 0:
            raise ValueError('limit must be >= 0')

        logger.info(f'Starting crawl(limit={limit}, pause_seconds={pause_seconds}, nr_attemps={nr_attempts})')

        # Resolve crawl input: use provided index or fetch one.
        if index is None:
            crawl_index = self.get_index(limit=limit, show_progress=show_progress)
        elif isinstance(index, Index):
            crawl_index = [index]
        else:
            crawl_index = index

        # Apply crawl limit even when an explicit index list is passed in.
        if limit is not None:
            crawl_index = crawl_index[:limit]

        total_pages = len(crawl_index)
        logger.info(f'Crawl index contains {total_pages} pages')

        processed_count = 0
        stored_count = 0
        failed_count = 0
        page_iterable: Iterable[Index] = crawl_index
        progress_pages = None
        if show_progress:
            progress_pages = click.progressbar(
                crawl_index,
                label='Getting page content',
                show_pos=True,
                show_percent=True,
                show_eta=True,
            )
            page_iterable = progress_pages.__enter__()
        try:
            for item in page_iterable:
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
                        self._sleep_with_pause_jitter(retry_sleep_seconds, apply_jitter=False)

                if not stored:
                    logger.debug(f'Skipping page {title} after retry exhaustion')
                    failed_count += 1
                else:
                    stored_count += 1

                processed_count += 1
                logger.info(f'Crawl progress: {processed_count}/{total_pages} (stored={stored_count}, failed={failed_count})')

                # Polite pause between pages.
                self._sleep_with_pause_jitter(pause_seconds)
        finally:
            if progress_pages is not None:
                progress_pages.__exit__(None, None, None)
            # Always flush buffered pages, even on interruption/error.
            flushed = self.flush()

        logger.info(f'Crawl completed: processed={processed_count}, stored={stored_count}, failed={failed_count}, flushed_remaining={flushed}')
        return flushed

    def store_pages(self, pages: Sequence[Page]) -> int:
        """Store multiple pages through buffered insert flow.

        Args:
            pages: Sequence of pages to store.

        Returns:
            Number of pages flushed.
        """
        logger.info(f'Storing {len(pages)} pages via buffered store_pages')
        for page in pages:
            self.store_page(page)
        return self.flush()

    def close(self) -> None:
        """Flush pending pages before shutdown."""
        logger.info('Closing client and flushing pending pages')
        self.flush()
        close_session = getattr(self._session, 'close', None)
        if callable(close_session):
            close_session()
