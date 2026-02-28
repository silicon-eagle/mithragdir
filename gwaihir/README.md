# Gwaihir

Minimal tools for crawling Tolkien Gateway pages and storing them locally.

## Setup

```bash
uv sync
```

## Quick start

```python
from pathlib import Path

from gwaihir.db.db import RedbookDatabase
from gwaihir.retriever.client import TolkienGatewayClient

db = RedbookDatabase(Path('storage/redbook.db'))
db._create_documents_table()

client = TolkienGatewayClient(
	base_url='https://tolkiengateway.net',
	db=db,
	batch_size=10,
)

index = client.get_index(limit=5)
print(index)

page = client.get_page('Gandalf')
print(page.title, page.pageid)

client.store_page(page)
client.flush()
client.close()
```

## Crawl (polite defaults)

Use a small limit while developing to avoid excessive requests.

```python
from pathlib import Path

from gwaihir.db.db import RedbookDatabase
from gwaihir.retriever.client import TolkienGatewayClient

db = RedbookDatabase(Path('storage/redbook.db'))
db._create_documents_table()

client = TolkienGatewayClient(
	base_url='https://tolkiengateway.net',
	db=db,
	batch_size=20,
)

stored_count = client.crawl(
	limit=25,
	pause_seconds=2.0,
)
print(f'Stored {stored_count} pages')
```

### Notes

- `get_index(limit=...)` fetches page metadata from MediaWiki `allpages`.
- `get_page(title)` fetches parsed page content (`action=parse`).
- `store_page(...)` buffers in memory and writes automatically when `batch_size` is reached.
- `flush()` forces pending buffered pages to be written to the database.
