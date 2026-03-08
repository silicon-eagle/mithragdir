# Gwaihir

Minimal tools for crawling Tolkien Gateway pages and storing them locally.

## Setup

```bash
uv sync
```

## CLI

Run the full pipeline (read data, chunk, then encode):

```bash
uv run gwaihir
```

Run only specific stages by repeating `--step`:

```bash
# Only ingest wiki/text data into SQLite
uv run gwaihir --step read-data

# Only chunk existing documents in SQLite
uv run gwaihir --step chunk

# Only encode existing chunks to Qdrant
uv run gwaihir --step encode

# Run chunk + encode, skip read-data
uv run gwaihir --step chunk --step encode
```

Show all options:

```bash
uv run gwaihir --help
```

Common options:

- `--db-path`: SQLite file path (default: `database/redbook.db`)
- `--text-source-folder`: location of local text sources (default: `database/texts`)
- `--chunk-size` and `--chunk-overlap`: chunking configuration
- `--qdrant-url`: Qdrant endpoint (can also be set via `QDRANT_URL`)
- `--qdrant-api-key`: optional API key (can also be set via `QDRANT_API_KEY`)

## Embedding model auth (Hugging Face)

The default embedding model is `google/embeddinggemma-300m` in `gwaihir/processing/embedding.py`.
This model is gated on Hugging Face, so you must authenticate before first use.

1. Request/confirm access to the model: `https://huggingface.co/google/embeddinggemma-300m`
2. Log in locally:

```bash
uv run huggingface-cli login
```

You can also use an environment variable instead of interactive login:

```bash
export HF_TOKEN=<your_hugging_face_token>
```

Recommended for local development: add it to a project `.env` file (already gitignored):

```bash
HF_TOKEN=<your_hugging_face_token>
```

Without auth, embedding-related tests and runtime embedding calls will skip/fail with `401` or `gated repo` errors.

## Quick start

```python
from pathlib import Path

from gwaihir.db.db import RedbookDatabase
from gwaihir.retriever.client import TolkienGatewayClient

db = RedbookDatabase(Path('storage/redbook.db'))
db._create_index_table()
db._create_page_table()

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
db._create_index_table()
db._create_page_table()

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
