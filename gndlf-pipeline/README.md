# Gndlf Pipeline

Minimal tools for crawling Tolkien Gateway pages and storing them locally.

## Setup

```bash
uv sync
```

## CLI

The CLI provides two subcommands for data ingestion and embedding:

Show available commands:

```bash
uv run gndlf-pipeline --help
```

### Global Options

Apply to all commands (specified before subcommand):

- `--db-url`: PostgreSQL URL (defaults to `DATABASE_URL` env var)
- `--dev`: Use DEV environment variables (`DEV_DATABASE_URL`, `DEV_QDRANT_URL`)
- `--log-level`: Logging level (default: `INFO`)
- `--log-dir`: Log file directory (default: `.log/`)

Example:

```bash
uv run gndlf-pipeline --dev wiki
```

### Crawl Wiki and Text Data

Ingest wiki pages and text data into PostgreSQL:

```bash
uv run gndlf-pipeline wiki
```

Common options:

- `--wiki-base-url`: Wiki base URL (default: `https://tolkiengateway.net`)
- `--index-limit`: Max number of index entries to fetch
- `--crawl-limit`: Max number of pages to crawl
- `--text-source-folder`: Local text sources directory (default: `database/texts`)
- `--progress/--no-progress`: Show progress output (default: enabled)

View all wiki options:

```bash
uv run gndlf-pipeline wiki --help
```

### Pipeline (Clear and/or Run)

Chunk documents and generate embeddings:

```bash
# Clear chunks + embeddings only
uv run gndlf-pipeline pipeline --clear

# Run chunk + embed pipeline only
uv run gndlf-pipeline pipeline --run

# Clear first, then run
uv run gndlf-pipeline pipeline --clear --run
```

Common options:

- `--clear/--no-clear`: Clear PostgreSQL chunks and reset Qdrant collection (default: disabled)
- `--run/--no-run`: Run chunking and embedding pipeline (default: disabled)
- `--chunk-size` and `--chunk-overlap`: Chunking configuration (defaults: 512, 64)
- `--encode-document-id`: Optional document ID filter when encoding chunks
- `--qdrant-url`: Qdrant endpoint (defaults to `QDRANT_URL` env var)
- `--progress/--no-progress`: Show progress output (default: enabled)

View all pipeline options:

```bash
uv run gndlf-pipeline pipeline --help
```

## Local Development

Start services locally:

```bash
docker compose -f docker-compose.postgres.yml up -d
docker compose -f docker-compose.qdrant.yml up -d
```

Configure environment variables in `.env` (see [.env.example](.env.example)).

Run pipeline with dev environment:

```bash
uv run gndlf-pipeline --dev pipeline --clear --run
```

## Embedding model auth (Hugging Face)

The default embedding model is `google/embeddinggemma-300m` in `processing/embedding.py`.
This model is gated on Hugging Face. Add `HF_TOKEN` to `.env` (see [.env.example](.env.example)):

```bash
HF_TOKEN=<your_hugging_face_token>
```

Without auth, embedding calls will fail with `401` errors.
