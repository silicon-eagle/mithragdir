# Gwaihir

Minimal tools for crawling Tolkien Gateway pages and storing them locally.

## Setup

```bash
uv sync
```

## CLI

The CLI provides two subcommands for different stages of the pipeline:

Show available commands:

```bash
uv run gwaihir --help
```

### Crawl Wiki and Text Data

Ingest wiki pages and text data into PostgreSQL:

```bash
uv run gwaihir wiki
```

Common options:

- `--wiki-base-url`: Wiki base URL (default: `https://tolkiengateway.net`)
- `--index-limit`: Max number of index entries to fetch
- `--crawl-limit`: Max number of pages to crawl
- `--text-source-folder`: location of local text sources (default: `database/texts`)
- `--progress/--no-progress`: Show progress output

Show all wiki options:

```bash
uv run gwaihir wiki --help
```

### Pipeline (Clear and/or Run)

Use one command for both clearing and execution:

```bash
# Clear chunks + embeddings only
uv run gwaihir pipeline --clear

# Run chunk + embed pipeline only
uv run gwaihir pipeline --run

# Clear first, then run
uv run gwaihir pipeline --clear --run
```

Common options:

- `--chunk-size` and `--chunk-overlap`: chunking configuration
- `--encode-document-id`: optional filter by document ID
- `--clear/--no-clear`: clear PostgreSQL chunks and reset Qdrant collection
- `--run/--no-run`: run chunking and embedding pipeline
- `--qdrant-url`: Qdrant endpoint (can also be set via `QDRANT_URL`)
- `--qdrant-api-key`: optional API key (can also be set via `QDRANT_API_KEY`)
- `--progress/--no-progress`: Show progress output

Show all pipeline options:

```bash
uv run gwaihir pipeline --help
```

### Global Options

Global options apply to all commands and are specified before the subcommand:

- `--db-url`: PostgreSQL URL. Can also be set via `DATABASE_URL`.
- `--log-level`: logging level (default: `INFO`)
- `--log-dir`: directory for log files

Example:

```bash
uv run gwaihir --db-url postgresql://redbook:redbook@localhost:5432/redbook wiki
```

## Run with PostgreSQL (Docker)

Start PostgreSQL locally:

```bash
docker compose -f docker-compose.postgres.yml up -d
```

## Run Qdrant (Docker)

Start Qdrant locally:

```bash
docker compose -f docker-compose.qdrant.yml up -d
```

Set local endpoint for the embedding pipeline:

```bash
export QDRANT_URL=http://localhost:6333
```

Stop Qdrant when finished:

```bash
docker compose -f docker-compose.qdrant.yml down
```

Run pipeline with local PostgreSQL and Qdrant:

```bash
export DATABASE_URL=postgresql://redbook:redbook@localhost:5432/redbook
uv run gwaihir pipeline
```

Or pass the URL directly:

```bash
uv run gwaihir --db-url postgresql://redbook:redbook@localhost:5432/redbook pipeline
```

## Embedding model auth (Hugging Face)

The default embedding model is `google/embeddinggemma-300m` in `processing/embedding.py`.
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
