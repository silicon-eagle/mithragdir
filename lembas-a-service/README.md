# Lembas: A Service

A Tolkien knowledge base service that ingests wiki data, chunks it, and generates hybrid embeddings for semantic search.

## Project Structure

- **[gwaihir/](gwaihir/)** — CLI tools for data ingestion, chunking, and embedding
  - `wiki` command: Crawl Tolkien Gateway and ingest text sources
   - `pipeline` command: Clear and/or run chunking + embedding
- **lembas-core/** — Shared database models and utilities
- **cirdan/** — Retrieval and search service (coming soon)

## Quick Start

1. **Setup:**
   ```bash
   uv sync
   ```

2. **Start containers:**
   ```bash
   docker compose -f docker-compose.postgres.yml up -d
   docker compose -f docker-compose.qdrant.yml up -d
   ```

3. **Set environment:**
   ```bash
   export DATABASE_URL=postgresql://redbook:redbook@localhost:5432/redbook
   export QDRANT_URL=http://localhost:6333
   ```

4. **Run the pipeline (three steps):**
   ```bash
   # 1. Crawl wiki and ingest text data
   uv run gwaihir wiki

   # 2. Clear old chunks/embeddings (optional)
   uv run gwaihir pipeline --clear

   # 3. Chunk + embed
   uv run gwaihir pipeline --run

   # (Or clear and rebuild in one command)
   uv run gwaihir pipeline --clear --run
   ```

See [gwaihir/README.md](gwaihir/README.md) for detailed CLI documentation.

## Database

- **Backend:** PostgreSQL 16-Alpine
- **ORM:** Peewee
- **Container:** `redbook-postgress` (via `docker-compose.postgres.yml`)

## Vector Store

- **Backend:** Qdrant v1.17.0
- **Collection:** `gwaihir_chunks` (hybrid: dense + sparse)
- **Container:** `redbook-qdrant` (via `docker-compose.qdrant.yml`)

## Embedding Model

Default: `google/embeddinggemma-300m` (requires Hugging Face authentication)

```bash
uv run huggingface-cli login
# or export HF_TOKEN=<your_token>
```

See [gwaihir/README.md](gwaihir/README.md) for full setup details.
