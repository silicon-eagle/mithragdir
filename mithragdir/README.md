# Mithragdir

A Tolkien knowledge base service that ingests wiki data, chunks it, and generates hybrid embeddings for semantic search.

## Project Structure

- **[gndlf-core/](gndlf-core/)** — Shared database models and utilities
  - `init-db` command: Initialize database schema
  - `delete-db` command: Remove database tables
- **[gndlf-pipeline/](gndlf-pipeline/)** — CLI tools for data ingestion, chunking, and embedding
  - `wiki` command: Crawl Tolkien Gateway and ingest text sources
  - `pipeline` command: Clear and/or run chunking + embedding
- **[gndlf-workflow/](gndlf-workflow/)** — FastAPI retrieval and search service (coming soon)

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

4. **Initialize database:**
   ```bash
   # Initialize database schema
   uv run gndlf-core init-db

   # Or target specific environment
   uv run gndlf-core init-db --target dev
   ```

5. **Run the pipeline (three sub-steps):**
   ```bash
   # 5a. Crawl wiki and ingest text data
   uv run gndlf-pipeline wiki

   # 5b. Clear old chunks/embeddings (optional)
   uv run gndlf-pipeline pipeline --clear

   # 5c. Chunk + embed
   uv run gndlf-pipeline pipeline --run

   # (Or clear and rebuild in one command)
   uv run gndlf-pipeline pipeline --clear --run
   ```

See [gndlf-pipeline/README.md](gndlf-pipeline/README.md) for detailed CLI documentation.

## Database

- **Backend:** PostgreSQL
- **ORM:** Peewee
- **Container:** `redbook-postgress` (via `docker-compose.postgres.yml`)

### Database Commands

Initialize database schema (required before running the pipeline):

```bash
uv run gndlf-core init-db
```

Initialize for a specific environment:

```bash
uv run gndlf-core init-db --target dev
uv run gndlf-core init-db --target prd
```

Remove all database tables:

```bash
uv run gndlf-core delete-db
```

See [gndlf-core/README.md](gndlf-core/README.md) for full database documentation.

## Vector Store

- **Backend:** Qdrant
- **Collection:** `gwaihir_chunks` (hybrid: dense + sparse)
- **Container:** `redbook-qdrant` (via `docker-compose.qdrant.yml`)

## Embedding Model

Default: `google/embeddinggemma-300m` (requires Hugging Face authentication)

```bash
uv run huggingface-cli login
# or export HF_TOKEN=<your_token>
```

See [gndlf-pipeline/README.md](gndlf-pipeline/README.md) for full setup details.
