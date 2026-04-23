# Lembas Core

Shared library for Lembas Service and Gwaihir Pipeline.

## CLI

`gndlf-core` includes a small Click CLI for database lifecycle tasks.

Show commands:

```bash
uv run gndlf-core --help
```

### Initialize Database

Creates tables for production or development database.

**Production** (uses `DATABASE_URL` env var):

```bash
uv run gndlf-core init-db
```

**Development** (uses `DEV_DATABASE_URL` env var):

```bash
uv run gndlf-core init-db --dev
```

### Delete Database

Deletes all app tables: `document`, `wiki_page`, `text`, `chunks`.

**Production**:

```bash
uv run gndlf-core delete-db
```

**Development**:

```bash
uv run gndlf-core delete-db --dev
```
