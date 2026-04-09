# Lembas Core

Shared library for Lembas Service and Gwaihir Pipeline.

## CLI

`gndlf-core` includes a small Click CLI for database lifecycle tasks.

Show commands:

```bash
uv run gndlf-core --help
```

### Initialize Database

Uses `--target` with env vars from `.env`:
- `--target prd` -> `PRD_DATABASE_URL`
- `--target dev` -> `DEV_DATABASE_URL`

```bash
uv run gndlf-core init-db
```

Explicit target example:

```bash
uv run gndlf-core init-db \
	--target dev
```

### Delete Database

Deletes all app tables (`document`, `page_index`, `wiki_page`, `text`, `chunks`).

```bash
uv run gndlf-core delete-db
```

## Examples: PRD and DEV

### Deploy PRD database tables

```bash
uv run gndlf-core init-db --target prd
```

### Deploy DEV database tables

```bash
uv run gndlf-core init-db --target dev
```

### Remove PRD database tables

```bash
uv run gndlf-core delete-db --target prd
```

### Remove DEV database tables

```bash
uv run gndlf-core delete-db --target dev
```
