from __future__ import annotations

import sqlite3
from pathlib import Path

import click
import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql

load_dotenv()

TABLES_IN_ORDER = [
    'index',
    'document',
    'wiki_page',
    'text',
    'chunks',
]


def _sqlite_row_count(connection: sqlite3.Connection, table_name: str) -> int:
    row = connection.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()
    return int(row[0]) if row else 0


def _sqlite_columns(connection: sqlite3.Connection, table_name: str) -> list[str]:
    rows = connection.execute(f'PRAGMA table_info("{table_name}")').fetchall()
    return [row[1] for row in rows]


def _postgres_columns(connection: psycopg2.extensions.connection, table_name: str) -> list[str]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        rows = cursor.fetchall()
    return [row[0] for row in rows]


def _copy_table(
    sqlite_connection: sqlite3.Connection,
    postgres_connection: psycopg2.extensions.connection,
    table_name: str,
) -> int:
    sqlite_cols = _sqlite_columns(sqlite_connection, table_name)
    postgres_cols = _postgres_columns(postgres_connection, table_name)
    common_cols = [column for column in sqlite_cols if column in postgres_cols]

    if not common_cols:
        return 0

    select_query = f'SELECT {", ".join(common_cols)} FROM "{table_name}"'
    rows = sqlite_connection.execute(select_query).fetchall()
    if not rows:
        return 0

    placeholders = sql.SQL(', ').join(sql.Placeholder() for _ in common_cols)
    insert_query = sql.SQL('INSERT INTO {table} ({columns}) VALUES ({values})').format(
        table=sql.Identifier(table_name),
        columns=sql.SQL(', ').join(sql.Identifier(column) for column in common_cols),
        values=placeholders,
    )

    with postgres_connection.cursor() as cursor:
        cursor.executemany(insert_query.as_string(postgres_connection), rows)

    return len(rows)


@click.command()
@click.option(
    '--sqlite-path',
    type=click.Path(path_type=Path, dir_okay=False),
    default=Path('..') / 'database' / 'redbook.db',
    show_default=True,
    help='Path to SQLite database file.',
)
@click.option(
    '--postgres-url',
    envvar='PRD_DATABASE_URL',
    default=None,
    help='PostgreSQL connection URL. Defaults to PRD_DATABASE_URL.',
)
def main(sqlite_path: Path, postgres_url: str) -> None:
    """Migrate data from SQLite to PostgreSQL."""
    if not sqlite_path.exists() or not sqlite_path.is_file():
        raise click.UsageError(f'SQLite file does not exist: {sqlite_path}')

    if not postgres_url:
        raise click.UsageError('Missing PostgreSQL URL. Provide --postgres-url or set PRD_DATABASE_URL.')

    sqlite_connection = sqlite3.connect(str(sqlite_path))
    postgres_connection = psycopg2.connect(postgres_url)

    try:
        total_sqlite_rows = 0
        total_copied_rows = 0
        for table_name in TABLES_IN_ORDER:
            sqlite_rows = _sqlite_row_count(sqlite_connection, table_name)
            copied = _copy_table(sqlite_connection, postgres_connection, table_name)
            click.echo(f'{table_name}: sqlite_rows={sqlite_rows}, copied_rows={copied}')
            total_sqlite_rows += sqlite_rows
            total_copied_rows += copied

        postgres_connection.commit()
        click.echo(f'Migration complete. Total sqlite_rows={total_sqlite_rows}, total_copied_rows={total_copied_rows}')
    finally:
        sqlite_connection.close()
        postgres_connection.close()


if __name__ == '__main__':
    main()
