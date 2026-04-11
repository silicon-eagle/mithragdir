from __future__ import annotations

import os

import click
from dotenv import load_dotenv

from core.db import RedbookDatabase

load_dotenv()


@click.group()
def cli() -> None:
    """gndlf-core database utilities."""


def _resolve_db_url(dev: bool) -> str:
    env_var = 'DEV_DATABASE_URL' if dev else 'DATABASE_URL'
    db_url = os.getenv(env_var)
    if not db_url:
        raise click.UsageError(f'Missing database URL. Set {env_var}.')
    return db_url


@cli.command('init-db')
@click.option(
    '--dev',
    is_flag=True,
    help='Use DEV_DATABASE_URL.',
)
def init_db(dev: bool) -> None:
    """Initialize database tables."""
    resolved_db_url = _resolve_db_url(dev)

    db = RedbookDatabase(db_url=resolved_db_url)
    db.deploy()
    db.close()

    target_name = 'dev' if dev else 'prd'
    click.echo(f'Database initialized for {target_name}.')


@cli.command('delete-db')
@click.option(
    '--dev',
    is_flag=True,
    help='Use DEV_DATABASE_URL.',
)
def delete_db(dev: bool) -> None:
    """Delete all application tables from PostgreSQL."""
    resolved_db_url = _resolve_db_url(dev)

    db = RedbookDatabase(db_url=resolved_db_url)
    db.delete_all_tables()
    db.close()

    target_name = 'dev' if dev else 'prd'
    click.echo(f'Dropped database tables for {target_name}: chunks, text, wiki_page, page_index, document')


if __name__ == '__main__':
    cli()
