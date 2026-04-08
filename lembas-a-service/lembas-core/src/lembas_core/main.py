from __future__ import annotations

import os

import click
from dotenv import load_dotenv

from lembas_core.db import RedbookDatabase

load_dotenv()


@click.group()
def cli() -> None:
    """lembas-core database utilities."""


def _resolve_db_url(target: str) -> str:
    env_var = "PRD_DATABASE_URL" if target == "prd" else "DEV_DATABASE_URL"
    db_url = os.getenv(env_var)
    if not db_url:
        raise click.UsageError(f"Missing database URL. Set {env_var}.")
    return db_url


@cli.command("init-db")
@click.option(
    "--target",
    type=click.Choice(["prd", "dev"], case_sensitive=False),
    default="prd",
    show_default=True,
    help="Database target environment.",
)
def init_db(target: str) -> None:
    """Initialize database tables."""
    normalized_target = target.lower()
    resolved_db_url = _resolve_db_url(normalized_target)

    db = RedbookDatabase(db_url=resolved_db_url)
    db.deploy()
    db.close()

    click.echo(f"Database initialized for {normalized_target}.")


@cli.command("delete-db")
@click.option(
    "--target",
    type=click.Choice(["prd", "dev"], case_sensitive=False),
    default="prd",
    show_default=True,
    help="Database target environment.",
)
def delete_db(target: str) -> None:
    """Delete all application tables from PostgreSQL."""
    normalized_target = target.lower()
    resolved_db_url = _resolve_db_url(normalized_target)

    db = RedbookDatabase(db_url=resolved_db_url)
    db.delete_all_tables()
    db.close()

    click.echo(
        f"Dropped database tables for {normalized_target}: chunks, text, wiki_page, index, document"
    )


if __name__ == "__main__":
    cli()
