import asyncio

import click
from rich.console import Console
from rich.panel import Panel

from workflow.run import extract_answer, run_graph

console = Console()


@click.command()
@click.argument('query', type=str)
def cli(query: str) -> None:
    """Run one workflow graph execution for a single query."""
    try:
        state = asyncio.run(run_graph(query))
    except ValueError as exc:
        raise click.BadParameter(str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(str(exc)) from exc

    answer = extract_answer(state)
    console.print(
        Panel(
            answer,
            title='Gndlf Workflow Answer',
            border_style='cyan',
            padding=(1, 2),
        )
    )


if __name__ == '__main__':
    cli()
