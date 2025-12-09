from typing import Annotated, Optional

import typer
from rich import print
from rich.console import Console

from flydelta import __version__

cli = typer.Typer(no_args_is_help=True)
console = Console(stderr=True)


@cli.callback(invoke_without_command=True)
def cli_main(
    version: Annotated[
        Optional[bool], typer.Option(..., help="Show version")
    ] = False,
):
    if version:
        print(__version__)
        raise typer.Exit()


@cli.command("serve")
def cli_serve(
    host: Annotated[str, typer.Option("--host", "-h", help="Host to bind to")] = "0.0.0.0",
    port: Annotated[int, typer.Option("--port", "-p", help="Port to bind to")] = 8815,
    table: Annotated[
        Optional[list[str]],
        typer.Option("--table", "-t", help="Table in format name=uri"),
    ] = None,
    pool_size: Annotated[
        int, typer.Option("--pool-size", help="DuckDB connection pool size")
    ] = 10,
    batch_size: Annotated[
        int, typer.Option("--batch-size", help="Rows per batch when streaming")
    ] = 100_000,
):
    """
    Start the flydelta Flight SQL server.

    Example:
        flydelta serve -t users=s3://bucket/users -t orders=/data/orders
    """
    from flydelta.server import serve

    tables = {}
    if table:
        for t in table:
            if "=" not in t:
                console.print(f"[red]Invalid table format: {t}[/red]")
                console.print("Use: name=uri (e.g., users=s3://bucket/users)")
                raise typer.Exit(1)
            name, uri = t.split("=", 1)
            tables[name] = uri

    if not tables:
        console.print("[yellow]Warning: No tables registered[/yellow]")

    console.print(f"[green]Starting flydelta on grpc://{host}:{port}[/green]")
    console.print(f"[green]Connection pool size: {pool_size}, batch size: {batch_size}[/green]")
    for name, uri in tables.items():
        console.print(f"  [blue]{name}[/blue] -> {uri}")

    serve(host=host, port=port, tables=tables, pool_size=pool_size, batch_size=batch_size)


@cli.command("query")
def cli_query(
    sql: Annotated[str, typer.Argument(help="SQL query to execute")],
    host: Annotated[str, typer.Option("--host", "-h", help="Server host")] = "localhost",
    port: Annotated[int, typer.Option("--port", "-p", help="Server port")] = 8815,
    output: Annotated[str, typer.Option("--output", "-o", help="Output format")] = "table",
):
    """
    Execute a SQL query against flydelta server.

    Example:
        flydelta query "SELECT * FROM users LIMIT 10"
    """
    from flydelta.client import Client

    location = f"grpc://{host}:{port}"

    with Client(location) as client:
        result = client.query(sql)

        if output == "table":
            console.print(result.to_pandas().to_string())
        elif output == "json":
            print(result.to_pandas().to_json(orient="records"))
        elif output == "csv":
            print(result.to_pandas().to_csv(index=False))
        else:
            console.print(f"[red]Unknown output format: {output}[/red]")
            raise typer.Exit(1)


@cli.command("tables")
def cli_tables(
    host: Annotated[str, typer.Option("--host", "-h", help="Server host")] = "localhost",
    port: Annotated[int, typer.Option("--port", "-p", help="Server port")] = 8815,
):
    """List available tables on flydelta server."""
    from flydelta.client import Client

    location = f"grpc://{host}:{port}"

    with Client(location) as client:
        tables = client.list_tables()
        if tables:
            for t in tables:
                console.print(f"  [blue]{t}[/blue]")
        else:
            console.print("[yellow]No tables registered[/yellow]")
