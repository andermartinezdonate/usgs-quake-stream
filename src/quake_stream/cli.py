"""CLI entrypoint for quake-stream."""

from __future__ import annotations

import click
from rich.console import Console
from rich.table import Table

from quake_stream.usgs_client import fetch_earthquakes

console = Console()


@click.group()
def cli():
    """USGS Quake Stream — real-time earthquake data pipeline."""


@cli.command()
@click.option("--period", default="hour", type=click.Choice(["hour", "day", "week", "significant"]))
@click.option("--min-mag", default=0.0, help="Minimum magnitude filter.")
@click.option("--limit", default=20, help="Max results to display.")
def recent(period: str, min_mag: float, limit: int):
    """Show recent earthquakes from USGS (no Kafka required)."""
    quakes = fetch_earthquakes(period=period, min_magnitude=min_mag)

    table = Table(title=f"Recent Earthquakes ({period})")
    table.add_column("Mag", style="bold", width=5)
    table.add_column("Place")
    table.add_column("Depth (km)", justify="right")
    table.add_column("Time (UTC)")

    for q in quakes[:limit]:
        color = "red" if q.magnitude >= 5.0 else "yellow" if q.magnitude >= 3.0 else "green"
        table.add_row(
            f"[{color}]{q.magnitude:.1f}[/]",
            q.place,
            f"{q.depth:.1f}",
            f"{q.time:%Y-%m-%d %H:%M}",
        )

    console.print(table)


@cli.command()
@click.option("--period", default="hour", type=click.Choice(["hour", "day", "week", "significant"]))
@click.option("--min-mag", default=0.0, help="Minimum magnitude filter.")
@click.option("--limit", default=25, help="Max results to display.")
@click.option("--refresh", default=30, help="Refresh interval in seconds.")
def dashboard(period: str, min_mag: float, limit: int, refresh: int):
    """Live auto-refreshing earthquake dashboard (no Kafka required)."""
    from quake_stream.dashboard import run_dashboard
    run_dashboard(period=period, min_magnitude=min_mag, limit=limit, refresh=refresh)


@cli.command()
@click.option("--broker", default="localhost:9092", help="Kafka bootstrap servers.")
@click.option("--period", default="hour", type=click.Choice(["hour", "day", "week", "significant"]))
@click.option("--interval", default=60, help="Polling interval in seconds.")
@click.option("--min-mag", default=0.0, help="Minimum magnitude filter.")
def produce(broker: str, period: str, interval: int, min_mag: float):
    """Start the Kafka producer (polls USGS and publishes events)."""
    from quake_stream.producer import run_producer
    run_producer(bootstrap_servers=broker, period=period, interval=interval, min_magnitude=min_mag)


@cli.command()
@click.option("--broker", default="localhost:9092", help="Kafka bootstrap servers.")
@click.option("--group", default="quake-display", help="Consumer group ID.")
def consume(broker: str, group: str):
    """Start the Kafka consumer (reads and displays events)."""
    from quake_stream.consumer import run_consumer
    run_consumer(bootstrap_servers=broker, group_id=group)


@cli.command("db-consumer")
@click.option("--broker", default="localhost:9092", help="Kafka bootstrap servers.")
@click.option("--group", default="quake-db-writer", help="Consumer group ID.")
def db_consumer(broker: str, group: str):
    """Start the DB consumer (Kafka → PostgreSQL)."""
    from quake_stream.db_consumer import run_db_consumer
    run_db_consumer(bootstrap_servers=broker, group_id=group)


@cli.command("web")
@click.option("--port", default=8501, help="Streamlit port.")
def web(port: int):
    """Launch the Streamlit web dashboard."""
    import subprocess
    import sys
    subprocess.run([
        sys.executable, "-m", "streamlit", "run",
        "src/quake_stream/dashboard_web.py",
        "--server.port", str(port),
        "--theme.base", "dark",
        "--server.headless", "true",
    ])
