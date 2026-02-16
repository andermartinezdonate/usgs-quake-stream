"""Live terminal dashboard for earthquake monitoring."""

from __future__ import annotations

import time
from datetime import datetime, timezone

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

from quake_stream.usgs_client import fetch_earthquakes

console = Console()


def _mag_color(mag: float) -> str:
    if mag >= 5.0:
        return "red"
    if mag >= 3.0:
        return "yellow"
    return "green"


def _build_table(period: str, min_magnitude: float, limit: int) -> Table:
    quakes = fetch_earthquakes(period=period, min_magnitude=min_magnitude)

    table = Table(
        title=f"Earthquakes ({period}) — {datetime.now(timezone.utc):%H:%M:%S UTC}",
        expand=True,
    )
    table.add_column("Mag", style="bold", width=6, justify="center")
    table.add_column("Place")
    table.add_column("Depth (km)", justify="right", width=12)
    table.add_column("Time (UTC)", width=18)
    table.add_column("Coords", width=22)

    for q in quakes[:limit]:
        color = _mag_color(q.magnitude)
        table.add_row(
            f"[{color}]{q.magnitude:.1f}[/]",
            q.place,
            f"{q.depth:.1f}",
            f"{q.time:%Y-%m-%d %H:%M}",
            f"{q.latitude:.2f}, {q.longitude:.2f}",
        )

    return table


def _build_stats(period: str, min_magnitude: float) -> Panel:
    quakes = fetch_earthquakes(period=period, min_magnitude=min_magnitude)
    total = len(quakes)
    mag5 = sum(1 for q in quakes if q.magnitude >= 5.0)
    mag3 = sum(1 for q in quakes if 3.0 <= q.magnitude < 5.0)
    mag_low = sum(1 for q in quakes if q.magnitude < 3.0)
    max_q = max(quakes, key=lambda q: q.magnitude) if quakes else None

    lines = [
        f"Total events: [bold]{total}[/]",
        f"[red]M5.0+: {mag5}[/]  [yellow]M3.0-4.9: {mag3}[/]  [green]<M3.0: {mag_low}[/]",
    ]
    if max_q:
        lines.append(f"Strongest: [bold red]M{max_q.magnitude:.1f}[/] — {max_q.place}")

    return Panel("\n".join(lines), title="Summary", border_style="blue")


def run_dashboard(
    period: str = "hour",
    min_magnitude: float = 0.0,
    limit: int = 25,
    refresh: int = 30,
) -> None:
    """Run a live-updating earthquake dashboard in the terminal."""
    layout = Layout()
    layout.split_column(
        Layout(name="stats", size=5),
        Layout(name="table"),
    )

    with Live(layout, console=console, refresh_per_second=1, screen=True) as live:
        while True:
            try:
                layout["stats"].update(_build_stats(period, min_magnitude))
                layout["table"].update(_build_table(period, min_magnitude, limit))
            except Exception as exc:
                layout["stats"].update(Panel(f"[red]Error: {exc}[/]", title="Status"))
            time.sleep(refresh)
