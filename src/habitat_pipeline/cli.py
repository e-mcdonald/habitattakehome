"""Typer CLI — the container entrypoint.

Subcommands:
    migrate       Apply sql/schema.sql
    run           Execute one stage or the whole pipeline for a source
    demo-offline  Feed tests/fixtures/sample_records.json through the pipeline
    report        Dump ops.pipeline_runs and ops.freshness
    sources       List discoverable sources
"""

from __future__ import annotations

import json
from enum import StrEnum
from pathlib import Path
from typing import Annotated

import typer

from habitat_pipeline.config import get_settings
from habitat_pipeline.extract import run_extract
from habitat_pipeline.load import connect, run_load_raw
from habitat_pipeline.load.runner import LoadResult
from habitat_pipeline.logging import configure_logging, get_logger
from habitat_pipeline.observability import fetch_freshness, fetch_recent_runs
from habitat_pipeline.sources import register_builtin_extractors
from habitat_pipeline.sources.registry import list_sources, load_source
from habitat_pipeline.transform import run_transform
from habitat_pipeline.validate import run_validate

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Habitat DFR auction pipeline.",
)
log = get_logger(__name__)


class Stage(StrEnum):
    EXTRACT = "extract"
    LOAD_RAW = "load-raw"
    VALIDATE = "validate"
    TRANSFORM = "transform"


@app.callback()
def _bootstrap() -> None:
    """Global setup: logging + built-in extractor registration."""
    settings = get_settings()
    configure_logging(env=settings.pipeline_env)
    register_builtin_extractors()


@app.command()
def migrate() -> None:
    """Apply sql/schema.sql to the target database."""
    settings = get_settings()
    schema_path = settings.pipeline_sql_dir / "schema.sql"
    if not schema_path.exists():
        typer.echo(f"schema not found: {schema_path}", err=True)
        raise typer.Exit(1)

    with connect(settings.database_url) as conn, conn.cursor() as cur:
        cur.execute(schema_path.read_text(encoding="utf-8"))
        conn.commit()
    log.info("migrate.applied", path=str(schema_path))


@app.command()
def sources() -> None:
    """List discoverable source configs."""
    settings = get_settings()
    for name in list_sources(settings.pipeline_sources_dir):
        typer.echo(name)


@app.command()
def run(
    source: Annotated[str, typer.Option(help="Source name (must exist under sources/).")],
    stage: Annotated[Stage | None, typer.Option(help="Single stage to run.")] = None,
    all_: Annotated[bool, typer.Option("--all", help="Run every stage in order.")] = False,
    limit: Annotated[
        int | None,
        typer.Option(help="Cap rows during extract (used by --all --limit for demos)."),
    ] = None,
) -> None:
    """Execute the pipeline for a single source."""
    if not stage and not all_:
        typer.echo("either --stage or --all is required", err=True)
        raise typer.Exit(2)

    settings = get_settings()
    cfg = load_source(settings.pipeline_sources_dir, source)

    with connect(settings.database_url) as conn:
        landing_path = _latest_landing_path(settings.pipeline_data_dir, cfg.name)

        if all_ or stage == Stage.EXTRACT:
            result = run_extract(
                conn=conn,
                source=cfg,
                data_dir=settings.pipeline_data_dir,
                limit=limit,
            )
            landing_path = result.landing_path
            typer.echo(f"extract: rows_written={result.rows_written} landing={result.landing_path}")

        if all_ or stage == Stage.LOAD_RAW:
            if landing_path is None:
                typer.echo("no landing JSONL available; run --stage extract first", err=True)
                raise typer.Exit(2)
            load_result: LoadResult = run_load_raw(conn=conn, source=cfg, landing_path=landing_path)
            typer.echo(
                f"load-raw: read={load_result.rows_read} inserted={load_result.rows_written}"
            )

        if all_ or stage == Stage.VALIDATE:
            v = run_validate(conn=conn, source=cfg)
            typer.echo(
                f"validate: read={v.rows_read} written={v.rows_written} rejected={v.rows_rejected}"
            )

        if all_ or stage == Stage.TRANSFORM:
            t = run_transform(conn=conn, source_name=cfg.name, sql_dir=settings.pipeline_sql_dir)
            typer.echo(f"transform: applied={t.files_applied}")


@app.command("demo-offline")
def demo_offline(
    source: Annotated[str, typer.Option(help="Source name.")] = "neso_dfr_results",
    fixture: Annotated[
        Path,
        typer.Option(help="Path to the fixture JSON file."),
    ] = Path("tests/fixtures/sample_records.json"),
) -> None:
    """Run load-raw -> validate -> transform against a fixture. No network."""
    settings = get_settings()
    cfg = load_source(settings.pipeline_sources_dir, source)

    if not fixture.exists():
        typer.echo(f"fixture not found: {fixture}", err=True)
        raise typer.Exit(2)

    # Convert the fixture (a JSON array) into a JSONL landing file that
    # load-raw understands unchanged.
    landing_dir = settings.pipeline_data_dir / "landing" / cfg.name
    landing_dir.mkdir(parents=True, exist_ok=True)
    landing_path = landing_dir / "offline-demo.jsonl"
    records = json.loads(fixture.read_text(encoding="utf-8"))
    with landing_path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record))
            fh.write("\n")
    typer.echo(f"offline demo: staged {len(records)} rows at {landing_path}")

    with connect(settings.database_url) as conn:
        load_result = run_load_raw(conn=conn, source=cfg, landing_path=landing_path)
        typer.echo(f"load-raw: read={load_result.rows_read} inserted={load_result.rows_written}")
        v = run_validate(conn=conn, source=cfg)
        typer.echo(
            f"validate: read={v.rows_read} written={v.rows_written} rejected={v.rows_rejected}"
        )
        t = run_transform(conn=conn, source_name=cfg.name, sql_dir=settings.pipeline_sql_dir)
        typer.echo(f"transform: applied={t.files_applied}")


@app.command()
def report() -> None:
    """Dump ops.pipeline_runs (last 20) and ops.freshness."""
    settings = get_settings()
    with connect(settings.database_url) as conn:
        runs = fetch_recent_runs(conn, limit=20)
        freshness = fetch_freshness(conn)

    typer.echo("=== ops.pipeline_runs (last 20) ===")
    typer.echo(json.dumps(runs, default=str, indent=2))
    typer.echo("")
    typer.echo("=== ops.freshness ===")
    typer.echo(json.dumps(freshness, default=str, indent=2))


def _latest_landing_path(data_dir: Path, source_name: str) -> Path | None:
    """Best-effort: locate the most recently written landing JSONL for a source."""
    landing_dir = data_dir / "landing" / source_name
    if not landing_dir.exists():
        return None
    files = sorted(landing_dir.glob("*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


if __name__ == "__main__":
    app()
