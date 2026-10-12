"""Scrape weekly comuna statistics and publish them as Parquet.

Bucket layout (everything under PREFIX):

    raw/semana_id=0199.jsonl.gz            untouched API responses for a finished week
    raw/_partial/semana_id=0199.jsonl.gz   checkpoint of a week still being scraped
    weekly/registros/semana_id=0199.parquet
    weekly/resumen/semana_id=0199.parquet
    registros.parquet   all weeks x comunas x crime categories (long format)
    resumen.parquet     all weeks x comunas, one row each with every scalar indicator
    semanas.parquet     week calendar used by the source
    comunas.parquet     region / provincia / comuna catalogue (CUT codes)
    manifest.json       what is in the bucket and when it was last updated
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime

import polars as pl

from .client import Blocked, Client
from .storage import Store

PREFIX = "v1/"
PARQUET = "application/vnd.apache.parquet"
SHORT_CACHE = "public, max-age=3600"

log = logging.getLogger(__name__)


class BudgetExhausted(Exception):
    pass


@dataclass(frozen=True)
class Comuna:
    region_id: int
    region: str
    provincia_id: int
    provincia: str
    comuna_id: int
    comuna: str


# --------------------------------------------------------------------------- catalogue


def fetch_semanas(client: Client) -> list[dict]:
    semanas = client.get_json("/api/semanas")
    return sorted(semanas, key=lambda s: s["id"])


def fetch_comunas(client: Client) -> list[Comuna]:
    out: list[Comuna] = []
    for r in client.get_json("/api/regiones"):
        for p in client.get_json(f"/api/regiones/{r['id']}/provincias"):
            for c in client.get_json(f"/api/provincias/{p['id']}/comunas"):
                out.append(Comuna(r["id"], r["nombre"], p["id"], p["nombre"], c["id"], c["nombre"]))
    out.sort(key=lambda c: c.comuna_id)
    log.info("catalogue: %d comunas", len(out))
    return out


def semanas_frame(semanas: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "semana_id": [s["id"] for s in semanas],
            "anno": [s["anno"] for s in semanas],
            "semana": [int(s["semana"].split()[-1]) for s in semanas],
            "fecha_desde": [date.fromisoformat(s["fecha_desde_iso"]) for s in semanas],
            "fecha_hasta": [date.fromisoformat(s["fecha_hasta_iso"]) for s in semanas],
            "nombre": [s["nombre"] for s in semanas],
        },
        schema={
            "semana_id": pl.Int32,
            "anno": pl.Int16,
            "semana": pl.Int8,
            "fecha_desde": pl.Date,
            "fecha_hasta": pl.Date,
            "nombre": pl.String,
        },
    )


def comunas_frame(comunas: list[Comuna]) -> pl.DataFrame:
    return pl.DataFrame(
        [c.__dict__ for c in comunas],
        schema={
            "region_id": pl.Int16,
            "region": pl.String,
            "provincia_id": pl.Int16,
            "provincia": pl.String,
            "comuna_id": pl.Int32,
            "comuna": pl.String,
        },
    )


# --------------------------------------------------------------------------- transform

REGISTRO_COUNTS = [
    "ultima_semana",
    "ultima_semana_anterior",
    "ultimos_28_dias",
    "ultimos_28_dias_anterior",
    "anno_a_la_fecha",
    "anno_a_la_fecha_anterior",
]


def _num(v: object) -> int | float | None:
    # The source uses "N.D." (no disponible) where a number is not available.
    return v if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def _key_columns(week: pl.DataFrame, comunas: pl.DataFrame, df: pl.DataFrame) -> pl.DataFrame:
    keys = week.select("semana_id", "anno", "semana", "fecha_desde", "fecha_hasta")
    return df.join(keys, on="semana_id", how="left").join(comunas, on="comuna_id", how="left")


def to_registros(records: list[dict], week: pl.DataFrame, comunas: pl.DataFrame) -> pl.DataFrame:
    rows = []
    for rec in records:
        data = rec["data"]
        umbrales = data.get("casos_umbrales") or {}
        for r in data.get("registros") or []:
            row = {"semana_id": rec["semana_id"], "comuna_id": rec["comuna_id"], "categoria": r["nombre"]}
            row |= {k: _num(r.get(k)) for k in REGISTRO_COUNTS}
            row["umbral"] = _num(r.get("umbral"))
            row["umbral_casos"] = _num(umbrales.get(r["nombre"]))
            rows.append(row)
    schema = {"semana_id": pl.Int32, "comuna_id": pl.Int32, "categoria": pl.String}
    schema |= {k: pl.Int32 for k in REGISTRO_COUNTS}
    schema |= {"umbral": pl.Float64, "umbral_casos": pl.Float64}
    df = pl.DataFrame(rows, schema=schema)
    df = _key_columns(week, comunas, df)
    lead = ["semana_id", "anno", "semana", "fecha_desde", "fecha_hasta", *comunas.columns, "categoria"]
    return df.select(lead + [c for c in df.columns if c not in lead]).sort("comuna_id", "categoria")


def to_resumen(records: list[dict], week: pl.DataFrame, comunas: pl.DataFrame) -> pl.DataFrame:
    rows = []
    for rec in records:
        scalars = {
            k: None if v == "N.D." else v for k, v in rec["data"].items() if not isinstance(v, (dict, list))
        }
        rows.append({"semana_id": rec["semana_id"], "comuna_id": rec["comuna_id"], **scalars, "fetched_at": rec["fetched_at"]})
    df = pl.DataFrame(rows, infer_schema_length=None).with_columns(
        pl.col("semana_id").cast(pl.Int32),
        pl.col("comuna_id").cast(pl.Int32),
        pl.col("fetched_at").str.to_datetime(time_zone="UTC"),
        # Quantities like "decomisos" (kg) come as int 0 or float; keep them all float.
        pl.col(pl.Int64).exclude("semana_id", "comuna_id").cast(pl.Float64),
    )
    df = _key_columns(week, comunas, df)
    lead = ["semana_id", "anno", "semana", "fecha_desde", "fecha_hasta", *comunas.columns]
    return df.select(lead + [c for c in df.columns if c not in lead]).sort("comuna_id")


# --------------------------------------------------------------------------- io helpers


def _parquet_bytes(df: pl.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.write_parquet(buf, compression="zstd", compression_level=10, statistics=True)
    return buf.getvalue()


def _jsonl_gz(records: list[dict]) -> bytes:
    lines = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in records)
    return gzip.compress(lines.encode(), compresslevel=9)


def _read_jsonl_gz(blob: bytes | None) -> list[dict]:
    if not blob:
        return []
    return [json.loads(line) for line in gzip.decompress(blob).decode().splitlines() if line]


def _week_key(kind: str, semana_id: int, ext: str) -> str:
    return f"{PREFIX}{kind}/semana_id={semana_id:04d}.{ext}"


def scraped_weeks(store: Store) -> set[int]:
    keys = store.list(f"{PREFIX}weekly/registros/")
    return {int(k.rsplit("=", 1)[1].split(".")[0]) for k in keys}


# --------------------------------------------------------------------------- scrape


def scrape_week(
    client: Client,
    store: Store,
    semana: dict,
    comunas: list[Comuna],
    *,
    deadline: float,
    checkpoint_every: int = 25,
) -> None:
    sid = semana["id"]
    partial_key = _week_key("raw/_partial", sid, "jsonl.gz")
    done = {r["comuna_id"]: r for r in _read_jsonl_gz(store.get(partial_key))}
    if done:
        log.info("week %d: resuming from checkpoint (%d/%d comunas)", sid, len(done), len(comunas))

    def checkpoint() -> None:
        store.put(partial_key, _jsonl_gz(list(done.values())), "application/gzip")

    started, fetched = time.monotonic(), 0
    try:
        for c in comunas:
            if c.comuna_id in done:
                continue
            if time.time() > deadline:
                raise BudgetExhausted
            data = client.get_json(f"/api/estadistica/{sid}/COMUNA/{c.comuna_id}")
            done[c.comuna_id] = {
                "semana_id": sid,
                "comuna_id": c.comuna_id,
                "fetched_at": datetime.now(UTC).isoformat(timespec="seconds"),
                "data": data,
            }
            fetched += 1
            if fetched % checkpoint_every == 0:
                checkpoint()
                rate = (time.monotonic() - started) / fetched
                log.info("week %d: %d/%d comunas (%.1fs/request)", sid, len(done), len(comunas), rate)
    except BaseException:
        if fetched:
            checkpoint()
        raise

    records = [done[c.comuna_id] for c in comunas]
    week = semanas_frame([semana])
    cdf = comunas_frame(comunas)
    store.put(_week_key("raw", sid, "jsonl.gz"), _jsonl_gz(records), "application/gzip")
    store.put(_week_key("weekly/registros", sid, "parquet"), _parquet_bytes(to_registros(records, week, cdf)), PARQUET)
    store.put(_week_key("weekly/resumen", sid, "parquet"), _parquet_bytes(to_resumen(records, week, cdf)), PARQUET)
    store.delete(partial_key)
    log.info("week %d (%s) done in %.0fs", sid, semana["nombre"], time.monotonic() - started)


def scrape(
    client: Client,
    store: Store,
    *,
    time_budget_s: float,
    only_weeks: set[int] | None = None,
    refresh: bool = False,
    max_weeks: int | None = None,
) -> int:
    """Scrape missing weeks, newest first. Returns the number of weeks completed."""
    deadline = time.time() + time_budget_s
    semanas = fetch_semanas(client)
    store.put(f"{PREFIX}semanas.parquet", _parquet_bytes(semanas_frame(semanas)), PARQUET, SHORT_CACHE)

    have = set() if refresh else scraped_weeks(store)
    todo = [s for s in reversed(semanas) if s["id"] not in have and (only_weeks is None or s["id"] in only_weeks)]
    if max_weeks is not None:
        todo = todo[:max_weeks]
    log.info("%d weeks published by source, %d already stored, %d to scrape", len(semanas), len(have), len(todo))
    if not todo:
        return 0

    comunas = fetch_comunas(client)
    store.put(f"{PREFIX}comunas.parquet", _parquet_bytes(comunas_frame(comunas)), PARQUET, SHORT_CACHE)

    completed = 0
    for s in todo:
        try:
            scrape_week(client, store, s, comunas, deadline=deadline)
        except BudgetExhausted:
            log.info("time budget exhausted; progress checkpointed, will resume next run")
            break
        completed += 1
    return completed


# --------------------------------------------------------------------------- consolidate


def build(store: Store, public_base_url: str | None = None) -> dict:
    """Concatenate weekly partitions into the single-file tables and write the manifest."""
    tables: dict[str, dict] = {}
    for table in ("registros", "resumen"):
        keys = store.list(f"{PREFIX}weekly/{table}/")
        if not keys:
            continue
        frames = [pl.read_parquet(io.BytesIO(store.get(k))) for k in keys]
        df = pl.concat(frames, how="diagonal_relaxed").sort("semana_id", "comuna_id")
        blob = _parquet_bytes(df)
        store.put(f"{PREFIX}{table}.parquet", blob, PARQUET, SHORT_CACHE)
        tables[table] = {"rows": df.height, "bytes": len(blob), "columns": dict(zip(df.columns, map(str, df.dtypes)))}
        log.info("built %s.parquet: %d rows, %.1f MB", table, df.height, len(blob) / 1e6)

    weeks = sorted(scraped_weeks(store))
    sem_blob = store.get(f"{PREFIX}semanas.parquet")
    sem = pl.read_parquet(io.BytesIO(sem_blob)) if sem_blob else None
    published = set(sem["semana_id"].to_list()) if sem is not None else set()

    def week_info(i: int) -> dict | None:
        if sem is None:
            return {"semana_id": i}
        row = sem.filter(pl.col("semana_id") == i).to_dicts()
        return {k: str(v) for k, v in row[0].items()} if row else {"semana_id": i}

    base = (public_base_url or "").rstrip("/")
    manifest = {
        "source": "https://leystop.carabineros.cl/estadistica/registros",
        "updated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "weeks_scraped": len(weeks),
        "weeks_published_by_source": len(published),
        "weeks_missing": sorted(published - set(weeks)),
        "first_week": week_info(weeks[0]) if weeks else None,
        "last_week": week_info(weeks[-1]) if weeks else None,
        "files": {
            f"{name}.parquet": (f"{base}/{PREFIX}{name}.parquet" if base else f"{PREFIX}{name}.parquet")
            for name in ("registros", "resumen", "semanas", "comunas")
        },
        "tables": tables,
    }
    store.put(
        f"{PREFIX}manifest.json",
        json.dumps(manifest, ensure_ascii=False, indent=2).encode(),
        "application/json",
        "public, max-age=300",
    )
    return manifest


__all__ = ["Blocked", "BudgetExhausted", "build", "scrape"]
