import json
from pathlib import Path

import polars as pl

from leystop.pipeline import Comuna, build, comunas_frame, semanas_frame, to_registros, to_resumen

FIX = Path(__file__).parent / "fixtures"
SEMANAS = [
    {"id": 1, "nombre": "SEMANA 01/2023 (del 01/01/2023 al 01/01/2023)", "anno": 2023, "semana": "SEMANA 01",
     "fecha_desde_iso": "2023-01-01", "fecha_hasta_iso": "2023-01-01"},
    {"id": 199, "nombre": "SEMANA 40/2026 (del 28/09/2026 al 04/10/2026)", "anno": 2026, "semana": "SEMANA 40",
     "fecha_desde_iso": "2026-09-28", "fecha_hasta_iso": "2026-10-04"},
]
COMUNAS = [Comuna(13, "REGIÓN METROPOLITANA DE SANTIAGO", 131, "SANTIAGO", 13114, "LAS CONDES")]


def _records(sid):
    data = json.loads((FIX / f"estadistica_{sid}_13114.json").read_text())
    return [{"semana_id": sid, "comuna_id": 13114, "fetched_at": "2026-10-12T15:00:00+00:00", "data": data}]


def test_registros_long_format():
    df = to_registros(_records(199), semanas_frame(SEMANAS[1:]), comunas_frame(COMUNAS))
    assert df.height == 21
    assert df["ultima_semana"].sum() == 153  # equals casos_ultima_semana in the response
    row = df.filter(pl.col("categoria") == "HOMICIDIOS Y FEMICIDIOS").row(0, named=True)
    assert row["comuna"] == "LAS CONDES" and row["anno"] == 2026 and row["semana"] == 40
    assert row["anno_a_la_fecha"] == 2 and row["umbral_casos"] is not None


def test_resumen_and_build(tmp_path):
    from leystop.pipeline import PREFIX, _parquet_bytes
    from leystop.storage import LocalStore

    store = LocalStore(tmp_path)
    cdf = comunas_frame(COMUNAS)
    for s in SEMANAS:
        recs = _records(s["id"])
        wk = semanas_frame([s])
        store.put(f"{PREFIX}weekly/registros/semana_id={s['id']:04d}.parquet", _parquet_bytes(to_registros(recs, wk, cdf)), "x")
        store.put(f"{PREFIX}weekly/resumen/semana_id={s['id']:04d}.parquet", _parquet_bytes(to_resumen(recs, wk, cdf)), "x")
    store.put(f"{PREFIX}semanas.parquet", _parquet_bytes(semanas_frame(SEMANAS)), "x")

    m = build(store, "https://data.example.org")
    assert m["weeks_scraped"] == 2 and m["weeks_missing"] == []
    res = pl.read_parquet(tmp_path / PREFIX / "resumen.parquet")
    assert res.height == 2
    assert res.schema["decomisos_anno_a_la_fecha"] == pl.Float64
    assert res.sort("semana_id")["casos_ultima_semana"].to_list() == [147, 153]
    reg = pl.read_parquet(tmp_path / PREFIX / "registros.parquet")
    assert reg.height == 42


class FakeClient:
    """Serves fixture data for every comuna; raises Blocked after `block_after` stats calls."""

    def __init__(self, block_after=None):
        self.block_after = block_after
        self.stats_calls = 0
        self.week = json.loads((FIX / "estadistica_199_13114.json").read_text())

    def get_json(self, path):
        from leystop.client import Blocked

        if path == "/api/semanas":
            return [{**s, "fecha_desde": "", "fecha_hasta": ""} for s in SEMANAS]
        if path == "/api/regiones":
            return [{"id": 13, "nombre": "RM"}]
        if path == "/api/regiones/13/provincias":
            return [{"id": 131, "nombre": "SANTIAGO"}]
        if path == "/api/provincias/131/comunas":
            return [{"id": i, "nombre": f"C{i}"} for i in range(13101, 13131)]
        assert path.startswith("/api/estadistica/") and "/COMUNA/" in path, path
        if self.block_after is not None and self.stats_calls >= self.block_after:
            raise Blocked("captcha")
        self.stats_calls += 1
        return self.week


def test_scrape_checkpoints_and_resumes(tmp_path):
    import pytest

    from leystop.client import Blocked
    from leystop.pipeline import PREFIX, build, scrape, scraped_weeks
    from leystop.storage import LocalStore

    store = LocalStore(tmp_path)
    with pytest.raises(Blocked):
        scrape(FakeClient(block_after=40), store, time_budget_s=3600)
    # Newest week first: 30 comunas of week 199 done, then 10 of week 1 checkpointed.
    assert scraped_weeks(store) == {199}
    assert store.list(f"{PREFIX}raw/_partial/") == [f"{PREFIX}raw/_partial/semana_id=0001.jsonl.gz"]

    resumed = FakeClient()
    assert scrape(resumed, store, time_budget_s=3600) == 1
    assert resumed.stats_calls == 20  # only the comunas missing from the checkpoint
    assert scraped_weeks(store) == {1, 199}
    assert store.list(f"{PREFIX}raw/_partial/") == []
    m = build(store)
    assert m["tables"]["registros"]["rows"] == 2 * 30 * 21
