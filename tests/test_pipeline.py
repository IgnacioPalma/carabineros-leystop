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
