# Carabineros Ley STOP – weekly crime statistics by comuna

Open, machine-readable copy of the weekly crime statistics that Carabineros de Chile
publishes at <https://leystop.carabineros.cl/estadistica/registros>. The site has no public API
and only shows one week and one territory at a time. This repo scrapes **every week × every
comuna** and publishes the result as Parquet on Cloudflare R2. A GitHub Actions job keeps it
up to date.

- **Coverage:** week 1 of 2023 (2023-01-01) to the latest week published by Carabineros.
  It covers every comuna listed on the site and 21 crime categories.
- **Update cadence:** checked every 6 hours. A new week appears shortly after Carabineros
  publishes it (usually early in the following week).
- **Status:** [`manifest.json`](https://pub-e60eb7dae9d6498ea3e5abf4943b68a1.r2.dev/v1/manifest.json)
  lists the weeks already scraped, the weeks still missing, and the last update time.

## Download

Base URL: `https://pub-e60eb7dae9d6498ea3e5abf4943b68a1.r2.dev/v1/`

| File | One row per | Description |
|---|---|---|
| [`registros.parquet`](https://pub-e60eb7dae9d6498ea3e5abf4943b68a1.r2.dev/v1/registros.parquet) | week × comuna × crime category | Main table: case counts per category (long format) |
| [`resumen.parquet`](https://pub-e60eb7dae9d6498ea3e5abf4943b68a1.r2.dev/v1/resumen.parquet) | week × comuna | All scalar indicators of the page: totals, rate, seizures, raids, recovered vehicles… |
| [`semanas.parquet`](https://pub-e60eb7dae9d6498ea3e5abf4943b68a1.r2.dev/v1/semanas.parquet) | week | Week calendar used by the source |
| [`comunas.parquet`](https://pub-e60eb7dae9d6498ea3e5abf4943b68a1.r2.dev/v1/comunas.parquet) | comuna | Region / provincia / comuna names and codes |

Also available:

- `v1/weekly/{registros,resumen}/semana_id=NNNN.parquet` has the same data split by week.
- `v1/raw/semana_id=NNNN.jsonl.gz` has the untouched API responses, one JSON line per
  comuna, for reproducibility.

### DuckDB

```sql
INSTALL httpfs; LOAD httpfs;
SELECT anno, comuna, categoria, SUM(ultima_semana) AS casos
FROM 'https://pub-e60eb7dae9d6498ea3e5abf4943b68a1.r2.dev/v1/registros.parquet'
WHERE semana = 40
GROUP BY ALL ORDER BY casos DESC LIMIT 20;
```

### Python (polars / pandas)

```python
import polars as pl
df = pl.read_parquet("https://pub-e60eb7dae9d6498ea3e5abf4943b68a1.r2.dev/v1/registros.parquet")

import pandas as pd
df = pd.read_parquet("https://pub-e60eb7dae9d6498ea3e5abf4943b68a1.r2.dev/v1/registros.parquet")
```

### R

```r
df <- arrow::read_parquet("https://pub-e60eb7dae9d6498ea3e5abf4943b68a1.r2.dev/v1/registros.parquet")
```

## Schema

Every table has these columns:

| Column | Type | Meaning |
|---|---|---|
| `semana_id` | int | Internal week id of the source (1 = first week of 2023, increasing) |
| `anno`, `semana` | int | Year and week number as labelled by the source (`SEMANA 40/2026`) |
| `fecha_desde`, `fecha_hasta` | date | First and last day of the week |
| `region_id`, `region` | int, str | Region code and name |
| `provincia_id`, `provincia` | int, str | Provincia code and name |
| `comuna_id`, `comuna` | int, str | Comuna code (**CUT**, Código Único Territorial; joins with INE/SUBDERE data) and name |

### `registros.parquet`

| Column | Meaning |
|---|---|
| `categoria` | Crime category, for example `HURTOS` or `ROBOS CON VIOLENCIA E INTIMIDACIÓN` (21 categories) |
| `ultima_semana` | Cases in the last 7 days up to `fecha_hasta` |
| `ultimos_28_dias` | Cases in the last 28 days up to `fecha_hasta` |
| `anno_a_la_fecha` | Cases from 1 January up to `fecha_hasta` |
| `*_anterior` | The same figure for the same period of the previous year |
| `umbral` | Threshold indicator shown per category in the table on the site |
| `umbral_casos` | Threshold indicator from the site's chart (`casos_umbrales`) |

### `resumen.parquet`

This table has one row per week and comuna, with every scalar field that the API returns.
Examples are `casos_ultima_semana`, `casos_ultimos_28_dias`, `casos_anno_a_la_fecha`
(each also with `_anterior`), `tasa_de_registro`, `total_umbrales`, `mayor_registro_{1..5}_{nombre,valor}`,
`decomisos_*` (drug seizures, in kg), `allanamientos_*`, `vehiculos_recuperados_*`,
`eventos_cop_*`, `especialidades_*`, `controles_*`, `fiscalizaciones_*`, `incautaciones_*`,
and `mes_informacion`. The column `fetched_at` records when the row was scraped. All
numeric indicators are stored as `Float64`.

### Caveats

- These are **police records (registros)** of Carabineros, not convictions. They do not
  include records of other institutions (for example, PDI).
- Weeks are cut at year boundaries. Some weeks are therefore 1 or 2 days long (for example,
  `SEMANA 01/2023` is only 2023-01-01). `ultima_semana` is still a full 7-day window.
  Because of this, adding `ultima_semana` over all weeks of a year does not give exactly
  the year's total. To get annual totals, use `anno_a_la_fecha` of the last week of the year.
- The source shows `N.D.` (no disponible) for some values. These values are `null` here.
- Each week is captured once, when it is first published. If Carabineros later revises
  older weeks, those weeks must be re-scraped (see `--refresh` below). `fetched_at` shows
  the capture time.
- The meaning of the field names comes from the labels on the website. Carabineros does
  not publish a data dictionary.

## How it works

`src/leystop/` is a small Python package:

1. `GET /api/semanas` gets the list of published weeks. `/api/regiones` →
   `/api/regiones/{id}/provincias` → `/api/provincias/{id}/comunas` gets the comuna catalogue.
2. For each week that is not yet in the bucket (newest first), the scraper sends
   `GET /api/estadistica/{semana_id}/COMUNA/{comuna_id}` for every comuna.
3. Every 25 comunas, it saves a checkpoint to R2. A run that stops partway resumes from the
   checkpoint next time.
4. When a week is complete, the scraper writes the raw responses and the weekly Parquet
   files. It then rebuilds the consolidated tables and `manifest.json`.

### Being a good citizen

The site processes these requests **one at a time**, at about 3 seconds each, and sending
requests in parallel does not make it faster. The site is also behind a WAF. If a client
sends bursts of requests or requests unknown URLs, the WAF flags the client IP and shows a
CAPTCHA. So the scraper:

- sends requests one at a time, with a minimum gap between them;
- only uses the URLs that the web app itself calls;
- **stops immediately** when it gets a CAPTCHA or an unexpected error page. It keeps its
  checkpoint and tries again at the next scheduled run. It never tries to solve or bypass
  the CAPTCHA.

Each week needs about 350 requests (about 20 minutes). The first full backfill of all
weeks needs about 70,000 requests, which is about 3–4 days of scheduled runs.

## Running it yourself

```bash
uv sync
uv run pytest
uv run leystop --local ./data scrape --max-weeks 1     # write to ./data instead of R2
uv run leystop --local ./data build                    # rebuild consolidated files
```

`scrape` options: `--weeks 199,198` (limit to these weeks), `--refresh` (re-scrape weeks
that are already stored), `--time-budget MINUTES`, and `--min-interval SECONDS`.

### GitHub Actions setup

The workflow is in `.github/workflows/scrape.yml`. It runs every 6 hours and can also be
started manually from the Actions tab.

| Kind | Name | Value |
|---|---|---|
| Secret | `R2_ACCOUNT_ID` | Cloudflare account id |
| Secret | `R2_ACCESS_KEY_ID` | R2 API token, *Object Read & Write*, limited to the bucket |
| Secret | `R2_SECRET_ACCESS_KEY` | Secret key of that token |
| Variable | `R2_BUCKET` | `carabineros-leystop` |
| Variable | `PUBLIC_BASE_URL` | `https://pub-e60eb7dae9d6498ea3e5abf4943b68a1.r2.dev` |

## License

The data is published by Carabineros de Chile. This repository only re-publishes it in a
more usable format. When you use the data, cite Carabineros de Chile – Plataforma Ley STOP
as the source.
