# Portolan Tests and Scripts

End-to-end tests and query scripts for [Portolan CLI](https://github.com/portolan-sdi/portolan-cli) (including its optional `[iceberg]` backend).

## Structure

```
e2e_tests/          End-to-end test scripts
queries/            Iceberg query scripts (BigQuery, DuckDB)
test-catalogs/      Test catalog data (git-ignored, local only)
.env.example        Environment configuration template
```

## Setup

1. Clone the repo and copy the environment template:

   ```bash
   cp .env.example .env
   ```

2. Edit `.env` with your values:

   | Variable | Description |
   | --- | --- |
   | `BASE_DIR` | Path to the parent directory containing the `portolan-cli` repo |
   | `GCS_BUCKET_PORTOLAN` | GCS bucket for the core portolan E2E test |
   | `GCS_BUCKET_PORTOLAKE_SQLITE` | GCS bucket for the SQLite-backend E2E test |
   | `GCS_BUCKET_PORTOLAKE_BIGLAKE` | GCS bucket for the BigLake E2E test |
   | `GCS_BUCKET_QUERIES` | GCS bucket for DuckDB / BigQuery query tests |
   | `STAC_BROWSER_BASE_URL` | Base URL for the STAC Browser instance |

   All scripts also accept CLI arguments and fall back to sensible defaults if `.env` is absent.

3. Authenticate with GCP:

   ```bash
   gcloud auth login
   gcloud auth application-default login
   ```

## E2E Tests

| Script | Description |
| --- | --- |
| `test_e2e_portolan.sh` | Core portolan-cli workflow (init, add, push, pull) |
| `test_e2e_portolake_biglake.sh` | Iceberg backend with BigLake Metastore + GCS |
| `test_e2e_portolake_sqlite.sh` | Iceberg backend with local SQLite catalog |

```bash
# Run with defaults from .env
./e2e_tests/test_e2e_portolan.sh

# Override catalog dir and bucket via CLI args
./e2e_tests/test_e2e_portolan.sh /path/to/catalog gs://my-bucket
```

## Queries

| Script | Description |
| --- | --- |
| `test_bigquery_iceberg_queries.sh` | Query Iceberg tables via BigQuery |
| `test_duckdb_iceberg_queries.sh` | Query Iceberg tables via DuckDB |

```bash
# Run with defaults from .env
./queries/test_duckdb_iceberg_queries.sh

# Override bucket via CLI arg
./queries/test_duckdb_iceberg_queries.sh gs://my-bucket
```

## Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) package manager
- [Portolan CLI](https://github.com/portolan-sdi/portolan-cli) checked out locally (install with `uv sync --extra iceberg` for Iceberg tests)
- `gcloud` CLI authenticated (`gcloud auth login` + `application-default login`)
- For BigLake tests: GCP project with BigLake API enabled
- For DuckDB tests: DuckDB CLI >= 1.5.0 with iceberg, httpfs, and spatial extensions
- For BigQuery tests: `bq` CLI available
