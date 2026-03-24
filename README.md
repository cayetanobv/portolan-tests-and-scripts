# Portolan Tests and Scripts

End-to-end tests and query scripts for [Portolan CLI](https://github.com/portolan-sdi/portolan-cli) and [Portolake](https://github.com/portolan-sdi/portolake).

## Structure

```
e2e_tests/      End-to-end test scripts
queries/        Iceberg query scripts (BigQuery, DuckDB)
```

## E2E Tests

| Script | Description |
| --- | --- |
| `test_e2e_portolan.sh` | Core portolan-cli workflow (init, add, push, pull) |
| `test_e2e_portolake_biglake.sh` | Portolake with BigLake Metastore + GCS |
| `test_e2e_portolake_sqlite.sh` | Portolake with local SQLite catalog |

## Queries

| Script | Description |
| --- | --- |
| `test_bigquery_iceberg_queries.sh` | Query Iceberg tables via BigQuery |
| `test_duckdb_iceberg_queries.sh` | Query Iceberg tables via DuckDB |

## Prerequisites

- Python 3.11+
- [Portolan CLI](https://github.com/portolan-sdi/portolan-cli) and [Portolake](https://github.com/portolan-sdi/portolake) installed
- For BigLake tests: GCP project with BigLake API enabled, `gcloud` authenticated
- For DuckDB tests: DuckDB CLI with iceberg, httpfs, and spatial extensions
