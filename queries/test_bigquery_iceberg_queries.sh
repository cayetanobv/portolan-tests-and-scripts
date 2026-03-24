#!/usr/bin/env bash
# BigQuery queries against portolake Iceberg tables via BigLake.
#
# Tests the same queries as test_duckdb_iceberg_queries.sh but using BigQuery,
# showing that the same Iceberg tables are accessible from both engines.
#
# Key differences from DuckDB:
#   - No iceberg_scan() — tables are standard BigQuery external tables
#   - No bearer tokens or version guessing — BigLake handles auth and metadata
#   - Tables must be registered first via CREATE EXTERNAL TABLE (setup step)
#   - BigQuery connection service account needs GCS objectViewer on the bucket
#   - ROWS is a reserved word in BigQuery — use row_count instead
#   - Column names are case-sensitive in Iceberg but BigQuery lowercases them
#   - No time travel via snapshot ID — BigQuery reads the latest snapshot only
#   - geometry column is BYTES (WKB) — use ST_GEOGFROMWKB() for spatial ops
#
# Setup (done once, included in this script):
#   1. Create a BigQuery dataset to hold the external tables
#   2. Grant GCS read access to the BigQuery connection service account
#   3. Create external tables pointing to Iceberg metadata.json files
#
# Prerequisites:
#   - gcloud CLI authenticated
#   - bq CLI available
#   - GCS bucket with Iceberg tables from E2E test
#   - BigQuery connection (CLOUD_RESOURCE type) in the same region
#
# Usage:
#   ./test_bigquery_iceberg_queries.sh
#   ./test_bigquery_iceberg_queries.sh gs://my-bucket my-project

set -euo pipefail

BUCKET="${1:-gs://test-catalog-portolake-biglake}"
GCP_PROJECT="${2:-$(gcloud config get-value project 2>/dev/null)}"
BQ_DATASET="portolake"
BQ_CONNECTION="iceberg_connection"
BQ_LOCATION="us"

echo "=== BigQuery Iceberg Query Tests ==="
echo ""
echo "  Bucket:     ${BUCKET}"
echo "  Project:    ${GCP_PROJECT}"
echo "  Dataset:    ${GCP_PROJECT}.${BQ_DATASET}"
echo "  Connection: ${BQ_CONNECTION}"
echo ""

ERRORS=0

run_query() {
    local label="$1"
    local sql="$2"
    echo "--- ${label} ---"
    if ! bq query --use_legacy_sql=false --project_id="${GCP_PROJECT}" "${sql}" 2>&1; then
        echo "  ERROR: query failed"
        ERRORS=$((ERRORS + 1))
    fi
    echo ""
}

# ---------------------------------------------------------------------------
# Step 0: Setup — create dataset, grant permissions, register tables
# ---------------------------------------------------------------------------

echo "Step 0: Setting up BigQuery external tables..."
echo ""

# Create dataset if it doesn't exist
bq mk --dataset --location="${BQ_LOCATION}" --project_id="${GCP_PROJECT}" \
    "${GCP_PROJECT}:${BQ_DATASET}" 2>/dev/null || true

# Find latest metadata.json for each table
AG_META=$(gcloud storage ls "${BUCKET}/portolake/agriculture/metadata/*.metadata.json" 2>/dev/null | sort | tail -1)
BD_META=$(gcloud storage ls "${BUCKET}/portolake/boundaries/metadata/*.metadata.json" 2>/dev/null | sort | tail -1)
OSM_META=$(gcloud storage ls "${BUCKET}/portolake/osm/metadata/*.metadata.json" 2>/dev/null | sort | tail -1)

echo "  Metadata files:"
echo "    agriculture: ${AG_META}"
echo "    boundaries:  ${BD_META}"
echo "    osm:         ${OSM_META}"

# Grant GCS read access to the connection service account (idempotent)
CONNECTION_SA=$(bq show --connection --format=json \
    --project_id="${GCP_PROJECT}" --location="${BQ_LOCATION}" \
    "${BQ_CONNECTION}" 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['cloudResource']['serviceAccountId'])" 2>/dev/null) || true

if [ -n "$CONNECTION_SA" ]; then
    echo "  Connection SA: ${CONNECTION_SA}"
    gcloud storage buckets add-iam-policy-binding "${BUCKET}" \
        --member="serviceAccount:${CONNECTION_SA}" \
        --role="roles/storage.objectViewer" \
        --quiet 2>/dev/null || true
    echo "  OK: GCS access granted"
else
    echo "  WARN: Could not find connection service account — tables may fail if permissions are missing"
fi

# Create external tables (CREATE OR REPLACE is idempotent)
CONNECTION_REF="projects/${GCP_PROJECT}/locations/${BQ_LOCATION}/connections/${BQ_CONNECTION}"

for table_info in "agriculture:${AG_META}" "boundaries:${BD_META}" "osm:${OSM_META}"; do
    table_name="${table_info%%:*}"
    metadata_uri="${table_info#*:}"
    bq query --use_legacy_sql=false --project_id="${GCP_PROJECT}" "
    CREATE OR REPLACE EXTERNAL TABLE \`${GCP_PROJECT}.${BQ_DATASET}.${table_name}\`
    WITH CONNECTION \`${CONNECTION_REF}\`
    OPTIONS (
      format = 'ICEBERG',
      uris = ['${metadata_uri}']
    );" 2>/dev/null
    echo "  OK: ${BQ_DATASET}.${table_name} registered"
done
echo ""

# ---------------------------------------------------------------------------
# 1. Basic table scan — row counts
# ---------------------------------------------------------------------------
run_query "1. Row counts per table" "
SELECT 'agriculture' AS collection, count(*) AS row_count
FROM \`${GCP_PROJECT}.${BQ_DATASET}.agriculture\`
UNION ALL
SELECT 'boundaries', count(*)
FROM \`${GCP_PROJECT}.${BQ_DATASET}.boundaries\`
UNION ALL
SELECT 'osm', count(*)
FROM \`${GCP_PROJECT}.${BQ_DATASET}.osm\`
ORDER BY collection;
"

# ---------------------------------------------------------------------------
# 2. Schema inspection — columns and types
# ---------------------------------------------------------------------------
run_query "2. Agriculture table schema" "
SELECT column_name, data_type
FROM \`${GCP_PROJECT}.${BQ_DATASET}.INFORMATION_SCHEMA.COLUMNS\`
WHERE table_name = 'agriculture'
ORDER BY ordinal_position;
"

# ---------------------------------------------------------------------------
# 3. Sample data — first 5 rows (non-geometry columns)
# ---------------------------------------------------------------------------
run_query "3. Agriculture — sample rows" "
SELECT taotlusaas, pollu_id, pindala_ha, ec_hcat_n, geohash_3
FROM \`${GCP_PROJECT}.${BQ_DATASET}.agriculture\`
LIMIT 5;
"

# ---------------------------------------------------------------------------
# 4. Spatial partition — geohash distribution
# ---------------------------------------------------------------------------
run_query "4. Agriculture — rows per geohash partition" "
SELECT geohash_3, count(*) AS row_count
FROM \`${GCP_PROJECT}.${BQ_DATASET}.agriculture\`
GROUP BY geohash_3
ORDER BY row_count DESC;
"

# ---------------------------------------------------------------------------
# 5. Spatial partition filter — single geohash (partition pruning)
# ---------------------------------------------------------------------------
run_query "5. Agriculture — filter by geohash_3 = 'ud5'" "
SELECT count(*) AS row_count,
       min(bbox_xmin) AS xmin, max(bbox_xmax) AS xmax,
       min(bbox_ymin) AS ymin, max(bbox_ymax) AS ymax
FROM \`${GCP_PROJECT}.${BQ_DATASET}.agriculture\`
WHERE geohash_3 = 'ud5';
"

# ---------------------------------------------------------------------------
# 6. Bounding box filter — spatial query via bbox columns
# ---------------------------------------------------------------------------
run_query "6. Agriculture — bbox spatial filter (Estonia ~25-26E, 58-59N)" "
SELECT count(*) AS rows_in_bbox
FROM \`${GCP_PROJECT}.${BQ_DATASET}.agriculture\`
WHERE bbox_xmin >= 25.0 AND bbox_xmax <= 26.0
  AND bbox_ymin >= 58.0 AND bbox_ymax <= 59.0;
"

# ---------------------------------------------------------------------------
# 7. Boundaries — country names
# ---------------------------------------------------------------------------
run_query "7. Boundaries — list countries (top 10 by population)" "
SELECT sovereignt AS country, pop_est AS population, geohash_4
FROM \`${GCP_PROJECT}.${BQ_DATASET}.boundaries\`
ORDER BY pop_est DESC
LIMIT 10;
"

# ---------------------------------------------------------------------------
# 8. OSM — POI types distribution
# ---------------------------------------------------------------------------
run_query "8. OSM — top amenity types in Spain" "
SELECT amenity, count(*) AS cnt
FROM \`${GCP_PROJECT}.${BQ_DATASET}.osm\`
WHERE amenity IS NOT NULL AND amenity != ''
GROUP BY amenity
ORDER BY cnt DESC
LIMIT 15;
"

# ---------------------------------------------------------------------------
# 9. OSM — spatial partition distribution
# ---------------------------------------------------------------------------
run_query "9. OSM — rows per geohash partition (top 15)" "
SELECT geohash_3, count(*) AS row_count
FROM \`${GCP_PROJECT}.${BQ_DATASET}.osm\`
GROUP BY geohash_3
ORDER BY row_count DESC
LIMIT 15;
"

# ---------------------------------------------------------------------------
# 10. OSM — filter single geohash (Madrid area ~ 'ezj')
# ---------------------------------------------------------------------------
run_query "10. OSM — POIs in Madrid area (geohash_3 = 'ezj')" "
SELECT amenity, count(*) AS cnt
FROM \`${GCP_PROJECT}.${BQ_DATASET}.osm\`
WHERE geohash_3 = 'ezj' AND amenity IS NOT NULL AND amenity != ''
GROUP BY amenity
ORDER BY cnt DESC
LIMIT 10;
"

# ---------------------------------------------------------------------------
# 11. BigQuery-native spatial query using ST_GEOGFROMWKB
#     (replaces DuckDB test 11 — BQ doesn't support Iceberg time travel,
#      but it does support native spatial functions on the geometry column)
# ---------------------------------------------------------------------------
run_query "11. Agriculture — native spatial query (features within 10km of Tallinn)" "
SELECT count(*) AS features_near_tallinn
FROM \`${GCP_PROJECT}.${BQ_DATASET}.agriculture\`
WHERE ST_DISTANCE(
  ST_GEOGFROMWKB(geometry),
  ST_GEOGPOINT(24.7536, 59.4370)
) < 10000;
"

# ---------------------------------------------------------------------------
# 12. Cross-table join — OSM POIs within boundary bbox
# ---------------------------------------------------------------------------
run_query "12. Cross-table — OSM POIs count per country (top 5 by bbox overlap)" "
WITH countries AS (
    SELECT sovereignt AS country, bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax
    FROM \`${GCP_PROJECT}.${BQ_DATASET}.boundaries\`
    WHERE bbox_xmin >= -10 AND bbox_xmax <= 5
      AND bbox_ymin >= 35 AND bbox_ymax <= 45
),
pois AS (
    SELECT bbox_xmin AS px, bbox_ymin AS py
    FROM \`${GCP_PROJECT}.${BQ_DATASET}.osm\`
)
SELECT c.country, count(*) AS poi_count
FROM countries c
JOIN pois p ON p.px BETWEEN c.bbox_xmin AND c.bbox_xmax
           AND p.py BETWEEN c.bbox_ymin AND c.bbox_ymax
GROUP BY c.country
ORDER BY poi_count DESC
LIMIT 5;
"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo "==========================================="
if [ "$ERRORS" -eq 0 ]; then
    echo "PASS: All BigQuery Iceberg queries succeeded"
else
    echo "FAIL: ${ERRORS} query(ies) failed"
fi
echo ""
echo "Key differences from DuckDB:"
echo "  - Tables are standard BQ external tables (no iceberg_scan)"
echo "  - Auth handled by BigQuery connection + IAM (no tokens)"
echo "  - No time travel (BQ reads latest snapshot only)"
echo "  - Native spatial: ST_GEOGFROMWKB(geometry) for spatial ops"
echo "  - Column names are lowercased"
echo "==========================================="
exit "$ERRORS"
