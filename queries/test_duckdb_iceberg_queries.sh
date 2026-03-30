#!/usr/bin/env bash
# DuckDB queries against the portolake Iceberg catalog on BigLake/GCS.
#
# Tests that the Iceberg tables are queryable via DuckDB iceberg_scan(),
# including spatial partition pruning and time travel (15 queries).
#
# Auth: Uses OAuth2 bearer token from gcloud ADC (not HMAC).
#       credential_chain picks up ~/.aws/credentials if present,
#       which causes HMAC auth failures against GCS.
#
# Version guessing (unsafe_enable_version_guessing):
#   DuckDB requires either a version-hint.text file in the metadata/
#   directory or an explicit metadata.json path to find the current table
#   version. BigLake REST catalog does not write version-hint.text, and
#   DuckDB 1.5 has a bug where passing a gs:// metadata.json path directly
#   causes incorrect URL concatenation (appends manifest path to the .json
#   URL instead of the table root).
#
#   The workaround is to point iceberg_scan() at the table root directory
#   and enable version guessing. Despite the "unsafe" name, this is safe
#   when using a REST catalog like BigLake — the catalog manages concurrency
#   and only committed metadata files exist in the directory. The "unsafe"
#   label warns about local filesystems where a crashed writer could leave
#   uncommitted metadata files, which does not apply here.
#
#   A future fix is to have portolake write version-hint.text after each
#   publish, which would remove the need for this flag entirely.
#
# Prerequisites:
#   - duckdb CLI >= 1.5.0 (pip install duckdb-cli)
#   - GCS bucket with Iceberg tables from E2E test
#   - gcloud auth application-default login
#
# Usage:
#   ./test_duckdb_iceberg_queries.sh
#   ./test_duckdb_iceberg_queries.sh gs://my-bucket

set -euo pipefail

BUCKET="${1:-gs://test-catalog-portolake-biglake}"

echo "=== DuckDB Iceberg Query Tests ==="
echo ""
echo "Bucket:   ${BUCKET}"
echo "DuckDB:   $(duckdb --version)"
echo ""

# Table root paths (not metadata.json — DuckDB 1.5 resolves version from dir)
AG_TABLE="${BUCKET}/portolake/agriculture"
BD_TABLE="${BUCKET}/portolake/boundaries"
OSM_TABLE="${BUCKET}/portolake/osm"

echo "Tables:"
echo "  agriculture: ${AG_TABLE}"
echo "  boundaries:  ${BD_TABLE}"
echo "  osm:         ${OSM_TABLE}"
echo ""

# Get OAuth2 token from gcloud ADC
GCS_TOKEN=$(gcloud auth application-default print-access-token 2>/dev/null)
if [ -z "$GCS_TOKEN" ]; then
    echo "FAIL: Could not get access token. Run: gcloud auth application-default login"
    exit 1
fi

ERRORS=0

# DuckDB preamble: load extensions + GCS auth via OAuth2 bearer token
#
# bearer_token: avoids credential_chain picking up ~/.aws/credentials as
#   HMAC keys, which causes 403 SignatureDoesNotMatch on GCS.
#
# unsafe_enable_version_guessing: lets DuckDB find the latest metadata.json
#   by scanning the metadata/ directory. Safe with BigLake (see header).
DUCKDB_INIT="
LOAD iceberg;
LOAD httpfs;
LOAD spatial;
CREATE SECRET (TYPE GCS, bearer_token '${GCS_TOKEN}');
SET unsafe_enable_version_guessing = true;
SET geometry_always_xy = true;
"

run_query() {
    local label="$1"
    local sql="$2"
    echo "--- ${label} ---"
    echo ""
    # Print the SQL query (strip leading whitespace, skip empty lines at start)
    echo "${sql}" | sed '/./,$!d' | sed 's/^/  /'
    echo ""
    if ! duckdb -c "${DUCKDB_INIT} ${sql}" 2>&1; then
        echo "  ERROR: query failed"
        ERRORS=$((ERRORS + 1))
    fi
    echo ""
}

# ---------------------------------------------------------------------------
# 1. Basic table scan — row counts
# ---------------------------------------------------------------------------
run_query "1. Row counts per table" "

SELECT 'agriculture' AS collection, count(*) AS rows
FROM iceberg_scan('${AG_TABLE}', allow_moved_paths := true)
UNION ALL
SELECT 'boundaries', count(*)
FROM iceberg_scan('${BD_TABLE}', allow_moved_paths := true)
UNION ALL
SELECT 'osm', count(*)
FROM iceberg_scan('${OSM_TABLE}', allow_moved_paths := true)
ORDER BY collection;
"

# ---------------------------------------------------------------------------
# 2. Schema inspection — columns and types
# ---------------------------------------------------------------------------
run_query "2. Agriculture table schema" "

DESCRIBE SELECT * FROM iceberg_scan('${AG_TABLE}', allow_moved_paths := true);
"

# ---------------------------------------------------------------------------
# 3. Sample data — first 5 rows (non-geometry columns)
# ---------------------------------------------------------------------------
run_query "3. Agriculture — sample rows" "

SELECT taotlusaas, pollu_id, pindala_ha, EC_hcat_n, geohash_3
FROM iceberg_scan('${AG_TABLE}', allow_moved_paths := true)
LIMIT 5;
"

# ---------------------------------------------------------------------------
# 4. Spatial partition — geohash distribution
# ---------------------------------------------------------------------------
run_query "4. Agriculture — rows per geohash partition" "

SELECT geohash_3, count(*) AS rows
FROM iceberg_scan('${AG_TABLE}', allow_moved_paths := true)
GROUP BY geohash_3
ORDER BY rows DESC;
"

# ---------------------------------------------------------------------------
# 5. Spatial partition filter — single geohash (partition pruning)
# ---------------------------------------------------------------------------
run_query "5. Agriculture — filter by geohash_3 = 'ud5'" "

SELECT count(*) AS rows, min(bbox_xmin) AS xmin, max(bbox_xmax) AS xmax,
       min(bbox_ymin) AS ymin, max(bbox_ymax) AS ymax
FROM iceberg_scan('${AG_TABLE}', allow_moved_paths := true)
WHERE geohash_3 = 'ud5';
"

# ---------------------------------------------------------------------------
# 6. Bounding box filter — spatial query via bbox columns
# ---------------------------------------------------------------------------
run_query "6. Agriculture — bbox spatial filter (Estonia ~25-26E, 58-59N)" "

SELECT count(*) AS rows_in_bbox
FROM iceberg_scan('${AG_TABLE}', allow_moved_paths := true)
WHERE bbox_xmin >= 25.0 AND bbox_xmax <= 26.0
  AND bbox_ymin >= 58.0 AND bbox_ymax <= 59.0;
"

# ---------------------------------------------------------------------------
# 7. Boundaries — country names
# ---------------------------------------------------------------------------
run_query "7. Boundaries — list countries" "

SELECT \"SOVEREIGNT\" AS country, \"POP_EST\" AS population, geohash_4
FROM iceberg_scan('${BD_TABLE}', allow_moved_paths := true)
ORDER BY \"POP_EST\" DESC
LIMIT 10;
"

# ---------------------------------------------------------------------------
# 8. OSM — POI types distribution
# ---------------------------------------------------------------------------
run_query "8. OSM — top amenity types in Spain" "

SELECT amenity, count(*) AS count
FROM iceberg_scan('${OSM_TABLE}', allow_moved_paths := true)
WHERE amenity IS NOT NULL AND amenity != ''
GROUP BY amenity
ORDER BY count DESC
LIMIT 15;
"

# ---------------------------------------------------------------------------
# 9. OSM — spatial partition distribution
# ---------------------------------------------------------------------------
run_query "9. OSM — rows per geohash partition (top 15)" "

SELECT geohash_3, count(*) AS rows
FROM iceberg_scan('${OSM_TABLE}', allow_moved_paths := true)
GROUP BY geohash_3
ORDER BY rows DESC
LIMIT 15;
"

# ---------------------------------------------------------------------------
# 10. OSM — filter single geohash (Madrid area ~ 'ezj')
# ---------------------------------------------------------------------------
run_query "10. OSM — POIs in Madrid area (geohash_3 = 'ezj')" "

SELECT amenity, count(*) AS count
FROM iceberg_scan('${OSM_TABLE}', allow_moved_paths := true)
WHERE geohash_3 = 'ezj' AND amenity IS NOT NULL AND amenity != ''
GROUP BY amenity
ORDER BY count DESC
LIMIT 10;
"

# ---------------------------------------------------------------------------
# 11. Time travel — list all snapshots with metadata
# ---------------------------------------------------------------------------
run_query "11. Agriculture — list snapshots (time travel)" "

SELECT
    snapshot_id,
    timestamp_ms AS snapshot_time,
    sequence_number
FROM iceberg_snapshots('${AG_TABLE}')
ORDER BY timestamp_ms;
"

# ---------------------------------------------------------------------------
# 11b–d. Time travel queries using snapshot_from_id
#
#   DuckDB's iceberg_scan() supports snapshot_from_id (UBIGINT) to query
#   a specific historical version. This is distinct from the 'version'
#   parameter which maps to metadata file version numbers.
#
#   We dynamically fetch the first and last snapshot IDs from the table.
# ---------------------------------------------------------------------------

FIRST_SNAP=$(duckdb -csv -noheader -c "${DUCKDB_INIT}
SELECT snapshot_id FROM iceberg_snapshots('${AG_TABLE}')
ORDER BY timestamp_ms ASC LIMIT 1;" 2>/dev/null | tail -1)

LAST_SNAP=$(duckdb -csv -noheader -c "${DUCKDB_INIT}
SELECT snapshot_id FROM iceberg_snapshots('${AG_TABLE}')
ORDER BY timestamp_ms DESC LIMIT 1;" 2>/dev/null | tail -1)

echo "  First snapshot: ${FIRST_SNAP}"
echo "  Last snapshot:  ${LAST_SNAP}"
echo ""

# ---------------------------------------------------------------------------
# 11b. Time travel — compare first snapshot vs current
#      Proves copy-on-write: old data files are still readable.
# ---------------------------------------------------------------------------
run_query "11b. Agriculture — row count at first snapshot (time travel)" "

SELECT count(*) AS rows,
       count(DISTINCT geohash_3) AS partitions,
       round(min(bbox_xmin), 2) AS west,
       round(max(bbox_xmax), 2) AS east
FROM iceberg_scan('${AG_TABLE}', allow_moved_paths := true,
    snapshot_from_id := ${FIRST_SNAP});
"

run_query "11b2. Agriculture — row count at current snapshot (compare with 11b)" "

SELECT count(*) AS rows,
       count(DISTINCT geohash_3) AS partitions,
       round(min(bbox_xmin), 2) AS west,
       round(max(bbox_xmax), 2) AS east
FROM iceberg_scan('${AG_TABLE}', allow_moved_paths := true,
    snapshot_from_id := ${LAST_SNAP});
"

# ---------------------------------------------------------------------------
# 11c. Time travel — sample rows from the first snapshot
#      Shows actual data from the oldest version.
# ---------------------------------------------------------------------------
run_query "11c. Agriculture — sample rows from first snapshot" "

SELECT taotlusaas, pollu_id, pindala_ha, EC_hcat_n, geohash_3
FROM iceberg_scan('${AG_TABLE}', allow_moved_paths := true,
    snapshot_from_id := ${FIRST_SNAP})
LIMIT 5;
"

# ---------------------------------------------------------------------------
# 11d. Time travel — partition distribution: first vs current
#      Compares geohash row counts across versions. With identical source
#      data re-published, rows per partition grow proportionally.
# ---------------------------------------------------------------------------
run_query "11d. Agriculture — partition distribution: first vs current" "

WITH first_ver AS (
    SELECT geohash_3, count(*) AS rows
    FROM iceberg_scan('${AG_TABLE}', allow_moved_paths := true,
        snapshot_from_id := ${FIRST_SNAP})
    GROUP BY geohash_3
),
current_ver AS (
    SELECT geohash_3, count(*) AS rows
    FROM iceberg_scan('${AG_TABLE}', allow_moved_paths := true,
        snapshot_from_id := ${LAST_SNAP})
    GROUP BY geohash_3
)
SELECT
    coalesce(f.geohash_3, c.geohash_3) AS geohash,
    f.rows AS first_rows,
    c.rows AS current_rows,
    c.rows - f.rows AS diff
FROM first_ver f
FULL OUTER JOIN current_ver c ON f.geohash_3 = c.geohash_3
ORDER BY geohash;
"

# ---------------------------------------------------------------------------
# 12. Spatial query — features within 10km of Tallinn (geodesic distance)
#     Uses ST_Distance_Spheroid for proper meter-based distance on WGS84.
#     ST_Distance_Spheroid requires POINT inputs, so we use ST_Centroid.
#     Compare with BigQuery test 11 which uses ST_DISTANCE on full polygons.
# ---------------------------------------------------------------------------
run_query "12. Agriculture — spatial query (centroids within 10km of Tallinn)" "

SELECT count(*) AS features_near_tallinn
FROM iceberg_scan('${AG_TABLE}', allow_moved_paths := true)
WHERE ST_Distance_Spheroid(
  ST_Centroid(ST_GeomFromWKB(geometry)),
  ST_Point(24.7536, 59.4370)
) < 10000;
"

# ---------------------------------------------------------------------------
# 13. Cross-table join — OSM POIs within boundary bbox
# ---------------------------------------------------------------------------
run_query "13. Cross-table — OSM POIs count per country (top 5 by bbox overlap)" "

WITH countries AS (
    SELECT \"SOVEREIGNT\" AS country, bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax
    FROM iceberg_scan('${BD_TABLE}', allow_moved_paths := true)
    WHERE bbox_xmin >= -10 AND bbox_xmax <= 5
      AND bbox_ymin >= 35 AND bbox_ymax <= 45
),
pois AS (
    SELECT bbox_xmin AS px, bbox_ymin AS py
    FROM iceberg_scan('${OSM_TABLE}', allow_moved_paths := true)
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
    echo "PASS: All DuckDB Iceberg queries succeeded"
else
    echo "FAIL: ${ERRORS} query(ies) failed"
fi
echo "==========================================="
exit "$ERRORS"
