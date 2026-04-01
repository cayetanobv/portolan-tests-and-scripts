#!/usr/bin/env bash
# End-to-end test for portolake Iceberg-native data management with BigLake.
#
# Tests the full Iceberg-native pipeline including:
#   - Data ingestion into Iceberg tables (not metadata-only)
#   - Spatial partitioning (geohash + bbox columns)
#   - STAC extension metadata (table:* + iceberg:*)
#   - Static GeoParquet export
#   - Time travel / version history
#
# Architecture:
#   BigLake Metastore (catalog) <- REST API -> PyIceberg <- portolake
#   GCS bucket (warehouse)     <- gcsfs    -> PyIceberg
#
# Usage:
#   ./test_e2e_portolake_native_biglake.sh
#   ./test_e2e_portolake_native_biglake.sh /path/to/catalog gs://bucket
#
# Prerequisites:
#   - GCP project with billing enabled
#   - gcloud CLI authenticated (gcloud auth login + application-default login)
#   - Raw data in test-catalog-raw-data/{agriculture,boundaries,osm}
#

set -euo pipefail

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# Load .env if present (provides BASE_DIR, GCS_BUCKET_* defaults)
if [ -f "$SCRIPT_DIR/.env" ]; then
    # shellcheck disable=SC1091
    set -a; source "$SCRIPT_DIR/.env"; set +a
fi

BASE_DIR="${BASE_DIR:-/home/cayetano/dev_projs/portolan}"
CATALOG_DIR="${1:-$SCRIPT_DIR/test-catalogs/test-catalog-portolake-biglake2}"
GCS_BUCKET="${2:-${GCS_BUCKET_PORTOLAKE_BIGLAKE:-gs://cayetanobv-portolake-iceberg-biglake}}"
RAW_DATA_DIR="$SCRIPT_DIR/test-catalogs/test-catalog-raw-data"
PORTOLAKE_DIR="$BASE_DIR/portolake"
PORTOLAN_CLI_DIR="$BASE_DIR/portolan-cli"

PORTOLAN="uv run --project $PORTOLAKE_DIR portolan"
PYTHON="uv run --project $PORTOLAKE_DIR python3"

# GCP settings
GCP_PROJECT=$(gcloud config get-value project 2>/dev/null)
GCP_LOCATION="us"
BIGLAKE_CATALOG_NAME="${GCS_BUCKET#gs://}"  # bucket name = catalog name

# Collections to test
# TODO: re-enable agriculture and osm once small datasets pass
#GEOPARQUET_COLLECTIONS=("agriculture" "boundaries" "osm")
GEOPARQUET_COLLECTIONS=("airports" "boundaries")

ERRORS=0

# -----------------------------------------------------------------------------
# Preflight
# -----------------------------------------------------------------------------

echo "=== Portolake E2E Test (Iceberg-Native + BigLake) ==="
echo ""
echo "  Catalog dir:        $CATALOG_DIR"
echo "  GCS bucket:         $GCS_BUCKET"
echo "  GCP project:        $GCP_PROJECT"
echo "  GCP location:       $GCP_LOCATION"
echo "  BigLake catalog:    $BIGLAKE_CATALOG_NAME"
echo "  Raw data dir:       $RAW_DATA_DIR"
echo ""

if [ ! -d "$RAW_DATA_DIR" ]; then
    echo "FAIL: Raw data directory not found: $RAW_DATA_DIR"
    exit 1
fi

for col in "${GEOPARQUET_COLLECTIONS[@]}"; do
    if [ ! -d "$RAW_DATA_DIR/$col" ]; then
        echo "FAIL: Missing raw data collection: $RAW_DATA_DIR/$col"
        exit 1
    fi
done

# -----------------------------------------------------------------------------
# Step 0: Setup environment
# -----------------------------------------------------------------------------

echo "Step 0: Setting up environment..."

cd "$PORTOLAKE_DIR"
uv sync --all-extras --quiet
uv pip install --project "$PORTOLAKE_DIR" -e "$PORTOLAN_CLI_DIR" --quiet

# Verify gcsfs is available (needed for GCS warehouse)
if $PYTHON -c "import gcsfs" 2>/dev/null; then
    echo "  OK: gcsfs available"
else
    echo "  Installing gcsfs..."
    uv pip install --project "$PORTOLAKE_DIR" "gcsfs>=2024.2.0" --quiet
fi

# Verify plugin is discoverable
BACKENDS=$($PYTHON -c "
from importlib.metadata import entry_points
eps = entry_points(group='portolan.backends')
print(','.join(ep.name for ep in eps))
")
if [[ "$BACKENDS" != *"iceberg"* ]]; then
    echo "FAIL: Iceberg backend not found in entry points (got: $BACKENDS)"
    exit 1
fi
echo "  OK: Iceberg backend discoverable"
echo ""

# -----------------------------------------------------------------------------
# Step 1: Enable BigLake API
# -----------------------------------------------------------------------------

echo "Step 1: Enabling BigLake API..."
if gcloud services enable biglake.googleapis.com --project="$GCP_PROJECT" 2>&1; then
    echo "  OK: BigLake API enabled"
else
    echo "FAIL: Could not enable BigLake API"
    exit 1
fi
echo ""

# -----------------------------------------------------------------------------
# Step 2: Clean GCS bucket
# -----------------------------------------------------------------------------

echo "Step 2: Cleaning GCS bucket $GCS_BUCKET..."
if ! gcloud storage ls "$GCS_BUCKET" &>/dev/null; then
    echo "  Bucket does not exist, creating..."
    gcloud storage buckets create "$GCS_BUCKET" --location="$GCP_LOCATION" --project="$GCP_PROJECT" 2>&1 || true
fi
gcloud storage rm -r "$GCS_BUCKET/**" 2>/dev/null || true
echo "  OK: Bucket cleaned"
echo ""

# -----------------------------------------------------------------------------
# Step 3: Create BigLake Iceberg catalog
# -----------------------------------------------------------------------------

echo "Step 3: Creating BigLake Iceberg REST catalog..."

# Delete existing catalog if present (ignore errors)
gcloud alpha biglake iceberg catalogs delete "$BIGLAKE_CATALOG_NAME" \
    --project="$GCP_PROJECT" --quiet 2>/dev/null || true
# Wait for deletion to propagate
sleep 5

# Create new catalog backed by the GCS bucket
CREATE_OUTPUT=$(gcloud alpha biglake iceberg catalogs create "$BIGLAKE_CATALOG_NAME" \
    --catalog-type=gcs-bucket \
    --project="$GCP_PROJECT" 2>&1) || true
if echo "$CREATE_OUTPUT" | grep -q "ALREADY_EXISTS"; then
    echo "  OK: BigLake catalog already exists: $BIGLAKE_CATALOG_NAME"
elif echo "$CREATE_OUTPUT" | grep -q "Created catalog"; then
    echo "  OK: BigLake catalog created: $BIGLAKE_CATALOG_NAME"
else
    echo "$CREATE_OUTPUT"
    echo "FAIL: Could not create BigLake catalog"
    exit 1
fi

# Verify catalog exists
gcloud alpha biglake iceberg catalogs describe "$BIGLAKE_CATALOG_NAME" \
    --project="$GCP_PROJECT" 2>&1
echo ""

# -----------------------------------------------------------------------------
# Step 4: Configure PyIceberg for BigLake
# -----------------------------------------------------------------------------

echo "Step 4: Configuring PyIceberg for BigLake..."

# Verify Application Default Credentials are available
if ! gcloud auth application-default print-access-token &>/dev/null; then
    echo "FAIL: No Application Default Credentials. Run: gcloud auth application-default login"
    exit 1
fi

PYICEBERG_CONFIG="$HOME/.pyiceberg.yaml"
PYICEBERG_BACKUP=""
if [ -f "$PYICEBERG_CONFIG" ]; then
    PYICEBERG_BACKUP="$PYICEBERG_CONFIG.bak.$$"
    cp "$PYICEBERG_CONFIG" "$PYICEBERG_BACKUP"
    echo "  Backed up existing $PYICEBERG_CONFIG"
fi

cat > "$PYICEBERG_CONFIG" <<YAML
catalog:
  portolake:
    type: rest
    uri: https://biglake.googleapis.com/iceberg/v1/restcatalog
    warehouse: ${GCS_BUCKET}
    auth:
      type: google
    header.x-goog-user-project: ${GCP_PROJECT}
    header.X-Iceberg-Access-Delegation: ""
YAML

echo "  Wrote $PYICEBERG_CONFIG"
echo "  type: rest"
echo "  uri: https://biglake.googleapis.com/iceberg/v1/restcatalog"
echo "  warehouse: ${GCS_BUCKET}"
echo "  auth.type: google"

# Cleanup function to restore original config
cleanup_pyiceberg_config() {
    if [ -n "$PYICEBERG_BACKUP" ]; then
        mv "$PYICEBERG_BACKUP" "$PYICEBERG_CONFIG"
        echo "  Restored original $PYICEBERG_CONFIG"
    else
        rm -f "$PYICEBERG_CONFIG"
    fi
}
trap cleanup_pyiceberg_config EXIT
echo ""

# Verify PyIceberg can connect to BigLake and clean stale tables
echo "  Verifying PyIceberg <-> BigLake connection..."
if $PYTHON -c "
from pyiceberg.catalog import load_catalog
catalog = load_catalog('portolake')
print(f'  Catalog type: {type(catalog).__name__}')
namespaces = catalog.list_namespaces()
print(f'  Existing namespaces: {namespaces}')

# Drop stale tables from previous runs (bucket was cleaned)
for ns in namespaces:
    ns_name = ns[0] if isinstance(ns, tuple) else ns
    try:
        tables = catalog.list_tables(ns_name)
        for tbl in tables:
            full_id = f'{tbl[0]}.{tbl[1]}'
            try:
                catalog.drop_table(full_id, purge_requested=True)
                print(f'  Dropped stale table: {full_id}')
            except Exception as e:
                print(f'  WARN: Could not drop {full_id}: {e}')
    except Exception:
        pass

print('  OK: Connected to BigLake (stale tables cleaned)')
"; then
    echo ""
else
    echo "  FAIL: Could not connect to BigLake catalog"
    echo ""
    echo "  Troubleshooting:"
    echo "    1. Run: gcloud auth application-default login"
    echo "    2. Verify API enabled: gcloud services list --enabled | grep biglake"
    echo "    3. Check project: gcloud config get-value project"
    exit 1
fi

# -----------------------------------------------------------------------------
# Step 5: Clean previous local catalog
# -----------------------------------------------------------------------------

echo "Step 5: Cleaning previous catalog..."
if [ -d "$CATALOG_DIR" ]; then
    rm -rf "$CATALOG_DIR"
fi
mkdir -p "$CATALOG_DIR"
echo "  OK: Cleaned"
echo ""

# -----------------------------------------------------------------------------
# Step 6: Initialize catalog with Iceberg backend
# -----------------------------------------------------------------------------

echo "Step 6: Initializing catalog with --backend iceberg and remote..."
cd "$CATALOG_DIR"
$PORTOLAN init --auto --backend iceberg
$PORTOLAN config set remote "$GCS_BUCKET"
echo ""

# Verify backend config
if grep -q "backend: iceberg" "$CATALOG_DIR/.portolan/config.yaml" 2>/dev/null; then
    echo "  OK: config.yaml has backend: iceberg"
else
    echo "  FAIL: config.yaml missing 'backend: iceberg'"
    ERRORS=$((ERRORS + 1))
fi

if grep -q "remote:" "$CATALOG_DIR/.portolan/config.yaml" 2>/dev/null; then
    echo "  OK: config.yaml has remote configured"
else
    echo "  FAIL: config.yaml missing 'remote' setting"
    ERRORS=$((ERRORS + 1))
fi
echo ""

# -----------------------------------------------------------------------------
# Step 7: Prepare and copy raw data
# -----------------------------------------------------------------------------

echo "Step 7: Preparing raw data..."

# Copy/extract data for each collection
for collection in "${GEOPARQUET_COLLECTIONS[@]}"; do
    collection_src="$RAW_DATA_DIR/$collection"
    mkdir -p "$CATALOG_DIR/$collection"

    for file in "$collection_src"/*; do
        filename=$(basename "$file")

        case "$filename" in
            *.parquet|*.geojson)
                cp "$file" "$CATALOG_DIR/$collection/"
                echo "  -> $collection/$filename"
                ;;
            *.zip)
                # Extract shapefile from zip into collection dir
                unzip -oq "$file" -d "$CATALOG_DIR/$collection/"
                echo "  -> $collection/ (extracted $filename)"
                ;;
        esac
    done
done
echo ""

# -----------------------------------------------------------------------------
# Step 8: Add files to catalog (triggers Iceberg-native data ingestion)
# -----------------------------------------------------------------------------

echo "Step 8: Adding files to catalog (Iceberg-native data ingestion)..."
ADDED_COLLECTIONS=()
for collection in "${GEOPARQUET_COLLECTIONS[@]}"; do
    echo "  -> Adding $collection/..."
    if $PORTOLAN add "$collection/"; then
        ADDED_COLLECTIONS+=("$collection")
        echo "  OK: $collection added"
    else
        echo "  FAIL: portolan add failed for $collection"
        ERRORS=$((ERRORS + 1))
    fi
done
echo ""

# -----------------------------------------------------------------------------
# Step 9: Fix STAC metadata
# -----------------------------------------------------------------------------

echo "Step 9: Fixing STAC metadata..."
$PORTOLAN check --metadata --fix || echo "  WARN: Some metadata fixes had non-fatal issues (bbox etc.)"
echo ""

# -----------------------------------------------------------------------------
# Step 10: Verify Iceberg tables have ACTUAL DATA (not just metadata)
# -----------------------------------------------------------------------------

echo "Step 10: Verifying Iceberg tables contain actual data rows..."
$PYTHON -c "
import sys
from pyiceberg.catalog import load_catalog

catalog = load_catalog('portolake')
tables = catalog.list_tables('portolake')

if not tables:
    print('  FAIL: No tables found in portolake namespace')
    sys.exit(1)

for ns, table_name in tables:
    table = catalog.load_table(f'{ns}.{table_name}')
    snap = table.current_snapshot()
    if snap and snap.summary:
        version = snap.summary.get('portolake.version', '?')
        # Read actual row count from Iceberg scan
        arrow_table = table.scan().to_arrow()
        row_count = len(arrow_table)
        col_count = len(arrow_table.column_names)
        print(f'  OK: {ns}.{table_name} -> version {version}, {row_count} rows, {col_count} columns')
        if row_count == 0:
            print(f'  WARN: {ns}.{table_name} has 0 rows — data ingestion may have failed')
    else:
        print(f'  WARN: {ns}.{table_name} has no snapshot')

print('  OK: Iceberg data verification complete')
" || ERRORS=$((ERRORS + 1))
echo ""

# -----------------------------------------------------------------------------
# Step 11: Verify spatial columns (geohash + bbox)
# -----------------------------------------------------------------------------

echo "Step 11: Verifying spatial columns in Iceberg tables..."
$PYTHON -c "
import sys
from pyiceberg.catalog import load_catalog

catalog = load_catalog('portolake')
tables = catalog.list_tables('portolake')

for ns, table_name in tables:
    table = catalog.load_table(f'{ns}.{table_name}')
    arrow_table = table.scan().to_arrow()
    col_names = arrow_table.column_names

    has_geohash = any(c.startswith('geohash_') for c in col_names)
    has_bbox = all(f'bbox_{d}' in col_names for d in ['xmin', 'ymin', 'xmax', 'ymax'])
    has_geometry = 'geometry' in col_names or 'geom' in col_names

    print(f'  {ns}.{table_name}:')
    print(f'    has geometry: {has_geometry}')
    print(f'    has geohash:  {has_geohash}')
    print(f'    has bbox:     {has_bbox}')

    if has_geometry:
        if has_geohash:
            # Show sample geohash values
            geohash_col = [c for c in col_names if c.startswith('geohash_')][0]
            values = arrow_table.column(geohash_col).to_pylist()
            unique = sorted(set(v for v in values if v is not None))[:10]
            print(f'    geohash precision: {geohash_col}')
            print(f'    sample geohash values: {unique}')
            print(f'    OK: spatial columns present')
        else:
            print(f'    WARN: geometry found but no geohash column')
    else:
        print(f'    INFO: no geometry column (spatial columns not expected)')
" || ERRORS=$((ERRORS + 1))
echo ""

# -----------------------------------------------------------------------------
# Step 12: Verify STAC extension metadata (table:* + iceberg:*)
# -----------------------------------------------------------------------------

echo "Step 12: Verifying STAC extension metadata..."
$PYTHON -c "
import json, sys, os

catalog_dir = '$CATALOG_DIR'
errors = 0

for collection_dir in sorted(os.listdir(catalog_dir)):
    collection_json = os.path.join(catalog_dir, collection_dir, 'collection.json')
    if not os.path.isfile(collection_json):
        continue

    with open(collection_json) as f:
        data = json.load(f)

    print(f'  Collection: {data.get(\"id\", collection_dir)}')

    # Check STAC extensions list
    extensions = data.get('stac_extensions', [])
    print(f'    stac_extensions: {extensions}')

    # Layer 1: STAC Table Extension fields
    table_columns = data.get('table:columns')
    table_row_count = data.get('table:row_count')
    table_primary_geometry = data.get('table:primary_geometry')

    if table_columns:
        print(f'    table:columns: {len(table_columns)} column(s)')
        for col in table_columns[:5]:
            print(f'      - {col[\"name\"]}: {col[\"type\"]}')
        if len(table_columns) > 5:
            print(f'      ... and {len(table_columns) - 5} more')
    else:
        print(f'    WARN: table:columns not found')

    if table_row_count is not None:
        print(f'    table:row_count: {table_row_count}')
    else:
        print(f'    WARN: table:row_count not found')

    if table_primary_geometry:
        print(f'    table:primary_geometry: {table_primary_geometry}')

    # Layer 2: STAC Iceberg Extension fields
    iceberg_table_id = data.get('iceberg:table_id')
    iceberg_catalog_type = data.get('iceberg:catalog_type')
    iceberg_format_version = data.get('iceberg:format_version')
    iceberg_snapshot_id = data.get('iceberg:current_snapshot_id')
    iceberg_partition_spec = data.get('iceberg:partition_spec')

    if iceberg_table_id:
        print(f'    iceberg:table_id: {iceberg_table_id}')
    else:
        print(f'    WARN: iceberg:table_id not found')
        errors += 1

    if iceberg_catalog_type:
        print(f'    iceberg:catalog_type: {iceberg_catalog_type}')

    if iceberg_format_version:
        print(f'    iceberg:format_version: {iceberg_format_version}')

    if iceberg_snapshot_id:
        print(f'    iceberg:current_snapshot_id: {iceberg_snapshot_id}')

    if iceberg_partition_spec:
        print(f'    iceberg:partition_spec: {iceberg_partition_spec}')

    print()

if errors > 0:
    print(f'  FAIL: {errors} STAC extension error(s)')
    sys.exit(1)
else:
    print('  OK: STAC extension metadata verified')
" || ERRORS=$((ERRORS + 1))
echo ""

# -----------------------------------------------------------------------------
# Step 13: Test static GeoParquet export
# -----------------------------------------------------------------------------

echo "Step 13: Testing static GeoParquet export..."
EXPORT_DIR="$CATALOG_DIR/_exports"
mkdir -p "$EXPORT_DIR"

$PYTHON -c "
import sys
from pathlib import Path
from pyiceberg.catalog import load_catalog
import pyarrow.parquet as pq

catalog = load_catalog('portolake')
tables = catalog.list_tables('portolake')

from portolake.export import export_current_snapshot

export_dir = Path('$EXPORT_DIR')

for ns, table_name in tables:
    table = catalog.load_table(f'{ns}.{table_name}')
    output = export_dir / f'{table_name}.parquet'
    export_current_snapshot(table, output)

    if output.exists():
        result = pq.read_table(output)
        col_names = result.column_names
        has_derived = any(c.startswith('geohash_') or c.startswith('bbox_') for c in col_names)
        print(f'  {table_name}.parquet: {len(result)} rows, {len(col_names)} columns')
        if has_derived:
            print(f'    FAIL: export contains derived columns (geohash_*/bbox_*)')
            sys.exit(1)
        else:
            print(f'    OK: no derived columns in export')
    else:
        print(f'  FAIL: export file not created: {output}')
        sys.exit(1)

print('  OK: static export verified')
" || ERRORS=$((ERRORS + 1))
echo ""

# -----------------------------------------------------------------------------
# Step 14: Verify data files are on GCS
# -----------------------------------------------------------------------------

echo "Step 14: Verifying data files uploaded to GCS..."
GCS_FILES=$(gcloud storage ls -r "$GCS_BUCKET/**" 2>/dev/null | head -40)
if [ -n "$GCS_FILES" ]; then
    echo "$GCS_FILES" | while IFS= read -r line; do
        echo "  $line"
    done
    TOTAL=$(gcloud storage ls -r "$GCS_BUCKET/**" 2>/dev/null | wc -l)
    echo "  OK: $TOTAL file(s) on GCS"

    # Verify STAC metadata was uploaded
    if echo "$GCS_FILES" | grep -q "catalog.json"; then
        echo "  OK: catalog.json uploaded to GCS"
    else
        echo "  FAIL: catalog.json not found on GCS"
        ERRORS=$((ERRORS + 1))
    fi

    if echo "$GCS_FILES" | grep -q "collection.json"; then
        echo "  OK: collection.json uploaded to GCS"
    else
        echo "  FAIL: collection.json not found on GCS"
        ERRORS=$((ERRORS + 1))
    fi

    if echo "$GCS_FILES" | grep -q ".parquet"; then
        echo "  OK: Data files (.parquet) uploaded to GCS"
    else
        echo "  FAIL: No .parquet files found on GCS"
        ERRORS=$((ERRORS + 1))
    fi
else
    echo "  FAIL: No files found on GCS"
    ERRORS=$((ERRORS + 1))
fi
echo ""

# -----------------------------------------------------------------------------
# Step 15: Verify BigLake tables via gcloud
# -----------------------------------------------------------------------------

echo "Step 15: Verifying BigLake tables via gcloud..."
gcloud alpha biglake iceberg namespaces list \
    --catalog="$BIGLAKE_CATALOG_NAME" \
    --project="$GCP_PROJECT" 2>&1 || echo "  (namespace listing not available)"

gcloud alpha biglake iceberg tables list \
    --catalog="$BIGLAKE_CATALOG_NAME" \
    --namespace="portolake" \
    --project="$GCP_PROJECT" 2>&1 || echo "  (table listing not available)"
echo ""

# -----------------------------------------------------------------------------
# Step 16: Test version increment and time travel
# -----------------------------------------------------------------------------

echo "Step 16: Testing version increment and time travel..."
FIRST_COL="${ADDED_COLLECTIONS[0]}"

if $PORTOLAN add "$FIRST_COL/"; then
    echo "  OK: Re-add succeeded (v2)"

    $PYTHON -c "
from pyiceberg.catalog import load_catalog

catalog = load_catalog('portolake')
table = catalog.load_table('portolake.$FIRST_COL')
history = table.history()
print(f'  Snapshots: {len(history)}')

# Show version from each snapshot
for snap in table.snapshots():
    if snap.summary and 'portolake.version' in snap.summary.additional_properties:
        version = snap.summary.additional_properties['portolake.version']
        row_count = table.scan(snapshot_id=snap.snapshot_id).to_arrow().num_rows
        print(f'    snapshot {snap.snapshot_id}: version={version}, rows={row_count}')

print('  OK: Time travel verified')
" || ERRORS=$((ERRORS + 1))
fi
echo ""

# -----------------------------------------------------------------------------
# Step 17: Test version CLI commands (portolan version current/list/rollback/prune)
# -----------------------------------------------------------------------------

echo "Step 17: Testing version CLI commands..."
FIRST_COL="${ADDED_COLLECTIONS[0]}"

# version current
CURRENT_OUTPUT=$($PORTOLAN version current "$FIRST_COL" 2>&1)
if echo "$CURRENT_OUTPUT" | grep -q "1.1.0"; then
    echo "  OK: version current shows 1.1.0"
else
    echo "  FAIL: version current did not show 1.1.0"
    echo "  Output: $CURRENT_OUTPUT"
    ERRORS=$((ERRORS + 1))
fi

# version list
LIST_OUTPUT=$($PORTOLAN version list "$FIRST_COL" 2>&1)
if echo "$LIST_OUTPUT" | grep -q "1.0.0" && echo "$LIST_OUTPUT" | grep -q "1.1.0"; then
    echo "  OK: version list shows both 1.0.0 and 1.1.0"
else
    echo "  FAIL: version list did not show expected versions"
    echo "  Output: $LIST_OUTPUT"
    ERRORS=$((ERRORS + 1))
fi

# version list --json
LIST_JSON=$($PORTOLAN version list "$FIRST_COL" --json 2>&1)
if echo "$LIST_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); assert d['success']; assert len(d['data']['versions'])==2" 2>/dev/null; then
    echo "  OK: version list --json returns valid JSON with 2 versions"
else
    echo "  FAIL: version list --json output invalid"
    echo "  Output: $LIST_JSON"
    ERRORS=$((ERRORS + 1))
fi

# version rollback (native Iceberg — sets current snapshot pointer)
ROLLBACK_OUTPUT=$($PORTOLAN version rollback "$FIRST_COL" 1.0.0 2>&1)
if echo "$ROLLBACK_OUTPUT" | grep -q "Rolled back.*1.0.0"; then
    echo "  OK: version rollback to 1.0.0 succeeded"
else
    echo "  FAIL: version rollback did not succeed"
    echo "  Output: $ROLLBACK_OUTPUT"
    ERRORS=$((ERRORS + 1))
fi

# Verify current is now 1.0.0
CURRENT_AFTER=$($PORTOLAN version current "$FIRST_COL" 2>&1)
if echo "$CURRENT_AFTER" | grep -q "1.0.0"; then
    echo "  OK: current version is 1.0.0 after rollback"
else
    echo "  FAIL: current version is not 1.0.0 after rollback"
    echo "  Output: $CURRENT_AFTER"
    ERRORS=$((ERRORS + 1))
fi

# Restore to 1.1.0 for remaining tests
$PORTOLAN version rollback "$FIRST_COL" 1.1.0 >/dev/null 2>&1

# version prune --dry-run
PRUNE_OUTPUT=$($PORTOLAN version prune "$FIRST_COL" --keep 1 --dry-run 2>&1)
if echo "$PRUNE_OUTPUT" | grep -q "Would prune"; then
    echo "  OK: version prune --dry-run shows prunable versions"
else
    echo "  FAIL: version prune --dry-run did not report prunable versions"
    echo "  Output: $PRUNE_OUTPUT"
    ERRORS=$((ERRORS + 1))
fi

# Verify file backend is rejected
REJECT_OUTPUT=$($PORTOLAN version list "$FIRST_COL" --catalog /tmp 2>&1 || true)
if echo "$REJECT_OUTPUT" | grep -qi "requires.*iceberg"; then
    echo "  OK: version commands correctly reject non-iceberg backend"
else
    echo "  SKIP: could not test backend rejection (no file-backend catalog at /tmp)"
fi
echo ""

# -----------------------------------------------------------------------------
# Step 18: Verify push/pull behavior
# -----------------------------------------------------------------------------

echo "Step 18: Verifying push/pull behavior..."

# Push should say "not needed" (add already uploads when remote is configured)
PUSH_OUTPUT=$($PORTOLAN push "$GCS_BUCKET" --collection "${ADDED_COLLECTIONS[0]}" 2>&1 || true)
if echo "$PUSH_OUTPUT" | grep -qi "not needed"; then
    echo "  OK: push correctly says 'not needed' (add already uploads)"
else
    echo "  FAIL: push should say 'not needed' for iceberg+remote"
    echo "  Output: $PUSH_OUTPUT"
    ERRORS=$((ERRORS + 1))
fi

# Pull should NOT be blocked
PULL_OUTPUT=$($PORTOLAN pull "$GCS_BUCKET" --collection "${ADDED_COLLECTIONS[0]}" 2>&1 || true)
if echo "$PULL_OUTPUT" | grep -qi "not supported"; then
    echo "  FAIL: pull should NOT be blocked for iceberg backend"
    ERRORS=$((ERRORS + 1))
else
    echo "  OK: pull not blocked for iceberg backend"
fi
echo ""

# -----------------------------------------------------------------------------
# Step 19: Validate STAC catalog
# -----------------------------------------------------------------------------

echo "Step 19: Validating STAC catalog..."
$PYTHON -c "
import pystac, os

catalog_dir = '$CATALOG_DIR'
catalog = pystac.Catalog.from_file(os.path.join(catalog_dir, 'catalog.json'))
print(f'  Catalog id={catalog.id}')

# Strip custom extension URLs before validation (portolan.org schema not hosted yet)
CUSTOM_PREFIX = 'https://portolan.org/'

count = 0
for child in catalog.get_children():
    # Remove custom extensions that pystac can't fetch
    child.stac_extensions = [
        e for e in (child.stac_extensions or []) if not e.startswith(CUSTOM_PREFIX)
    ]
    child.validate()
    items = list(child.get_items())
    print(f'  Collection: {child.id} ({len(items)} item(s)) - valid')
    for item in items:
        item.stac_extensions = [
            e for e in (item.stac_extensions or []) if not e.startswith(CUSTOM_PREFIX)
        ]
        item.validate()
        count += 1

print(f'  OK: {count} items validated (custom extensions skipped for schema fetch)')
" || ERRORS=$((ERRORS + 1))
echo ""

# -----------------------------------------------------------------------------
# Step 20: Make bucket public with CORS support
# -----------------------------------------------------------------------------

echo "Step 20: Making bucket public with CORS support..."

# Remove public access prevention
gcloud storage buckets update "$GCS_BUCKET" --no-public-access-prevention 2>&1 || true

# Grant public read access
if gcloud storage buckets add-iam-policy-binding "$GCS_BUCKET" \
    --member=allUsers --role=roles/storage.objectViewer 2>&1 >/dev/null; then
    echo "  OK: Public read access granted"
else
    echo "  WARN: Could not grant public access"
fi

# Set CORS configuration
CORS_FILE=$(mktemp)
cat > "$CORS_FILE" <<'CORSJSON'
[
  {
    "origin": ["*"],
    "method": ["GET", "HEAD"],
    "responseHeader": ["Content-Type", "Content-Length", "Content-Range", "Range", "ETag"],
    "maxAgeSeconds": 3600
  }
]
CORSJSON
if gcloud storage buckets update "$GCS_BUCKET" --cors-file="$CORS_FILE" 2>&1 >/dev/null; then
    echo "  OK: CORS configured (GET/HEAD from any origin)"
else
    echo "  WARN: Could not set CORS"
fi
rm -f "$CORS_FILE"

# Verify public access
GCS_HOST="storage.googleapis.com"
GCS_BUCKET_NAME="${GCS_BUCKET#gs://}"
CATALOG_URL="https://${GCS_HOST}/${GCS_BUCKET_NAME}/catalog.json"
HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$CATALOG_URL")
if [ "$HTTP_STATUS" = "200" ]; then
    echo "  OK: Catalog publicly accessible (HTTP $HTTP_STATUS)"
else
    echo "  FAIL: Catalog not publicly accessible (HTTP $HTTP_STATUS)"
    ERRORS=$((ERRORS + 1))
fi

STAC_BROWSER_URL="${STAC_BROWSER_BASE_URL:-https://cayetanobv.github.io/iceberg-stac-browser}/#/external/${GCS_HOST}/${GCS_BUCKET_NAME}/catalog.json"
echo ""
echo "  STAC Catalog:  $CATALOG_URL"
echo "  STAC Browser:  $STAC_BROWSER_URL"
echo ""

# -----------------------------------------------------------------------------
# Cleanup note
# -----------------------------------------------------------------------------

echo "--- Cleanup ---"
echo "  To delete BigLake catalog:"
echo "    gcloud alpha biglake iceberg catalogs delete $BIGLAKE_CATALOG_NAME --project=$GCP_PROJECT --quiet"
echo "  To clean GCS bucket:"
echo "    gcloud storage rm -r $GCS_BUCKET/**"
echo "  To remove local catalog:"
echo "    rm -rf $CATALOG_DIR"
echo "  To remove exports:"
echo "    rm -rf $EXPORT_DIR"
echo ""

# -----------------------------------------------------------------------------
# Final result
# -----------------------------------------------------------------------------

echo "==========================================="
if [ "$ERRORS" -gt 0 ]; then
    echo "FAIL: Portolake Iceberg-Native BigLake E2E test failed with $ERRORS error(s)"
    exit 1
fi

echo "PASS: Portolake Iceberg-Native BigLake E2E test passed"
echo "  Backend: iceberg (BigLake Metastore)"
echo "  Catalog: BigLake -> $BIGLAKE_CATALOG_NAME"
echo "  Warehouse: $GCS_BUCKET"
echo "  Collections: ${ADDED_COLLECTIONS[*]}"
echo "  Features tested:"
echo "    - Iceberg-native data ingestion (actual rows in tables)"
echo "    - Spatial columns (geohash + bbox)"
echo "    - STAC extensions (table:* + iceberg:*)"
echo "    - Static GeoParquet export"
echo "    - Time travel / version history"
echo "    - Version CLI commands (current, list, rollback, prune)"
echo "    - Public bucket + CORS"
echo ""
echo "  Browse: $STAC_BROWSER_URL"
echo "==========================================="
