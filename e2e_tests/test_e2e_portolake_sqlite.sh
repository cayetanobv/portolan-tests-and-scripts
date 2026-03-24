#!/usr/bin/env bash
# End-to-end test for portolake (Iceberg backend) features.
#
# Tests the full workflow: init --backend iceberg → add → verify iceberg tables
# → verify push is blocked → verify versions via backend.
#
# Uses only geoparquet-compatible raw data (parquet, geojson).
# Skips raster (elevation/tif) and FileGDB (fire) data.
#
# Usage:
#   ./test_e2e_portolake.sh
#
# Prerequisites:
#   - Raw data in test-catalog-raw-data/{agriculture,boundaries}
#   - GCS credentials configured (for bucket cleanup/verification)
#   - Local dev versions of portolan-cli and portolake repos
#

set -euo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BASE_DIR="/home/cayetano/dev_projs/portolan"
CATALOG_DIR="${1:-$SCRIPT_DIR/test-catalogs/test-catalog-portolake-sqlite}"
GCS_BUCKET="${2:-gs://cayetanobv-portolake-iceberg-sqlite}"
RAW_DATA_DIR="$SCRIPT_DIR/test-catalogs/test-catalog-raw-data"
PORTOLAKE_DIR="$BASE_DIR/portolake"
PORTOLAN_CLI_DIR="$BASE_DIR/portolan-cli"

# Use portolake's venv (has both portolake + portolan-cli)
PORTOLAN="uv run --project $PORTOLAKE_DIR portolan"
PYTHON="uv run --project $PORTOLAKE_DIR python3"

# Only geoparquet-compatible collections (skip elevation/tif, fire/FileGDB)
GEOPARQUET_COLLECTIONS=("agriculture" "boundaries")

ERRORS=0

# ─────────────────────────────────────────────────────────────────────────────
# Preflight checks
# ─────────────────────────────────────────────────────────────────────────────

echo "=== Portolake E2E Test ==="
echo ""
echo "  Catalog dir:    $CATALOG_DIR"
echo "  GCS bucket:     $GCS_BUCKET"
echo "  Raw data dir:   $RAW_DATA_DIR"
echo "  Portolake dir:  $PORTOLAKE_DIR"
echo "  Portolan CLI:   $PORTOLAN_CLI_DIR"
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

# ─────────────────────────────────────────────────────────────────────────────
# Step 0: Install local portolan-cli (dev version) into portolake's venv
# ─────────────────────────────────────────────────────────────────────────────

echo "Step 0: Setting up environment..."

# Sync portolake dependencies first
cd "$PORTOLAKE_DIR"
uv sync --all-extras --quiet

# Install local portolan-cli dev version (overrides PyPI release)
uv pip install --project "$PORTOLAKE_DIR" -e "$PORTOLAN_CLI_DIR" --quiet

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

# Verify portolan-cli version has --backend flag
if $PORTOLAN init --help 2>&1 | grep -q "\-\-backend"; then
    echo "  OK: portolan init --backend flag available"
else
    echo "FAIL: portolan init --backend flag not found (is local portolan-cli installed?)"
    exit 1
fi
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 1: Clean GCS bucket
# ─────────────────────────────────────────────────────────────────────────────

BUCKET_NAME="${GCS_BUCKET#gs://}"
BUCKET_NAME="${BUCKET_NAME%%/*}"

echo "Step 1: Cleaning GCS bucket gs://$BUCKET_NAME..."
if ! gcloud storage ls "gs://$BUCKET_NAME" &>/dev/null; then
    echo "FAIL: Cannot access gs://$BUCKET_NAME — run 'gcloud auth login' first"
    exit 1
fi
gcloud storage rm -r "gs://$BUCKET_NAME/**" 2>/dev/null || true
echo "  OK: Bucket cleaned"
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 2: Clean previous catalog
# ─────────────────────────────────────────────────────────────────────────────

echo "Step 2: Cleaning previous catalog..."
if [ -d "$CATALOG_DIR" ]; then
    rm -rf "$CATALOG_DIR"
fi
mkdir -p "$CATALOG_DIR"
echo "  OK: Cleaned"
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 3: Initialize catalog with Iceberg backend
# ─────────────────────────────────────────────────────────────────────────────

echo "Step 3: Initializing catalog with --backend iceberg..."
cd "$CATALOG_DIR"
$PORTOLAN init --auto --backend iceberg
echo ""

# Verify iceberg backend artifacts
echo "  Checking backend artifacts..."
# Note: iceberg.db is created lazily on first add, not at init time
if [ -f "$CATALOG_DIR/.portolan/iceberg.db" ]; then
    echo "  OK: .portolan/iceberg.db exists (SQLite catalog)"
else
    echo "  INFO: .portolan/iceberg.db not yet created (created on first add)"
fi

if [ -f "$CATALOG_DIR/versions.json" ]; then
    echo "  FAIL: versions.json should NOT exist for iceberg backend"
    ERRORS=$((ERRORS + 1))
else
    echo "  OK: No versions.json (iceberg handles versioning)"
fi

if [ -f "$CATALOG_DIR/.portolan/state.json" ]; then
    echo "  FAIL: state.json should NOT exist for iceberg backend"
    ERRORS=$((ERRORS + 1))
else
    echo "  OK: No state.json (iceberg handles state)"
fi

# Verify config.yaml has backend setting
if grep -q "backend: iceberg" "$CATALOG_DIR/.portolan/config.yaml" 2>/dev/null; then
    echo "  OK: config.yaml has backend: iceberg"
else
    echo "  FAIL: config.yaml missing 'backend: iceberg'"
    ERRORS=$((ERRORS + 1))
fi
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 4: Copy geoparquet-compatible raw data
# ─────────────────────────────────────────────────────────────────────────────

echo "Step 4: Copying raw data (geoparquet-compatible only)..."
for collection in "${GEOPARQUET_COLLECTIONS[@]}"; do
    collection_src="$RAW_DATA_DIR/$collection"
    mkdir -p "$CATALOG_DIR/$collection"

    for file in "$collection_src"/*; do
        filename=$(basename "$file")

        # Skip non-geoparquet formats
        case "$filename" in
            *.parquet|*.geojson)
                # Place files directly in collection dir (flat structure)
                # portolan add will detect items from file stems
                cp "$file" "$CATALOG_DIR/$collection/"
                echo "  -> $collection/$filename"
                ;;
            *)
                echo "  -- Skipping $filename (not geoparquet-compatible)"
                ;;
        esac
    done
done
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 5: Scan catalog
# ─────────────────────────────────────────────────────────────────────────────

echo "Step 5: Scanning catalog..."
$PORTOLAN scan --tree || true
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 6: Add files to catalog
# ─────────────────────────────────────────────────────────────────────────────

echo "Step 6: Adding files to catalog..."
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

# ─────────────────────────────────────────────────────────────────────────────
# Step 7: Fix STAC metadata
# ─────────────────────────────────────────────────────────────────────────────

echo "Step 7: Fixing STAC metadata..."
$PORTOLAN check --metadata --fix || echo "  WARN: Some metadata fixes had non-fatal issues (bbox etc.)"
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 8: Verify Iceberg backend has version data
# ─────────────────────────────────────────────────────────────────────────────

echo "Step 8: Verifying Iceberg version tracking..."
$PYTHON -c "
import sys
from pathlib import Path
from pyiceberg.catalog.sql import SqlCatalog

catalog_root = Path('$CATALOG_DIR')
db_path = catalog_root / '.portolan' / 'iceberg.db'

if not db_path.exists():
    print('  FAIL: iceberg.db not found')
    sys.exit(1)

catalog = SqlCatalog(
    'portolake',
    **{
        'type': 'sql',
        'uri': f'sqlite:///{db_path}',
        'warehouse': f'file:///{catalog_root}/.portolan/warehouse',
    },
)

try:
    namespaces = catalog.list_namespaces()
    print(f'  Namespaces: {namespaces}')
except Exception as e:
    print(f'  WARN: list_namespaces failed: {e}')

try:
    tables = catalog.list_tables('portolake')
    print(f'  Tables: {tables}')

    if not tables:
        print('  FAIL: No tables found in portolake namespace')
        sys.exit(1)

    for ns, table_name in tables:
        table = catalog.load_table(f'{ns}.{table_name}')
        snap = table.current_snapshot()
        if snap and snap.summary:
            version = snap.summary.get('portolake.version', '?')
            print(f'  OK: {ns}.{table_name} -> version {version}')

            # Verify hrefs are relative (not absolute paths)
            import json
            assets_json = snap.summary.additional_properties.get('portolake.assets', '{}')
            assets = json.loads(assets_json)
            for name, asset in assets.items():
                href = asset.get('href', '')
                if href.startswith('/'):
                    print(f'  FAIL: Absolute href in iceberg snapshot: {href}')
                    sys.exit(1)
                if not href.startswith(f'{table_name}/'):
                    print(f'  FAIL: Href not relative to collection: {href}')
                    sys.exit(1)
            print(f'  OK: All hrefs in {table_name} are catalog-root-relative')
        else:
            print(f'  WARN: {ns}.{table_name} has no snapshot')

except Exception as e:
    print(f'  FAIL: Error inspecting tables: {e}')
    sys.exit(1)

print('  OK: Iceberg version tracking verified')
" || ERRORS=$((ERRORS + 1))
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 9: Verify collection-level versions.json still created by add
# ─────────────────────────────────────────────────────────────────────────────

echo "Step 9: Checking collection versions.json..."
for collection in "${ADDED_COLLECTIONS[@]}"; do
    versions_file="$CATALOG_DIR/$collection/versions.json"
    if [ -f "$versions_file" ]; then
        version=$($PYTHON -c "
import json
data = json.load(open('$versions_file'))
print(data.get('current_version', 'none'))
")
        echo "  OK: $collection/versions.json exists (version: $version)"

        # Verify hrefs are catalog-root-relative
        $PYTHON -c "
import json, sys
data = json.load(open('$versions_file'))
for v in data['versions']:
    for name, asset in v['assets'].items():
        href = asset['href']
        if not href.startswith('$collection/'):
            print(f'  FAIL: Bad href in $collection: {href}')
            sys.exit(1)
print('  OK: All hrefs in $collection are catalog-root-relative')
" || ERRORS=$((ERRORS + 1))
    else
        echo "  INFO: $collection/versions.json not found (iceberg may handle this differently)"
    fi
done
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 10: Verify push is blocked for iceberg backend (no remote configured)
# ─────────────────────────────────────────────────────────────────────────────

echo "Step 10: Verifying push is blocked for iceberg backend..."
for collection in "${ADDED_COLLECTIONS[@]}"; do
    PUSH_OUTPUT=$($PORTOLAN push "$GCS_BUCKET" --collection "$collection" 2>&1 || true)
    if echo "$PUSH_OUTPUT" | grep -qi "not supported"; then
        echo "  OK: push correctly blocked for $collection (iceberg backend, no remote)"
    else
        echo "  FAIL: push should have been blocked for iceberg backend without remote"
        echo "  Output: $PUSH_OUTPUT"
        ERRORS=$((ERRORS + 1))
    fi
    break  # Only need to test once
done
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 11: Verify pull is NOT blocked for iceberg backend
# ─────────────────────────────────────────────────────────────────────────────

echo "Step 11: Verifying pull is not blocked for iceberg backend..."
PULL_OUTPUT=$($PORTOLAN pull "$GCS_BUCKET" --collection "${ADDED_COLLECTIONS[0]}" 2>&1 || true)
if echo "$PULL_OUTPUT" | grep -qi "not supported"; then
    echo "  FAIL: pull should NOT be blocked for iceberg backend"
    ERRORS=$((ERRORS + 1))
else
    echo "  OK: pull not blocked for iceberg backend"
fi
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 12: Verify portolan list and info work
# ─────────────────────────────────────────────────────────────────────────────

echo "Step 12: Verifying CLI commands..."
if $PORTOLAN list 2>&1; then
    echo "  OK: portolan list succeeded"
else
    echo "  WARN: portolan list failed (may not support iceberg yet)"
fi

if $PORTOLAN info 2>&1; then
    echo "  OK: portolan info succeeded"
else
    echo "  WARN: portolan info failed (may not support iceberg yet)"
fi
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 13: Verify STAC catalog is valid
# ─────────────────────────────────────────────────────────────────────────────

echo "Step 13: Validating STAC catalog structure..."
$PYTHON -c "
import pystac, os, json

catalog_dir = '$CATALOG_DIR'
catalog = pystac.Catalog.from_file(os.path.join(catalog_dir, 'catalog.json'))
print(f'  Catalog id={catalog.id}')

# Validate structure without fetching remote schemas
# (Iceberg STAC extension schema may not be hosted yet)
count = 0
for child in catalog.get_children():
    items = list(child.get_items())
    print(f'  Collection: {child.id} ({len(items)} item(s))')
    for item in items:
        assets_str = ', '.join(item.assets.keys())
        print(f'    Item: {item.id} (assets: {assets_str})')
        count += 1

# Verify collections have correct stac_extensions
for child in catalog.get_children():
    coll_path = os.path.join(catalog_dir, child.id, 'collection.json')
    if os.path.exists(coll_path):
        coll_data = json.load(open(coll_path))
        exts = coll_data.get('stac_extensions', [])
        has_iceberg = any('iceberg' in e for e in exts)
        if has_iceberg:
            print(f'  OK: {child.id} has Iceberg STAC extension')
        else:
            print(f'  INFO: {child.id} missing Iceberg STAC extension')

print(f'  OK: {count} items found, catalog structure valid')
" || ERRORS=$((ERRORS + 1))
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 14: Test add → update cycle (version increment)
# ─────────────────────────────────────────────────────────────────────────────

echo "Step 14: Testing version increment (re-add to trigger new version)..."
# Pick the first collection and re-add to test version bumping
FIRST_COL="${ADDED_COLLECTIONS[0]}"
echo "  -> Re-adding $FIRST_COL/..."
if $PORTOLAN add "$FIRST_COL/"; then
    echo "  OK: Re-add succeeded"

    # Check if version incremented in iceberg
    $PYTHON -c "
from pathlib import Path
from pyiceberg.catalog.sql import SqlCatalog

catalog_root = Path('$CATALOG_DIR')
db_path = catalog_root / '.portolan' / 'iceberg.db'

catalog = SqlCatalog(
    'portolake',
    **{
        'type': 'sql',
        'uri': f'sqlite:///{db_path}',
        'warehouse': f'file:///{catalog_root}/.portolan/warehouse',
    },
)

tables = catalog.list_tables('portolake')
for ns, table_name in tables:
    table = catalog.load_table(f'{ns}.{table_name}')
    history = table.history()
    if len(history) > 1:
        print(f'  OK: {ns}.{table_name} has {len(history)} snapshots (version history works)')
    else:
        print(f'  INFO: {ns}.{table_name} has {len(history)} snapshot(s)')
" || ERRORS=$((ERRORS + 1))
else
    echo "  WARN: Re-add did not produce new version (may be no-op if unchanged)"
fi
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 15: Test remote upload on add (SQLite + remote = single-user prod mode)
# ─────────────────────────────────────────────────────────────────────────────

echo "Step 15: Testing remote upload on add (SQLite + remote)..."

# Clean GCS bucket first
BUCKET_NAME="${GCS_BUCKET#gs://}"
BUCKET_NAME="${BUCKET_NAME%%/*}"
echo "  Cleaning GCS bucket gs://$BUCKET_NAME..."
if ! gcloud storage ls "gs://$BUCKET_NAME" &>/dev/null; then
    echo "  SKIP: Cannot access gs://$BUCKET_NAME — skipping remote upload test"
else
    gcloud storage rm -r "gs://$BUCKET_NAME/**" 2>/dev/null || true

    # Configure remote
    $PORTOLAN config set remote "$GCS_BUCKET"
    echo "  OK: Configured remote=$GCS_BUCKET"

    # Create a new top-level collection for the remote test
    # (avoid nested dirs which create invalid collection names)
    REMOTE_TEST_COL="remote-test"
    RAW_SOURCE=$(find "$RAW_DATA_DIR" -name "*.geojson" 2>/dev/null | head -1)
    mkdir -p "$CATALOG_DIR/$REMOTE_TEST_COL"
    cp "$RAW_SOURCE" "$CATALOG_DIR/$REMOTE_TEST_COL/"
    echo "  -> Adding $REMOTE_TEST_COL/ (should upload to GCS)..."
    if $PORTOLAN add "$REMOTE_TEST_COL/"; then
        echo "  OK: Add with remote upload succeeded"

        # Verify files were uploaded
        GCS_FILES=$(gcloud storage ls -r "$GCS_BUCKET/**" 2>/dev/null | head -20)
        if [ -n "$GCS_FILES" ]; then
            TOTAL=$(gcloud storage ls -r "$GCS_BUCKET/**" 2>/dev/null | wc -l)
            echo "  OK: $TOTAL file(s) uploaded to GCS"

            if echo "$GCS_FILES" | grep -q "catalog.json"; then
                echo "  OK: catalog.json uploaded"
            else
                echo "  FAIL: catalog.json not found on GCS"
                ERRORS=$((ERRORS + 1))
            fi

            if echo "$GCS_FILES" | grep -q ".parquet"; then
                echo "  OK: Data files uploaded"
            else
                echo "  FAIL: No .parquet files on GCS"
                ERRORS=$((ERRORS + 1))
            fi
        else
            echo "  FAIL: No files found on GCS after add with remote"
            ERRORS=$((ERRORS + 1))
        fi

        # Verify push now says "not needed"
        PUSH_OUTPUT=$($PORTOLAN push "$GCS_BUCKET" --collection "$REMOTE_TEST_COL" 2>&1 || true)
        if echo "$PUSH_OUTPUT" | grep -qi "not needed"; then
            echo "  OK: push correctly says 'not needed' with remote configured"
        else
            echo "  FAIL: push should say 'not needed' when remote is configured"
            echo "  Output: $PUSH_OUTPUT"
            ERRORS=$((ERRORS + 1))
        fi
    else
        echo "  FAIL: Re-add with remote failed"
        ERRORS=$((ERRORS + 1))
    fi
fi
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Final result
# ─────────────────────────────────────────────────────────────────────────────

echo "==========================================="
if [ "$ERRORS" -gt 0 ]; then
    echo "FAIL: Portolake E2E test failed with $ERRORS error(s)"
    exit 1
fi

echo "PASS: Portolake E2E test passed"
echo "  Backend: iceberg (SQLite)"
echo "  Collections tested: ${ADDED_COLLECTIONS[*]}"
echo "  Catalog: $CATALOG_DIR"
echo "  Remote: $GCS_BUCKET"
echo "==========================================="
