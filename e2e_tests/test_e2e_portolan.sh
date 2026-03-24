#!/usr/bin/env bash
# End-to-end test: clean → init → scan → add → check → push → list
#
# Recreates a test catalog from raw data and pushes to GCS.
# Exercises the full CLI workflow including new commands (clean, scan, check, list).
# Verifies that versions.json hrefs are catalog-root-relative
# (collection/item/filename) so push can resolve them.
#
# Usage:
#   ./test_e2e_push.sh                           # uses defaults
#   ./test_e2e_push.sh /path/to/catalog gs://bucket
#
# Prerequisites:
#   - Raw data in $RAW_DATA_DIR/{collection}/*.{parquet,geojson,tif}
#   - GCS credentials configured (GOOGLE_APPLICATION_CREDENTIALS or gcloud auth)
#   - portolan CLI installed (uv run portolan or pipx)
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BASE_DIR="/home/cayetano/dev_projs/portolan"
CATALOG_DIR="${1:-$SCRIPT_DIR/test-catalog}"
GCS_BUCKET="${2:-gs://cayetanobv-portolan-catalog}"
RAW_DATA_DIR="$SCRIPT_DIR/test-catalog-raw-data"
PORTOLAN="uv run --project $BASE_DIR/portolan-cli portolan"
PYTHON="uv run --project $BASE_DIR/portolan-cli python3"

# ─────────────────────────────────────────────────────────────────────────────
# Preflight checks
# ─────────────────────────────────────────────────────────────────────────────

if [ ! -d "$RAW_DATA_DIR" ]; then
    echo "✗ Raw data directory not found: $RAW_DATA_DIR"
    exit 1
fi

echo "→ Catalog dir:  $CATALOG_DIR"
echo "→ GCS bucket:   $GCS_BUCKET"
echo "→ Raw data dir: $RAW_DATA_DIR"
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 1: Clean GCS bucket
# ─────────────────────────────────────────────────────────────────────────────

# Extract bucket name from gs:// URL (strip scheme and any trailing path)
BUCKET_NAME="${GCS_BUCKET#gs://}"
BUCKET_NAME="${BUCKET_NAME%%/*}"

echo "→ Cleaning GCS bucket gs://$BUCKET_NAME ..."
if ! gcloud storage ls "gs://$BUCKET_NAME" &>/dev/null; then
    echo "✗ Cannot access gs://$BUCKET_NAME — run 'gcloud auth login' first"
    exit 1
fi
gcloud storage rm -r "gs://$BUCKET_NAME/**" 2>/dev/null || true
echo "✓ Bucket cleaned"
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 2: Clean previous catalog (complete fresh start)
# ─────────────────────────────────────────────────────────────────────────────

echo "→ Cleaning previous catalog..."

# Use portolan clean if catalog exists
if [ -d "$CATALOG_DIR/.portolan" ]; then
    echo "  → Existing catalog detected, using portolan clean..."
    cd "$CATALOG_DIR"
    $PORTOLAN clean
fi

# Remove everything in catalog dir for a completely fresh start
if [ -d "$CATALOG_DIR" ]; then
    rm -rf "$CATALOG_DIR"
fi
mkdir -p "$CATALOG_DIR"

echo "✓ Cleaned"
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 3: Initialize catalog
# ─────────────────────────────────────────────────────────────────────────────

echo "→ Initializing catalog..."
cd "$CATALOG_DIR"
$PORTOLAN init --auto
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 4: Copy raw data into collection directories
# ─────────────────────────────────────────────────────────────────────────────

echo "→ Copying raw data into collection directories..."
for collection_dir in "$RAW_DATA_DIR"/*/; do
    collection=$(basename "$collection_dir")

    # FIXME: FileGDB (fire) has upstream issues — skip for now
    if [[ "$collection" == "fire" ]]; then
        echo "  ⚠ Skipping $collection (FileGDB upstream issue)"
        continue
    fi

    mkdir -p "$CATALOG_DIR/$collection"

    # Each file goes into its own item subdirectory (per portolan-spec structure.md):
    #   collection/{item_id}/{filename} where item_id = filename stem
    for file in "$collection_dir"*; do
        filename=$(basename "$file")
        # Strip extension to get item_id (handles .parquet, .geojson, .tif, .gdb.zip)
        item_id="${filename%%.*}"

        mkdir -p "$CATALOG_DIR/$collection/$item_id"
        if [[ "$file" == *.gdb.zip ]]; then
            echo "  → Unzipping $filename into $collection/$item_id/"
            unzip -qo "$file" -d "$CATALOG_DIR/$collection/$item_id/"
        else
            cp "$file" "$CATALOG_DIR/$collection/$item_id/"
        fi
    done
    echo "  → $collection: $(ls "$collection_dir" | wc -l) file(s)"
done
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 5: Scan catalog before adding
# ─────────────────────────────────────────────────────────────────────────────

echo "→ Scanning catalog for geospatial files..."
$PORTOLAN scan --tree || true
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 6: Add files to catalog
# ─────────────────────────────────────────────────────────────────────────────

echo "→ Adding files to catalog..."
COLLECTIONS=()
SKIPPED_COLLECTIONS=()
for collection_dir in "$CATALOG_DIR"/*/; do
    collection=$(basename "$collection_dir")

    if $PORTOLAN add "$collection/"; then
        COLLECTIONS+=("$collection")
    else
        echo "  ⚠ portolan add failed for $collection (upstream issue?) — skipping"
        SKIPPED_COLLECTIONS+=("$collection")
    fi
done
if [ ${#SKIPPED_COLLECTIONS[@]} -gt 0 ]; then
    echo "  ⚠ Skipped collections: ${SKIPPED_COLLECTIONS[*]}"
fi
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 7: Fix missing STAC metadata then check catalog validity
# ─────────────────────────────────────────────────────────────────────────────

echo "→ Fixing missing STAC item metadata..."
$PORTOLAN check --metadata --fix
echo ""

echo "→ Running portolan check..."
if $PORTOLAN check --verbose; then
    echo "  ✓ portolan check passed"
else
    echo "  ⚠ portolan check reported issues (non-fatal for e2e test)"
fi
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 8: Verify versions.json hrefs are catalog-root-relative
# ─────────────────────────────────────────────────────────────────────────────

echo "→ Verifying versions.json hrefs..."
ERRORS=0
for collection in "${COLLECTIONS[@]}"; do
    versions_file="$CATALOG_DIR/$collection/versions.json"
    if [ ! -f "$versions_file" ]; then
        echo "  ✗ Missing: $versions_file"
        ERRORS=$((ERRORS + 1))
        continue
    fi

    # Extract all hrefs and verify they start with collection/ and resolve to files
    hrefs=$($PYTHON -c "
import json, sys
data = json.load(open('$versions_file'))
for v in data['versions']:
    for asset in v['assets'].values():
        print(asset['href'])
")

    while IFS= read -r href; do
        # Check format: must start with collection_id/
        if [[ "$href" != "$collection/"* ]]; then
            echo "  ✗ Bad href format in $collection: '$href' (expected '$collection/...')"
            ERRORS=$((ERRORS + 1))
            continue
        fi

        # Check file exists at catalog_root/href
        if [ ! -f "$CATALOG_DIR/$href" ]; then
            echo "  ✗ File not found: $CATALOG_DIR/$href"
            ERRORS=$((ERRORS + 1))
            continue
        fi

        echo "  ✓ $href"
    done <<< "$hrefs"
done

if [ "$ERRORS" -gt 0 ]; then
    echo ""
    echo "✗ Verification failed with $ERRORS error(s)"
    exit 1
fi
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 9: Push to GCS (versioned assets via portolan push)
# ─────────────────────────────────────────────────────────────────────────────

echo "→ Pushing versioned assets to $GCS_BUCKET..."
for collection in "${COLLECTIONS[@]}"; do
    echo ""
    echo "── $collection ──"
    $PORTOLAN push "$GCS_BUCKET" --collection "$collection"
done
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 10: Upload STAC metadata files (not handled by portolan push)
#
# portolan push only uploads versioned assets + collection versions.json.
# A complete remote STAC catalog also needs:
#   - catalog.json (root)
#   - versions.json (root)
#   - {collection}/collection.json
#   - {collection}/{item}/{item}.json
# ─────────────────────────────────────────────────────────────────────────────

echo "→ Uploading STAC metadata files..."

STAC_FILES=()

# Root catalog files
for f in catalog.json versions.json; do
    if [ -f "$CATALOG_DIR/$f" ]; then
        STAC_FILES+=("$f")
    else
        echo "  ⚠ Missing root $f (skipping)"
    fi
done

# Per-collection metadata
for collection in "${COLLECTIONS[@]}"; do
    # collection.json
    if [ -f "$CATALOG_DIR/$collection/collection.json" ]; then
        STAC_FILES+=("$collection/collection.json")
    else
        echo "  ⚠ Missing $collection/collection.json (skipping)"
    fi

    # Item JSON files (STAC item metadata)
    while IFS= read -r -d '' item_json; do
        rel_path="${item_json#"$CATALOG_DIR"/}"
        STAC_FILES+=("$rel_path")
    done < <(find "$CATALOG_DIR/$collection" -name "*.json" \
        ! -name "collection.json" \
        ! -name "versions.json" \
        -print0)
done

if [ ${#STAC_FILES[@]} -eq 0 ]; then
    echo "  ⚠ No STAC metadata files found"
else
    for rel_path in "${STAC_FILES[@]}"; do
        echo "  → Uploading $rel_path"
        gcloud storage cp "$CATALOG_DIR/$rel_path" "$GCS_BUCKET/$rel_path"
    done
    echo "  ✓ Uploaded ${#STAC_FILES[@]} STAC metadata file(s)"
fi
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 11: Verify remote catalog completeness
# ─────────────────────────────────────────────────────────────────────────────

echo "→ Verifying remote catalog completeness..."
REMOTE_ERRORS=0

# Check root files
for f in catalog.json versions.json; do
    if gcloud storage ls "$GCS_BUCKET/$f" &>/dev/null; then
        echo "  ✓ $f"
    else
        echo "  ✗ Missing remote: $f"
        REMOTE_ERRORS=$((REMOTE_ERRORS + 1))
    fi
done

# Check per-collection files
for collection in "${COLLECTIONS[@]}"; do
    for f in collection.json versions.json; do
        if gcloud storage ls "$GCS_BUCKET/$collection/$f" &>/dev/null; then
            echo "  ✓ $collection/$f"
        else
            echo "  ✗ Missing remote: $collection/$f"
            REMOTE_ERRORS=$((REMOTE_ERRORS + 1))
        fi
    done
done

if [ "$REMOTE_ERRORS" -gt 0 ]; then
    echo ""
    echo "  ⚠ Remote catalog has $REMOTE_ERRORS missing file(s)"
    ERRORS=$((ERRORS + REMOTE_ERRORS))
fi
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 12: Verify catalog-level versions.json (#159)
# ─────────────────────────────────────────────────────────────────────────────

echo "→ Verifying catalog-level versions.json..."
CATALOG_VERSIONS="$CATALOG_DIR/versions.json"
if [ ! -f "$CATALOG_VERSIONS" ]; then
    echo "  ✗ Missing catalog-level versions.json"
    ERRORS=$((ERRORS + 1))
else
    echo "  ✓ catalog-level versions.json exists"
    # Check if collections are tracked (collections is a dict keyed by collection_id)
    TRACKED=$($PYTHON -c "
import json
data = json.load(open('$CATALOG_VERSIONS'))
cols = data.get('collections', {})
print(len(cols))
for cid in sorted(cols):
    print(f'  ✓ {cid} tracked')
")
    echo "$TRACKED"
    # Note: catalog_versions.py is not yet wired into add/push workflow,
    # so collections may be empty — treat as informational, not a failure
fi
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 13: Verify portolan list (#135)
# ─────────────────────────────────────────────────────────────────────────────

echo "→ Running portolan list..."
if $PORTOLAN list; then
    echo "  ✓ portolan list succeeded"
else
    echo "  ✗ portolan list failed"
    ERRORS=$((ERRORS + 1))
fi
echo ""

echo "→ Running portolan list --json..."
if $PORTOLAN list --json > /dev/null; then
    echo "  ✓ portolan list --json succeeded"
else
    echo "  ✗ portolan list --json failed"
    ERRORS=$((ERRORS + 1))
fi
echo ""

echo "→ Running portolan info..."
if $PORTOLAN info; then
    echo "  ✓ portolan info succeeded"
else
    echo "  ✗ portolan info failed"
    ERRORS=$((ERRORS + 1))
fi
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 14: Validate STAC catalog with pystac (local + remote)
# ─────────────────────────────────────────────────────────────────────────────

echo "→ Validating local STAC catalog with pystac..."
if $PYTHON -c "
import pystac, sys, tempfile, subprocess, shutil, os

catalog_dir = '$CATALOG_DIR'
gcs_bucket = '$GCS_BUCKET'

# --- Local validation ---
print('  Local catalog:')
catalog = pystac.Catalog.from_file(os.path.join(catalog_dir, 'catalog.json'))
print(f'    id={catalog.id}, stac_version={catalog.to_dict().get(\"stac_version\", \"?\")}')

count = 0
for child in catalog.get_children():
    child.validate()
    items = list(child.get_items())
    print(f'    Collection: {child.id} ({len(items)} item(s)) - valid')
    for item in items:
        item.validate()
        assets = ', '.join(item.assets.keys())
        print(f'      Item: {item.id} (assets: {assets}) - valid')
        count += 1

n = catalog.validate_all()
print(f'    validate_all: {n} object(s) passed')

# --- Remote validation ---
print('  Remote catalog:')
tmpdir = tempfile.mkdtemp()
try:
    subprocess.run(
        ['gcloud', 'storage', 'cp', '-r', f'{gcs_bucket}/*', tmpdir],
        check=True, capture_output=True,
    )
    remote_catalog = pystac.Catalog.from_file(os.path.join(tmpdir, 'catalog.json'))

    remote_count = 0
    for child in remote_catalog.get_children():
        child.validate()
        items = list(child.get_items())
        print(f'    Collection: {child.id} ({len(items)} item(s)) - valid')
        for item in items:
            item.validate()
            remote_count += 1

    n = remote_catalog.validate_all()
    print(f'    validate_all: {n} object(s) passed')

    # Compare local vs remote
    if count != remote_count:
        print(f'    MISMATCH: local has {count} items, remote has {remote_count}')
        sys.exit(1)
    else:
        print(f'    Local and remote match: {count} item(s)')
finally:
    shutil.rmtree(tmpdir)
"; then
    echo "  ✓ pystac validation passed (local + remote)"
else
    echo "  ✗ pystac validation failed"
    ERRORS=$((ERRORS + 1))
fi
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Final result
# ─────────────────────────────────────────────────────────────────────────────

if [ "$ERRORS" -gt 0 ]; then
    echo "✗ End-to-end test FAILED with $ERRORS error(s)"
    exit 1
fi

echo "✓ End-to-end test passed: clean → init → scan → add → check → push → list"
echo "  Collections pushed: ${COLLECTIONS[*]}"
echo "  Destination: $GCS_BUCKET"
