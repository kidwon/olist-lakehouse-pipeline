#!/usr/bin/env bash
# Upload the raw CSVs to a Unity Catalog Volume, deploy the bundle and build the replay plan.
# Prerequisites: `databricks auth login --host <your Free Edition URL>` and scripts/download_olist.sh.
set -euo pipefail
cd "$(dirname "$0")/.."
CATALOG=workspace

databricks schemas create olist_raw "$CATALOG" 2>/dev/null || echo "schema olist_raw exists"
databricks volumes create "$CATALOG" olist_raw files MANAGED 2>/dev/null || echo "volume files exists"
databricks fs cp -r --overwrite data/source "dbfs:/Volumes/$CATALOG/olist_raw/files/source"

databricks bundle validate
databricks bundle deploy
databricks bundle run olist_setup
echo "Now run the daily job once per replay day:  databricks bundle run olist_daily"
