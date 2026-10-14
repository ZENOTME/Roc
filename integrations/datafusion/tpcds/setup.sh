#!/usr/bin/env bash
# Fetches the pinned TPC-DS corpus and generates its dataset.
#
#   integrations/datafusion/tpcds/setup.sh [data-dir] [scale-factor]
#
# Queries: DataFusion 55.1.0 datafusion/core/tests/tpc-ds (99 files, 103
# statements), written to queries/. Data: DuckDB's tpcds extension (dsdgen),
# exported as one Parquet file per table to <data-dir>/sf<scale-factor>.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
data="${1:-$here/../../../../roc-data/tpcds}"
sf="${2:-1}"
ref="55.1.0"

mkdir -p "$here/queries"
for i in $(seq 1 99); do
  out="$here/queries/q$i.sql"
  [ -s "$out" ] && continue
  curl -fsSL "https://raw.githubusercontent.com/apache/datafusion/$ref/datafusion/core/tests/tpc-ds/$i.sql" -o "$out"
done
echo "queries: $(ls "$here/queries" | wc -l) files"

mkdir -p "$data"
target="$data/sf$sf"
if [ ! -f "$target/store_sales.parquet" ]; then
  rm -rf "$target" "$data/sf$sf.duckdb"
  duckdb "$data/sf$sf.duckdb" <<SQL
INSTALL tpcds;
LOAD tpcds;
CALL dsdgen(sf = $sf);
EXPORT DATABASE '$target' (FORMAT parquet);
SQL
fi
echo "data: $target"
ls "$target"
