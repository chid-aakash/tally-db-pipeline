#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUNDLE_DIR="${1:-/Users/aakashchid/workshop/sena/office/_workspace-admin/data-dumps/tally-xml-exports}"
COMPANY_NAME="${2:-Avinash Industries - Chennai Unit - 2025-26}"

cd "$ROOT_DIR"
source .venv/bin/activate

echo "Running replay bundle regression from: $BUNDLE_DIR"
tally-db-pipeline replay-bundle --directory "$BUNDLE_DIR" --company "$COMPANY_NAME"

echo
echo "Current local report:"
tally-db-pipeline report
