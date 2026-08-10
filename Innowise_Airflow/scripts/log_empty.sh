#!/usr/bin/env bash
set -euo pipefail

file_path="$1"
echo "$(date -Iseconds) File is empty: ${file_path}"
