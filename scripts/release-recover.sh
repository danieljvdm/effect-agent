#!/usr/bin/env bash
set -euo pipefail

: "${RELEASE_SOURCE_ROOT:?The candidate checkout is required}"
cd "$(dirname "$0")/.."
exec ./node_modules/.bin/vp run --no-cache release:checked-publish
