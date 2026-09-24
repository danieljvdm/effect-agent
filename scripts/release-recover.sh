#!/usr/bin/env bash
set -euo pipefail

: "${RELEASE_SOURCE_ROOT:?The candidate checkout is required}"
# Nested release tasks must use the candidate's Vite+ and Vitest installation.
export PATH="$RELEASE_SOURCE_ROOT/node_modules/.bin:$PATH"
cd "$(dirname "$0")/.."
exec ./node_modules/.bin/vp run --no-cache release:checked-publish
