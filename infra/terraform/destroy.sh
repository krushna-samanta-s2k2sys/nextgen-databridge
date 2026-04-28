#!/usr/bin/env bash
# Delegates to the root down.sh — kept here for backwards compatibility.
exec "$(cd "$(dirname "$0")/../.." && pwd)/down.sh" "$@"
