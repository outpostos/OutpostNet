#!/bin/sh
# Expected exit 1: run the final blackhole regression against the old engine.
set -eu
NET_HOL_ROOT=$(git rev-parse --show-toplevel)
NET_HOL_SCRATCH=$(mktemp -d)
trap 'rm -f "$NET_HOL_SCRATCH/gossip.go" "$NET_HOL_SCRATCH/overlay.json"; rmdir "$NET_HOL_SCRATCH"' EXIT
cd "$NET_HOL_ROOT"
git show 0394e49:pkg/state/gossip.go > "$NET_HOL_SCRATCH/gossip.go"
python3 - "$NET_HOL_ROOT" "$NET_HOL_SCRATCH" <<'PY'
import json, os, sys
root, scratch = sys.argv[1:]
with open(os.path.join(scratch, 'overlay.json'), 'w') as f:
    json.dump({'Replace': {os.path.join(root, 'pkg/state/gossip.go'):
                          os.path.join(scratch, 'gossip.go')}}, f)
PY
go test -overlay "$NET_HOL_SCRATCH/overlay.json" -race -count=1 -v ./pkg/state \
    -run '^TestGossipRunPushIsolatesBlackholedPeers$'
