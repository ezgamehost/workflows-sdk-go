#!/usr/bin/env bash
# Copies workflows-stack's protocol vectors verbatim into testdata/vectors and records the commit.
#   scripts/sync-vectors.sh ~/code/workflows-stack [<commit>]
# With a commit, the vectors come from that commit (git show), not the checkout's files.
set -euo pipefail
src=${1:?usage: scripts/sync-vectors.sh <workflows-stack checkout> [<commit>]}
rev=$(git -C "$src" rev-parse "${2:-HEAD}")
mkdir -p testdata/vectors
rm -f testdata/vectors/*.json
for f in $(git -C "$src" ls-tree --name-only "$rev" protocol/vectors/ | grep '\.json$'); do
  git -C "$src" show "$rev:$f" > "testdata/vectors/$(basename "$f")"
done
echo "ezgamehost/workflows-stack protocol/vectors at $rev (verbatim; change them there)" > testdata/vectors/SOURCE
echo "$rev" > .workflows-stack-rev
