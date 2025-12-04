#!/usr/bin/env bash
set -euo pipefail
# clear any git lock files 
rm -f .git/index.lock 2>/dev/null
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_ROOT"

echo "[host_auto_push] Repo: $REPO_ROOT"

# make sure the auto push file is in the right git repo
if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "[host_auto_push] Not in a git repo, exiting."
  exit 0
fi

# configure identity 
git config user.name  "junewayne"
git config user.email "ethan128@me.com"

echo "[host_auto_push] Staging DuckDB + data/"
git add data/ || true

# if nothing staged, simply echo no changes to commit
if git diff --cached --quiet; then
  echo "[host_auto_push] No changes to commit."
  exit 0
fi

# otherwise commit the following message
COMMIT_MSG="Auto-update DuckDB snapshot $(date -Iseconds)"
echo "[host_auto_push] Committing with message: $COMMIT_MSG"

git commit -m "$COMMIT_MSG" || {
  echo "[host_auto_push] Nothing to commit."
  exit 0
}

# push to main
BRANCH="main"
echo "[host_auto_push] Pushing to origin/$BRANCH"
git push origin "$BRANCH"

echo "[host_auto_push] Done."
