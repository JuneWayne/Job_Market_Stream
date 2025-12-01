#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="/app"

echo "[auto_push] Using repo at: $REPO_ROOT"
echo "[auto_push] Listing files at $REPO_ROOT:"
ls -a "$REPO_ROOT"

# Show any git-related env vars (for debugging)
echo "[auto_push] Git-related env vars:"
env | grep -E '^GIT_' || echo "[auto_push] (none)"

# Hard reset any weird Git env that might be inherited from the base image
unset GIT_DIR || true
unset GIT_WORK_TREE || true
unset GIT_INDEX_FILE || true

echo "[auto_push] After unset, Git-related env vars:"
env | grep -E '^GIT_' || echo "[auto_push] (none)"

# Now check if Git sees /app as a repo
if ! git -C "$REPO_ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "[auto_push] $REPO_ROOT is STILL not a valid Git repo from inside the container."
  echo "[auto_push] Skipping auto-push so the container does not crash."
  exit 0
fi

BRANCH="${GIT_BRANCH:-main}"

echo "[auto_push] Configuring git user..."
git -C "$REPO_ROOT" config user.name  "${GIT_USER_NAME}"
git -C "$REPO_ROOT" config user.email "${GIT_USER_EMAIL}"

echo "[auto_push] Staging artifacts..."
git -C "$REPO_ROOT" add data/ jobs.duckdb || true

if git -C "$REPO_ROOT" diff --cached --quiet; then
  echo "[auto_push] No changes to commit."
  exit 0
fi

COMMIT_MSG="Auto-update DuckDB snapshot $(date -Iseconds)"
echo "[auto_push] Committing with message: $COMMIT_MSG"

if ! git -C "$REPO_ROOT" commit -m "$COMMIT_MSG"; then
  echo "[auto_push] Nothing to commit (race)."
  exit 0
fi

echo "[auto_push] Pushing..."

if [ -n "${GITHUB_PAT:-}" ] && [ -n "${GIT_REMOTE_REPO:-}" ]; then
  git -C "$REPO_ROOT" push "https://${GITHUB_PAT}@${GIT_REMOTE_REPO}" "$BRANCH"
else
  git -C "$REPO_ROOT" push origin "$BRANCH"
fi
