#!/usr/bin/env bash
set -euo pipefail

# Branch to push to
BRANCH="${GIT_BRANCH:-main}"

# Configure git identity inside the container
git config user.name  "${GIT_USER_NAME:-Job Bot}"
git config user.email "${GIT_USER_EMAIL:-bot@example.com}"

# Stage only the artifacts you care about
# Adjust paths if needed
git add data/ jobs.duckdb || true

# If nothing changed, exit quietly
if git diff --cached --quiet; then
  echo "[auto_push] No changes to commit."
  exit 0
fi

COMMIT_MSG="Auto-update DuckDB snapshot $(date -Iseconds)"

echo "[auto_push] Committing with message: $COMMIT_MSG"
git commit -m "$COMMIT_MSG" || {
  echo "[auto_push] Nothing to commit (race)."
  exit 0
}

echo "[auto_push] Pushing..."

# Push using PAT if provided
if [ -n "${GIT_PAT:-}" ] && [ -n "${GIT_REMOTE_REPO:-}" ]; then
  git push "https://${GIT_PAT}@github.com/${GIT_REMOTE_REPO}.git" "$BRANCH"
else
  git push origin "$BRANCH"
fi

echo "[auto_push] Done."
