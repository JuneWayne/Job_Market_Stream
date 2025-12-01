#!/usr/bin/env bash
set -euo pipefail

BRANCH="${GIT_BRANCH:-main}"
GIT_USER_NAME="${GIT_USER_NAME:-Job Market Bot}"
GIT_USER_EMAIL="${GIT_USER_EMAIL:-bot@example.com}"

git config user.name  "${GIT_USER_NAME}"
git config user.email "${GIT_USER_EMAIL}"

while true; do
  echo "[auto_push] Checking for changes..."

  # Stage artifacts
  git add data/ || true

  if git diff --cached --quiet; then
    echo "[auto_push] No changes to commit."
  else
    COMMIT_MSG="Auto-update DuckDB snapshot $(date -Iseconds)"
    echo "[auto_push] Committing with message: $COMMIT_MSG"
    git commit -m "$COMMIT_MSG" || echo "[auto_push] Nothing to commit (race)."

    echo "[auto_push] Pushing..."
    if [ -n "${GIT_PAT:-}" ] && [ -n "${GIT_REMOTE_REPO:-}" ]; then
      git push "https://${GIT_PAT}@github.com/${GIT_REMOTE_REPO}.git" "$BRANCH" || echo "[auto_push] Push failed."
    else
      git push origin "$BRANCH" || echo "[auto_push] Push failed (origin)."
    fi
  fi

  echo "[auto_push] Sleeping 3600s..."
  sleep 3600
done
