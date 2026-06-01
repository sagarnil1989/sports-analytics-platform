#!/usr/bin/env bash
# deploy.sh — Deploy Azure Function app with automatic change preview and git tagging.
#
# Usage:
#   ./deploy.sh              # deploy cricket_display (default)
#   ./deploy.sh display      # deploy cricket_display
#   ./deploy.sh ingestion    # deploy cricket_ingestion
#
# What it does:
#   1. Shows which files changed since the last deploy (git diff)
#   2. Asks you to confirm before deploying
#   3. Runs: func azure functionapp publish <app>
#   4. Tags the commit so the next run knows what was last deployed

set -euo pipefail

APP=${1:-display}

case "$APP" in
  display)
    FUNC_APP_NAME="func-ramanuj-display"
    FUNC_DIR="src/functions/cricket_display"
    TAG_PREFIX="deploy-display"
    ;;
  ingestion)
    FUNC_APP_NAME="func-ramanuj"
    FUNC_DIR="src/functions/cricket_ingestion"
    TAG_PREFIX="deploy-ingestion"
    ;;
  *)
    echo "Unknown app: $APP. Use 'display' or 'ingestion'."
    exit 1
    ;;
esac

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$REPO_ROOT"

# ── 1. Find last deploy tag ────────────────────────────────────────────────────

LAST_TAG=$(git tag -l "${TAG_PREFIX}-*" | sort | tail -1)

echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Deploying: $FUNC_APP_NAME"
echo "  Source:    $FUNC_DIR"
echo "═══════════════════════════════════════════════════════"

# ── 2. Show what changed ───────────────────────────────────────────────────────

if [ -z "$LAST_TAG" ]; then
  echo ""
  echo "  No previous deploy tag found — this looks like the first deploy."
  echo "  All files in $FUNC_DIR will be uploaded."
  echo ""
  CHANGED_FILES=$(git ls-files "$FUNC_DIR")
else
  echo ""
  echo "  Last deploy: $LAST_TAG"
  CHANGED_FILES=$(git diff "$LAST_TAG" --name-only -- "$FUNC_DIR")

  if [ -z "$CHANGED_FILES" ]; then
    echo ""
    echo "  No changes since last deploy. Nothing to deploy."
    echo ""
    read -r -p "  Deploy anyway? [y/N] " FORCE
    if [[ ! "$FORCE" =~ ^[Yy]$ ]]; then
      echo "  Aborted."
      exit 0
    fi
  else
    echo ""
    echo "  Files changed since last deploy:"
    echo "$CHANGED_FILES" | sed 's/^/    ✦ /'
    echo ""
    echo "  Diff summary:"
    git diff "$LAST_TAG" --stat -- "$FUNC_DIR" | sed 's/^/    /'
  fi
fi

# ── 3. Confirm ─────────────────────────────────────────────────────────────────

echo ""
read -r -p "  Deploy to $FUNC_APP_NAME? [y/N] " CONFIRM
if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
  echo "  Aborted."
  exit 0
fi

# ── 4. Deploy ──────────────────────────────────────────────────────────────────

echo ""
echo "  Running: func azure functionapp publish $FUNC_APP_NAME"
echo "───────────────────────────────────────────────────────"
cd "$FUNC_DIR"
func azure functionapp publish "$FUNC_APP_NAME"
cd "$REPO_ROOT"

# ── 5. Tag the commit ──────────────────────────────────────────────────────────

NEW_TAG="${TAG_PREFIX}-$(date +%Y%m%d-%H%M)"
git tag "$NEW_TAG"
echo ""
echo "  Tagged as: $NEW_TAG"
echo "  Next deploy will diff from this point."
echo ""
echo "  Done."
