#!/usr/bin/env bash
# =============================================================================
# Lakehouse Federation Demo - One-click Deploy
#
# Usage:
#   ./lakehouse_federation_demo_resource_deploy.sh
# =============================================================================

set -euo pipefail
cd "$(dirname "$0")"

export PATH="$HOME/.local/bin:$PATH"

echo "================================================"
echo "  Lakehouse Federation Demo - Deploy"
echo "================================================"
echo ""

# Check prerequisites
bash scripts/prerequisites.sh

# Run interactive deploy
uv run python scripts/deploy.py "$@"
