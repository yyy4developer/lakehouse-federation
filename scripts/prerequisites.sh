#!/usr/bin/env bash
# =============================================================================
# Prerequisites Check
# Verifies required CLI tools are installed
# =============================================================================

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

ok()   { echo -e "  ${GREEN}✓${NC} $1"; }
warn() { echo -e "  ${YELLOW}!${NC} $1"; }
fail() { echo -e "  ${RED}✗${NC} $1"; }

MISSING=()

check_cmd() {
    local cmd=$1
    local install_hint=$2
    local required=${3:-true}

    if command -v "$cmd" &>/dev/null; then
        ok "$cmd $(command -v "$cmd")"
    elif [ "$required" = "true" ]; then
        fail "$cmd not found — $install_hint"
        MISSING+=("$cmd")
    else
        warn "$cmd not found (optional) — $install_hint"
    fi
}

echo ""
echo "Checking prerequisites..."
echo ""

check_cmd "terraform" "brew install terraform"
check_cmd "jq"        "brew install jq"
check_cmd "uv"        "curl -LsSf https://astral.sh/uv/install.sh | sh"

# Cloud CLIs (checked but not required at this stage)
check_cmd "aws"       "brew install awscli"       false
check_cmd "az"        "brew install azure-cli"     false
check_cmd "gcloud"    "brew install google-cloud-sdk" false
check_cmd "databricks" "brew install databricks"   false
check_cmd "psql"      "brew install libpq"         false
check_cmd "sqlcmd"    "brew install sqlcmd"         false

echo ""

if [ ${#MISSING[@]} -gt 0 ]; then
    echo -e "${RED}Missing required tools: ${MISSING[*]}${NC}"
    echo "Install them and re-run this script."
    exit 1
fi

echo -e "${GREEN}All required prerequisites met.${NC}"
echo ""
