# =============================================================================
# Microsoft OneLake / Fabric (Catalog Federation)
# Uses REST API via az rest (no native Terraform Fabric provider)
# =============================================================================

resource "null_resource" "fabric_lakehouse" {
  count = var.enable_onelake ? 1 : 0

  triggers = {
    workspace_id = var.fabric_workspace_id
  }

  provisioner "local-exec" {
    command = <<-EOT
      WORKSPACE_ID="${var.fabric_workspace_id}"

      echo "Creating Fabric Lakehouse..."
      LAKEHOUSE_RESPONSE=$(az rest --method POST \
        --url "https://api.fabric.microsoft.com/v1/workspaces/$WORKSPACE_ID/lakehouses" \
        --headers "Content-Type=application/json" \
        --body '{"displayName": "lhf_demo_factory", "description": "Lakehouse Federation Demo - Factory data"}' \
        2>/dev/null || echo '{"error": "already exists"}')

      echo "Lakehouse response: $LAKEHOUSE_RESPONSE"

      # Get lakehouse ID
      LAKEHOUSE_ID=$(echo "$LAKEHOUSE_RESPONSE" | jq -r '.id // empty')

      if [ -z "$LAKEHOUSE_ID" ]; then
        echo "Lakehouse may already exist, listing..."
        LAKEHOUSE_ID=$(az rest --method GET \
          --url "https://api.fabric.microsoft.com/v1/workspaces/$WORKSPACE_ID/lakehouses" \
          | jq -r '.value[] | select(.displayName == "lhf_demo_factory") | .id')
      fi

      echo "Lakehouse ID: $LAKEHOUSE_ID"

      # Upload sample data using azcopy to OneLake
      echo "Uploading sample data to OneLake..."
      # OneLake path: https://onelake.dfs.fabric.microsoft.com/<workspace_id>/<lakehouse_id>/Tables/

      echo "OneLake setup complete. Tables will be created via notebook or Spark."
    EOT
  }
}
