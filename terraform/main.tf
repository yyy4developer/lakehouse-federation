terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.58"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.0"
    }
  }
}

locals {
  skip_aws = !(var.enable_glue || var.enable_redshift || (var.enable_postgres && var.cloud == "aws"))
  skip_gcp = !var.enable_bigquery
}

# -----------------------------------------------------------------------------
# AWS Provider
# -----------------------------------------------------------------------------
provider "aws" {
  region                      = var.aws_region
  skip_credentials_validation = local.skip_aws
  skip_requesting_account_id  = local.skip_aws
  skip_metadata_api_check     = local.skip_aws

  default_tags {
    tags = {
      Project   = "lakehouse-federation-demo"
      ManagedBy = "terraform"
    }
  }
}

# -----------------------------------------------------------------------------
# Azure Provider
# -----------------------------------------------------------------------------
provider "azurerm" {
  features {}
  subscription_id                 = var.azure_subscription_id != "" ? var.azure_subscription_id : "00000000-0000-0000-0000-000000000000"
  resource_provider_registrations = "none"
}

# -----------------------------------------------------------------------------
# Google Cloud Provider
# -----------------------------------------------------------------------------
provider "google" {
  project     = local.skip_gcp ? "unused" : var.gcp_project_id
  region      = var.gcp_region
  credentials = var.gcp_credentials_json != "" ? var.gcp_credentials_json : null
}

# -----------------------------------------------------------------------------
# Databricks Provider (workspace-level, OAuth U2M via CLI)
# Run: databricks auth login --host <workspace-url>
# -----------------------------------------------------------------------------
provider "databricks" {
  host = var.databricks_host
}
