#!/usr/bin/env python3
"""Upload demo Delta tables to OneLake (Fabric Lakehouse).

Usage:
  python onelake_upload.py <workspace_id> <lakehouse_id>

Requires: az login (Azure AD token used for OneLake auth)
Uses: pyarrow to write Parquet + manual Delta log creation via DFS REST API.
"""
import hashlib
import io
import json
import subprocess
import sys
import time
import uuid
from datetime import date, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests


def get_azure_token(tenant_id: str | None = None) -> str:
    """Get Azure AD bearer token for OneLake (storage.azure.com resource).

    If tenant_id is provided, gets a token for that specific tenant
    (useful when Fabric workspace is in a different tenant than the current az login).
    """
    cmd = ["az", "account", "get-access-token", "--resource", "https://storage.azure.com/", "-o", "json"]
    if tenant_id:
        cmd += ["--tenant", tenant_id]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return json.loads(result.stdout)["accessToken"]


def make_production_plans() -> pd.DataFrame:
    """Generate production_plans table (20 rows)."""
    base = date(2024, 1, 8)
    rows = []
    products = ["Widget-A", "Widget-B", "Gear-X", "Gear-Y", "Shaft-S1"]
    for i in range(1, 21):
        machine_id = ((i - 1) % 10) + 1
        product = products[(i - 1) % len(products)]
        plan_date = base + timedelta(days=(i - 1) * 2)
        target_qty = 100 + (i * 15)
        actual_qty = target_qty - ((i * 7) % 30)
        rows.append({
            "plan_id": i,
            "machine_id": machine_id,
            "product_name": product,
            "plan_date": plan_date.isoformat(),
            "target_quantity": target_qty,
            "actual_quantity": actual_qty,
            "status": "completed" if i <= 15 else "in_progress",
        })
    return pd.DataFrame(rows)


def make_inventory_levels() -> pd.DataFrame:
    """Generate inventory_levels table (30 rows)."""
    base = date(2024, 1, 10)
    materials = ["Steel-Rod", "Copper-Wire", "Aluminum-Sheet", "Rubber-Seal", "Bearing-6205", "Lubricant-G3"]
    rows = []
    for i in range(1, 31):
        machine_id = ((i - 1) % 10) + 1
        material = materials[(i - 1) % len(materials)]
        record_date = base + timedelta(days=(i - 1))
        qty_on_hand = 500 - (i * 12)
        reorder_point = 100
        rows.append({
            "inventory_id": i,
            "machine_id": machine_id,
            "material_name": material,
            "record_date": record_date.isoformat(),
            "quantity_on_hand": max(qty_on_hand, 50),
            "reorder_point": reorder_point,
            "unit": "pcs" if "Bearing" in material or "Seal" in material else "kg",
            "warehouse_location": f"WH-{chr(65 + (i % 4))}",
        })
    return pd.DataFrame(rows)


class OneLakeDfsClient:
    """Upload files to OneLake via Azure DFS REST API."""

    def __init__(self, workspace_id: str, lakehouse_id: str, token: str):
        self.base_url = f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_id}"
        self.headers = {"Authorization": f"Bearer {token}"}

    def _create_file(self, path: str):
        """Create (touch) a file on OneLake."""
        url = f"{self.base_url}/{path}?resource=file"
        resp = requests.put(url, headers=self.headers)
        resp.raise_for_status()

    def _append_data(self, path: str, data: bytes, position: int = 0):
        """Append data to a file."""
        url = f"{self.base_url}/{path}?action=append&position={position}"
        resp = requests.patch(url, headers={**self.headers, "Content-Type": "application/octet-stream"}, data=data)
        resp.raise_for_status()

    def _flush_data(self, path: str, content_length: int):
        """Flush (commit) the file."""
        url = f"{self.base_url}/{path}?action=flush&position={content_length}"
        resp = requests.patch(url, headers=self.headers)
        resp.raise_for_status()

    def upload_file(self, path: str, data: bytes):
        """Upload a file to OneLake (create + append + flush)."""
        self._create_file(path)
        self._append_data(path, data)
        self._flush_data(path, len(data))


def write_delta_table(client: OneLakeDfsClient, table_name: str, df: pd.DataFrame):
    """Write a DataFrame as a Delta table to OneLake using DFS API."""
    table = pa.Table.from_pandas(df)

    # Write parquet file to buffer
    buf = io.BytesIO()
    pq.write_table(table, buf)
    parquet_bytes = buf.getvalue()

    # Generate file name with UUID (Fabric-compatible format)
    file_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, table_name))
    parquet_filename = f"part-00000-{file_uuid}-c000.snappy.parquet"
    parquet_path = f"Tables/{table_name}/{parquet_filename}"

    print(f"  Uploading {parquet_path} ({len(parquet_bytes)} bytes)...")
    client.upload_file(parquet_path, parquet_bytes)

    # Build Delta log entry (version 0) - Fabric-compatible format
    schema_json = json.dumps({
        "type": "struct",
        "fields": [
            {"name": col, "type": _arrow_to_delta_type(table.schema.field(col).type), "nullable": True, "metadata": {}}
            for col in table.schema.names
        ],
    })

    timestamp_ms = int(time.time() * 1000)
    table_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{table_name}-meta"))

    log_entry = "\n".join([
        json.dumps({"commitInfo": {
            "timestamp": timestamp_ms,
            "operation": "WRITE",
            "operationParameters": {"mode": "Overwrite", "partitionBy": "[]"},
            "isBlindAppend": False,
        }}),
        json.dumps({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
        json.dumps({"metaData": {
            "id": table_uuid,
            "name": table_name,
            "format": {"provider": "parquet", "options": {}},
            "schemaString": schema_json,
            "partitionColumns": [],
            "configuration": {},
            "createdTime": timestamp_ms,
        }}),
        json.dumps({"add": {
            "path": parquet_filename,
            "partitionValues": {},
            "size": len(parquet_bytes),
            "modificationTime": timestamp_ms,
            "dataChange": True,
            "stats": json.dumps({"numRecords": len(df)}),
        }}),
    ]) + "\n"

    log_path = f"Tables/{table_name}/_delta_log/00000000000000000000.json"
    print(f"  Uploading {log_path}...")
    client.upload_file(log_path, log_entry.encode("utf-8"))


def _arrow_to_delta_type(arrow_type) -> str:
    """Map PyArrow type to Delta Lake schema type string."""
    if pa.types.is_int64(arrow_type) or pa.types.is_int32(arrow_type):
        return "long"
    if pa.types.is_float64(arrow_type):
        return "double"
    if pa.types.is_boolean(arrow_type):
        return "boolean"
    return "string"


def _detect_fabric_tenant(workspace_id: str, current_token: str) -> str | None:
    """Try current token; if OneLake returns TenantClusterDetailsNotFound, try other known tenants."""
    # Quick check with current token
    url = f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}?resource=filesystem"
    resp = requests.get(url, headers={"Authorization": f"Bearer {current_token}"})
    if resp.status_code != 404 or "TenantClusterDetailsNotFound" not in resp.text:
        return None  # Current tenant works

    # List available tenants and try each
    result = subprocess.run(
        ["az", "account", "list", "--query", "[].tenantId", "-o", "json"],
        capture_output=True, text=True, check=True,
    )
    current_tenant = subprocess.run(
        ["az", "account", "show", "--query", "tenantId", "-o", "tsv"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()

    tenants = set(json.loads(result.stdout))
    tenants.discard(current_tenant)

    for tenant_id in tenants:
        print(f"  Trying tenant {tenant_id}...")
        try:
            alt_token = get_azure_token(tenant_id)
            resp = requests.get(url, headers={"Authorization": f"Bearer {alt_token}"})
            if resp.status_code != 404 or "TenantClusterDetailsNotFound" not in resp.text:
                print(f"  Found Fabric tenant: {tenant_id}")
                return tenant_id
        except subprocess.CalledProcessError:
            continue
    return None


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <workspace_id> <lakehouse_id> [tenant_id]", file=sys.stderr)
        sys.exit(1)

    workspace_id = sys.argv[1]
    lakehouse_id = sys.argv[2]
    tenant_id = sys.argv[3] if len(sys.argv) > 3 else None

    print("Getting Azure AD token for OneLake...")
    token = get_azure_token(tenant_id)

    if not tenant_id:
        print("Checking OneLake access...")
        alt_tenant = _detect_fabric_tenant(workspace_id, token)
        if alt_tenant:
            print(f"Switching to Fabric tenant {alt_tenant} for OneLake access...")
            token = get_azure_token(alt_tenant)
            tenant_id = alt_tenant

    client = OneLakeDfsClient(workspace_id, lakehouse_id, token)

    tables = [
        ("production_plans", make_production_plans()),
        ("inventory_levels", make_inventory_levels()),
    ]

    print(f"Uploading {len(tables)} tables to Lakehouse {lakehouse_id}...")
    for table_name, df in tables:
        print(f"  Writing {table_name} ({len(df)} rows)...")
        write_delta_table(client, table_name, df)
        print(f"  {table_name} uploaded successfully.")

    print("OneLake data upload complete.")


if __name__ == "__main__":
    main()
