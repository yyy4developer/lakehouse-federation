#!/usr/bin/env python3
"""Set up Snowflake-managed Iceberg tables on S3.

Architecture:
  Snowflake (managed Iceberg) -> S3 (Parquet + metadata)
  Databricks reads via CONNECTION_SNOWFLAKE (catalog federation) + S3 external location

This script:
1. Creates a Storage Integration (S3 access)
2. Retrieves Snowflake's IAM user ARN and updates the IAM role trust policy
3. Creates an External Volume
4. Creates Snowflake-managed Iceberg tables (from SQL files)
5. Adds Japanese comments via ALTER ICEBERG TABLE (from SQL file)
6. Inserts seed data (from SQL files)

Usage:
  python snowflake_iceberg_setup.py --account-url <url> --user <user> --password <pass> \
    --warehouse <wh> --database <db> --schema <schema> \
    --s3-bucket <bucket> --aws-region <region> \
    --iam-role-arn <arn> --iam-role-name <name>

Requires: snowflake-connector-python, boto3
"""
import argparse
import json
import os
import sys
import time

import boto3
import snowflake.connector

SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "sql", "snowflake_iceberg")


def get_connection(args):
    """Create a Snowflake connection."""
    account = args.account_url.replace("https://", "").replace(".snowflakecomputing.com", "").strip("/")
    return snowflake.connector.connect(
        account=account,
        user=args.user,
        password=args.password,
        warehouse=args.warehouse,
        role="ACCOUNTADMIN",
    )


def execute_sql_file(cursor, filepath, replacements=None):
    """Execute a SQL file, optionally replacing placeholders."""
    with open(filepath) as f:
        sql = f.read()
    if replacements:
        for key, val in replacements.items():
            sql = sql.replace(key, val)
    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if stmt:
            cursor.execute(stmt)


def setup_storage_integration(cur, args):
    """Create Storage Integration and retrieve IAM info."""
    integration_name = "LHF_S3_INTEGRATION"
    s3_path = f"s3://{args.s3_bucket}/snowflake_iceberg/"

    print(f"  Creating Storage Integration {integration_name}...")
    cur.execute(f"""
        CREATE OR REPLACE STORAGE INTEGRATION {integration_name}
            TYPE = EXTERNAL_STAGE
            STORAGE_PROVIDER = 'S3'
            STORAGE_AWS_ROLE_ARN = '{args.iam_role_arn}'
            ENABLED = TRUE
            STORAGE_ALLOWED_LOCATIONS = ('{s3_path}')
    """)

    print("  Retrieving Snowflake IAM user ARN...")
    cur.execute(f"DESC INTEGRATION {integration_name}")
    props = {row[0]: row[2] for row in cur.fetchall()}
    sf_iam_user_arn = props.get("STORAGE_AWS_IAM_USER_ARN", "")
    sf_external_id = props.get("STORAGE_AWS_EXTERNAL_ID", "")

    if not sf_iam_user_arn or not sf_external_id:
        print(f"  ERROR: Could not get Snowflake IAM info. Props: {props}", file=sys.stderr)
        sys.exit(1)

    print(f"  Snowflake IAM User ARN: {sf_iam_user_arn}")
    print(f"  Snowflake External ID: {sf_external_id}")

    return integration_name, sf_iam_user_arn, sf_external_id


def setup_external_volume(cur, args):
    """Create External Volume pointing to S3."""
    volume_name = "LHF_ICEBERG_VOLUME"
    s3_path = f"s3://{args.s3_bucket}/snowflake_iceberg/"

    print(f"  Creating External Volume {volume_name}...")
    cur.execute(f"""
        CREATE OR REPLACE EXTERNAL VOLUME {volume_name}
            STORAGE_LOCATIONS = (
                (
                    NAME = 'lhf-s3-iceberg'
                    STORAGE_BASE_URL = '{s3_path}'
                    STORAGE_PROVIDER = 'S3'
                    STORAGE_AWS_ROLE_ARN = '{args.iam_role_arn}'
                )
            )
    """)

    # Retrieve external volume's IAM external ID
    cur.execute(f"DESC EXTERNAL VOLUME {volume_name}")
    ev_external_id = None
    for row in cur.fetchall():
        if "STORAGE_LOCATION_1" in str(row[1]):
            try:
                loc_info = json.loads(row[3])
                ev_external_id = loc_info.get("STORAGE_AWS_EXTERNAL_ID", "")
            except (ValueError, TypeError, IndexError):
                pass

    return volume_name, ev_external_id


def update_iam_trust_policy(args, sf_iam_user_arn, external_ids):
    """Update IAM role trust policy with all Snowflake external IDs."""
    print(f"  Updating IAM role {args.iam_role_name} trust policy...")
    iam = boto3.client("iam", region_name=args.aws_region)

    valid_ids = [eid for eid in external_ids if eid]
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": sf_iam_user_arn},
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": valid_ids if len(valid_ids) > 1 else valid_ids[0],
                }
            },
        }],
    }
    iam.update_assume_role_policy(
        RoleName=args.iam_role_name,
        PolicyDocument=json.dumps(trust_policy),
    )
    print(f"  IAM trust policy updated with {len(valid_ids)} external ID(s).")
    print("  Waiting 15s for IAM propagation...")
    time.sleep(15)


def main():
    parser = argparse.ArgumentParser(description="Snowflake Iceberg setup for Databricks Catalog Federation")
    parser.add_argument("--account-url", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--s3-bucket", required=True)
    parser.add_argument("--aws-region", required=True)
    parser.add_argument("--iam-role-arn", required=True)
    parser.add_argument("--iam-role-name", required=True)
    args = parser.parse_args()

    print(f"Connecting to Snowflake ({args.account_url})...")
    conn = get_connection(args)
    cur = conn.cursor()

    try:
        # Ensure database and schema exist
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {args.database}")
        cur.execute(f"USE DATABASE {args.database}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {args.schema}")
        cur.execute(f"USE SCHEMA {args.schema}")

        # Step 1: Storage Integration
        storage_int, sf_iam_user_arn, storage_ext_id = setup_storage_integration(cur, args)

        # Step 2: External Volume
        ext_volume, ev_ext_id = setup_external_volume(cur, args)

        # Step 3: Update IAM trust policy with all external IDs
        update_iam_trust_policy(args, sf_iam_user_arn, [storage_ext_id, ev_ext_id])

        # Step 4: Create Iceberg tables (from SQL files)
        print("  Creating Iceberg table: operational_metrics...")
        execute_sql_file(cur, os.path.join(SQL_DIR, "create_operational_metrics.sql"),
                         {"{external_volume}": ext_volume})

        print("  Creating Iceberg table: safety_incidents...")
        execute_sql_file(cur, os.path.join(SQL_DIR, "create_safety_incidents.sql"),
                         {"{external_volume}": ext_volume})

        # Step 5: Add Japanese comments
        print("  Adding table/column comments...")
        execute_sql_file(cur, os.path.join(SQL_DIR, "comments.sql"))

        # Step 6: Insert seed data
        print("  Inserting operational_metrics data (50 rows)...")
        cur.execute("DELETE FROM operational_metrics")
        execute_sql_file(cur, os.path.join(SQL_DIR, "insert_operational_metrics.sql"))

        print("  Inserting safety_incidents data (20 rows)...")
        cur.execute("DELETE FROM safety_incidents")
        execute_sql_file(cur, os.path.join(SQL_DIR, "insert_safety_incidents.sql"))

        # Verify
        cur.execute("SELECT COUNT(*) FROM operational_metrics")
        om_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM safety_incidents")
        si_count = cur.fetchone()[0]
        print(f"  Verification: operational_metrics={om_count}, safety_incidents={si_count}")

        print("Snowflake Iceberg setup complete.")
        print("  Databricks will read these tables via Snowflake Catalog Federation")
        print("  (CONNECTION_SNOWFLAKE + S3 external location).")

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
