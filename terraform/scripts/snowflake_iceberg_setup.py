#!/usr/bin/env python3
"""Set up Snowflake Iceberg tables with CATALOG_SYNC to AWS Glue.

This script:
1. Creates a Storage Integration (S3 access)
2. Retrieves Snowflake's IAM user ARN and updates the IAM role trust policy
3. Creates a Catalog Integration (Glue sync)
4. Creates an External Volume
5. Creates Iceberg tables with CATALOG_SYNC
6. Inserts seed data

Usage:
  python snowflake_iceberg_setup.py --account-url <url> --user <user> --password <pass> \
    --warehouse <wh> --database <db> --schema <schema> \
    --s3-bucket <bucket> --aws-region <region> --aws-account-id <id> \
    --glue-database <glue_db> --iam-role-arn <arn> --iam-role-name <name>

Requires: snowflake-connector-python, boto3
"""
import argparse
import json
import sys
import time

import boto3
import snowflake.connector


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


def setup_storage_integration(cur, args):
    """Create Storage Integration and update IAM trust policy."""
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

    # Retrieve Snowflake's IAM user ARN and external ID
    print("  Retrieving Snowflake IAM user ARN...")
    cur.execute(f"DESC INTEGRATION {integration_name}")
    props = {row[0]: row[1] for row in cur.fetchall()}
    sf_iam_user_arn = props.get("STORAGE_AWS_IAM_USER_ARN", "")
    sf_external_id = props.get("STORAGE_AWS_EXTERNAL_ID", "")

    if not sf_iam_user_arn or not sf_external_id:
        print(f"  ERROR: Could not get Snowflake IAM info. Props: {props}", file=sys.stderr)
        sys.exit(1)

    print(f"  Snowflake IAM User ARN: {sf_iam_user_arn}")
    print(f"  Snowflake External ID: {sf_external_id}")

    # Update IAM role trust policy with actual Snowflake principal
    print(f"  Updating IAM role {args.iam_role_name} trust policy...")
    iam = boto3.client("iam", region_name=args.aws_region)
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": sf_iam_user_arn},
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": sf_external_id,
                }
            },
        }],
    }
    iam.update_assume_role_policy(
        RoleName=args.iam_role_name,
        PolicyDocument=json.dumps(trust_policy),
    )
    print("  IAM trust policy updated. Waiting 15s for propagation...")
    time.sleep(15)

    return integration_name


def setup_catalog_integration(cur, args):
    """Create Catalog Integration for Glue sync."""
    integration_name = "LHF_GLUE_CATALOG_INTEGRATION"

    print(f"  Creating Catalog Integration {integration_name}...")
    cur.execute(f"""
        CREATE OR REPLACE CATALOG INTEGRATION {integration_name}
            CATALOG_SOURCE = GLUE
            CATALOG_NAMESPACE = '{args.glue_database}'
            TABLE_FORMAT = ICEBERG
            GLUE_AWS_ROLE_ARN = '{args.iam_role_arn}'
            GLUE_CATALOG_ID = '{args.aws_account_id}'
            GLUE_REGION = '{args.aws_region}'
            ENABLED = TRUE
    """)

    # Retrieve Snowflake's Glue IAM user ARN and update trust policy
    cur.execute(f"DESC INTEGRATION {integration_name}")
    props = {row[0]: row[1] for row in cur.fetchall()}
    glue_iam_user_arn = props.get("GLUE_AWS_IAM_USER_ARN", "")
    glue_external_id = props.get("GLUE_AWS_EXTERNAL_ID", "")

    if glue_iam_user_arn and glue_external_id:
        print(f"  Glue IAM User ARN: {glue_iam_user_arn}")
        print(f"  Updating IAM role for Glue access...")
        iam = boto3.client("iam", region_name=args.aws_region)

        # Get current trust policy and add Glue principal
        role = iam.get_role(RoleName=args.iam_role_name)
        trust_policy = role["Role"]["AssumeRolePolicyDocument"]

        # Add Glue principal if not already present
        glue_statement = {
            "Effect": "Allow",
            "Principal": {"AWS": glue_iam_user_arn},
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": glue_external_id,
                }
            },
        }
        # Check if already exists
        existing_principals = [s.get("Principal", {}).get("AWS", "") for s in trust_policy["Statement"]]
        if glue_iam_user_arn not in existing_principals:
            trust_policy["Statement"].append(glue_statement)
            iam.update_assume_role_policy(
                RoleName=args.iam_role_name,
                PolicyDocument=json.dumps(trust_policy),
            )
            print("  IAM trust policy updated with Glue principal.")
            time.sleep(10)

    return integration_name


def setup_external_volume(cur, args, storage_integration):
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
    return volume_name


def create_iceberg_tables(cur, args, external_volume, catalog_integration):
    """Create Iceberg tables with CATALOG_SYNC to Glue."""

    print("  Creating Iceberg table: operational_metrics...")
    cur.execute(f"""
        CREATE OR REPLACE ICEBERG TABLE operational_metrics (
            metric_id      INT,
            machine_id     INT,
            metric_date    DATE,
            oee_score      FLOAT,
            availability_pct FLOAT,
            performance_pct  FLOAT,
            quality_pct      FLOAT
        )
        CATALOG = 'SNOWFLAKE'
        EXTERNAL_VOLUME = '{external_volume}'
        BASE_LOCATION = 'operational_metrics/'
        CATALOG_SYNC = '{catalog_integration}'
    """)

    print("  Creating Iceberg table: safety_incidents...")
    cur.execute(f"""
        CREATE OR REPLACE ICEBERG TABLE safety_incidents (
            incident_id      INT,
            machine_id       INT,
            incident_date    DATE,
            severity         VARCHAR(20),
            description      VARCHAR(200),
            corrective_action VARCHAR(200),
            resolved         BOOLEAN
        )
        CATALOG = 'SNOWFLAKE'
        EXTERNAL_VOLUME = '{external_volume}'
        BASE_LOCATION = 'safety_incidents/'
        CATALOG_SYNC = '{catalog_integration}'
    """)


def insert_seed_data(cur):
    """Insert seed data into Iceberg tables."""

    print("  Inserting operational_metrics data (50 rows)...")
    cur.execute("DELETE FROM operational_metrics")
    # Generate 50 rows: 5 dates x 10 machines
    values = []
    dates = ["2024-01-15", "2024-02-15", "2024-03-15", "2024-04-15", "2024-05-15"]
    for i, d in enumerate(dates):
        for m in range(1, 11):
            mid = i * 10 + m
            oee = round(0.65 + (m * 0.025) + (i * 0.01), 3)
            avail = round(0.85 + (m * 0.01) - (i * 0.005), 3)
            perf = round(0.80 + (m * 0.015) + (i * 0.008), 3)
            qual = round(0.90 + (m * 0.008) - (i * 0.003), 3)
            values.append(f"({mid}, {m}, '{d}', {oee}, {avail}, {perf}, {qual})")

    cur.execute(f"""
        INSERT INTO operational_metrics (metric_id, machine_id, metric_date, oee_score, availability_pct, performance_pct, quality_pct)
        VALUES {', '.join(values)}
    """)

    print("  Inserting safety_incidents data (20 rows)...")
    cur.execute("DELETE FROM safety_incidents")
    incidents = [
        (1, 1, "2024-01-10", "HIGH", "Hydraulic line rupture near press", "Replaced hydraulic line and added shielding", True),
        (2, 2, "2024-01-18", "MEDIUM", "Robot arm exceeded safe speed limit", "Recalibrated speed sensors and updated firmware", True),
        (3, 3, "2024-02-02", "LOW", "Minor oil leak on floor near machine", "Cleaned area and replaced gasket", True),
        (4, 4, "2024-02-14", "HIGH", "Emergency stop triggered by overheating", "Replaced cooling fan and thermal paste", True),
        (5, 5, "2024-02-22", "MEDIUM", "Spindle guard loose during operation", "Tightened guard bolts and added locking washers", True),
        (6, 6, "2024-03-05", "LOW", "Noise level exceeded threshold", "Added sound dampening enclosure", True),
        (7, 7, "2024-03-12", "HIGH", "Teach pendant cable caught in joint", "Rerouted cable and added cable management", True),
        (8, 8, "2024-03-20", "MEDIUM", "Light curtain false trigger", "Cleaned sensors and recalibrated", True),
        (9, 9, "2024-04-01", "LOW", "Minor vibration anomaly detected", "Balanced rotating components", True),
        (10, 10, "2024-04-08", "HIGH", "Hydraulic pressure spike", "Replaced pressure relief valve", True),
        (11, 1, "2024-04-15", "MEDIUM", "Electrical arc in control panel", "Replaced contactor and inspected wiring", True),
        (12, 2, "2024-04-22", "LOW", "Grease buildup on safety sensors", "Cleaned and scheduled preventive maintenance", True),
        (13, 3, "2024-05-03", "MEDIUM", "Unexpected motion during maintenance", "Updated lockout/tagout procedures", True),
        (14, 4, "2024-05-10", "HIGH", "Drive belt snapped during operation", "Replaced belt and added tension monitor", True),
        (15, 5, "2024-05-18", "LOW", "Coolant splash outside containment", "Extended splash guard", True),
        (16, 6, "2024-05-25", "MEDIUM", "PLC communication timeout", "Replaced communication module", False),
        (17, 7, "2024-06-02", "HIGH", "Collision detection failure", "Upgraded collision sensors", False),
        (18, 8, "2024-06-10", "LOW", "Excessive dust accumulation", "Installed air filtration system", False),
        (19, 9, "2024-06-18", "MEDIUM", "Battery backup failure during outage", "Replaced UPS batteries", False),
        (20, 10, "2024-06-25", "HIGH", "Pressure gauge reading inaccurate", "Calibrated gauges and added redundant sensor", False),
    ]
    vals = []
    for inc in incidents:
        resolved = "TRUE" if inc[6] else "FALSE"
        vals.append(f"({inc[0]}, {inc[1]}, '{inc[2]}', '{inc[3]}', '{inc[4]}', '{inc[5]}', {resolved})")

    cur.execute(f"""
        INSERT INTO safety_incidents (incident_id, machine_id, incident_date, severity, description, corrective_action, resolved)
        VALUES {', '.join(vals)}
    """)


def main():
    parser = argparse.ArgumentParser(description="Snowflake Iceberg + Glue CATALOG_SYNC setup")
    parser.add_argument("--account-url", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--s3-bucket", required=True)
    parser.add_argument("--aws-region", required=True)
    parser.add_argument("--aws-account-id", required=True)
    parser.add_argument("--glue-database", required=True)
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
        storage_int = setup_storage_integration(cur, args)

        # Step 2: Catalog Integration (Glue sync)
        catalog_int = setup_catalog_integration(cur, args)

        # Step 3: External Volume
        ext_volume = setup_external_volume(cur, args, storage_int)

        # Step 4: Create Iceberg tables
        create_iceberg_tables(cur, args, ext_volume, catalog_int)

        # Step 5: Insert seed data
        insert_seed_data(cur)

        # Verify
        cur.execute("SELECT COUNT(*) FROM operational_metrics")
        om_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM safety_incidents")
        si_count = cur.fetchone()[0]
        print(f"  Verification: operational_metrics={om_count}, safety_incidents={si_count}")

        print("Snowflake Iceberg setup complete. Tables synced to Glue.")

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
