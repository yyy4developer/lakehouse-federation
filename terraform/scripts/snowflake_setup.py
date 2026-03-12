#!/usr/bin/env python3
"""Set up Snowflake database, schema, tables, and seed data for Query Federation.

Usage:
  python snowflake_setup.py --account-url <url> --user <user> --password <pass> \
    --warehouse <wh> --database <db> --schema <schema>

Requires: snowflake-connector-python
"""
import argparse
import os
import sys

import snowflake.connector


def get_connection(args):
    """Create a Snowflake connection from CLI args."""
    # Extract account identifier from URL: https://xxx.snowflakecomputing.com -> xxx
    account = args.account_url.replace("https://", "").replace(".snowflakecomputing.com", "").strip("/")
    return snowflake.connector.connect(
        account=account,
        user=args.user,
        password=args.password,
        warehouse=args.warehouse,
        role="ACCOUNTADMIN",
    )


def execute_sql_file(cursor, filepath, schema_prefix=""):
    """Execute a SQL file, optionally prefixing table names with schema."""
    with open(filepath) as f:
        sql = f.read()
    if schema_prefix:
        # Prefix unqualified table names in CREATE TABLE / INSERT INTO statements
        for keyword in ("CREATE TABLE IF NOT EXISTS ", "INSERT INTO ", "COMMENT ON TABLE ", "COMMENT ON COLUMN "):
            sql = sql.replace(keyword, f"{keyword}{schema_prefix}.")
        # Fix double-dot from COMMENT ON COLUMN schema.table.column -> already correct
        sql = sql.replace(f"{schema_prefix}.{schema_prefix}.", f"{schema_prefix}.")
    # Split on semicolons and execute each statement
    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if stmt:
            cursor.execute(stmt)


def main():
    parser = argparse.ArgumentParser(description="Snowflake Query Federation setup")
    parser.add_argument("--account-url", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()

    sql_dir = os.path.join(os.path.dirname(__file__), "..", "sql", "snowflake")

    print(f"Connecting to Snowflake ({args.account_url})...")
    conn = get_connection(args)
    cur = conn.cursor()

    try:
        # Create database and schema
        print(f"  Creating database {args.database}...")
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {args.database}")
        cur.execute(f"USE DATABASE {args.database}")

        print(f"  Creating schema {args.schema}...")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {args.schema}")
        cur.execute(f"USE SCHEMA {args.schema}")

        # Create tables
        print("  Creating equipment_specs table...")
        execute_sql_file(cur, os.path.join(sql_dir, "create_equipment_specs.sql"))

        print("  Creating spare_parts_inventory table...")
        execute_sql_file(cur, os.path.join(sql_dir, "create_spare_parts_inventory.sql"))

        # Insert data (truncate first for idempotency)
        print("  Inserting equipment_specs data (10 rows)...")
        cur.execute("TRUNCATE TABLE IF EXISTS equipment_specs")
        execute_sql_file(cur, os.path.join(sql_dir, "insert_equipment_specs.sql"))

        print("  Inserting spare_parts_inventory data (30 rows)...")
        cur.execute("TRUNCATE TABLE IF EXISTS spare_parts_inventory")
        execute_sql_file(cur, os.path.join(sql_dir, "insert_spare_parts_inventory.sql"))

        # Add comments (Snowflake supports COMMENT ON TABLE/COLUMN natively)
        print("  Adding table/column comments...")
        execute_sql_file(cur, os.path.join(sql_dir, "comments.sql"))

        # Verify
        cur.execute("SELECT COUNT(*) FROM equipment_specs")
        eq_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM spare_parts_inventory")
        sp_count = cur.fetchone()[0]
        print(f"  Verification: equipment_specs={eq_count} rows, spare_parts_inventory={sp_count} rows")

        print("Snowflake setup complete.")

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
