# =============================================================================
# Redshift Data API Statements
# Creates tables, inserts dummy data, and adds comments in Redshift Serverless
# =============================================================================

# -----------------------------------------------------------------------------
# DDL: Create Tables
# -----------------------------------------------------------------------------

resource "aws_redshiftdata_statement" "create_sensor_readings" {
  workgroup_name = aws_redshiftserverless_workgroup.demo.workgroup_name
  database       = aws_redshiftserverless_namespace.demo.db_name
  sql            = file("${path.module}/sql/create_sensor_readings.sql")
}

resource "aws_redshiftdata_statement" "create_production_events" {
  workgroup_name = aws_redshiftserverless_workgroup.demo.workgroup_name
  database       = aws_redshiftserverless_namespace.demo.db_name
  sql            = file("${path.module}/sql/create_production_events.sql")
}

resource "aws_redshiftdata_statement" "create_quality_inspections" {
  workgroup_name = aws_redshiftserverless_workgroup.demo.workgroup_name
  database       = aws_redshiftserverless_namespace.demo.db_name
  sql            = file("${path.module}/sql/create_quality_inspections.sql")
}

# -----------------------------------------------------------------------------
# DML: Insert Dummy Data
# -----------------------------------------------------------------------------

resource "aws_redshiftdata_statement" "insert_sensor_readings" {
  workgroup_name = aws_redshiftserverless_workgroup.demo.workgroup_name
  database       = aws_redshiftserverless_namespace.demo.db_name
  sql            = file("${path.module}/sql/insert_sensor_readings.sql")

  depends_on = [aws_redshiftdata_statement.create_sensor_readings]
}

resource "aws_redshiftdata_statement" "insert_production_events" {
  workgroup_name = aws_redshiftserverless_workgroup.demo.workgroup_name
  database       = aws_redshiftserverless_namespace.demo.db_name
  sql            = file("${path.module}/sql/insert_production_events.sql")

  depends_on = [aws_redshiftdata_statement.create_production_events]
}

resource "aws_redshiftdata_statement" "insert_quality_inspections" {
  workgroup_name = aws_redshiftserverless_workgroup.demo.workgroup_name
  database       = aws_redshiftserverless_namespace.demo.db_name
  sql            = file("${path.module}/sql/insert_quality_inspections.sql")

  depends_on = [aws_redshiftdata_statement.create_quality_inspections]
}

# -----------------------------------------------------------------------------
# Comments: Table and Column descriptions
# Applied after all inserts to ensure tables exist
# -----------------------------------------------------------------------------

resource "aws_redshiftdata_statement" "comments" {
  workgroup_name = aws_redshiftserverless_workgroup.demo.workgroup_name
  database       = aws_redshiftserverless_namespace.demo.db_name
  sql            = file("${path.module}/sql/comments.sql")

  depends_on = [
    aws_redshiftdata_statement.insert_sensor_readings,
    aws_redshiftdata_statement.insert_production_events,
    aws_redshiftdata_statement.insert_quality_inspections,
  ]
}
