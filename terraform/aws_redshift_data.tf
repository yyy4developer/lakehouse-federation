# =============================================================================
# Redshift Data API Statements
# =============================================================================

# DDL
resource "aws_redshiftdata_statement" "create_sensor_readings" {
  count          = var.enable_redshift ? 1 : 0
  workgroup_name = aws_redshiftserverless_workgroup.demo[0].workgroup_name
  database       = aws_redshiftserverless_namespace.demo[0].db_name
  sql            = file("${path.module}/sql/redshift/create_sensor_readings.sql")
}

resource "aws_redshiftdata_statement" "create_production_events" {
  count          = var.enable_redshift ? 1 : 0
  workgroup_name = aws_redshiftserverless_workgroup.demo[0].workgroup_name
  database       = aws_redshiftserverless_namespace.demo[0].db_name
  sql            = file("${path.module}/sql/redshift/create_production_events.sql")
}

resource "aws_redshiftdata_statement" "create_quality_inspections" {
  count          = var.enable_redshift ? 1 : 0
  workgroup_name = aws_redshiftserverless_workgroup.demo[0].workgroup_name
  database       = aws_redshiftserverless_namespace.demo[0].db_name
  sql            = file("${path.module}/sql/redshift/create_quality_inspections.sql")
}

# DML
resource "aws_redshiftdata_statement" "insert_sensor_readings" {
  count          = var.enable_redshift ? 1 : 0
  workgroup_name = aws_redshiftserverless_workgroup.demo[0].workgroup_name
  database       = aws_redshiftserverless_namespace.demo[0].db_name
  sql            = file("${path.module}/sql/redshift/insert_sensor_readings.sql")
  depends_on     = [aws_redshiftdata_statement.create_sensor_readings]
}

resource "aws_redshiftdata_statement" "insert_production_events" {
  count          = var.enable_redshift ? 1 : 0
  workgroup_name = aws_redshiftserverless_workgroup.demo[0].workgroup_name
  database       = aws_redshiftserverless_namespace.demo[0].db_name
  sql            = file("${path.module}/sql/redshift/insert_production_events.sql")
  depends_on     = [aws_redshiftdata_statement.create_production_events]
}

resource "aws_redshiftdata_statement" "insert_quality_inspections" {
  count          = var.enable_redshift ? 1 : 0
  workgroup_name = aws_redshiftserverless_workgroup.demo[0].workgroup_name
  database       = aws_redshiftserverless_namespace.demo[0].db_name
  sql            = file("${path.module}/sql/redshift/insert_quality_inspections.sql")
  depends_on     = [aws_redshiftdata_statement.create_quality_inspections]
}

# Comments
resource "aws_redshiftdata_statement" "comments" {
  count          = var.enable_redshift ? 1 : 0
  workgroup_name = aws_redshiftserverless_workgroup.demo[0].workgroup_name
  database       = aws_redshiftserverless_namespace.demo[0].db_name
  sql            = file("${path.module}/sql/redshift/comments.sql")

  depends_on = [
    aws_redshiftdata_statement.insert_sensor_readings,
    aws_redshiftdata_statement.insert_production_events,
    aws_redshiftdata_statement.insert_quality_inspections,
  ]
}
