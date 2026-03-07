# =============================================================================
# Amazon RDS PostgreSQL (for AWS workspace deployments)
# =============================================================================

resource "aws_security_group" "postgres" {
  count = (var.enable_postgres && var.cloud == "aws") ? 1 : 0

  name_prefix = "${var.project_prefix}-postgres-"
  vpc_id      = aws_vpc.main[0].id
  description = "Security group for RDS PostgreSQL"

  ingress {
    description = "PostgreSQL - range 1"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/1"]
  }

  ingress {
    description = "PostgreSQL - range 2"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["128.0.0.0/1"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_prefix}-postgres-sg"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_db_subnet_group" "postgres" {
  count = (var.enable_postgres && var.cloud == "aws") ? 1 : 0

  name       = "${var.project_prefix}-postgres"
  subnet_ids = aws_subnet.public[*].id

  tags = {
    Name = "${var.project_prefix}-postgres-subnet-group"
  }
}

resource "aws_db_instance" "postgres" {
  count = (var.enable_postgres && var.cloud == "aws") ? 1 : 0

  identifier     = "${var.project_prefix}-postgres"
  engine         = "postgres"
  engine_version = "16"
  instance_class = "db.t4g.micro"

  allocated_storage = 20
  storage_type      = "gp3"

  db_name  = local.postgres_db_name
  username = "pgadmin"
  password = var.postgres_admin_password

  db_subnet_group_name   = aws_db_subnet_group.postgres[0].name
  vpc_security_group_ids = [aws_security_group.postgres[0].id]
  publicly_accessible    = true
  skip_final_snapshot    = true

  tags = {
    Name = "${var.project_prefix}-postgres"
  }
}

# Initialize PostgreSQL with tables and data
resource "null_resource" "postgres_init" {
  count = (var.enable_postgres && var.cloud == "aws") ? 1 : 0

  triggers = {
    instance_id = aws_db_instance.postgres[0].id
  }

  provisioner "local-exec" {
    command = <<-EOT
      export PGPASSWORD='${var.postgres_admin_password}'
      PGHOST='${aws_db_instance.postgres[0].address}'

      echo "Creating tables..."
      psql -h "$PGHOST" -U pgadmin -d ${local.postgres_db_name} -f ${path.module}/sql/postgres/create_maintenance_logs.sql
      psql -h "$PGHOST" -U pgadmin -d ${local.postgres_db_name} -f ${path.module}/sql/postgres/create_work_orders.sql

      echo "Inserting data..."
      psql -h "$PGHOST" -U pgadmin -d ${local.postgres_db_name} -f ${path.module}/sql/postgres/insert_maintenance_logs.sql
      psql -h "$PGHOST" -U pgadmin -d ${local.postgres_db_name} -f ${path.module}/sql/postgres/insert_work_orders.sql

      echo "Adding comments..."
      psql -h "$PGHOST" -U pgadmin -d ${local.postgres_db_name} -f ${path.module}/sql/postgres/comments.sql

      echo "PostgreSQL initialization complete."
    EOT
  }

  depends_on = [aws_db_instance.postgres]
}
