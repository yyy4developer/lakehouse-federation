# =============================================================================
# AWS Glue カタログ データベース & テーブル
# 工場マスタデータ（Parquet, Delta, Iceberg形式）
#
# テーブル登録方針:
#   - sensors（Parquet）:             Terraform管理（aws_glue_catalog_table）
#   - machines（Delta）:              ETLジョブ管理（boto3でSparkデータソース形式登録）
#   - quality_inspections（Iceberg）: ETLジョブ管理（writeTo + Glueカタログ）
#
# Delta・Icebergテーブルは、適切なメタデータ（Delta _delta_log、
# Iceberg metadata_location）を確保するためにGlue ETLジョブで登録する。
# =============================================================================

resource "aws_glue_catalog_database" "factory_master" {
  name         = local.glue_database_name
  description  = "Lakehouse Federationデモ用の工場マスタデータ - センサー、機械、品質検査データを格納"
  location_uri = "s3://${aws_s3_bucket.glue_data.id}/factory_master/"
}

# -----------------------------------------------------------------------------
# テーブル: sensors（Parquet, 20行）- センサーマスタデータ
# Terraformでテーブル定義を管理。データはGlue ETLジョブで生成。
# -----------------------------------------------------------------------------

resource "aws_glue_catalog_table" "sensors" {
  database_name = aws_glue_catalog_database.factory_master.name
  name          = "sensors"
  description   = "センサーマスタデータ - 工場内全センサーの種類、単位、設置場所を管理するレジストリ"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
    "typeOfData"     = "file"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.glue_data.id}/factory_master/sensors/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }

    columns {
      name    = "sensor_id"
      type    = "int"
      comment = "センサーの一意識別子"
    }
    columns {
      name    = "sensor_name"
      type    = "string"
      comment = "センサーのモデル/コード名（例：TMP-A01）"
    }
    columns {
      name    = "sensor_type"
      type    = "string"
      comment = "測定種類：温度、圧力、振動、湿度、流量、回転数"
    }
    columns {
      name    = "unit"
      type    = "string"
      comment = "測定単位（℃、bar、mm/s、%、l/min、rpm）"
    }
    columns {
      name    = "location"
      type    = "string"
      comment = "工場内の物理的な設置場所"
    }
    columns {
      name    = "installed_date"
      type    = "string"
      comment = "センサー設置日（YYYY-MM-DD形式）"
    }
  }

  depends_on = [null_resource.run_glue_job]
}

# -----------------------------------------------------------------------------
# Glue ETLジョブで管理されるテーブル（Terraform管理外）:
#   - machines（Delta）             → boto3でSparkデータソース形式として登録
#   - quality_inspections（Iceberg）→ Spark Icebergカタログ経由で登録
#
# テーブル説明・カラムコメントはETLジョブスクリプト
# （scripts/generate_data.py）内で設定。
# -----------------------------------------------------------------------------
