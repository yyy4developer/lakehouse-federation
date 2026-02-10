-- Databricks notebook source

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Lakehouse Federationデモ: 工場センサー＆品質データ
-- MAGIC
-- MAGIC このノートブックは **Databricks Lakehouse Federation** を使用して、
-- MAGIC 2つの外部データソースを横断したクエリを実演します。
-- MAGIC
-- MAGIC | カタログ | ソース | テーブル | フォーマット | データ種別 |
-- MAGIC |---------|--------|---------|------------|-----------|
-- MAGIC | `glue_factory` | AWS Glue（S3バックエンド） | `sensors` | **Parquet** | マスタデータ |
-- MAGIC | `glue_factory` | AWS Glue（S3バックエンド） | `machines` | **Delta** | マスタデータ |
-- MAGIC | `glue_factory` | AWS Glue（S3バックエンド） | `quality_inspections` | **Iceberg** | 検査データ |
-- MAGIC | `redshift_factory` | Amazon Redshift Serverless | `sensor_readings` | - | トランザクションデータ |
-- MAGIC | `redshift_factory` | Amazon Redshift Serverless | `production_events` | - | トランザクションデータ |
-- MAGIC | `redshift_factory` | Amazon Redshift Serverless | `quality_inspections` | - | 検査データ |
-- MAGIC
-- MAGIC **主要なデモ内容:**
-- MAGIC 1. Glue・Redshiftそれぞれの単一ソースクエリ
-- MAGIC 2. GlueとRedshiftを跨いだクロスソースJOIN
-- MAGIC 3. 全6テーブルを組み合わせたマルチテーブル分析
-- MAGIC 4. テーブルメタデータの確認（テーブル説明、カラムコメント、フォーマット）

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 1. Glueカタログの探索（マスタデータ）

-- COMMAND ----------

-- Glue外部カタログ内のスキーマ一覧
SHOW SCHEMAS IN glue_factory;

-- COMMAND ----------

-- 工場マスタデータベース内のテーブル一覧
SHOW TABLES IN glue_factory.lhf_demo_factory_master;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1a. テーブルメタデータの確認 - テーブル説明＆フォーマット
-- MAGIC
-- MAGIC テーブル説明とカラムコメントがFederation経由で正しく表示されることを確認します。

-- COMMAND ----------

-- sensorsテーブル（Parquet）の詳細情報 - テーブル説明とカラムコメントの確認
DESCRIBE TABLE EXTENDED glue_factory.lhf_demo_factory_master.sensors;

-- COMMAND ----------

-- machinesテーブル（Delta）の詳細情報 - テーブル説明とカラムコメントの確認
DESCRIBE TABLE EXTENDED glue_factory.lhf_demo_factory_master.machines;

-- COMMAND ----------

-- quality_inspectionsテーブル（Iceberg）の詳細情報 - テーブル説明とカラムコメントの確認
DESCRIBE TABLE EXTENDED glue_factory.lhf_demo_factory_master.quality_inspections;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 2. Glueクエリ: センサーマスタデータ（Parquet, 20行）

-- COMMAND ----------

-- 全センサー一覧
SELECT * FROM glue_factory.lhf_demo_factory_master.sensors;

-- COMMAND ----------

-- 種類別センサー数
SELECT
  sensor_type AS `センサー種類`,
  COUNT(*) AS `センサー数`,
  COLLECT_LIST(sensor_name) AS `センサー名一覧`
FROM glue_factory.lhf_demo_factory_master.sensors
GROUP BY sensor_type
ORDER BY `センサー数` DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Glueクエリ: 機械マスタデータ（Delta, 10行）

-- COMMAND ----------

-- 全機械一覧
SELECT * FROM glue_factory.lhf_demo_factory_master.machines;

-- COMMAND ----------

-- 製造ライン・稼働状態別の機械数
SELECT
  production_line AS `製造ライン`,
  factory AS `工場建屋`,
  status AS `稼働状態`,
  COUNT(*) AS `機械数`
FROM glue_factory.lhf_demo_factory_master.machines
GROUP BY production_line, factory, status
ORDER BY production_line;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Glueクエリ: 品質検査データ（Iceberg, 50行）

-- COMMAND ----------

-- 最近の品質検査結果
SELECT * FROM glue_factory.lhf_demo_factory_master.quality_inspections
ORDER BY inspection_time DESC
LIMIT 20;

-- COMMAND ----------

-- 検査結果サマリー
SELECT
  result AS `検査結果`,
  COUNT(*) AS `検査件数`,
  ROUND(AVG(defect_count), 1) AS `平均不良数`,
  SUM(defect_count) AS `合計不良数`
FROM glue_factory.lhf_demo_factory_master.quality_inspections
GROUP BY result
ORDER BY `検査件数` DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 5. Redshiftカタログの探索（トランザクションデータ）

-- COMMAND ----------

SHOW SCHEMAS IN redshift_factory;

-- COMMAND ----------

SHOW TABLES IN redshift_factory.public;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5a. Redshiftテーブルメタデータの確認

-- COMMAND ----------

-- Redshift sensor_readingsの詳細情報 - テーブル/カラムコメントの確認
DESCRIBE TABLE EXTENDED redshift_factory.public.sensor_readings;

-- COMMAND ----------

-- Redshift production_eventsの詳細情報
DESCRIBE TABLE EXTENDED redshift_factory.public.production_events;

-- COMMAND ----------

-- Redshift quality_inspectionsの詳細情報
DESCRIBE TABLE EXTENDED redshift_factory.public.quality_inspections;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 6. Redshiftクエリ: センサー読取データ（100行）

-- COMMAND ----------

-- 最近のセンサー読取値
SELECT * FROM redshift_factory.public.sensor_readings
ORDER BY reading_time DESC
LIMIT 20;

-- COMMAND ----------

-- ステータス別の読取統計
SELECT
  status AS `ステータス`,
  COUNT(*) AS `読取件数`,
  ROUND(AVG(value), 2) AS `平均値`,
  ROUND(MIN(value), 2) AS `最小値`,
  ROUND(MAX(value), 2) AS `最大値`
FROM redshift_factory.public.sensor_readings
GROUP BY status
ORDER BY `読取件数` DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Redshiftクエリ: 生産イベントデータ（30行）

-- COMMAND ----------

-- 最近の生産イベント
SELECT * FROM redshift_factory.public.production_events
ORDER BY event_time DESC
LIMIT 20;

-- COMMAND ----------

-- イベント種別サマリー
SELECT
  event_type AS `イベント種別`,
  COUNT(*) AS `イベント件数`,
  ROUND(AVG(duration_minutes), 1) AS `平均所要時間_分`
FROM redshift_factory.public.production_events
GROUP BY event_type
ORDER BY `イベント件数` DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8. Redshiftクエリ: 品質検査データ（40行）

-- COMMAND ----------

-- Redshiftの品質検査結果
SELECT * FROM redshift_factory.public.quality_inspections
ORDER BY inspection_time DESC
LIMIT 20;

-- COMMAND ----------

-- 検査員別サマリー
SELECT
  inspector_name AS `検査員名`,
  COUNT(*) AS `合計検査数`,
  COUNT(CASE WHEN result = 'pass' THEN 1 END) AS `合格数`,
  COUNT(CASE WHEN result = 'fail' THEN 1 END) AS `不合格数`,
  COUNT(CASE WHEN result = 'warning' THEN 1 END) AS `警告数`
FROM redshift_factory.public.quality_inspections
GROUP BY inspector_name
ORDER BY `合計検査数` DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 9. クロスソースJOIN: センサー（Glue/Parquet）× 読取値（Redshift）
-- MAGIC
-- MAGIC **Glueのマスタデータ**と**Redshiftのトランザクションデータ**をJOINして、
-- MAGIC 危険・警告レベルのセンサー読取値にセンサー詳細情報を付加します。

-- COMMAND ----------

SELECT
  s.sensor_name AS `センサー名`,
  s.sensor_type AS `センサー種類`,
  s.unit AS `単位`,
  s.location AS `設置場所`,
  r.machine_id AS `機械ID`,
  r.reading_time AS `読取日時`,
  r.value AS `測定値`,
  r.status AS `読取ステータス`
FROM glue_factory.lhf_demo_factory_master.sensors s
JOIN redshift_factory.public.sensor_readings r
  ON s.sensor_id = r.sensor_id
WHERE r.status IN ('warning', 'critical')
ORDER BY r.reading_time DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 10. クロスソースJOIN: 機械（Glue/Delta）× イベント（Redshift）
-- MAGIC
-- MAGIC 機械のマスタデータと生産イベントを結合し、
-- MAGIC メンテナンスやエラーのパターンを製造ライン別に分析します。

-- COMMAND ----------

SELECT
  m.machine_name AS `機械名`,
  m.production_line AS `製造ライン`,
  m.factory AS `工場建屋`,
  m.status AS `機械稼働状態`,
  e.event_type AS `イベント種別`,
  e.event_time AS `イベント日時`,
  e.duration_minutes AS `所要時間_分`,
  e.description AS `説明`
FROM glue_factory.lhf_demo_factory_master.machines m
JOIN redshift_factory.public.production_events e
  ON m.machine_id = e.machine_id
WHERE e.event_type IN ('error', 'maintenance')
ORDER BY e.event_time DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 11. クロスソースJOIN: 品質検査データ（Glue/Iceberg + Redshift）
-- MAGIC
-- MAGIC **両ソース**の品質検査結果を比較します。
-- MAGIC GlueにはIceberg形式の検査データ、Redshiftにも独自の検査レコードがあります。

-- COMMAND ----------

SELECT
  m.machine_name AS `機械名`,
  m.production_line AS `製造ライン`,
  'Glue (Iceberg)' AS `データソース`,
  gqi.inspection_time AS `検査日時`,
  gqi.result AS `検査結果`,
  gqi.defect_count AS `不良数`,
  gqi.inspector_name AS `検査員名`
FROM glue_factory.lhf_demo_factory_master.quality_inspections gqi
JOIN glue_factory.lhf_demo_factory_master.machines m
  ON gqi.machine_id = m.machine_id
WHERE gqi.result != 'pass'

UNION ALL

SELECT
  m.machine_name AS `機械名`,
  m.production_line AS `製造ライン`,
  'Redshift' AS `データソース`,
  rqi.inspection_time AS `検査日時`,
  rqi.result AS `検査結果`,
  rqi.defect_count AS `不良数`,
  rqi.inspector_name AS `検査員名`
FROM redshift_factory.public.quality_inspections rqi
JOIN glue_factory.lhf_demo_factory_master.machines m
  ON rqi.machine_id = m.machine_id
WHERE rqi.result != 'pass'

ORDER BY `検査日時` DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 12. 全テーブルJOIN: 工場センサー総合分析
-- MAGIC
-- MAGIC Glue・Redshiftの**全テーブル**を結合し、
-- MAGIC 機械ごとのセンサー詳細付き異常検知ビューを構築します。

-- COMMAND ----------

SELECT
  m.machine_name AS `機械名`,
  m.production_line AS `製造ライン`,
  m.factory AS `工場建屋`,
  s.sensor_name AS `センサー名`,
  s.sensor_type AS `センサー種類`,
  s.unit AS `単位`,
  r.reading_time AS `読取日時`,
  r.value AS `測定値`,
  r.status AS `読取ステータス`
FROM glue_factory.lhf_demo_factory_master.machines m
JOIN redshift_factory.public.sensor_readings r
  ON m.machine_id = r.machine_id
JOIN glue_factory.lhf_demo_factory_master.sensors s
  ON r.sensor_id = s.sensor_id
WHERE r.status != 'normal'
ORDER BY m.production_line, r.reading_time DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 13. 機械別異常サマリー
-- MAGIC
-- MAGIC 全センサー種類を対象に、機械ごとの警告・危険読取値を集計します。

-- COMMAND ----------

SELECT
  m.machine_name AS `機械名`,
  m.production_line AS `製造ライン`,
  m.factory AS `工場建屋`,
  COUNT(*) AS `総読取数`,
  COUNT(CASE WHEN r.status = 'warning' THEN 1 END) AS `警告件数`,
  COUNT(CASE WHEN r.status = 'critical' THEN 1 END) AS `危険件数`,
  ROUND(
    COUNT(CASE WHEN r.status IN ('warning', 'critical') THEN 1 END) * 100.0 / COUNT(*),
    1
  ) AS `異常率_パーセント`
FROM glue_factory.lhf_demo_factory_master.machines m
JOIN redshift_factory.public.sensor_readings r
  ON m.machine_id = r.machine_id
GROUP BY m.machine_name, m.production_line, m.factory
ORDER BY `危険件数` DESC, `警告件数` DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 14. 機械ダウンタイム分析
-- MAGIC
-- MAGIC エラー・メンテナンスイベントと機械マスタデータを結合し、
-- MAGIC 製造ライン別の合計ダウンタイムを算出します。

-- COMMAND ----------

SELECT
  m.production_line AS `製造ライン`,
  m.factory AS `工場建屋`,
  e.event_type AS `イベント種別`,
  COUNT(*) AS `イベント件数`,
  SUM(e.duration_minutes) AS `合計ダウンタイム_分`,
  ROUND(SUM(e.duration_minutes) / 60.0, 1) AS `合計ダウンタイム_時間`
FROM glue_factory.lhf_demo_factory_master.machines m
JOIN redshift_factory.public.production_events e
  ON m.machine_id = e.machine_id
WHERE e.event_type IN ('error', 'maintenance')
  AND e.duration_minutes IS NOT NULL
GROUP BY m.production_line, m.factory, e.event_type
ORDER BY `合計ダウンタイム_分` DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 15. 機械別品質スコア（両ソース統合）
-- MAGIC
-- MAGIC **Glue（Iceberg）とRedshift**の両方の検査データを使用して、
-- MAGIC 機械ごとの品質スコアを算出します。

-- COMMAND ----------

WITH all_inspections AS (
  SELECT machine_id, result, defect_count, 'Glue (Iceberg)' AS source
  FROM glue_factory.lhf_demo_factory_master.quality_inspections
  UNION ALL
  SELECT machine_id, result, defect_count, 'Redshift' AS source
  FROM redshift_factory.public.quality_inspections
)
SELECT
  m.machine_name AS `機械名`,
  m.production_line AS `製造ライン`,
  COUNT(*) AS `合計検査数`,
  COUNT(CASE WHEN ai.result = 'pass' THEN 1 END) AS `合格数`,
  COUNT(CASE WHEN ai.result = 'fail' THEN 1 END) AS `不合格数`,
  ROUND(
    COUNT(CASE WHEN ai.result = 'pass' THEN 1 END) * 100.0 / COUNT(*),
    1
  ) AS `合格率_パーセント`,
  SUM(ai.defect_count) AS `合計不良数`
FROM all_inspections ai
JOIN glue_factory.lhf_demo_factory_master.machines m
  ON ai.machine_id = m.machine_id
GROUP BY m.machine_name, m.production_line
ORDER BY `合格率_パーセント` ASC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 16. 機械ヘルスダッシュボード（センサー＋イベント＋品質検査）
-- MAGIC
-- MAGIC 全6テーブルから、センサー異常、ダウンタイムイベント、
-- MAGIC 品質検査結果を統合した機械ごとの総合サマリーを構築します。

-- COMMAND ----------

WITH sensor_summary AS (
  SELECT
    r.machine_id,
    COUNT(CASE WHEN r.status = 'warning' THEN 1 END) AS sensor_warnings,
    COUNT(CASE WHEN r.status = 'critical' THEN 1 END) AS sensor_criticals
  FROM redshift_factory.public.sensor_readings r
  GROUP BY r.machine_id
),
event_summary AS (
  SELECT
    e.machine_id,
    COUNT(CASE WHEN e.event_type = 'error' THEN 1 END) AS error_count,
    SUM(CASE WHEN e.event_type = 'maintenance' THEN e.duration_minutes ELSE 0 END) AS maintenance_min
  FROM redshift_factory.public.production_events e
  GROUP BY e.machine_id
),
quality_summary AS (
  SELECT machine_id, result, defect_count FROM glue_factory.lhf_demo_factory_master.quality_inspections
  UNION ALL
  SELECT machine_id, result, defect_count FROM redshift_factory.public.quality_inspections
),
quality_agg AS (
  SELECT
    machine_id,
    COUNT(*) AS total_inspections,
    COUNT(CASE WHEN result = 'fail' THEN 1 END) AS failed_inspections,
    SUM(defect_count) AS total_defects
  FROM quality_summary
  GROUP BY machine_id
)
SELECT
  m.machine_name AS `機械名`,
  m.production_line AS `製造ライン`,
  m.factory AS `工場建屋`,
  m.status AS `機械稼働状態`,
  COALESCE(ss.sensor_warnings, 0) AS `センサー警告数`,
  COALESCE(ss.sensor_criticals, 0) AS `センサー危険数`,
  COALESCE(es.error_count, 0) AS `エラーイベント数`,
  COALESCE(es.maintenance_min, 0) AS `メンテナンス時間_分`,
  COALESCE(qa.total_inspections, 0) AS `品質検査数`,
  COALESCE(qa.failed_inspections, 0) AS `不合格検査数`,
  COALESCE(qa.total_defects, 0) AS `合計不良数`
FROM glue_factory.lhf_demo_factory_master.machines m
LEFT JOIN sensor_summary ss ON m.machine_id = ss.machine_id
LEFT JOIN event_summary es ON m.machine_id = es.machine_id
LEFT JOIN quality_agg qa ON m.machine_id = qa.machine_id
ORDER BY (COALESCE(ss.sensor_criticals, 0) + COALESCE(es.error_count, 0) + COALESCE(qa.failed_inspections, 0)) DESC;
