-- Databricks notebook source

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 工場データ統合デモ
-- MAGIC # Unity Catalog によるデータガバナンス
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC | 章 | テーマ | 機能 | 内容 |
-- MAGIC |----|--------|------|------|
-- MAGIC | **第1章** | メタデータ統合 | Lakehouse Federation | 外部データソースの接続・クエリ・メタデータの一元管理 |
-- MAGIC | **第2章** | 資産の健全化 | Discover / Lineage | データ資産の検索・探索、リネージによる依存関係の可視化 |
-- MAGIC | **第3章** | 統制の一元化 | 権限管理 | Unity Catalogによるアクセス制御の一元管理 |
-- MAGIC | **第4章** | 生成AI活用 | Genie | 自然言語によるデータ探索・分析 |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 設定
-- MAGIC
-- MAGIC デプロイ時のカタログ名に合わせて、以下のウィジェットを更新してください。
-- MAGIC 正しい値は `deploy_result.md` に記載されています。
-- MAGIC
-- MAGIC - **query_prefix**: Query Federation カタログの prefix（例: `lhf_query_ab12`）
-- MAGIC - **catalog_prefix**: Catalog Federation カタログの prefix（例: `lhf_catalog_ab12`）
-- MAGIC - **db_prefix**: データベース/スキーマ名の prefix（例: `lhf_demo`）

-- COMMAND ----------

CREATE WIDGET TEXT query_prefix DEFAULT 'lhf_query';
CREATE WIDGET TEXT catalog_prefix DEFAULT 'lhf_catalog';
CREATE WIDGET TEXT db_prefix DEFAULT 'lhf_demo';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC # 第1章: メタデータ統合（Lakehouse Federation）
-- MAGIC
-- MAGIC ## 概要
-- MAGIC
-- MAGIC Lakehouse Federationにより、外部データソースのメタデータ（スキーマ、テーブル説明、
-- MAGIC カラムコメント等）をUnity Catalogに統合し、**データを移動せずに**一元管理します。
-- MAGIC
-- MAGIC ## アーキテクチャ
-- MAGIC
-- MAGIC ```
-- MAGIC ┌─────────────────────────────────────────────────────────────────────────────────┐
-- MAGIC │                          Databricks Unity Catalog                               │
-- MAGIC │                                                                                 │
-- MAGIC │  ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐                │
-- MAGIC │  │ *_catalog_glue   │ │ *_query_redshift  │ │ *_query_postgres │                │
-- MAGIC │  │ (Catalog Fed.)   │ │ (Query Fed.)      │ │ (Query Fed.)     │                │
-- MAGIC │  └────────┬─────────┘ └────────┬─────────┘ └────────┬─────────┘                │
-- MAGIC │           │                     │                     │                          │
-- MAGIC │  ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐                │
-- MAGIC │  │ *_query_synapse  │ │ *_query_bigquery  │ │*_catalog_onelake │                │
-- MAGIC │  │ (Query Fed.)     │ │ (Query Fed.)      │ │ (Catalog Fed.)   │                │
-- MAGIC │  └────────┬─────────┘ └────────┬─────────┘ └────────┬─────────┘                │
-- MAGIC │           │                     │                     │                          │
-- MAGIC └───────────┼─────────────────────┼─────────────────────┼──────────────────────────┘
-- MAGIC             │                     │                     │
-- MAGIC   ┌─────────▼──────┐   ┌─────────▼──────┐   ┌─────────▼──────┐
-- MAGIC   │ Azure Synapse  │   │ Google BigQuery │   │ Microsoft      │
-- MAGIC   │ (JDBC)         │   │ (JDBC)          │   │ OneLake/Fabric │
-- MAGIC   └────────────────┘   └─────────────────┘   └────────────────┘
-- MAGIC ```
-- MAGIC
-- MAGIC ## Catalog Federation と Query Federation の違い
-- MAGIC
-- MAGIC | 項目 | Catalog Federation | Query Federation |
-- MAGIC |------|-------------------|------------------|
-- MAGIC | **対象** | Glue, OneLake | Redshift, PostgreSQL, Synapse, BigQuery |
-- MAGIC | **接続方式** | メタデータAPI → ストレージ直接読取 | JDBC経由でクエリをプッシュダウン |
-- MAGIC | **データの流れ** | S3/ADLS → Databricks (Spark実行) | 外部エンジンがクエリ実行 → 結果返却 |
-- MAGIC | **パフォーマンス** | Sparkの並列処理で大量スキャンに強い | フィルタ・集約のプッシュダウンで効率的 |
-- MAGIC
-- MAGIC ### 本デモのデータ構成
-- MAGIC
-- MAGIC > デプロイ時に選択したソースのみ利用可能です。未デプロイのセクションはスキップしてください。
-- MAGIC
-- MAGIC | カタログ | スキーマ | テーブル | データ種別 | 行数 |
-- MAGIC |---------|---------|---------|-----------|------|
-- MAGIC | `${catalog_prefix}_glue` | `${db_prefix}_factory_master` | `sensors` | センサーマスタ (Parquet) | 20 |
-- MAGIC | `${catalog_prefix}_glue` | `${db_prefix}_factory_master` | `machines` | 機械マスタ (Delta) | 10 |
-- MAGIC | `${catalog_prefix}_glue` | `${db_prefix}_factory_master` | `quality_inspections` | 品質検査 (Iceberg) | 50 |
-- MAGIC | `${query_prefix}_redshift` | `public` | `sensor_readings` | センサー読取値 | 100 |
-- MAGIC | `${query_prefix}_redshift` | `public` | `production_events` | 生産イベント | 30 |
-- MAGIC | `${query_prefix}_redshift` | `public` | `quality_inspections` | 品質検査 | 40 |
-- MAGIC | `${query_prefix}_postgres` | `public` | `maintenance_logs` | 保守ログ | 30 |
-- MAGIC | `${query_prefix}_postgres` | `public` | `work_orders` | 作業指示書 | 25 |
-- MAGIC | `${query_prefix}_synapse` | `dbo` | `shift_schedules` | シフトスケジュール | 40 |
-- MAGIC | `${query_prefix}_synapse` | `dbo` | `energy_consumption` | 電力消費量 | 50 |
-- MAGIC | `${query_prefix}_bigquery` | `${db_prefix}_factory` | `downtime_records` | 稼働停止記録 | 35 |
-- MAGIC | `${query_prefix}_bigquery` | `${db_prefix}_factory` | `cost_allocation` | コスト配分 | 30 |
-- MAGIC | `${catalog_prefix}_onelake` | `default` | `production_plans` | 生産計画 | 20 |
-- MAGIC | `${catalog_prefix}_onelake` | `default` | `inventory_levels` | 在庫水準 | 30 |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 1.1 Catalog Federation: AWS Glueカタログ
-- MAGIC
-- MAGIC Glue Data Catalogに登録されたテーブルを、Unity Catalog経由で透過的に参照します。
-- MAGIC Databricksは **S3上のデータを直接読み取り** 、Sparkエンジンで処理します。

-- COMMAND ----------

-- センサーマスタデータ（Parquet）
SELECT * FROM IDENTIFIER('${catalog_prefix}_glue' || '.' || '${db_prefix}_factory_master' || '.sensors');

-- COMMAND ----------

-- 機械マスタデータ（Delta）
SELECT * FROM IDENTIFIER('${catalog_prefix}_glue' || '.' || '${db_prefix}_factory_master' || '.machines');

-- COMMAND ----------

-- 品質検査データ（Iceberg）
SELECT * FROM IDENTIFIER('${catalog_prefix}_glue' || '.' || '${db_prefix}_factory_master' || '.quality_inspections');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 1.2 Query Federation: Amazon Redshift
-- MAGIC
-- MAGIC Redshift Serverlessに対してJDBC経由でクエリを発行します。
-- MAGIC フィルタや集約は **Redshift側にプッシュダウン** され、結果のみがDatabricksに返却されます。

-- COMMAND ----------

-- センサー読取データ
SELECT * FROM IDENTIFIER('${query_prefix}_redshift' || '.public.sensor_readings');

-- COMMAND ----------

-- 生産イベントデータ
SELECT * FROM IDENTIFIER('${query_prefix}_redshift' || '.public.production_events');

-- COMMAND ----------

-- 品質検査データ
SELECT * FROM IDENTIFIER('${query_prefix}_redshift' || '.public.quality_inspections');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 1.3 Query Federation: PostgreSQL
-- MAGIC
-- MAGIC PostgreSQL (AWS RDS or Azure Flexible Server) に対してJDBC経由でクエリを発行します。
-- MAGIC 保守ログや作業指示書など、運用系データを参照します。

-- COMMAND ----------

-- 保守ログ
SELECT * FROM IDENTIFIER('${query_prefix}_postgres' || '.public.maintenance_logs');

-- COMMAND ----------

-- 作業指示書
SELECT * FROM IDENTIFIER('${query_prefix}_postgres' || '.public.work_orders');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 1.4 Query Federation: Azure Synapse
-- MAGIC
-- MAGIC Azure Synapse Analytics (Serverless SQL Pool) に対してJDBC経由でクエリを発行します。
-- MAGIC シフト管理やエネルギー消費のデータを参照します。

-- COMMAND ----------

-- シフトスケジュール
SELECT * FROM IDENTIFIER('${query_prefix}_synapse' || '.dbo.shift_schedules');

-- COMMAND ----------

-- 電力消費量
SELECT * FROM IDENTIFIER('${query_prefix}_synapse' || '.dbo.energy_consumption');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 1.5 Query Federation: Google BigQuery
-- MAGIC
-- MAGIC Google BigQueryに対してJDBC経由でクエリを発行します。
-- MAGIC 稼働停止記録やコスト配分データを参照します。

-- COMMAND ----------

-- 稼働停止記録
SELECT * FROM IDENTIFIER('${query_prefix}_bigquery' || '.' || '${db_prefix}_factory' || '.downtime_records');

-- COMMAND ----------

-- コスト配分
SELECT * FROM IDENTIFIER('${query_prefix}_bigquery' || '.' || '${db_prefix}_factory' || '.cost_allocation');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 1.6 Catalog Federation: Microsoft OneLake (Fabric)
-- MAGIC
-- MAGIC Microsoft Fabricの OneLake に格納されたデータを、
-- MAGIC Catalog Federation経由で透過的に参照します。

-- COMMAND ----------

-- 生産計画
SELECT * FROM IDENTIFIER('${catalog_prefix}_onelake' || '.default.production_plans');

-- COMMAND ----------

-- 在庫水準
SELECT * FROM IDENTIFIER('${catalog_prefix}_onelake' || '.default.inventory_levels');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC # 第2章: 資産の健全化（Discover / Lineage）
-- MAGIC
-- MAGIC データ資産の **発見・理解** と **依存関係の可視化** により、
-- MAGIC 組織のデータ資産を健全に管理します。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2.1 Discover（データ検索・探索）
-- MAGIC
-- MAGIC Unity Catalogの **Discover** 機能は、組織内のすべてのデータ資産を
-- MAGIC 検索・閲覧・理解するためのデータカタログUIです。
-- MAGIC
-- MAGIC ### 主な機能
-- MAGIC
-- MAGIC | 機能 | 説明 |
-- MAGIC |------|------|
-- MAGIC | **全文検索** | テーブル名・カラム名・説明文をキーワードで横断検索 |
-- MAGIC | **フィルタリング** | カタログ・スキーマ・タグ・オーナーで絞り込み |
-- MAGIC | **メタデータ閲覧** | テーブル説明・カラムコメント・スキーマ情報を一覧表示 |
-- MAGIC | **AI生成ドキュメント** | テーブル・カラムの説明をAIが自動生成（提案） |
-- MAGIC
-- MAGIC ### デモ手順（UIで実施）
-- MAGIC
-- MAGIC 1. **Catalog Explorer** を開く（左サイドバー → Catalog）
-- MAGIC 2. 検索バーで `sensors` や `machines` を検索
-- MAGIC    - 複数のFederationカタログからテーブルがヒットすることを確認
-- MAGIC 3. テーブルを選択 → **テーブル説明**・**カラムコメント**（日本語）を確認

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 2.2 Lineage（データリネージ）
-- MAGIC
-- MAGIC Unity Catalogの **Lineage** 機能は、データの流れを自動的に追跡し、
-- MAGIC テーブル間・カラム間の依存関係を可視化します。
-- MAGIC
-- MAGIC ### デモの流れ
-- MAGIC
-- MAGIC 1. Federationデータをクロスソース **JOIN** して分析テーブルを作成
-- MAGIC 2. UIでリネージグラフを確認 → 外部ソースからの依存が可視化される

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.2a 分析用カタログ・スキーマの準備

-- COMMAND ----------

-- 分析結果を格納するスキーマを作成
CREATE CATALOG IF NOT EXISTS yunyi_catalog;
CREATE SCHEMA IF NOT EXISTS yunyi_catalog.lhf_demo
COMMENT '工場データ分析用スキーマ - Federationデータから生成した分析テーブルを格納';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.2b 機械ヘルスサマリーテーブルの作成
-- MAGIC
-- MAGIC 複数ソースをクロスソースJOINし、機械ごとの総合ヘルスサマリーを作成します。
-- MAGIC デプロイしたソースに応じてJOIN対象を調整してください。

-- COMMAND ----------

-- Glue + Redshift の基本JOIN (最小構成)
-- PostgreSQL / Synapse / BigQuery をデプロイした場合は、追加JOINを下のセルで実行
CREATE OR REPLACE TABLE yunyi_catalog.lhf_demo.machine_health_summary
COMMENT '機械別総合ヘルスサマリー - 複数Federationソースを統合して生成'
AS
WITH sensor_summary AS (
  SELECT
    r.machine_id,
    COUNT(CASE WHEN r.status = 'warning' THEN 1 END) AS sensor_warnings,
    COUNT(CASE WHEN r.status = 'critical' THEN 1 END) AS sensor_criticals
  FROM IDENTIFIER('${query_prefix}_redshift' || '.public.sensor_readings') r
  GROUP BY r.machine_id
),
event_summary AS (
  SELECT
    e.machine_id,
    COUNT(CASE WHEN e.event_type = 'error' THEN 1 END) AS error_count,
    SUM(CASE WHEN e.event_type = 'maintenance' THEN e.duration_minutes ELSE 0 END) AS maintenance_minutes
  FROM IDENTIFIER('${query_prefix}_redshift' || '.public.production_events') e
  GROUP BY e.machine_id
),
quality_all AS (
  SELECT machine_id, result, defect_count FROM IDENTIFIER('${catalog_prefix}_glue' || '.' || '${db_prefix}_factory_master' || '.quality_inspections')
  UNION ALL
  SELECT machine_id, result, defect_count FROM IDENTIFIER('${query_prefix}_redshift' || '.public.quality_inspections')
),
quality_agg AS (
  SELECT
    machine_id,
    COUNT(*) AS total_inspections,
    COUNT(CASE WHEN result = 'pass' THEN 1 END) AS passed_inspections,
    COUNT(CASE WHEN result = 'fail' THEN 1 END) AS failed_inspections,
    SUM(defect_count) AS total_defects
  FROM quality_all
  GROUP BY machine_id
)
SELECT
  m.machine_id,
  m.machine_name,
  m.production_line,
  m.factory,
  m.status AS machine_status,
  COALESCE(ss.sensor_warnings, 0) AS sensor_warning_count,
  COALESCE(ss.sensor_criticals, 0) AS sensor_critical_count,
  COALESCE(es.error_count, 0) AS error_event_count,
  COALESCE(es.maintenance_minutes, 0) AS total_maintenance_minutes,
  COALESCE(qa.total_inspections, 0) AS total_inspection_count,
  COALESCE(qa.passed_inspections, 0) AS passed_inspection_count,
  COALESCE(qa.failed_inspections, 0) AS failed_inspection_count,
  COALESCE(qa.total_defects, 0) AS total_defect_count,
  ROUND(COALESCE(qa.passed_inspections, 0) * 100.0 / NULLIF(qa.total_inspections, 0), 1) AS quality_pass_rate_pct
FROM IDENTIFIER('${catalog_prefix}_glue' || '.' || '${db_prefix}_factory_master' || '.machines') m
LEFT JOIN sensor_summary ss ON m.machine_id = ss.machine_id
LEFT JOIN event_summary es ON m.machine_id = es.machine_id
LEFT JOIN quality_agg qa ON m.machine_id = qa.machine_id;

-- COMMAND ----------

-- 作成したテーブルの確認
SELECT * FROM yunyi_catalog.lhf_demo.machine_health_summary
ORDER BY sensor_critical_count DESC, error_event_count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.2c 追加ソースのJOIN（デプロイ済みの場合）

-- COMMAND ----------

-- PostgreSQL: 保守履歴の統合
-- (enable_postgres = true の場合に実行)
SELECT
  m.machine_id,
  m.machine_name,
  COUNT(ml.log_id) AS maintenance_log_count,
  COUNT(wo.order_id) AS work_order_count,
  COUNT(CASE WHEN wo.status = 'open' THEN 1 END) AS open_work_orders
FROM IDENTIFIER('${catalog_prefix}_glue' || '.' || '${db_prefix}_factory_master' || '.machines') m
LEFT JOIN IDENTIFIER('${query_prefix}_postgres' || '.public.maintenance_logs') ml ON m.machine_id = ml.machine_id
LEFT JOIN IDENTIFIER('${query_prefix}_postgres' || '.public.work_orders') wo ON m.machine_id = wo.machine_id
GROUP BY m.machine_id, m.machine_name
ORDER BY open_work_orders DESC;

-- COMMAND ----------

-- Synapse: シフト・エネルギーの統合
-- (enable_synapse = true の場合に実行)
SELECT
  m.machine_id,
  m.machine_name,
  COUNT(DISTINCT ss.shift_id) AS total_shifts,
  ROUND(SUM(ec.kwh_consumed), 2) AS total_kwh,
  ROUND(SUM(ec.cost_usd), 2) AS total_energy_cost_usd
FROM IDENTIFIER('${catalog_prefix}_glue' || '.' || '${db_prefix}_factory_master' || '.machines') m
LEFT JOIN IDENTIFIER('${query_prefix}_synapse' || '.dbo.shift_schedules') ss ON m.machine_id = ss.machine_id
LEFT JOIN IDENTIFIER('${query_prefix}_synapse' || '.dbo.energy_consumption') ec ON m.machine_id = ec.machine_id
GROUP BY m.machine_id, m.machine_name
ORDER BY total_energy_cost_usd DESC;

-- COMMAND ----------

-- BigQuery: 稼働停止・コスト分析
-- (enable_bigquery = true の場合に実行)
SELECT
  m.machine_id,
  m.machine_name,
  COUNT(dr.record_id) AS downtime_incidents,
  ROUND(SUM(ca.amount_usd), 2) AS total_allocated_cost_usd
FROM IDENTIFIER('${catalog_prefix}_glue' || '.' || '${db_prefix}_factory_master' || '.machines') m
LEFT JOIN IDENTIFIER('${query_prefix}_bigquery' || '.' || '${db_prefix}_factory' || '.downtime_records') dr ON m.machine_id = dr.machine_id
LEFT JOIN IDENTIFIER('${query_prefix}_bigquery' || '.' || '${db_prefix}_factory' || '.cost_allocation') ca ON m.machine_id = ca.machine_id
GROUP BY m.machine_id, m.machine_name
ORDER BY downtime_incidents DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.2d リネージの確認（UIで実施）
-- MAGIC
-- MAGIC 作成した分析テーブルのリネージをCatalog Explorer UIで確認します。
-- MAGIC
-- MAGIC #### 手順
-- MAGIC
-- MAGIC 1. **Catalog Explorer** を開く（左サイドバー → Catalog）
-- MAGIC 2. `yunyi_catalog` → `lhf_demo` → `machine_health_summary` を選択
-- MAGIC 3. **Lineage** タブをクリック
-- MAGIC 4. 複数の外部ソース（Glue/Redshift等）からの依存関係がグラフに表示されることを確認
-- MAGIC
-- MAGIC > **ポイント**: Federation経由の外部テーブルからのデータの流れが、
-- MAGIC > Unity Catalogのリネージグラフで自動的に追跡されます。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC # 第3章: 統制の一元化（権限管理）
-- MAGIC
-- MAGIC ## 概要
-- MAGIC
-- MAGIC Unity Catalogは、すべてのデータ資産に対するアクセス制御を **一つの場所** で一元管理します。
-- MAGIC Federation経由の外部テーブルに対しても、同じ権限管理の仕組みが適用されます。
-- MAGIC
-- MAGIC ## 権限管理の主な特徴
-- MAGIC
-- MAGIC | 特徴 | 説明 |
-- MAGIC |------|------|
-- MAGIC | **階層型アクセス制御** | カタログ → スキーマ → テーブルの階層で権限を継承 |
-- MAGIC | **GRANT / REVOKE** | 標準SQLでユーザー・グループに権限を付与・取消 |
-- MAGIC | **データフィルタリング** | 行レベル・列レベルのアクセス制御が可能 |
-- MAGIC | **外部テーブルへの適用** | Federation経由の外部テーブルにも同じ権限モデルが適用される |
-- MAGIC | **監査ログ** | 誰が・いつ・何にアクセスしたかを自動記録 |
-- MAGIC
-- MAGIC ## デモ手順（UIで実施）
-- MAGIC
-- MAGIC ### 1. カタログレベルの権限確認
-- MAGIC 1. **Catalog Explorer** → Federationカタログを選択
-- MAGIC 2. **Permissions** タブで権限設定を確認
-- MAGIC
-- MAGIC ### 2. 権限付与の実演
-- MAGIC 以下のSQLで権限管理を実施できます。

-- COMMAND ----------

-- 権限管理の例（実行前にユーザー/グループ名を適切に変更してください）

-- カタログへのアクセス権付与
-- GRANT USE CATALOG ON CATALOG ${catalog_prefix}_glue TO `data_team`;
-- GRANT USE CATALOG ON CATALOG ${query_prefix}_redshift TO `data_team`;

-- スキーマへのアクセス権付与
-- GRANT USE SCHEMA ON SCHEMA ${catalog_prefix}_glue.${db_prefix}_factory_master TO `data_team`;

-- テーブルへの SELECT 権限付与
-- GRANT SELECT ON TABLE ${catalog_prefix}_glue.${db_prefix}_factory_master.sensors TO `analyst`;

-- 権限の取消
-- REVOKE SELECT ON TABLE ${catalog_prefix}_glue.${db_prefix}_factory_master.sensors FROM `analyst`;

-- COMMAND ----------

-- 現在の権限を確認
SHOW GRANTS ON CATALOG yunyi_catalog;

-- COMMAND ----------

SHOW GRANTS ON SCHEMA yunyi_catalog.lhf_demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC # 第4章: 生成AI活用（Genie）
-- MAGIC
-- MAGIC ## Genie Spaceとは
-- MAGIC
-- MAGIC **Genie** は、Databricksに搭載されたAI機能で、
-- MAGIC 自然言語でデータに対する質問を行い、自動的にSQLクエリを生成・実行します。
-- MAGIC
-- MAGIC ### Genie Spaceの作成手順（UIで実施）
-- MAGIC
-- MAGIC 1. **左サイドバー** → **Genie** をクリック
-- MAGIC 2. **「New」** をクリックしてGenie Spaceを新規作成
-- MAGIC 3. 以下の情報を設定:
-- MAGIC    - **タイトル**: `工場データ分析`
-- MAGIC    - **テーブル**: `yunyi_catalog.lhf_demo.machine_health_summary`
-- MAGIC 4. **保存** をクリック
-- MAGIC
-- MAGIC ### 質問例
-- MAGIC
-- MAGIC | 質問例 | 期待される分析 |
-- MAGIC |--------|---------------|
-- MAGIC | `一番異常が多い機械はどれですか？` | sensor_critical_count + error_event_countの集計 |
-- MAGIC | `製造ラインごとの品質合格率を教えて` | production_line別のquality_pass_rate_pct集計 |
-- MAGIC | `メンテナンス時間が最も長い機械は？` | total_maintenance_minutesの降順ランキング |
-- MAGIC | `A棟の機械の稼働状況を教えて` | factory = 'A棟' のフィルタリング |
-- MAGIC
-- MAGIC > **ポイント**: Genieはテーブルスキーマとカラムコメント（日本語設定済み）を参照して、
-- MAGIC > 適切なSQLを自動生成します。メタデータの品質がGenieの回答精度に直結します。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC # まとめ
-- MAGIC
-- MAGIC | 章 | テーマ | 機能 | 実演内容 |
-- MAGIC |----|--------|------|---------|
-- MAGIC | **第1章** | メタデータ統合 | Lakehouse Federation | 最大6ソース (Glue, Redshift, PostgreSQL, Synapse, BigQuery, OneLake) の統合 |
-- MAGIC | **第2章** | 資産の健全化 | Discover / Lineage | クロスソースJOINとリネージ可視化 |
-- MAGIC | **第3章** | 統制の一元化 | 権限管理 | カタログ・スキーマ・テーブル階層でのアクセス制御 |
-- MAGIC | **第4章** | 生成AI活用 | Genie | 自然言語によるデータ探索・分析 |
-- MAGIC
-- MAGIC **Unity Catalogによる統合データガバナンス**:
-- MAGIC - **メタデータ統合**: 最大6つの外部ソースをUnity Catalogで一元管理（データ移動不要）
-- MAGIC - **マルチクラウド**: AWS / Azure / GCP のデータソースに対応
-- MAGIC - **資産の健全化**: Discover/Lineage でデータ資産の全体像と依存関係を把握
-- MAGIC - **統制の一元化**: 外部テーブルを含めた一元的なアクセス制御
-- MAGIC - **生成AI活用**: メタデータを活かした自然言語データ探索
