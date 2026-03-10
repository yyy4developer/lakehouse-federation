# Databricks Lakehouse Federation Demo

マルチクラウド対応の Lakehouse Federation デモ環境です。
AWS Glue、Amazon Redshift、PostgreSQL、Azure Synapse、Google BigQuery のデータに対して、
Databricks Unity Catalog から Federation でクエリを実行します。

## アーキテクチャ

```
┌──────────────────────────────────────────────────────────────────────┐
│                     Databricks Unity Catalog                        │
│                                                                      │
│  Catalog Federation            Query Federation                      │
│  ┌────────────────┐   ┌──────────────┐ ┌──────────────┐             │
│  │lhf_catalog_glue│   │lhf_query_    │ │lhf_query_    │             │
│  │   (AWS Glue)   │   │  redshift    │ │  postgres    │             │
│  └───────┬────────┘   └──────┬───────┘ └──────┬───────┘             │
│                        ┌──────────────┐ ┌──────────────┐             │
│                        │lhf_query_    │ │lhf_query_    │             │
│                        │  synapse     │ │  bigquery    │             │
│                        └──────┬───────┘ └──────┬───────┘             │
└──────────┼───────────────────┼─────────────────┼─────────────────────┘
           │                   │                 │
  ┌────────▼────────┐ ┌───────▼───────┐ ┌───────▼───────┐
  │  AWS Glue       │ │ Redshift /    │ │ BigQuery      │
  │  (S3 直接参照)  │ │ PostgreSQL /  │ │               │
  │                 │ │ Synapse (JDBC)│ │               │
  └─────────────────┘ └───────────────┘ └───────────────┘
```

## Federation ソース対応表

| ソース | Type | AWS Workspace | Azure Workspace |
|--------|------|:---:|:---:|
| AWS Glue | Catalog | O | - |
| Amazon Redshift | Query | O | O |
| PostgreSQL | Query | O (RDS) | O (Azure Flexible) |
| Azure Synapse | Query | O | O |
| Google BigQuery | Query | O | O |

## データテーマ: 工場生産管理

全テーブルが `machine_id` (1-10) を共通キーとして JOIN 可能です。

| ソース | テーブル | 行数 | 内容 |
|--------|---------|------|------|
| Glue | sensors, machines, quality_inspections | 20/10/50 | マスタ + 品質検査 |
| Redshift | sensor_readings, production_events, quality_inspections | 100/30/40 | トランザクション |
| PostgreSQL | machines, maintenance_logs, work_orders | 10/30/25 | 保守・作業管理 |
| Synapse | shift_schedules, energy_consumption | 40/50 | シフト・電力 |
| BigQuery | downtime_records, cost_allocation | 35/30 | 停止・コスト |

---

## クイックスタート

### 1. 前提条件 (CLI ツール)

| ツール | 必須 | インストール | 確認コマンド |
|--------|:----:|-------------|-------------|
| Terraform | Yes | `brew install terraform` | `terraform version` |
| uv | Yes | `curl -LsSf https://astral.sh/uv/install.sh \| sh` | `uv --version` |
| jq | Yes | `brew install jq` | `jq --version` |
| AWS CLI | AWS ソース使用時 | `brew install awscli` | `aws --version` |
| Azure CLI | Azure ソース使用時 | `brew install azure-cli` | `az --version` |
| gcloud CLI | BigQuery 使用時 | `brew install google-cloud-sdk` | `gcloud --version` |
| Databricks CLI | Yes | `brew install databricks` | `databricks --version` |
| psql | PostgreSQL 使用時 | `brew install libpq` | `psql --version` |
| sqlcmd | Synapse 使用時 | `brew install sqlcmd` | `sqlcmd --version` |

### 2. 事前準備

#### クラウド認証

sandbox 環境の使用を推奨します。

| Provider | 推奨アカウント | 認証コマンド |
|----------|--------------|------------|
| AWS | Profile: `aws-sandbox-field-eng_databricks-sandbox-admin` | `aws sso login --profile aws-sandbox-field-eng_databricks-sandbox-admin` |
| Azure | Subscription: `azure-sandbox-field-eng` (`edd4cc45-...`) | `az login` → `az account set -s azure-sandbox-field-eng` |
| GCP | Project: `gcp-sandbox-field-eng` | 下記参照 |
| Databricks | デプロイ時にブラウザ認証 (OAuth U2M) | 自動 |

#### GCP 認証 (BigQuery 使用時)

```bash
# 1. gcloud CLI でログイン (ブラウザが開く)
gcloud auth login

# 2. Application Default Credentials を設定 (Terraform が使用)
gcloud auth application-default login

# 3. プロジェクトを設定
gcloud config set project gcp-sandbox-field-eng
# ※ "lacks an 'environment' tag" という警告が出ますが、無視して OK です
#    最後に "Updated property [core/project]." と表示されれば成功です
```

> **Note**: `gcloud auth login` と `gcloud auth application-default login` は別の認証です。
> 前者は gcloud CLI 用、後者は Terraform/SDK 用の ADC (Application Default Credentials) です。
> BigQuery を使う場合は**両方**実行してください。

#### BigQuery: GCP サービスアカウント key JSON

BigQuery を使用する場合、Databricks の connection に SA key JSON が必須です。
デプロイスクリプトが **SA 作成 → 権限付与 → key 生成を自動実行** するため、事前準備は不要です。
(GCP SA key JSON path の入力で空白 Enter するだけで自動作成されます)

<details>
<summary>手動で SA key を作成する場合</summary>

```bash
gcloud iam service-accounts create lhf-demo --project=gcp-sandbox-field-eng --display-name="LHF Demo"
gcloud projects add-iam-policy-binding gcp-sandbox-field-eng \
  --member="serviceAccount:lhf-demo@gcp-sandbox-field-eng.iam.gserviceaccount.com" \
  --role="roles/bigquery.admin"
gcloud iam service-accounts keys create ~/gcp-sa-key.json \
  --iam-account=lhf-demo@gcp-sandbox-field-eng.iam.gserviceaccount.com
```
</details>

#### Databricks ワークスペース

デプロイ先の Databricks workspace URL が必要です (admin 権限が必要)。
- 既存の workspace を使用可能 (sandbox 環境を推奨)
- 新規作成する場合: go/fevm で作成 (sandbox を推奨)

### 3. デプロイ実行

```bash
./lakehouse_federation_demo_resource_deploy.sh
```

対話フローで以下を順に入力します:

| ステップ | 入力内容 | 備考 |
|---------|---------|------|
| Cloud | `aws` or `azure` | Databricks workspace のクラウド |
| Workspace URL | `https://xxx.cloud.databricks.com` | 必須 |
| Sources | チェックボックスで選択 | Glue は AWS のみ |
| Prefix | 自動生成 (`lhf_xxxx`) | そのまま Enter で OK。変更も可 |
| AWS profile | SSO profile 名 | 例: `aws-sandbox-field-eng_databricks-sandbox-admin` |
| Databricks auth | ブラウザが開く | OAuth U2M で自動認証 |
| Passwords | 各ソース用 | Redshift/PostgreSQL/Synapse 選択時のみ |
| Azure subscription ID | GUID | Azure ソース選択時 (`az login` 済みなら自動取得) |
| GCP project ID | 例: `gcp-sandbox-field-eng` | BigQuery 選択時のみ |
| GCP SA key JSON path | 空白 Enter で自動作成 | BigQuery 選択時 |

### 4. デプロイ結果確認

デプロイ完了後、`deploy_result.md` が生成されます。各リソースの URL、カタログツリー、接続テスト結果を確認できます。

### 5. デモ実行

1. `deploy_result.md` の Workspace URL を開く
2. **Demo Notebook (UI用 Widget版)** リンクで対話型ノートブックを開く
3. SQL Warehouse (Pro/Serverless) をアタッチ
4. "Run All" で実行 (上部に Widget が表示され、値を変更可能)

Job 用ノートブック (`federation_demo.sql`) はデプロイ時に自動実行済みです。

### 6. 再デプロイ / クリーンアップ

```bash
# 既存環境に再デプロイ (対話なし)
./lakehouse_federation_demo_resource_deploy.sh --redeploy

# 全リソース削除
./lakehouse_federation_demo_resource_deploy.sh --destroy
```

### 7. 手動デプロイ

対話スクリプトを使わず手動でデプロイする場合:

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# terraform.tfvars を編集
terraform init
terraform plan
terraform apply
```

---

## トラブルシューティング

### Azure 全般

#### RBAC (ロール割り当て) の伝播遅延

Azure のロール割り当てはリソース作成後すぐには反映されず、**60〜90秒の伝播遅延**があります。

**症状**: `terraform apply` 中に Storage Blob Data Contributor や Contributor の権限が足りないエラーが出る

**対策**:
- Terraform の `time_sleep` リソースで 90 秒待機を組み込み済み
- それでも失敗する場合: `terraform apply` を再実行すれば通る
- `time_sleep` は CREATE 時にのみ実行される。新しいロール割り当てを追加した場合は、`terraform taint 'time_sleep.role_propagation[0]'` で強制再実行が必要

#### Azure Cross-Tenant ワークスペース (Storage Credential エラー)

**症状**: `Registering a storage credential requires the contributor role over the corresponding access connector`

**原因**: Databricks ワークスペースが存在する Azure テナント/サブスクリプションと、リソースを作成するサブスクリプションのテナントが異なる場合、Access Connector の RBAC がテナントを跨げないため発生

**対策**:
- デプロイスクリプトが自動検出し、`use_workspace_default_storage = true` を設定
- 手動デプロイの場合: `terraform.tfvars` に以下を追加:
  ```hcl
  use_workspace_default_storage  = true
  workspace_default_storage_url  = "abfss://<container>@<account>.dfs.core.windows.net/<workspace_id>"
  ```
- ワークスペースのデフォルトストレージ URL は Databricks ワークスペースの Storage Credential 一覧から確認可能

### Azure Synapse

#### "Could not obtain exclusive lock on database 'model'" エラー

**症状**: Serverless SQL pool での `CREATE DATABASE` が model ロック競合で失敗

**対策**:
- 初回 60 秒待機 + 最大 12 回リトライを組み込み済み
- `sqlcmd` は exit 0 を返しても内部で失敗している場合がある → `SELECT COUNT(*) FROM sys.databases WHERE name = 'X'` で DB 存在を検証
- 失敗が続く場合: 数分待ってから `terraform apply` を再実行

#### ファイアウォールルール作成失敗

- Azure Policy が `0.0.0.0-255.255.255.255` を拒否する場合がある
- Terraform は自動的に2つの広範囲ルール（`1.0.0.0-126.x`, `128.0.0.0-254.x`）で回避

#### Serverless SQL pool の制限事項

- **INSERT 不可**: テーブル作成+INSERT ではなく、VIEW with VALUES を使用
- **認証**: SQL 認証ではなく AAD トークンが必要 (`--authentication-method=ActiveDirectoryDefault`)
- **エンドポイント**: Serverless は `-ondemand.sql.azuresynapse.net` を使用
- **sp_addextendedproperty**: Serverless では非対応 (非致命的エラー、無視して OK)

#### Databricks から "InvocationTargetException"

- ファイアウォールルールが Databricks の IP を許可しているか確認
- `trustServerCertificate = "true"` がコネクション設定に含まれているか確認
- 接続先が ondemand エンドポイントであることを確認

### AWS 全般

#### AWS SSO トークン期限切れ

- `aws sso login --profile <profile-name>` で再認証
- Azure デプロイ時でも AWS Provider が `GetCallerIdentity` を呼ぶため、AWS 認証が必要な場合がある

#### Glue: "should be given assume role permissions"

- IAM ロール作成直後は trust policy の反映に 15 秒程度かかる
- Terraform に自動リトライ (3回) を組み込み済み

#### Glue テーブルが Databricks から見えない

1. Lake Formation admin が正しく設定されているか確認
2. 各テーブルに `IAM_ALLOWED_PRINCIPALS` 権限があるか確認:
   ```bash
   aws lakeformation list-permissions --resource-type TABLE
   ```

#### Redshift: "Connection test failed"

- Redshift Serverless が AVAILABLE 状態か確認
- Security Group で 5439 ポートが開いているか確認

### PostgreSQL

#### macOS で `psql` が見つからない

- `brew install libpq` 後、PATH に追加が必要:
  ```bash
  export PATH="/opt/homebrew/opt/libpq/bin:$PATH"
  ```
- Terraform の init スクリプトは自動で PATH を検出済み

### BigQuery

#### SA の権限不足で terraform destroy が失敗

- サービスアカウントに `roles/bigquery.dataEditor` が必要（テーブル削除に必須）
- destroy が途中で失敗した場合: `bq rm -f --table <project>:<dataset>.<table>` で手動削除

### Databricks

#### OAuth トークン期限切れ

- `databricks auth login --host <workspace-url>` で再認証

#### Databricks Provider の `connection_name` ドリフト

- `databricks_catalog` リソースで plan 時に `connection_name` の差分が毎回表示される
- `lifecycle { ignore_changes = [connection_name] }` で対策済み

### Terraform 全般

#### `terraform apply` を再実行しても `time_sleep` が走らない

- `time_sleep` は CREATE 時にのみ実行される
- 新しいリソースを追加した場合:
  ```bash
  terraform taint 'time_sleep.role_propagation[0]'
  terraform apply
  ```

#### Synapse の "resource already exists" エラー

- Synapse ワークスペースを destroy → 再作成した直後に発生
- Azure の soft-delete 期間 (数分) を待つか、`project_prefix` を変更して新しいリソース名を使用

---

## ファイル構成

```
lakehouse_federation/
├── lakehouse_federation_demo_resource_deploy.sh  # ワンクリックデプロイ
├── databricks.yml                                 # DAB 設定
├── pyproject.toml                                 # Python 依存 (uv)
├── scripts/
│   ├── deploy.py                                  # 対話式デプロイスクリプト
│   └── prerequisites.sh                           # CLI チェック
├── notebooks/
│   ├── federation_demo_template.sql               # ノートブックテンプレート
│   ├── federation_demo_aws.sql                    # AWS 版 (テンプレートから生成)
│   ├── federation_demo_azure.sql                  # Azure 版 (テンプレートから生成)
│   └── federation_demo_interactive.sql            # 対話型 Widget 版
└── terraform/
    ├── main.tf                                    # Providers
    ├── variables.tf                               # 変数定義
    ├── outputs.tf                                 # 出力値
    ├── terraform.tfvars.example                   # 設定サンプル
    ├── aws_s3.tf, aws_iam.tf, aws_networking.tf   # AWS 基盤
    ├── aws_glue.tf, aws_glue_etl.tf               # Glue (Catalog Federation)
    ├── aws_lakeformation.tf                       # Lake Formation
    ├── aws_redshift.tf, aws_redshift_data.tf      # Redshift (Query Federation)
    ├── aws_rds_postgres.tf                        # PostgreSQL on AWS (RDS)
    ├── azure_resource_group.tf                    # Azure 共通
    ├── azure_catalog_storage.tf                   # Azure カタログストレージ
    ├── azure_synapse.tf                           # Synapse (Query Federation)
    ├── azure_postgres.tf                          # PostgreSQL on Azure (Flexible)
    ├── azure_onelake.tf                           # OneLake (未実装)
    ├── gcp_bigquery.tf                            # BigQuery (Query Federation)
    ├── databricks_credentials.tf                  # Storage/Service Credentials
    ├── databricks_connection.tf                   # Connections
    ├── databricks_catalog.tf                      # Foreign Catalogs + Union Catalog
    ├── databricks_external.tf                     # External Location (Glue)
    ├── scripts/generate_data.py                   # Glue ETL データ生成
    └── sql/                                       # DDL/DML (ソース別)
        ├── redshift/                              # Redshift SQL
        ├── postgres/                              # PostgreSQL SQL
        ├── synapse/                               # Synapse SQL
        └── bigquery/                              # BigQuery SQL
```

---

## TODO

- [ ] **OneLake (Microsoft Fabric) 対応**: Catalog Federation で Fabric Lakehouse をソースとして追加 (Azure workspace のみ)
- [ ] **deploy_result.md の精度向上**: カタログ/スキーマ構造をデプロイ後に Databricks API から動的取得して正確に反映
- [ ] **Snowflake Iceberg 対応の復活**: 以前の Snowflake Iceberg テーブル → Glue Catalog Federation を再実装
- [ ] **Azure workspace での Glue Catalog Federation**: クロスクラウド Catalog Federation の検証
- [ ] **CI/CD**: GitHub Actions で terraform validate / plan の自動チェック
- [ ] **デモノートブックの自動生成改善**: テンプレートからの生成でデプロイ済みソースのみのセクションを動的に含める
