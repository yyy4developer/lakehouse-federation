"""Cloud authentication and credential collection."""

from __future__ import annotations

import json
import os
import random
import re
import string
import subprocess
import sys
from pathlib import Path

import questionary

from .config import SOURCES, console, random_suffix


class AuthManager:
    def check_cloud_auth(self, cloud: str, sources: list[str]) -> dict:
        console.print("\n[bold]認証チェック...[/bold]")
        creds: dict = {}
        self._check_aws(cloud, sources)
        self._check_azure(sources, creds)
        self._check_gcp(sources, creds)
        console.print()
        return creds

    def ensure_aws_auth(self, sources: list[str]) -> None:
        needs_aws = any(s in sources for s in ["glue", "redshift", "postgres"])
        if not needs_aws:
            return

        if self._aws_auth_ok():
            profile = os.environ.get("AWS_PROFILE", "default")
            console.print(f"  [green]✓[/green] AWS 認証済み (profile: {profile})")
            return

        profile = os.environ.get("AWS_PROFILE", "") or self._discover_aws_profile()
        if profile:
            os.environ["AWS_PROFILE"] = profile
            if self._aws_auth_ok():
                console.print(f"  [green]✓[/green] AWS 認証済み (profile: {profile})")
                return
            console.print(f"  SSO ログイン実行中... (profile: {profile})")
            subprocess.run(["aws", "sso", "login", "--profile", profile], timeout=120)
            if self._aws_auth_ok():
                console.print(f"  [green]✓[/green] AWS 認証済み (profile: {profile})")
                return

        console.print("[red]✗ AWS 認証失敗。`aws sso login` を実行してから再試行してください。[/red]")
        sys.exit(1)

    def ensure_azure_auth(self, sources: list[str]) -> None:
        if not any(s in sources for s in ["synapse", "onelake"]):
            return
        try:
            result = subprocess.run(
                ["az", "account", "show", "--query", "id", "-o", "tsv"],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0 and result.stdout.strip():
                console.print(f"  [green]✓[/green] Azure 認証済み (subscription: {result.stdout.strip()})")
            else:
                console.print("[red]✗ Azure 未認証。`az login` を実行してから再試行してください。[/red]")
                sys.exit(1)
        except FileNotFoundError:
            console.print("[yellow]! az CLI が見つかりません[/yellow]")

    @staticmethod
    def _aws_auth_ok() -> bool:
        try:
            result = subprocess.run(
                ["aws", "sts", "get-caller-identity"],
                capture_output=True, text=True, timeout=10,
            )
            return result.returncode == 0 and "ExpiredToken" not in result.stderr
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return False

    @staticmethod
    def _discover_aws_profile() -> str:
        try:
            result = subprocess.run(
                ["aws", "configure", "list-profiles"],
                capture_output=True, text=True, timeout=5,
            )
            profiles = [p.strip() for p in result.stdout.splitlines()]
            for p in profiles:
                if "sandbox-field-eng" in p and "sandbox-admin" in p:
                    return p
            for p in profiles:
                if "sandbox-field-eng" in p and ("admin" in p or "power-user" in p):
                    return p
        except Exception:
            pass
        return ""

    def _check_aws(self, cloud: str, sources: list[str]) -> None:
        needs_aws = cloud == "aws" or any(s in sources for s in ["glue", "redshift", "snowflake_iceberg"])
        if not (needs_aws or ("postgres" in sources and cloud == "aws")):
            return
        try:
            if self._aws_auth_ok():
                profile = os.environ.get("AWS_PROFILE", "default")
                console.print(f"  [green]✓[/green] AWS 認証済み (profile: {profile})")
            else:
                console.print("  [yellow]![/yellow] AWS 未認証またはトークン期限切れ")
                sso_profiles = self._find_sso_profiles()
                default_profile = sso_profiles[0] if sso_profiles else ""
                profile = questionary.text("AWS SSO profile name:", default=default_profile).ask()

                if profile:
                    os.environ["AWS_PROFILE"] = profile
                    if self._aws_auth_ok():
                        console.print(f"  [green]✓[/green] AWS 認証済み (profile: {profile})")
                    else:
                        console.print(f"  [yellow]SSO ログインが必要です...[/yellow]")
                        result = subprocess.run(["aws", "sso", "login", "--profile", profile], timeout=120)
                        if result.returncode != 0 or not self._aws_auth_ok():
                            console.print("  [red]✗[/red] AWS SSO login 失敗")
                            if not questionary.confirm("続行しますか?", default=False).ask():
                                sys.exit(1)
                        else:
                            console.print(f"  [green]✓[/green] AWS 認証済み (profile: {profile})")
                else:
                    console.print("  [red]✗[/red] AWS 未認証 — [yellow]aws configure[/yellow] を実行してください")
                    if not questionary.confirm("続行しますか?", default=False).ask():
                        sys.exit(1)
        except FileNotFoundError:
            console.print("  [yellow]![/yellow] aws CLI が見つかりません")

    @staticmethod
    def _find_sso_profiles() -> list[str]:
        try:
            result = subprocess.run(
                ["aws", "configure", "list-profiles"],
                capture_output=True, text=True, timeout=5,
            )
            all_profiles = [p.strip() for p in result.stdout.splitlines()]
            for pattern in [
                lambda p: "sandbox-field-eng" in p and "sandbox-admin" in p,
                lambda p: "sandbox-field-eng" in p,
                lambda p: "sandbox" in p.lower(),
            ]:
                matches = [p for p in all_profiles if pattern(p)]
                if matches:
                    return matches
        except Exception:
            pass
        return []

    def _check_azure(self, sources: list[str], creds: dict) -> None:
        if not any(s in sources for s in ["synapse", "onelake"]):
            return
        try:
            result = subprocess.run(
                ["az", "account", "show", "--query", "id", "-o", "tsv"],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0 and result.stdout.strip():
                sub_id = result.stdout.strip()
                console.print(f"  [green]✓[/green] Azure 認証済み (subscription: {sub_id})")
                creds["azure_subscription_id"] = sub_id
            else:
                console.print("  [red]✗[/red] Azure 未認証 — [yellow]az login[/yellow] を実行してください")
                if not questionary.confirm("続行しますか?", default=False).ask():
                    sys.exit(1)
        except FileNotFoundError:
            console.print("  [yellow]![/yellow] az CLI が見つかりません")

    @staticmethod
    def _check_gcp(sources: list[str], creds: dict) -> None:
        if "bigquery" not in sources:
            return
        try:
            result = subprocess.run(
                ["gcloud", "auth", "list", "--filter=status:ACTIVE", "--format=value(account)"],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0 and result.stdout.strip():
                console.print(f"  [green]✓[/green] GCP 認証済み ({result.stdout.strip()})")
                proj = subprocess.run(
                    ["gcloud", "config", "get-value", "project"],
                    capture_output=True, text=True, timeout=10,
                )
                if proj.returncode == 0 and proj.stdout.strip():
                    creds["gcp_project_id"] = proj.stdout.strip()
                    console.print(f"  [green]✓[/green] GCP project: {proj.stdout.strip()}")
            else:
                console.print("  [red]✗[/red] GCP 未認証 — [yellow]gcloud auth application-default login[/yellow] を実行してください")
                if not questionary.confirm("続行しますか?", default=False).ask():
                    sys.exit(1)
        except FileNotFoundError:
            console.print("  [yellow]![/yellow] gcloud CLI が見つかりません")


class CredentialCollector:
    def collect(self, cloud: str, sources: list[str], auto_creds: dict | None = None) -> dict:
        creds = dict(auto_creds or {})
        default_pw = self._generate_password()

        if "redshift" in sources:
            console.print(f"  [dim]デフォルト password: {default_pw}[/dim]")
            pw = questionary.password("Redshift admin password (空白 Enter = 自動生成):").ask()
            creds["redshift_admin_password"] = pw if pw else default_pw

        if "postgres" in sources:
            if "redshift_admin_password" not in creds:
                console.print(f"  [dim]デフォルト password: {default_pw}[/dim]")
            pw = questionary.password("PostgreSQL admin password (空白 Enter = 自動生成):").ask()
            creds["postgres_admin_password"] = pw if pw else default_pw

        if "synapse" in sources:
            if "redshift_admin_password" not in creds and "postgres_admin_password" not in creds:
                console.print(f"  [dim]デフォルト password: {default_pw}[/dim]")
            pw = questionary.password("Azure Synapse admin password (空白 Enter = 自動生成):").ask()
            creds["synapse_admin_password"] = pw if pw else default_pw
            if "azure_subscription_id" not in creds:
                creds["azure_subscription_id"] = questionary.text(
                    "Azure subscription ID:", validate=lambda x: len(x) > 0 or "必須です",
                ).ask()

        if "onelake" in sources:
            if "azure_subscription_id" not in creds:
                creds["azure_subscription_id"] = questionary.text(
                    "Azure subscription ID:", validate=lambda x: len(x) > 0 or "必須です",
                ).ask()
            creds["fabric_workspace_id"] = questionary.text(
                "Fabric workspace ID (GUID):", validate=lambda x: len(x) > 0 or "必須です",
            ).ask()

        if cloud == "azure" and "postgres" in sources:
            if "azure_subscription_id" not in creds:
                creds["azure_subscription_id"] = questionary.text(
                    "Azure subscription ID:", validate=lambda x: len(x) > 0 or "必須です",
                ).ask()

        if "snowflake" in sources or "snowflake_iceberg" in sources:
            self._collect_snowflake(creds, sources)

        if "bigquery" in sources:
            self._collect_bigquery(creds)

        return creds

    def _collect_snowflake(self, creds: dict, sources: list[str]) -> None:
        console.print("\n[bold]Snowflake 設定[/bold]")
        console.print("  [dim]Snowflake Trial: https://signup.snowflake.com (無料)[/dim]")

        raw_url = questionary.text(
            "Snowflake URL (Account URL または Web UI URL):",
            validate=lambda x: len(x) > 0 or "必須です",
        ).ask()

        raw_url = raw_url.strip().rstrip("/").rstrip("#").rstrip("/")
        if "app.snowflake.com/" in raw_url:
            m = re.search(r"app\.snowflake\.com/([^/]+)/([^/#]+)", raw_url)
            if m:
                org, acct = m.group(1), m.group(2)
                raw_url = f"https://{org}-{acct}.snowflakecomputing.com"
                console.print(f"  [dim]Account URL に変換: {raw_url}[/dim]")
        if not raw_url.startswith("https://"):
            raw_url = f"https://{raw_url}"
        creds["snowflake_account_url"] = raw_url

        creds["snowflake_user"] = questionary.text(
            "Snowflake ユーザー名:", validate=lambda x: len(x) > 0 or "必須です",
        ).ask()
        creds["snowflake_password"] = questionary.password(
            "Snowflake password:", validate=lambda x: len(x) > 0 or "パスワードは必須です",
        ).ask()
        wh = questionary.text("Snowflake warehouse (デフォルト: COMPUTE_WH):", default="COMPUTE_WH").ask()
        creds["snowflake_warehouse"] = wh

        self._validate_snowflake(creds)

        if "snowflake_iceberg" in sources and "glue" not in sources:
            console.print("[yellow]⚠ Snowflake Iceberg は AWS Glue が必要です。Glue を自動有効化します。[/yellow]")
            sources.append("glue")

    @staticmethod
    def _validate_snowflake(creds: dict) -> None:
        console.print("  Snowflake 接続テスト中...")
        try:
            import snowflake.connector
            account = creds["snowflake_account_url"].replace("https://", "").replace(".snowflakecomputing.com", "").strip("/")
            conn = snowflake.connector.connect(
                account=account,
                user=creds["snowflake_user"],
                password=creds["snowflake_password"],
                warehouse=creds["snowflake_warehouse"],
                role="ACCOUNTADMIN",
                login_timeout=15,
            )
            cur = conn.cursor()
            cur.execute("SELECT CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_WAREHOUSE()")
            row = cur.fetchone()
            console.print(f"  [green]✓[/green] Snowflake 接続成功 (account={row[0]}, user={row[1]}, warehouse={row[2]})")
            cur.close()
            conn.close()
        except Exception as e:
            console.print(f"  [red]✗ Snowflake 接続失敗: {e}[/red]")
            if not questionary.confirm("続行しますか?", default=False).ask():
                sys.exit(1)

    @staticmethod
    def _collect_bigquery(creds: dict) -> None:
        default_project = creds.get("gcp_project_id", "")
        creds["gcp_project_id"] = questionary.text(
            "GCP project ID:", default=default_project,
            validate=lambda x: len(x) > 0 or "必須です",
        ).ask()
        key_path_input = questionary.text("GCP SA key JSON path (空白 = 自動作成):", default="").ask()
        if key_path_input:
            key_path = Path(key_path_input).expanduser()
            if key_path.exists():
                creds["gcp_credentials_json"] = key_path.read_text()
            else:
                console.print(f"[red]File not found: {key_path}[/red]")
                sys.exit(1)
        else:
            creds["gcp_credentials_json"] = _create_gcp_sa_key(creds["gcp_project_id"])

    @staticmethod
    def _generate_password() -> str:
        suffix = random_suffix(6)
        if not any(c.isdigit() for c in suffix):
            suffix = suffix[:5] + random.choice(string.digits)
        return f"LhfDemo#{suffix}"


def _create_gcp_sa_key(project_id: str) -> str:
    sa_name = "lhf-demo"
    sa_email = f"{sa_name}@{project_id}.iam.gserviceaccount.com"
    key_path = Path.home() / f"gcp-sa-key-{project_id}.json"

    check = subprocess.run(
        ["gcloud", "iam", "service-accounts", "describe", sa_email, f"--project={project_id}"],
        capture_output=True, text=True, timeout=15,
    )
    if check.returncode != 0:
        console.print(f"  SA 作成中: {sa_email}")
        result = subprocess.run(
            ["gcloud", "iam", "service-accounts", "create", sa_name,
             f"--project={project_id}", "--display-name=LHF Demo"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            console.print(f"[red]SA 作成失敗: {result.stderr}[/red]")
            sys.exit(1)
    else:
        console.print(f"  SA 既存: {sa_email}")

    console.print("  BigQuery 権限付与中...")
    subprocess.run(
        ["gcloud", "projects", "add-iam-policy-binding", project_id,
         f"--member=serviceAccount:{sa_email}",
         "--role=roles/bigquery.admin",
         "--condition=None", "--quiet"],
        capture_output=True, text=True, timeout=30,
    )

    if key_path.exists():
        try:
            key_data = json.loads(key_path.read_text())
            if key_data.get("project_id") == project_id:
                console.print(f"  [green]✓[/green] 既存 key 使用: {key_path}")
                return key_path.read_text()
        except (json.JSONDecodeError, KeyError):
            pass

    console.print(f"  key JSON 生成中: {key_path}")
    result = subprocess.run(
        ["gcloud", "iam", "service-accounts", "keys", "create", str(key_path),
         f"--iam-account={sa_email}"],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        console.print(f"[red]key 生成失敗: {result.stderr}[/red]")
        sys.exit(1)

    console.print(f"  [green]✓[/green] SA key 自動作成完了: {key_path}")
    return key_path.read_text()
