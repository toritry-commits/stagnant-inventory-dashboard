#!/usr/bin/env python3
"""
BigQuery簿価計算結果をExcelに出力（イレギュラー修正対応版）

使用方法:
    1. overrides.json に修正内容を記載
    2. python export_with_overrides.py を実行

overrides.json の例:
{
  "期間": {
    "start_date": "2025-03-01",
    "end_date": "2025-11-30"
  },
  "修正": [
    {
      "在庫ID": 12345,
      "除売却日": "2025-10-15",
      "コメント": "売却日を10/15に修正"
    },
    {
      "在庫ID": 67890,
      "期末簿価": 50000,
      "コメント": "簿価を5万円に修正"
    }
  ]
}

修正可能なカラム:
  - 除売却日, 減損日, 貸手リース開始日
  - 期首簿価, 期末簿価, 取得原価
  - 期中減価償却費, 期中減損損失
  - その他の数値・日付カラム
"""

import sys
import os
import json
import subprocess
import tempfile
from pathlib import Path
from datetime import datetime

try:
    import pandas as pd
except ImportError:
    print("必要なパッケージをインストールしてください:")
    print("  pip install pandas openpyxl")
    sys.exit(1)


# 設定
SCRIPT_DIR = Path(__file__).parent
SQL_FILE = SCRIPT_DIR / "../sql/select_stock_valuation_summary.sql"
OVERRIDES_FILE = SCRIPT_DIR / "overrides.json"
BQ_CMD = r"C:\Users\User\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin\bq.cmd"


def load_overrides():
    """修正設定を読み込み"""
    if not OVERRIDES_FILE.exists():
        # デフォルト設定を作成
        default = {
            "期間": {
                "start_date": "2025-03-01",
                "end_date": "2025-11-30"
            },
            "修正": []
        }
        with open(OVERRIDES_FILE, "w", encoding="utf-8") as f:
            json.dump(default, f, ensure_ascii=False, indent=2)
        print(f"デフォルト設定ファイルを作成しました: {OVERRIDES_FILE}")
        return default

    with open(OVERRIDES_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def build_query(config):
    """期間設定を反映したSQLを生成"""
    with open(SQL_FILE, "r", encoding="utf-8") as f:
        base_sql = f.read()

    # 期間設定を置換
    period = config.get("期間", {})
    start_date = period.get("start_date", "2025-03-01")
    end_date = period.get("end_date", "2025-11-30")

    # WITH句の日付を置換
    sql = base_sql.replace(
        "DATE('2025-03-01') AS start_date",
        f"DATE('{start_date}') AS start_date"
    ).replace(
        "DATE('2025-11-30') AS end_date",
        f"DATE('{end_date}') AS end_date"
    )

    return sql


def apply_overrides(df, config):
    """修正をDataFrameに適用"""
    overrides = config.get("修正", [])

    if not overrides:
        print("修正なし")
        return df

    print(f"修正件数: {len(overrides)} 件")

    for override in overrides:
        stock_id = override.get("在庫ID")
        if stock_id is None:
            continue

        # 該当行を検索
        mask = df["在庫ID"] == stock_id
        if not mask.any():
            print(f"  警告: 在庫ID {stock_id} が見つかりません")
            continue

        # 各カラムを上書き
        for col, val in override.items():
            if col in ["在庫ID", "コメント"]:
                continue
            if col in df.columns:
                df.loc[mask, col] = val
                print(f"  在庫ID {stock_id}: {col} = {val}")
            else:
                print(f"  警告: カラム '{col}' が存在しません")

    return df


def run_bq_query(sql):
    """BigQueryでクエリ実行"""
    # 一時SQLファイルを作成
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False, encoding='utf-8') as tmp:
        tmp.write(sql)
        tmp_sql = tmp.name

    # 一時CSVファイル
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as tmp:
        tmp_csv = tmp.name

    try:
        cmd = f'"{BQ_CMD}" query --use_legacy_sql=false --format=csv --max_rows=1000000 < "{tmp_sql}" > "{tmp_csv}"'

        result = subprocess.run(cmd, capture_output=True, text=True, shell=True)

        if result.returncode != 0:
            print(f"エラー: bqコマンド失敗")
            print(result.stderr)
            return None

        # CSVを読み込み
        df = pd.read_csv(tmp_csv, encoding='utf-8')
        return df

    finally:
        for f in [tmp_sql, tmp_csv]:
            if os.path.exists(f):
                os.remove(f)


def main():
    print("=" * 60)
    print("簿価計算 Excel出力ツール（イレギュラー修正対応版）")
    print("=" * 60)

    # 設定読み込み
    print("\n1. 設定読み込み...")
    config = load_overrides()

    period = config.get("期間", {})
    print(f"   期間: {period.get('start_date')} 〜 {period.get('end_date')}")

    # SQL生成
    print("\n2. SQL生成...")
    sql = build_query(config)

    # BigQuery実行
    print("\n3. BigQuery実行中...")
    df = run_bq_query(sql)

    if df is None:
        print("クエリ実行に失敗しました")
        sys.exit(1)

    print(f"   取得件数: {len(df):,} 件")

    # 修正適用
    print("\n4. 修正適用...")
    df = apply_overrides(df, config)

    # Excel出力
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = SCRIPT_DIR / f"簿価計算_{timestamp}.xlsx"

    print(f"\n5. Excel出力: {output_file.name}")
    df.to_excel(output_file, index=False, engine="openpyxl")

    print(f"\n完了: {output_file}")
    print(f"ファイルサイズ: {os.path.getsize(output_file) / 1024:.1f} KB")

    return str(output_file)


if __name__ == "__main__":
    main()
