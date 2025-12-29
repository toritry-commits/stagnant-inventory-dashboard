#!/usr/bin/env python3
"""
BigQueryクエリ結果をExcelファイルに出力するツール

使用方法:
    python export_to_excel.py <SQLファイルパス> [出力ファイルパス]

例:
    python export_to_excel.py ../sql/select_stock_valuation_summary.sql
    python export_to_excel.py ../sql/select_stock_valuation_summary.sql output.xlsx
"""

import sys
import os
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


def run_query_and_export(sql_file: str, output_file: str = None):
    """SQLファイルを実行してExcelに出力"""

    # SQLファイル読み込み
    sql_path = Path(sql_file)
    if not sql_path.exists():
        print(f"エラー: SQLファイルが見つかりません: {sql_file}")
        sys.exit(1)

    print(f"SQLファイル: {sql_path.name}")
    print("BigQueryでクエリ実行中...")

    # 一時CSVファイルを作成
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as tmp:
        tmp_csv = tmp.name

    try:
        # bqコマンドでCSV出力（ファイルから読み込み、ファイルへ出力）
        bq_cmd = r"C:\Users\User\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin\bq.cmd"

        # コマンドを文字列で構築
        cmd = f'"{bq_cmd}" query --use_legacy_sql=false --format=csv --max_rows=1000000 < "{sql_path.absolute()}" > "{tmp_csv}"'

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            shell=True
        )

        if result.returncode != 0:
            print(f"エラー: bqコマンド失敗 (code={result.returncode})")
            print(f"stderr: {result.stderr}")
            sys.exit(1)

        # CSVを読み込み
        df = pd.read_csv(tmp_csv, encoding='utf-8')

    finally:
        # 一時ファイル削除
        if os.path.exists(tmp_csv):
            os.remove(tmp_csv)

    print(f"取得件数: {len(df):,} 件")

    # 出力ファイル名の決定
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"{sql_path.stem}_{timestamp}.xlsx"

    # Excelに保存
    print(f"Excelファイル出力中: {output_file}")
    df.to_excel(output_file, index=False, engine="openpyxl")

    abs_path = Path(output_file).absolute()
    print(f"完了: {abs_path}")
    print(f"ファイルサイズ: {os.path.getsize(output_file) / 1024:.1f} KB")

    return str(abs_path)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    sql_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None

    run_query_and_export(sql_file, output_file)
