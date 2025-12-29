"""
簿価計算ロジック（Python版）

このファイルは、BigQuery SQLに移植するための参照用Pythonコードです。
実際の簿価計算ロジックをここに記述してください。

【記入ガイド】
1. 関数名・変数名は意味が分かるように命名
2. 計算ロジックの各ステップにコメントを記載
3. 使用するデータ項目（テーブル・カラム）を明記
4. 計算例を関数のdocstringに記載

【SQLへの移植方針】
- Pythonの関数 → BigQueryのCTE
- forループ → GROUP BY / WINDOW関数
- if文 → CASE WHEN
- リスト操作 → ARRAY関数
"""

# ======================================
# ここに簿価計算のPythonコードを記述
# ======================================

import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import pandas as pd
import numpy as np
from datetime import date
from collections import OrderedDict
import re
import os
import threading
import time

# --- グローバル定数定義 ---
SUPPLIER_COLUMN_NAME = 'サプライヤー名'
SAMPLE_COLUMN_NAME = 'sample'
INSPECTED_AT_COL = '入庫検品完了日'
LEASE_REACQUISITION_DATE_COL = 'リース再取得日'
IMPOSSIBLED_AT_COL = '除売却日'
LEASE_FIRST_SHIPPED_AT_COL = '貸手リース開始日'
COST_COLUMN_NAME = '取得原価'
IMPAIRMENT_DATE_COL = '減損損失日'
CLASSIFICATION_OF_IMPOSSIBILITY_COL = '破損紛失分類'
ASSET_CLASSIFICATION_COLUMN_NAME = '資産分類'
ACCOUNTING_STATUS_COLUMN_NAME = '会計ステータス'
FIRST_SHIPPED_AT_CALCULATED_COL = '_計算用初回出荷日'
DISPLAY_NAME_FIRST_SHIPPED_AT = '供与開始日(初回出荷日)'
ACQUISITION_COST_KISHU_COL = '期首取得原価'
ACQUISITION_COST_INCREASE_COL = '増加取得原価'
ACQUISITION_COST_DECREASE_COL = '減少取得原価'
ACQUISITION_COST_KIMATSU_COL = '期末取得原価'
SHOKYAKU_ALPHA_COL = '償却α'
SHOKYAKU_BETA_COL = '償却β'
SHOKYAKU_GAMMA_COL = '償却γ'
AMORTIZATION_MONTHS_KISHU_COL = '期首償却月数'
AMORTIZATION_MONTHS_SHOKYAKU_COL = '償却償却月数'
AMORTIZATION_MONTHS_INCREASE_COL = '増加償却月数'
AMORTIZATION_MONTHS_DECREASE_COL = '減少償却月数'
AMORTIZATION_MONTHS_KIMATSU_COL = '期末償却月数'
MONTHLY_DEPRECIATION_COL = '月次償却額'
ACCUMULATED_DEPRECIATION_KISHU_COL = '期首減価償却累計額'
ACCUMULATED_DEPRECIATION_INCREASE_COL = '増加減価償却累計額'
ACCUMULATED_DEPRECIATION_DECREASE_COL = '減少減価償却累計額'
ACCUMULATED_DEPRECIATION_KIMATSU_COL = '期末減価償却累計額'
IMPAIRMENT_LOSS_ACCUMULATED_KISHU_COL = '期首減損損失累計額'
IMPAIRMENT_LOSS_ACCUMULATED_INCREASE_COL = '増加減損損失累計額'
IMPAIRMENT_LOSS_ACCUMULATED_DECREASE_COL = '減少減損損失累計額'
IMPAIRMENT_LOSS_ACCUMULATED_KIMATSU_COL = '期末減損損失累計額'
INTERIM_DEPRECIATION_EXPENSE_COL = '期中減価償却費'
INTERIM_IMPAIRMENT_LOSS_COL = '期中減損損失'
OPENING_BOOK_VALUE_COL = '期首簿価'
INCREASE_BOOK_VALUE_COL = '増加簿価'
DECREASE_BOOK_VALUE_COL = '減少簿価'
CLOSING_BOOK_VALUE_COL = '期末簿価'

# --- 計算関数定義（従来のロジックを維持） ---
def calculate_months_diff(date1, date2):
    if pd.isna(date1) or pd.isna(date2): return 0
    return (date1.year - date2.year) * 12 + date1.month - date2.month

def calculate_shokyaku_alpha(row, start_date_param):
    first_shipped_at = row.get(DISPLAY_NAME_FIRST_SHIPPED_AT, pd.NaT)
    impairment_date = row.get(IMPAIRMENT_DATE_COL, pd.NaT)
    impossibled_at = row.get(IMPOSSIBLED_AT_COL, pd.NaT)
    lease_first_shipped_at_col_val = row.get(LEASE_FIRST_SHIPPED_AT_COL, pd.NaT)
    classification_of_impossibility = row.get(CLASSIFICATION_OF_IMPOSSIBILITY_COL, None)
    if pd.isna(first_shipped_at) or first_shipped_at >= start_date_param: return 0
    if pd.notna(impairment_date) and impairment_date < start_date_param and \
            ((pd.isna(impossibled_at) and pd.isna(lease_first_shipped_at_col_val)) or
             (pd.notna(impossibled_at) and impossibled_at > impairment_date) or
             (pd.notna(lease_first_shipped_at_col_val) and lease_first_shipped_at_col_val > impairment_date)):
        return max(calculate_months_diff(impairment_date, first_shipped_at) + 1, 0)
    if pd.notna(lease_first_shipped_at_col_val) and lease_first_shipped_at_col_val < start_date_param:
         return max(calculate_months_diff(lease_first_shipped_at_col_val, first_shipped_at), 0)
    if pd.notna(impossibled_at) and impossibled_at < start_date_param:
        if classification_of_impossibility == "庫内紛失／棚卸差異":
             return max(calculate_months_diff(impossibled_at, first_shipped_at) + 1, 0)
        return max(calculate_months_diff(impossibled_at, first_shipped_at), 0)
    return calculate_months_diff(start_date_param, first_shipped_at)

def calculate_shokyaku_beta(row, end_date_param):
    first_shipped_at = row.get(DISPLAY_NAME_FIRST_SHIPPED_AT, pd.NaT)
    impairment_date = row.get(IMPAIRMENT_DATE_COL, pd.NaT)
    impossibled_at = row.get(IMPOSSIBLED_AT_COL, pd.NaT)
    lease_first_shipped_at_col_val = row.get(LEASE_FIRST_SHIPPED_AT_COL, pd.NaT)
    classification_of_impossibility = row.get(CLASSIFICATION_OF_IMPOSSIBILITY_COL, None)
    if pd.isna(first_shipped_at) or first_shipped_at > end_date_param: return 0
    effective_end_date = end_date_param
    if pd.notna(impairment_date) and impairment_date <= end_date_param and \
            ((pd.isna(impossibled_at) and pd.isna(lease_first_shipped_at_col_val)) or
             (pd.notna(impossibled_at) and impossibled_at > impairment_date) or
             (pd.notna(lease_first_shipped_at_col_val) and lease_first_shipped_at_col_val > impairment_date)):
        effective_end_date = min(effective_end_date, impairment_date)
        return max(calculate_months_diff(effective_end_date, first_shipped_at) + 1, 0)
    if pd.notna(lease_first_shipped_at_col_val) and lease_first_shipped_at_col_val <= end_date_param:
        effective_end_date = min(effective_end_date, lease_first_shipped_at_col_val)
        return max(calculate_months_diff(effective_end_date, first_shipped_at), 0)
    if pd.notna(impossibled_at) and impossibled_at <= end_date_param:
        effective_end_date = min(effective_end_date, impossibled_at)
        if classification_of_impossibility == "庫内紛失／棚卸差異":
            return max(calculate_months_diff(effective_end_date, first_shipped_at) + 1, 0)
        return max(calculate_months_diff(effective_end_date, first_shipped_at), 0)
    return calculate_months_diff(effective_end_date, first_shipped_at) + 1

def calculate_shokyaku_gamma(row):
    lease_reacquisition_date = row.get(LEASE_REACQUISITION_DATE_COL, pd.NaT)
    first_shipped_at = row.get(DISPLAY_NAME_FIRST_SHIPPED_AT, pd.NaT)
    impairment_date = row.get(IMPAIRMENT_DATE_COL, pd.NaT)
    impossibled_at = row.get(IMPOSSIBLED_AT_COL, pd.NaT)
    lease_first_shipped_at_col_val = row.get(LEASE_FIRST_SHIPPED_AT_COL, pd.NaT)
    classification_of_impossibility = row.get(CLASSIFICATION_OF_IMPOSSIBILITY_COL, None)
    if pd.isna(lease_reacquisition_date): return 0
    lease_reacquisition_date_month_start = lease_reacquisition_date.replace(day=1) if pd.notna(lease_reacquisition_date) else pd.NaT
    if pd.isna(lease_reacquisition_date_month_start): return 0
    if pd.notna(impairment_date) and impairment_date < lease_reacquisition_date_month_start and \
            ((pd.isna(impossibled_at) and pd.isna(lease_first_shipped_at_col_val)) or \
             (pd.notna(impossibled_at) and impossibled_at > impairment_date) or \
             (pd.notna(lease_first_shipped_at_col_val) and lease_first_shipped_at_col_val > impairment_date)):
        return max(calculate_months_diff(impairment_date, first_shipped_at) + 1, 0)
    if pd.notna(lease_first_shipped_at_col_val) and lease_first_shipped_at_col_val < lease_reacquisition_date_month_start:
        return max(calculate_months_diff(lease_first_shipped_at_col_val, first_shipped_at), 0)
    if pd.notna(impossibled_at) and impossibled_at < lease_reacquisition_date_month_start:
        if classification_of_impossibility == "庫内紛失／棚卸差異":
            return max(calculate_months_diff(impossibled_at, first_shipped_at) + 1, 0)
        return max(calculate_months_diff(impossibled_at, first_shipped_at), 0)
    if pd.notna(first_shipped_at):
        return calculate_months_diff(lease_reacquisition_date, first_shipped_at)
    return 0

def classify_asset(row):
    supplier_name_val = row.get(SUPPLIER_COLUMN_NAME, None)
    sample_val = row.get(SAMPLE_COLUMN_NAME, False)
    if sample_val is True: return "サンプル品"
    if pd.isna(supplier_name_val): return "賃貸用固定資産"
    supplier_name_str = str(supplier_name_val)
    if "リース・レベシェア" in supplier_name_str: return "リース資産(借手リース)"
    elif "レベシェア" in supplier_name_str: return "レベシェア品"
    elif "法人小物管理用" in supplier_name_str: return "小物等"
    else: return "賃貸用固定資産"

def determine_accounting_status(row, end_date_param):
    asset_class = row.get(ASSET_CLASSIFICATION_COLUMN_NAME, None)
    inspected_at_val = row.get(INSPECTED_AT_COL, pd.NaT)
    impossibled_at_val = row.get(IMPOSSIBLED_AT_COL, pd.NaT)
    classification_val = row.get(CLASSIFICATION_OF_IMPOSSIBILITY_COL, None)
    lease_specific_shipped_at_val = row.get(LEASE_FIRST_SHIPPED_AT_COL, pd.NaT)
    if asset_class == "レベシェア品": return "計上外(レベシェア)"
    if asset_class == "小物等": return "仕入高(小物等)"
    if asset_class == "サンプル品": return "研究開発費(サンプル品)"
    if pd.isna(inspected_at_val) or (pd.notna(inspected_at_val) and inspected_at_val > end_date_param):
        return "計上外(入庫検品前)"
    if pd.notna(impossibled_at_val) and impossibled_at_val <= end_date_param:
        if pd.notna(classification_val):
            if classification_val in ["売却（顧客）", "売却（法人案件）", "売却（EC）"]: return "仕入高(売却)"
            if classification_val in ["貸倒/所有権放棄", "貸倒"]: return "雑費(除却)"
        return "家具廃棄損(除却)"
    if pd.notna(lease_specific_shipped_at_val) and lease_specific_shipped_at_val <= end_date_param:
        return "リース債権(貸手リース)"
    return asset_class

def calculate_lease_reacquisition_date(supplier_name_val):
    if pd.isna(supplier_name_val): return pd.NaT
    supplier_name_str = str(supplier_name_val)
    match_kanmu = re.search(r"^株式会社カンム 契約No\.2022000(\d)$", supplier_name_str)
    if match_kanmu:
        try:
            val = pd.to_datetime(f"2024-{match_kanmu.group(1)}-01", errors='coerce')
            return val.normalize() if pd.notna(val) else pd.NaT
        except ValueError: return pd.NaT
    if supplier_name_str.startswith("三井住友トラスト・パナソニックファイナンス株式会社(リースバック品)_契約開始"):
        match_code = re.search(r'_契約開始(\d{4})$', supplier_name_str)
        if match_code:
            contract_code = match_code.group(1)
            if len(contract_code) == 4:
                try:
                    base_date = pd.to_datetime(f"20{contract_code[:2]}-{contract_code[2:]}-01", errors='coerce')
                    if pd.isna(base_date): return pd.NaT
                    return (base_date + pd.DateOffset(months=30) + pd.offsets.MonthEnd(0))
                except ValueError: return pd.NaT
    return pd.NaT

def calculate_first_shipped_at_calculated(row):
    supplier_name = row.get(SUPPLIER_COLUMN_NAME)
    first_shipped_date_from_csv_col = row.get(DISPLAY_NAME_FIRST_SHIPPED_AT, pd.NaT)
    lease_first_shipped_at_from_csv_col = row.get(LEASE_FIRST_SHIPPED_AT_COL, pd.NaT)
    default_ship_date = pd.NaT
    if pd.notna(first_shipped_date_from_csv_col):
        default_ship_date = first_shipped_date_from_csv_col
    elif pd.notna(lease_first_shipped_at_from_csv_col):
        default_ship_date = lease_first_shipped_at_from_csv_col
    if pd.notna(supplier_name):
        supplier_name_str = str(supplier_name)
        kanmu_match = re.search(r"^株式会社カンム 契約No\.2022000(\d)$", supplier_name_str)
        if kanmu_match:
            try:
                val = pd.to_datetime(f"2022-{int(kanmu_match.group(1)):02d}-01", errors='coerce')
                return val.normalize() if pd.notna(val) else pd.NaT
            except ValueError: return pd.NaT
        smbc_match = re.search(r"三井住友トラスト・パナソニックファイナンス株式会社\(リースバック品\)_契約開始(\d{4})$", supplier_name_str)
        if smbc_match:
            contract_code = smbc_match.group(1)
            if len(contract_code) == 4:
                try:
                    base_date = pd.to_datetime(f"20{contract_code[:2]}-{contract_code[2:]}-01", errors='coerce')
                    if pd.isna(base_date): return pd.NaT
                    return (base_date + pd.offsets.MonthEnd(0))
                except ValueError: return pd.NaT
    return default_ship_date

def calculate_initial_cost(row, start_date_param):
    supplier_name = str(row.get(SUPPLIER_COLUMN_NAME, ""))
    sample = row.get(SAMPLE_COLUMN_NAME, False)
    inspected_at = row.get(INSPECTED_AT_COL, pd.NaT)
    lease_reacquisition_date = row.get(LEASE_REACQUISITION_DATE_COL, pd.NaT)
    impossibled_at = row.get(IMPOSSIBLED_AT_COL, pd.NaT)
    lease_first_shipped_at = row.get(LEASE_FIRST_SHIPPED_AT_COL, pd.NaT)
    cost = row.get(COST_COLUMN_NAME, 0)
    if (re.search(r'レベシェア', supplier_name) and not re.search(r'リース・レベシェア', supplier_name)) or \
       re.search(r'法人小物管理用', supplier_name) or sample: return 0
    if pd.isna(inspected_at): return 0
    if pd.notna(inspected_at) and inspected_at >= start_date_param: return 0
    if pd.notna(lease_reacquisition_date):
        lease_reacquisition_date_month_start = lease_reacquisition_date.replace(day=1)
        if lease_reacquisition_date_month_start >= start_date_param: return 0
    if (pd.notna(impossibled_at) and impossibled_at < start_date_param) or \
       (pd.notna(lease_first_shipped_at) and lease_first_shipped_at < start_date_param): return 0
    return cost

def calculate_acquisition_cost_increase(row, start_date_param, end_date_param):
    supplier_name = str(row.get(SUPPLIER_COLUMN_NAME, ""))
    sample = row.get(SAMPLE_COLUMN_NAME, False)
    lease_reacquisition_date = row.get(LEASE_REACQUISITION_DATE_COL, pd.NaT)
    inspected_at = row.get(INSPECTED_AT_COL, pd.NaT)
    impossibled_at = row.get(IMPOSSIBLED_AT_COL, pd.NaT)
    lease_first_shipped_at = row.get(LEASE_FIRST_SHIPPED_AT_COL, pd.NaT)
    cost = row.get(COST_COLUMN_NAME, 0)
    if (re.search(r'レベシェア', supplier_name) and not re.search(r'リース・レベシェア', supplier_name)) or \
       re.search(r'法人小物管理用', supplier_name) or sample: return 0
    if pd.notna(lease_reacquisition_date) and (start_date_param <= lease_reacquisition_date <= end_date_param):
        lease_reacquisition_date_month_start = lease_reacquisition_date.replace(day=1)
        condition_impossibled_ok = pd.isna(impossibled_at) or (pd.notna(impossibled_at) and impossibled_at >= lease_reacquisition_date_month_start)
        condition_lease_shipped_ok = pd.isna(lease_first_shipped_at) or (pd.notna(lease_first_shipped_at) and lease_first_shipped_at >= lease_reacquisition_date_month_start)
        if condition_impossibled_ok and condition_lease_shipped_ok: return cost
    if pd.notna(inspected_at) and (start_date_param <= inspected_at <= end_date_param) and pd.isna(lease_reacquisition_date): return cost
    return 0

def calculate_acquisition_cost_decrease(row, start_date_param, end_date_param):
    supplier_name = str(row.get(SUPPLIER_COLUMN_NAME, ""))
    sample = row.get(SAMPLE_COLUMN_NAME, False)
    impossibled_at = row.get(IMPOSSIBLED_AT_COL, pd.NaT)
    lease_first_shipped_at = row.get(LEASE_FIRST_SHIPPED_AT_COL, pd.NaT)
    lease_reacquisition_date = row.get(LEASE_REACQUISITION_DATE_COL, pd.NaT)
    cost = row.get(COST_COLUMN_NAME, 0)
    if (re.search(r'レベシェア', supplier_name) and not re.search(r'リース・レベシェア', supplier_name)) or \
            re.search(r'法人小物管理用', supplier_name) or sample: return 0
    if pd.notna(impossibled_at) and impossibled_at < start_date_param: return 0
    if pd.notna(lease_first_shipped_at) and lease_first_shipped_at < start_date_param: return 0
    if pd.notna(lease_reacquisition_date):
        lease_reacquisition_date_month_start = lease_reacquisition_date.replace(day=1)
        if pd.notna(impossibled_at) and impossibled_at < lease_reacquisition_date_month_start: return 0
        if pd.notna(lease_first_shipped_at) and lease_first_shipped_at < lease_reacquisition_date_month_start: return 0
    decrease_due_to_impossibled = pd.notna(impossibled_at) and (start_date_param <= impossibled_at <= end_date_param)
    decrease_due_to_lease_transfer = pd.notna(lease_first_shipped_at) and (start_date_param <= lease_first_shipped_at <= end_date_param)
    if decrease_due_to_impossibled or decrease_due_to_lease_transfer: return cost
    return 0
    
def calculate_acquisition_cost_kimatsu(row, end_date_param):
    supplier_name = str(row.get(SUPPLIER_COLUMN_NAME, ""))
    sample = row.get(SAMPLE_COLUMN_NAME, False)
    kishu_cost = row.get(ACQUISITION_COST_KISHU_COL,0)
    increase_cost = row.get(ACQUISITION_COST_INCREASE_COL,0)
    decrease_cost = row.get(ACQUISITION_COST_DECREASE_COL,0)
    cost = kishu_cost + increase_cost - decrease_cost
    if (re.search(r'レベシェア', supplier_name) and not re.search(r'リース・レベシェア', supplier_name)) or \
       re.search(r'法人小物管理用', supplier_name) or sample: return 0
    accounting_status_at_end = row.get(ACCOUNTING_STATUS_COLUMN_NAME, "")
    if accounting_status_at_end not in ["賃貸用固定資産", "リース資産(借手リース)"]: return 0
    return max(0, cost)

def calculate_shokyaku_months_kimatsu(row, end_date_param):
    accounting_status = row.get(ACCOUNTING_STATUS_COLUMN_NAME, "")
    if accounting_status not in ["賃貸用固定資産", "リース資産(借手リース)"]: return 0
    shokyaku_beta = row.get(SHOKYAKU_BETA_COL, 0)
    depreciation_period_years = pd.to_numeric(row.get('耐用年数', 0), errors='coerce')
    depreciation_period_months = 0
    if pd.notna(depreciation_period_years) and depreciation_period_years > 0:
        depreciation_period_months = depreciation_period_years * 12
    else: return 0
    return min(shokyaku_beta, depreciation_period_months)

def calculate_amortization_months_kishu(row, start_date_param):
    first_shipped_at = row.get(DISPLAY_NAME_FIRST_SHIPPED_AT, pd.NaT)
    depreciation_period_years = pd.to_numeric(row.get('耐用年数', 0), errors='coerce')
    depreciation_period_months = 0
    if pd.notna(depreciation_period_years) and depreciation_period_years > 0:
        depreciation_period_months = depreciation_period_years * 12
    else: return 0
    lease_first_shipped_at = row.get(LEASE_FIRST_SHIPPED_AT_COL, pd.NaT)
    impossibled_at = row.get(IMPOSSIBLED_AT_COL, pd.NaT)
    if (pd.notna(lease_first_shipped_at) and lease_first_shipped_at < start_date_param) or \
       (pd.notna(impossibled_at) and impossibled_at < start_date_param):
        return 0
    if pd.notna(first_shipped_at) and first_shipped_at < start_date_param:
        shokyaku_alpha = row.get(SHOKYAKU_ALPHA_COL, 0) 
        return min(shokyaku_alpha, depreciation_period_months)
    return 0

def calculate_amortization_months_shokyaku(row, start_date_param, end_date_param):
    supplier_name = str(row.get(SUPPLIER_COLUMN_NAME, ""))
    sample = row.get(SAMPLE_COLUMN_NAME, False)
    inspected_at = row.get(INSPECTED_AT_COL, pd.NaT)
    lease_reacquisition_date_val = row.get(LEASE_REACQUISITION_DATE_COL, pd.NaT)
    depreciation_period_years = pd.to_numeric(row.get('耐用年数', 0), errors='coerce')
    depreciation_period_months = 0
    if pd.notna(depreciation_period_years) and depreciation_period_years > 0:
        depreciation_period_months = depreciation_period_years * 12
    shokyaku_alpha = row.get(SHOKYAKU_ALPHA_COL, 0)
    shokyaku_beta = row.get(SHOKYAKU_BETA_COL, 0)
    shokyaku_gamma = row.get(SHOKYAKU_GAMMA_COL, 0)
    if (re.search(r'レベシェア', supplier_name) and not re.search(r'リース・レベシェア', supplier_name)) or \
       re.search(r'法人小物管理用', supplier_name) or sample: return 0
    if pd.notna(inspected_at) and inspected_at > end_date_param: return 0
    lease_reacquisition_date_month_start = pd.NaT
    if pd.notna(lease_reacquisition_date_val):
        lease_reacquisition_date_month_start = lease_reacquisition_date_val.replace(day=1)
    if pd.notna(lease_reacquisition_date_month_start) and lease_reacquisition_date_month_start > end_date_param: return 0
    if pd.notna(lease_reacquisition_date_month_start) and lease_reacquisition_date_month_start >= start_date_param:
        val = min(depreciation_period_months, shokyaku_beta) - min(depreciation_period_months, shokyaku_gamma)
        return max(0, val)
    else:
        val = min(depreciation_period_months, shokyaku_beta) - min(depreciation_period_months, shokyaku_alpha)
        return max(0, val)

def calculate_amortization_months_increase(row, start_date_param, end_date_param):
    supplier_name = str(row.get(SUPPLIER_COLUMN_NAME, ""))
    sample = row.get(SAMPLE_COLUMN_NAME, False)
    lease_reacquisition_date_val = row.get(LEASE_REACQUISITION_DATE_COL, pd.NaT)
    impossibled_at = row.get(IMPOSSIBLED_AT_COL, pd.NaT)
    lease_first_shipped_at_col_val = row.get(LEASE_FIRST_SHIPPED_AT_COL, pd.NaT)
    depreciation_period_years = pd.to_numeric(row.get('耐用年数', 0), errors='coerce')
    depreciation_period_months = 0
    if pd.notna(depreciation_period_years) and depreciation_period_years > 0:
        depreciation_period_months = depreciation_period_years * 12
    shokyaku_gamma = row.get(SHOKYAKU_GAMMA_COL, 0)
    if (re.search(r'レベシェア', supplier_name) and not re.search(r'リース・レベシェア', supplier_name)) or \
       re.search(r'法人小物管理用', supplier_name) or sample: return 0
    lease_reacquisition_date_month_start = pd.NaT
    if pd.notna(lease_reacquisition_date_val):
        lease_reacquisition_date_month_start = lease_reacquisition_date_val.replace(day=1)
    condition_reacquisition_out_of_period = False
    if pd.isna(lease_reacquisition_date_month_start) or \
       lease_reacquisition_date_month_start < start_date_param or \
       lease_reacquisition_date_month_start > end_date_param:
        condition_reacquisition_out_of_period = True
    condition_event_before_reacquisition = False
    if pd.notna(lease_reacquisition_date_month_start):
        if (pd.notna(impossibled_at) and impossibled_at < lease_reacquisition_date_month_start) or \
           (pd.notna(lease_first_shipped_at_col_val) and lease_first_shipped_at_col_val < lease_reacquisition_date_month_start):
            condition_event_before_reacquisition = True
    if condition_reacquisition_out_of_period or condition_event_before_reacquisition: return 0
    return min(shokyaku_gamma, depreciation_period_months)

def calculate_amortization_months_decrease(row, start_date_param, end_date_param):
    supplier_name = str(row.get(SUPPLIER_COLUMN_NAME, ""))
    sample = row.get(SAMPLE_COLUMN_NAME, False)
    classification_of_impossibility = row.get(CLASSIFICATION_OF_IMPOSSIBILITY_COL, None)
    impossibled_at = row.get(IMPOSSIBLED_AT_COL, pd.NaT)
    lease_first_shipped_at_col_val = row.get(LEASE_FIRST_SHIPPED_AT_COL, pd.NaT)
    lease_reacquisition_date_val = row.get(LEASE_REACQUISITION_DATE_COL, pd.NaT)
    depreciation_period_years = pd.to_numeric(row.get('耐用年数', 0), errors='coerce')
    depreciation_period_months = 0
    if pd.notna(depreciation_period_years) and depreciation_period_years > 0:
        depreciation_period_months = depreciation_period_years * 12
    shokyaku_beta = row.get(SHOKYAKU_BETA_COL, 0)
    if (re.search(r'レベシェア', supplier_name) and not re.search(r'リース・レベシェア', supplier_name)) or \
       re.search(r'法人小物管理用', supplier_name) or sample: return 0
    if classification_of_impossibility == "レベシェア品": return 0
    if (pd.notna(impossibled_at) and impossibled_at < start_date_param) or \
       (pd.notna(lease_first_shipped_at_col_val) and lease_first_shipped_at_col_val < start_date_param): return 0
    lease_reacquisition_date_month_start = pd.NaT
    if pd.notna(lease_reacquisition_date_val):
        lease_reacquisition_date_month_start = lease_reacquisition_date_val.replace(day=1)
    if pd.notna(lease_reacquisition_date_month_start): 
        if (pd.notna(impossibled_at) and impossibled_at < lease_reacquisition_date_month_start) or \
           (pd.notna(lease_first_shipped_at_col_val) and lease_first_shipped_at_col_val < lease_reacquisition_date_month_start): return 0
    if pd.notna(lease_first_shipped_at_col_val) and lease_first_shipped_at_col_val > end_date_param: return 0
    if pd.isna(lease_first_shipped_at_col_val) and \
       (pd.isna(impossibled_at) or (pd.notna(impossibled_at) and impossibled_at > end_date_param)): return 0
    return min(shokyaku_beta, depreciation_period_months)

def calculate_accumulated_depreciation_kishu(row, start_date_param):
    supplier_name = str(row.get(SUPPLIER_COLUMN_NAME, ""))
    sample = row.get(SAMPLE_COLUMN_NAME, False)
    amortization_months_kishu_val = row.get(AMORTIZATION_MONTHS_KISHU_COL, 0)
    lease_reacquisition_date_val = row.get(LEASE_REACQUISITION_DATE_COL, pd.NaT)
    cost = row.get(COST_COLUMN_NAME, 0)
    depreciation_period_years = pd.to_numeric(row.get('耐用年数', 0), errors='coerce')
    depreciation_period_months = 0
    if pd.notna(depreciation_period_years) and depreciation_period_years > 0:
        depreciation_period_months = depreciation_period_years * 12
    shokyaku_alpha = row.get(SHOKYAKU_ALPHA_COL, 0)
    monthly_depreciation_amount = row.get(MONTHLY_DEPRECIATION_COL, 0)
    if (re.search(r'レベシェア', supplier_name) and not re.search(r'リース・レベシェア', supplier_name)) or \
       re.search(r'法人小物管理用', supplier_name) or sample: return 0
    if amortization_months_kishu_val == 0: return 0
    lease_reacquisition_date_month_start = pd.NaT
    if pd.notna(lease_reacquisition_date_val):
        lease_reacquisition_date_month_start = lease_reacquisition_date_val.replace(day=1)
    if pd.notna(lease_reacquisition_date_month_start) and start_date_param <= lease_reacquisition_date_month_start: return 0
    term_alpha_months_for_calc = shokyaku_alpha
    if depreciation_period_months > 0:
        term_alpha_months_for_calc = min(depreciation_period_months, shokyaku_alpha)
    condition_cost_lt_term_alpha = False
    if depreciation_period_months > 0:
        condition_cost_lt_term_alpha = cost < term_alpha_months_for_calc 
    condition_fully_depreciated = False
    if depreciation_period_months > 0 and depreciation_period_months <= shokyaku_alpha:
        condition_fully_depreciated = True
    if condition_cost_lt_term_alpha or condition_fully_depreciated: return cost
    return monthly_depreciation_amount * amortization_months_kishu_val

def calculate_accumulated_depreciation_kimatsu(row, end_date_param):
    supplier_name = str(row.get(SUPPLIER_COLUMN_NAME, ""))
    sample = row.get(SAMPLE_COLUMN_NAME, False)
    amortization_months_kimatsu_val = row.get(AMORTIZATION_MONTHS_KIMATSU_COL, 0) 
    lease_reacquisition_date_val = row.get(LEASE_REACQUISITION_DATE_COL, pd.NaT)
    cost = row.get(COST_COLUMN_NAME, 0)
    depreciation_period_years = pd.to_numeric(row.get('耐用年数', 0), errors='coerce')
    depreciation_period_months = 0
    if pd.notna(depreciation_period_years) and depreciation_period_years > 0:
        depreciation_period_months = depreciation_period_years * 12
    shokyaku_beta = row.get(SHOKYAKU_BETA_COL, 0)
    monthly_depreciation_amount = row.get(MONTHLY_DEPRECIATION_COL, 0)
    if (re.search(r'レベシェア', supplier_name) and not re.search(r'リース・レベシェア', supplier_name)) or \
       re.search(r'法人小物管理用', supplier_name) or sample: return 0
    if amortization_months_kimatsu_val == 0: return 0
    lease_reacquisition_date_month_start = pd.NaT
    if pd.notna(lease_reacquisition_date_val):
        lease_reacquisition_date_month_start = lease_reacquisition_date_val.replace(day=1)
    if pd.notna(lease_reacquisition_date_month_start) and lease_reacquisition_date_month_start > end_date_param: return 0
    term_beta_months_for_calc = amortization_months_kimatsu_val 
    condition_cost_lt_term_beta = False
    if depreciation_period_months > 0:
         condition_cost_lt_term_beta = cost < term_beta_months_for_calc
    condition_fully_depreciated = False
    if depreciation_period_months > 0 and depreciation_period_months <= shokyaku_beta:
        condition_fully_depreciated = True
    if condition_cost_lt_term_beta or condition_fully_depreciated: return cost
    return monthly_depreciation_amount * amortization_months_kimatsu_val

def calculate_accumulated_depreciation_increase(row):
    supplier_name = str(row.get(SUPPLIER_COLUMN_NAME, ""))
    sample = row.get(SAMPLE_COLUMN_NAME, False)
    amortization_months_increase_val = row.get(AMORTIZATION_MONTHS_INCREASE_COL, 0)
    cost = row.get(COST_COLUMN_NAME, 0)
    depreciation_period_years = pd.to_numeric(row.get('耐用年数', 0), errors='coerce')
    depreciation_period_months = 0
    if pd.notna(depreciation_period_years) and depreciation_period_years > 0:
        depreciation_period_months = depreciation_period_years * 12
    shokyaku_gamma = row.get(SHOKYAKU_GAMMA_COL, 0)
    monthly_depreciation_amount = row.get(MONTHLY_DEPRECIATION_COL, 0)
    if (re.search(r'レベシェア', supplier_name) and not re.search(r'リース・レベシェア', supplier_name)) or \
       re.search(r'法人小物管理用', supplier_name) or sample: return 0
    if amortization_months_increase_val == 0: return 0
    term_gamma_months_for_calc = shokyaku_gamma
    if depreciation_period_months > 0:
        term_gamma_months_for_calc = min(depreciation_period_months, shokyaku_gamma)
    condition_cost_lt_term_gamma = False
    if depreciation_period_months > 0:
        condition_cost_lt_term_gamma = cost < term_gamma_months_for_calc
    condition_dp_eq_increase_months = False
    if depreciation_period_months > 0:
         condition_dp_eq_increase_months = (depreciation_period_months == amortization_months_increase_val)
    if condition_cost_lt_term_gamma or condition_dp_eq_increase_months: return cost
    return monthly_depreciation_amount * amortization_months_increase_val

def calculate_accumulated_depreciation_decrease(row):
    supplier_name = str(row.get(SUPPLIER_COLUMN_NAME, ""))
    sample = row.get(SAMPLE_COLUMN_NAME, False)
    amortization_months_decrease_val = row.get(AMORTIZATION_MONTHS_DECREASE_COL, 0)
    cost = row.get(COST_COLUMN_NAME, 0)
    depreciation_period_years = pd.to_numeric(row.get('耐用年数', 0), errors='coerce')
    depreciation_period_months = 0
    if pd.notna(depreciation_period_years) and depreciation_period_years > 0:
        depreciation_period_months = depreciation_period_years * 12
    shokyaku_beta = row.get(SHOKYAKU_BETA_COL, 0) 
    monthly_depreciation_amount = row.get(MONTHLY_DEPRECIATION_COL, 0)
    if (re.search(r'レベシェア', supplier_name) and not re.search(r'リース・レベシェア', supplier_name)) or \
       re.search(r'法人小物管理用', supplier_name) or sample: return 0
    if amortization_months_decrease_val == 0: return 0
    term_beta_months_for_calc = shokyaku_beta
    if depreciation_period_months > 0:
        term_beta_months_for_calc = min(depreciation_period_months, shokyaku_beta)
    condition_cost_lt_term_beta = False
    if depreciation_period_months > 0:
        condition_cost_lt_term_beta = cost < term_beta_months_for_calc
    condition_dp_eq_decrease_months = False
    if depreciation_period_months > 0:
        condition_dp_eq_decrease_months = (depreciation_period_months == amortization_months_decrease_val)
    if condition_cost_lt_term_beta or condition_dp_eq_decrease_months: return cost
    return monthly_depreciation_amount * amortization_months_decrease_val

def calculate_interim_depreciation_expense(row):
    supplier_name = str(row.get(SUPPLIER_COLUMN_NAME, ""))
    sample = row.get(SAMPLE_COLUMN_NAME, False)
    amortization_months_shokyaku_val = row.get(AMORTIZATION_MONTHS_SHOKYAKU_COL, 0)
    cost = row.get(COST_COLUMN_NAME, 0)
    depreciation_period_years = pd.to_numeric(row.get('耐用年数', 0), errors='coerce')
    depreciation_period_months = 0
    if pd.notna(depreciation_period_years) and depreciation_period_years > 0:
        depreciation_period_months = depreciation_period_years * 12
    shokyaku_alpha = row.get(SHOKYAKU_ALPHA_COL, 0)
    shokyaku_beta = row.get(SHOKYAKU_BETA_COL, 0)
    monthly_depreciation_amount = row.get(MONTHLY_DEPRECIATION_COL, 0)
    if (re.search(r'レベシェア', supplier_name) and not re.search(r'リース・レベシェア', supplier_name)) or \
       re.search(r'法人小物管理用', supplier_name) or sample: return 0
    if amortization_months_shokyaku_val == 0: return 0
    total_depreciable_by_period_val = monthly_depreciation_amount * depreciation_period_months if depreciation_period_months > 0 else cost
    kishu_dep_amount_val = monthly_depreciation_amount * shokyaku_alpha
    kimatsu_dep_amount_candidate_val = monthly_depreciation_amount * shokyaku_beta
    if (depreciation_period_months > 0 and cost <= total_depreciable_by_period_val and \
        cost > kishu_dep_amount_val and cost < kimatsu_dep_amount_candidate_val):
        return cost - kishu_dep_amount_val
    term_alpha_min_months_val = shokyaku_alpha
    if depreciation_period_months > 0:
        term_alpha_min_months_val = min(shokyaku_alpha, depreciation_period_months)
    min_alpha_dep_amount = monthly_depreciation_amount * term_alpha_min_months_val
    if depreciation_period_months > 0 and cost < min_alpha_dep_amount:
        return 0
    if depreciation_period_months > 0 and depreciation_period_months <= shokyaku_beta:
        val1 = monthly_depreciation_amount * (depreciation_period_months -1 if depreciation_period_months > 0 else 0)
        val2 = monthly_depreciation_amount * (amortization_months_shokyaku_val -1 if amortization_months_shokyaku_val > 0 else 0)
        return cost - val1 + val2
    return monthly_depreciation_amount * amortization_months_shokyaku_val

def calculate_new_impairment_loss_kishu(row, start_date_param):
    supplier_name = str(row.get(SUPPLIER_COLUMN_NAME, ""))
    sample = row.get(SAMPLE_COLUMN_NAME, False)
    impairment_date = row.get(IMPAIRMENT_DATE_COL, pd.NaT)
    acquisition_cost_kishu = row.get(ACQUISITION_COST_KISHU_COL, 0)
    accumulated_depreciation_kishu = row.get(ACCUMULATED_DEPRECIATION_KISHU_COL, 0)
    if (re.search(r'レベシェア', supplier_name) and not re.search(r'リース・レベシェア', supplier_name)) or \
       re.search(r'法人小物管理用', supplier_name) or sample: return 0
    if pd.notna(impairment_date) and impairment_date < start_date_param:
        book_value_kishu = acquisition_cost_kishu - accumulated_depreciation_kishu
        return max(0, book_value_kishu) 
    else: return 0

def calculate_new_impairment_loss_kimatsu(row, end_date_param):
    supplier_name = str(row.get(SUPPLIER_COLUMN_NAME, ""))
    sample = row.get(SAMPLE_COLUMN_NAME, False)
    lease_reacquisition_date = row.get(LEASE_REACQUISITION_DATE_COL, pd.NaT)
    impairment_date = row.get(IMPAIRMENT_DATE_COL, pd.NaT)
    acquisition_cost_kimatsu = row.get(ACQUISITION_COST_KIMATSU_COL, 0)
    accumulated_depreciation_kimatsu = row.get(ACCUMULATED_DEPRECIATION_KIMATSU_COL, 0)
    if (re.search(r'レベシェア', supplier_name) and not re.search(r'リース・レベシェア', supplier_name)) or \
       re.search(r'法人小物管理用', supplier_name) or sample: return 0
    lease_reacquisition_date_month_start = pd.NaT
    if pd.notna(lease_reacquisition_date):
        lease_reacquisition_date_month_start = lease_reacquisition_date.replace(day=1)
    if pd.notna(lease_reacquisition_date_month_start) and lease_reacquisition_date_month_start > end_date_param: return 0
    if pd.notna(impairment_date) and impairment_date <= end_date_param:
        book_value_kimatsu = acquisition_cost_kimatsu - accumulated_depreciation_kimatsu
        return max(0, book_value_kimatsu)
    return 0

def calculate_new_impairment_loss_increase(row, start_date_param, end_date_param):
    supplier_name = str(row.get(SUPPLIER_COLUMN_NAME, ""))
    sample = row.get(SAMPLE_COLUMN_NAME, False)
    impairment_date = row.get(IMPAIRMENT_DATE_COL, pd.NaT)
    lease_reacquisition_date = row.get(LEASE_REACQUISITION_DATE_COL, pd.NaT)
    inspected_at = row.get(INSPECTED_AT_COL, pd.NaT)
    acquisition_cost_increase = row.get(ACQUISITION_COST_INCREASE_COL, 0)
    accumulated_depreciation_increase = row.get(ACCUMULATED_DEPRECIATION_INCREASE_COL, 0)
    if (re.search(r'レベシェア', supplier_name) and not re.search(r'リース・レベシェア', supplier_name)) or \
       re.search(r'法人小物管理用', supplier_name) or sample: return 0
    if pd.isna(impairment_date): return 0
    lease_reacquisition_date_month_start = pd.NaT
    if pd.notna(lease_reacquisition_date):
        lease_reacquisition_date_month_start = lease_reacquisition_date.replace(day=1)
    if pd.notna(lease_reacquisition_date_month_start) and pd.notna(impairment_date) and \
       impairment_date > lease_reacquisition_date_month_start: return 0
    if pd.isna(lease_reacquisition_date) and pd.notna(inspected_at) and pd.notna(impairment_date) and \
       impairment_date > inspected_at : return 0
    book_value_increase_at_impairment = acquisition_cost_increase - accumulated_depreciation_increase
    return max(0, book_value_increase_at_impairment)

def calculate_new_impairment_loss_decrease(row, end_date_param):
    supplier_name = str(row.get(SUPPLIER_COLUMN_NAME, ""))
    sample = row.get(SAMPLE_COLUMN_NAME, False)
    impairment_date = row.get(IMPAIRMENT_DATE_COL, pd.NaT)
    acquisition_cost_decrease = row.get(ACQUISITION_COST_DECREASE_COL, 0)
    accumulated_depreciation_decrease = row.get(ACCUMULATED_DEPRECIATION_DECREASE_COL, 0)
    if (re.search(r'レベシェア', supplier_name) and not re.search(r'リース・レベシェア', supplier_name)) or \
       re.search(r'法人小物管理用', supplier_name) or sample: return 0
    if pd.notna(impairment_date) and impairment_date > end_date_param: return 0
    if pd.isna(impairment_date): return 0
    if pd.notna(impairment_date) and impairment_date <= end_date_param:
        book_value_decrease = acquisition_cost_decrease - accumulated_depreciation_decrease
        return max(0, book_value_decrease)
    return 0
        
def calculate_new_interim_impairment_loss(row, start_date_param, end_date_param):
    supplier_name = str(row.get(SUPPLIER_COLUMN_NAME, ""))
    sample = row.get(SAMPLE_COLUMN_NAME, False)
    lease_reacquisition_date = row.get(LEASE_REACQUISITION_DATE_COL, pd.NaT)
    inspected_at = row.get(INSPECTED_AT_COL, pd.NaT)
    impairment_date = row.get(IMPAIRMENT_DATE_COL, pd.NaT)
    
    # CASE 3で新しい計算式に使用する値
    impairment_loss_accumulated_decrease = row.get(IMPAIRMENT_LOSS_ACCUMULATED_DECREASE_COL, 0)
    impairment_loss_accumulated_kimatsu = row.get(IMPAIRMENT_LOSS_ACCUMULATED_KIMATSU_COL, 0)

    # CASE 1
    if (re.search(r'レベシェア', supplier_name) and not re.search(r'リース・レベシェア', supplier_name)) or \
       re.search(r'法人小物管理用', supplier_name) or sample:
        return 0

    # CASE 2
    lease_reacquisition_date_month_start = pd.NaT
    if pd.notna(lease_reacquisition_date):
        lease_reacquisition_date_month_start = lease_reacquisition_date.replace(day=1)
    if pd.notna(lease_reacquisition_date_month_start) and lease_reacquisition_date_month_start > end_date_param:
        return 0
        
    # CASE 3
    condition_inspected_by_period_end = pd.notna(inspected_at) and inspected_at <= end_date_param
    condition_impairment_in_period = pd.notna(impairment_date) and \
                                     start_date_param <= impairment_date <= end_date_param
                                     
    if condition_inspected_by_period_end and condition_impairment_in_period:
        # ★★★ 新しい計算式: max(減少減損損失累計額, 期末減損損失累計額) ★★★
        calculated_value = max(impairment_loss_accumulated_decrease, impairment_loss_accumulated_kimatsu)
        return calculated_value 
        
    # ELSE (CASE 4)
    return 0

def calculate_opening_book_value(row):
    return row.get(ACQUISITION_COST_KISHU_COL, 0) - row.get(ACCUMULATED_DEPRECIATION_KISHU_COL, 0) - row.get(IMPAIRMENT_LOSS_ACCUMULATED_KISHU_COL, 0)

def calculate_increase_book_value(row):
    return row.get(ACQUISITION_COST_INCREASE_COL, 0) - row.get(ACCUMULATED_DEPRECIATION_INCREASE_COL, 0) - row.get(IMPAIRMENT_LOSS_ACCUMULATED_INCREASE_COL, 0)

def calculate_decrease_book_value(row):
    return row.get(ACQUISITION_COST_DECREASE_COL, 0) - row.get(ACCUMULATED_DEPRECIATION_DECREASE_COL, 0) - row.get(IMPAIRMENT_LOSS_ACCUMULATED_DECREASE_COL, 0)

def calculate_closing_book_value(row):
    kimatsu_cost = row.get(ACQUISITION_COST_KIMATSU_COL, 0)
    kimatsu_dep_acc = row.get(ACCUMULATED_DEPRECIATION_KIMATSU_COL, 0)
    kimatsu_imp_acc = row.get(IMPAIRMENT_LOSS_ACCUMULATED_KIMATSU_COL, 0)
    closing_bv = kimatsu_cost - kimatsu_dep_acc - kimatsu_imp_acc
    accounting_status = row.get(ACCOUNTING_STATUS_COLUMN_NAME, "")
    if accounting_status not in ["賃貸用固定資産", "リース資産(借手リース)"]: return 0
    return max(0, closing_bv)

# --- データ処理関数 ---
def load_and_initial_process(file_path):
    """最適化されたCSV読み込み"""
    try:
        df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
    except Exception as e:
        print(f"CSV読み込みエラー: {e}")
        return pd.DataFrame()
    
    if df.empty: 
        return pd.DataFrame()
    
    # 重複列の削除
    df = df.loc[:, ~df.columns.duplicated(keep='first')]
    return df

def process_dataframe_with_progress(df_original, start_date_input_val, end_date_input_val, progress_callback=None):
    """進捗表示付きデータ処理（改善版）"""
    if df_original.empty:
        return pd.DataFrame()
    
    def update_progress(step, total_steps, message):
        if progress_callback:
            progress = int((step / total_steps) * 100)
            progress_callback(progress, message)
    
    total_steps = 30
    current_step = 0
    
    try:
        update_progress(current_step, total_steps, "データを初期化中...")
        df_to_process = df_original.copy()
        df_to_process = df_to_process.loc[:, ~df_to_process.columns.duplicated(keep='first')]
        start_date_dt = pd.to_datetime(start_date_input_val)
        end_date_dt = pd.to_datetime(end_date_input_val)
        df_to_process['期首日(計算基準日)'] = start_date_dt
        df_to_process['期末日(計算基準日)'] = end_date_dt
        current_step += 1

        update_progress(current_step, total_steps, "サンプル列を処理中...")
        if SAMPLE_COLUMN_NAME in df_to_process.columns:
            df_to_process[SAMPLE_COLUMN_NAME] = df_to_process[SAMPLE_COLUMN_NAME].apply(lambda x: True if isinstance(x, str) and x.strip().upper() == 'TRUE' else (True if x is True else False))
        else: 
            df_to_process[SAMPLE_COLUMN_NAME] = False
        current_step += 1

        update_progress(current_step, total_steps, "日付データを正規化中...")
        date_cols_to_normalize_early = [DISPLAY_NAME_FIRST_SHIPPED_AT, LEASE_FIRST_SHIPPED_AT_COL]
        for col in date_cols_to_normalize_early:
            if col in df_to_process.columns: 
                df_to_process[col] = pd.to_datetime(df_to_process[col], errors='coerce').dt.normalize()
            else: 
                df_to_process[col] = pd.NaT
        current_step += 1

        update_progress(current_step, total_steps, "初回出荷日を計算中...")
        df_to_process[FIRST_SHIPPED_AT_CALCULATED_COL] = df_to_process.apply(calculate_first_shipped_at_calculated, axis=1)
        df_to_process[FIRST_SHIPPED_AT_CALCULATED_COL] = pd.to_datetime(df_to_process[FIRST_SHIPPED_AT_CALCULATED_COL], errors='coerce').dt.normalize()
        df_to_process[DISPLAY_NAME_FIRST_SHIPPED_AT] = df_to_process[FIRST_SHIPPED_AT_CALCULATED_COL]
        current_step += 1

        update_progress(current_step, total_steps, "その他の日付列を正規化中...")
        date_cols_to_normalize_later = [INSPECTED_AT_COL, IMPOSSIBLED_AT_COL, IMPAIRMENT_DATE_COL]
        for col in date_cols_to_normalize_later:
            if col in df_to_process.columns: 
                df_to_process[col] = pd.to_datetime(df_to_process[col], errors='coerce').dt.normalize()
            elif col == IMPAIRMENT_DATE_COL: 
                df_to_process[col] = pd.NaT
        current_step += 1

        update_progress(current_step, total_steps, "資産分類を決定中...")
        if SUPPLIER_COLUMN_NAME in df_to_process.columns or SAMPLE_COLUMN_NAME in df_to_process.columns:
            df_to_process[ASSET_CLASSIFICATION_COLUMN_NAME] = df_to_process.apply(classify_asset, axis=1)
        else:
            df_to_process[ASSET_CLASSIFICATION_COLUMN_NAME] = "賃貸用固定資産"
        current_step += 1
        
        update_progress(current_step, total_steps, "リース再取得日を計算中...")
        if SUPPLIER_COLUMN_NAME in df_to_process.columns:
            df_to_process[LEASE_REACQUISITION_DATE_COL] = df_to_process[SUPPLIER_COLUMN_NAME].apply(calculate_lease_reacquisition_date)
            df_to_process[LEASE_REACQUISITION_DATE_COL] = pd.to_datetime(df_to_process[LEASE_REACQUISITION_DATE_COL], errors='coerce').dt.normalize()
        else: 
            df_to_process[LEASE_REACQUISITION_DATE_COL] = pd.NaT
        current_step += 1

        update_progress(current_step, total_steps, "会計ステータスを決定中...")
        if ASSET_CLASSIFICATION_COLUMN_NAME in df_to_process.columns:
            df_to_process[ACCOUNTING_STATUS_COLUMN_NAME] = df_to_process.apply(determine_accounting_status, args=(end_date_dt,), axis=1)
        else:
            df_to_process[ACCOUNTING_STATUS_COLUMN_NAME] = "計算エラー"
        current_step += 1

        update_progress(current_step, total_steps, "取得原価を計算中...")
        df_to_process[ACQUISITION_COST_KISHU_COL] = df_to_process.apply(calculate_initial_cost, args=(start_date_dt,), axis=1)
        current_step += 1
        
        update_progress(current_step, total_steps, "増加取得原価を計算中...")
        df_to_process[ACQUISITION_COST_INCREASE_COL] = df_to_process.apply(calculate_acquisition_cost_increase, args=(start_date_dt, end_date_dt), axis=1)
        current_step += 1
        
        update_progress(current_step, total_steps, "減少取得原価を計算中...")
        df_to_process[ACQUISITION_COST_DECREASE_COL] = df_to_process.apply(calculate_acquisition_cost_decrease, args=(start_date_dt, end_date_dt), axis=1)
        current_step += 1
        
        update_progress(current_step, total_steps, "期末取得原価を計算中...")
        df_to_process[ACQUISITION_COST_KIMATSU_COL] = df_to_process.apply(calculate_acquisition_cost_kimatsu, args=(end_date_dt,), axis=1)
        current_step += 1
        
        update_progress(current_step, total_steps, "償却αを計算中...")
        df_to_process[SHOKYAKU_ALPHA_COL] = df_to_process.apply(calculate_shokyaku_alpha, args=(start_date_dt,), axis=1)
        current_step += 1
        
        update_progress(current_step, total_steps, "償却βを計算中...")
        df_to_process[SHOKYAKU_BETA_COL] = df_to_process.apply(calculate_shokyaku_beta, args=(end_date_dt,), axis=1)
        current_step += 1
        
        update_progress(current_step, total_steps, "償却γを計算中...")
        df_to_process[SHOKYAKU_GAMMA_COL] = df_to_process.apply(calculate_shokyaku_gamma, axis=1)
        current_step += 1

        update_progress(current_step, total_steps, "数値列を変換中...")
        if MONTHLY_DEPRECIATION_COL in df_to_process.columns:
            df_to_process[MONTHLY_DEPRECIATION_COL] = pd.to_numeric(df_to_process[MONTHLY_DEPRECIATION_COL], errors='coerce').fillna(0)
        else:
            df_to_process[MONTHLY_DEPRECIATION_COL] = 0
        if '耐用年数' in df_to_process.columns: 
            df_to_process['耐用年数'] = pd.to_numeric(df_to_process['耐用年数'], errors='coerce').fillna(0)
        else: 
            df_to_process['耐用年数'] = 0
        if COST_COLUMN_NAME in df_to_process.columns: 
            df_to_process[COST_COLUMN_NAME] = pd.to_numeric(df_to_process[COST_COLUMN_NAME], errors='coerce').fillna(0)
        else: 
            df_to_process[COST_COLUMN_NAME] = 0
        current_step += 1

        update_progress(current_step, total_steps, "期首償却月数を計算中...")
        df_to_process[AMORTIZATION_MONTHS_KISHU_COL] = df_to_process.apply(calculate_amortization_months_kishu, args=(start_date_dt,), axis=1)
        current_step += 1
        
        update_progress(current_step, total_steps, "償却償却月数を計算中...")
        df_to_process[AMORTIZATION_MONTHS_SHOKYAKU_COL] = df_to_process.apply(calculate_amortization_months_shokyaku, args=(start_date_dt, end_date_dt,), axis=1)
        current_step += 1
        
        update_progress(current_step, total_steps, "増加償却月数を計算中...")
        df_to_process[AMORTIZATION_MONTHS_INCREASE_COL] = df_to_process.apply(calculate_amortization_months_increase, args=(start_date_dt, end_date_dt,), axis=1)
        current_step += 1
        
        update_progress(current_step, total_steps, "減少償却月数を計算中...")
        df_to_process[AMORTIZATION_MONTHS_DECREASE_COL] = df_to_process.apply(calculate_amortization_months_decrease, args=(start_date_dt, end_date_dt,), axis=1)
        current_step += 1
        
        update_progress(current_step, total_steps, "期末償却月数を計算中...")
        df_to_process[AMORTIZATION_MONTHS_KIMATSU_COL] = df_to_process.apply(calculate_shokyaku_months_kimatsu, args=(end_date_dt,), axis=1)
        current_step += 1

        update_progress(current_step, total_steps, "期首減価償却累計額を計算中...")
        df_to_process[ACCUMULATED_DEPRECIATION_KISHU_COL] = df_to_process.apply(calculate_accumulated_depreciation_kishu, args=(start_date_dt,), axis=1).round(0)
        current_step += 1
        
        update_progress(current_step, total_steps, "増加減価償却累計額を計算中...")
        df_to_process[ACCUMULATED_DEPRECIATION_INCREASE_COL] = df_to_process.apply(calculate_accumulated_depreciation_increase, axis=1).round(0)
        current_step += 1
        
        update_progress(current_step, total_steps, "減少減価償却累計額を計算中...")
        df_to_process[ACCUMULATED_DEPRECIATION_DECREASE_COL] = df_to_process.apply(calculate_accumulated_depreciation_decrease, axis=1).round(0)
        current_step += 1
        
        update_progress(current_step, total_steps, "期中減価償却費を計算中...")
        df_to_process[INTERIM_DEPRECIATION_EXPENSE_COL] = df_to_process.apply(calculate_interim_depreciation_expense, axis=1).round(0)
        current_step += 1
        
        update_progress(current_step, total_steps, "期末減価償却累計額を計算中...")
        df_to_process[ACCUMULATED_DEPRECIATION_KIMATSU_COL] = df_to_process.apply(calculate_accumulated_depreciation_kimatsu, args=(end_date_dt,), axis=1).round(0)
        current_step += 1

        update_progress(current_step, total_steps, "期首減損損失累計額を計算中...")
        df_to_process[IMPAIRMENT_LOSS_ACCUMULATED_KISHU_COL] = df_to_process.apply(calculate_new_impairment_loss_kishu, args=(start_date_dt,), axis=1).round(0)
        current_step += 1
        
        update_progress(current_step, total_steps, "増加減損損失累計額を計算中...")
        df_to_process[IMPAIRMENT_LOSS_ACCUMULATED_INCREASE_COL] = df_to_process.apply(calculate_new_impairment_loss_increase, args=(start_date_dt, end_date_dt,), axis=1).round(0)
        current_step += 1
        
        update_progress(current_step, total_steps, "減少減損損失累計額を計算中...")
        df_to_process[IMPAIRMENT_LOSS_ACCUMULATED_DECREASE_COL] = df_to_process.apply(calculate_new_impairment_loss_decrease, args=(end_date_dt,), axis=1).round(0)
        current_step += 1
        
        update_progress(current_step, total_steps, "期末減損損失累計額を計算中...")
        df_to_process[IMPAIRMENT_LOSS_ACCUMULATED_KIMATSU_COL] = df_to_process.apply(calculate_new_impairment_loss_kimatsu, args=(end_date_dt,), axis=1).round(0)
        current_step += 1
        
        update_progress(current_step, total_steps, "期中減損損失を計算中...")
        df_to_process[INTERIM_IMPAIRMENT_LOSS_COL] = df_to_process.apply(calculate_new_interim_impairment_loss, args=(start_date_dt, end_date_dt,), axis=1).round(0)
        current_step += 1
        
        update_progress(current_step, total_steps, "簿価を計算中...")
        df_to_process[OPENING_BOOK_VALUE_COL] = df_to_process.apply(calculate_opening_book_value, axis=1).round(0)
        df_to_process[INCREASE_BOOK_VALUE_COL] = df_to_process.apply(calculate_increase_book_value, axis=1).round(0)
        df_to_process[DECREASE_BOOK_VALUE_COL] = df_to_process.apply(calculate_decrease_book_value, axis=1).round(0)
        df_to_process[CLOSING_BOOK_VALUE_COL] = df_to_process.apply(calculate_closing_book_value, axis=1).round(0)
        current_step += 1

        update_progress(current_step, total_steps, "カラム順序を整理中...")
        # 重複列の削除
        df_to_process = df_to_process.loc[:, ~df_to_process.columns.duplicated(keep='first')]
        if 'Unnamed: 14' in df_to_process.columns:
            df_to_process.drop(columns=['Unnamed: 14'], inplace=True, errors='ignore')

        # 指定されたカラム配置順序に変更
        desired_column_order = [
            '期首日(計算基準日)', '期末日(計算基準日)', '在庫id', 'パーツid', 'パーツ名', '耐用年数', 
            'サプライヤー名', '取得原価', '資産分類', '会計ステータス', '入庫検品完了日', 
            '供与開始日(初回出荷日)', '減損損失日', '除売却日', '破損紛失分類', '売却案件名',
            '貸手リース開始日', '貸手リース案件名', 'リース再取得日', '月次償却額',
            '期首取得原価', '期首減価償却累計額', '期首減損損失累計額', '期首簿価',
            '増加取得原価', '増加減価償却累計額', '増加減損損失累計額', '増加簿価',
            '減少取得原価', '減少減価償却累計額', '減少減損損失累計額', '減少簿価',
            '期中減価償却費', '期中減損損失',
            '期末取得原価', '期末減価償却累計額', '期末減損損失累計額', '期末簿価'
        ]
        
        # 実際に存在するカラムのみを含む最終的なカラム順序を作成
        final_column_order = []
        for col in desired_column_order:
            if col in df_to_process.columns:
                final_column_order.append(col)
        
        # 指定されていないカラムがあれば末尾に追加
        for col in df_to_process.columns:
            if col not in final_column_order:
                final_column_order.append(col)
        
        # カラム順序を適用
        df_to_process = df_to_process[final_column_order]
        
        # 不要な列を削除
        columns_to_drop_final = [
            SAMPLE_COLUMN_NAME, SHOKYAKU_ALPHA_COL, SHOKYAKU_BETA_COL, SHOKYAKU_GAMMA_COL,
            AMORTIZATION_MONTHS_KISHU_COL, AMORTIZATION_MONTHS_SHOKYAKU_COL,
            AMORTIZATION_MONTHS_INCREASE_COL, AMORTIZATION_MONTHS_DECREASE_COL,
            AMORTIZATION_MONTHS_KIMATSU_COL,
            FIRST_SHIPPED_AT_CALCULATED_COL
        ]
        columns_to_drop_existing_final = [col for col in columns_to_drop_final if col in df_to_process.columns]
        if columns_to_drop_existing_final:
            df_to_process = df_to_process.drop(columns=columns_to_drop_existing_final, errors='ignore')
        current_step += 1

        update_progress(current_step, total_steps, "日付の書式を設定中...")
        date_columns_to_format_output = [
            '期首日(計算基準日)', '期末日(計算基準日)', INSPECTED_AT_COL, IMPOSSIBLED_AT_COL,
            LEASE_FIRST_SHIPPED_AT_COL, IMPAIRMENT_DATE_COL, LEASE_REACQUISITION_DATE_COL,
            DISPLAY_NAME_FIRST_SHIPPED_AT
        ]
        for col_name in date_columns_to_format_output:
            if col_name in df_to_process.columns:
                df_to_process[col_name] = df_to_process[col_name].apply(lambda x: x.strftime('%Y/%m/%d') if isinstance(x, pd.Timestamp) and pd.notna(x) else ("" if pd.isna(x) else x))
        current_step += 1
        
        update_progress(current_step, total_steps, "最終処理中...")
        df_to_process = df_to_process.loc[:, ~df_to_process.columns.duplicated(keep='first')]
        current_step += 1
        
        update_progress(total_steps, total_steps, "処理完了")
        return df_to_process
        
    except Exception as e:
        print(f"処理エラー: {e}")
        return pd.DataFrame()

class FixedAssetCalculatorGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("固定資産台帳 計算・出力アプリ (修正版)")
        self.root.geometry("700x550")
        
        # 変数定義
        self.csv_file_path = tk.StringVar(value='fixed_assets.csv')
        self.start_date = tk.StringVar(value='2024-03-01')
        self.end_date = tk.StringVar(value='2025-02-28')
        self.processing = False
        
        self.setup_ui()
        
    def setup_ui(self):
        # メインフレーム
        main_frame = ttk.Frame(self.root, padding="20")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # タイトルラベル
        title_label = ttk.Label(main_frame, text="固定資産台帳 計算・出力アプリ (修正版)", 
                               font=('Arial', 16, 'bold'))
        title_label.grid(row=0, column=0, columnspan=3, pady=20)
        
        # 改善情報表示
        info_label = ttk.Label(main_frame, 
                              text="✓ 安定性向上\n✓ 詳細な進捗表示\n✓ エラーハンドリング強化", 
                              font=('Arial', 9), foreground="green")
        info_label.grid(row=1, column=0, columnspan=3, pady=10)
        
        # CSVファイル選択
        ttk.Label(main_frame, text="入力CSVファイル:").grid(row=2, column=0, sticky=tk.W, pady=10)
        ttk.Entry(main_frame, textvariable=self.csv_file_path, width=40).grid(row=2, column=1, padx=10)
        ttk.Button(main_frame, text="参照", command=self.browse_input_file).grid(row=2, column=2)
        
        # 期首日
        ttk.Label(main_frame, text="期首日 (YYYY-MM-DD):").grid(row=3, column=0, sticky=tk.W, pady=10)
        self.start_date_entry = ttk.Entry(main_frame, textvariable=self.start_date, width=20)
        self.start_date_entry.grid(row=3, column=1, sticky=tk.W, padx=10)
        
        # 期末日
        ttk.Label(main_frame, text="期末日 (YYYY-MM-DD):").grid(row=4, column=0, sticky=tk.W, pady=10)
        self.end_date_entry = ttk.Entry(main_frame, textvariable=self.end_date, width=20)
        self.end_date_entry.grid(row=4, column=1, sticky=tk.W, padx=10)
        
        # 処理ボタン
        self.process_button = ttk.Button(main_frame, text="計算実行", 
                                        command=self.process_data)
        self.process_button.grid(row=5, column=0, columnspan=3, pady=30)
        
        # ステータスラベル
        self.status_label = ttk.Label(main_frame, text="", foreground="blue")
        self.status_label.grid(row=6, column=0, columnspan=3, pady=10)
        
        # 進捗率ラベル
        self.progress_percent_label = ttk.Label(main_frame, text="", foreground="green", font=('Arial', 14, 'bold'))
        self.progress_percent_label.grid(row=7, column=0, columnspan=3, pady=5)
        
        # プログレスバー
        self.progress = ttk.Progressbar(main_frame, mode='determinate', maximum=100)
        self.progress.grid(row=8, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=10)
        self.progress.grid_remove()
        
        # 処理時間表示ラベル
        self.time_label = ttk.Label(main_frame, text="", foreground="gray")
        self.time_label.grid(row=9, column=0, columnspan=3, pady=5)
        
        # パフォーマンス情報表示
        self.perf_label = ttk.Label(main_frame, text="", foreground="purple", font=('Arial', 9))
        self.perf_label.grid(row=10, column=0, columnspan=3, pady=5)
        
        # グリッドの重み設定
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        
    def browse_input_file(self):
        filename = filedialog.askopenfilename(
            title="CSVファイルを選択",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        if filename:
            self.csv_file_path.set(filename)
            
    def validate_inputs(self):
        # ファイル存在確認
        if not os.path.exists(self.csv_file_path.get()):
            messagebox.showerror("エラー", f"ファイルが見つかりません: {self.csv_file_path.get()}")
            return False
            
        # 日付妥当性確認
        try:
            start = pd.to_datetime(self.start_date.get())
            end = pd.to_datetime(self.end_date.get())
            if start >= end:
                messagebox.showerror("エラー", "期首日は期末日より前の日付を設定してください")
                return False
        except:
            messagebox.showerror("エラー", "日付の形式が正しくありません (YYYY-MM-DD)")
            return False
            
        return True
        
    def process_data(self):
        if self.processing:
            return
            
        if not self.validate_inputs():
            return
            
        self.processing = True
        self.process_button.config(state='disabled')
        self.progress.grid()
        self.progress_percent_label.grid()
        self.perf_label.grid()
        self.start_time = time.time()
        
        # 処理開始情報表示
        self.perf_label.config(text="処理を開始しています...")
        
        # 別スレッドで処理実行
        thread = threading.Thread(target=self.run_processing)
        thread.start()
        
    def progress_callback(self, percent, message):
        """進捗率とメッセージを更新するコールバック関数"""
        def update_ui():
            self.progress['value'] = percent
            self.status_label.config(text=message)
            self.progress_percent_label.config(text=f"{percent}%")
            
            # 経過時間の表示
            if hasattr(self, 'start_time'):
                elapsed = time.time() - self.start_time
                self.time_label.config(text=f"経過時間: {elapsed:.1f}秒")
            
        self.root.after(0, update_ui)
        
    def run_processing(self):
        try:
            # データ読み込み
            self.update_status("CSVファイルを読み込み中...")
            load_start = time.time()
            df_original = load_and_initial_process(self.csv_file_path.get())
            load_time = time.time() - load_start
            
            if df_original.empty:
                self.show_error("CSVファイルが空か、データが読み取れませんでした")
                return
            
            # メモリ使用量情報
            try:
                memory_usage = df_original.memory_usage(deep=True).sum() / 1024 / 1024  # MB
                self.update_perf_info(f"データ読み込み: {load_time:.2f}秒 | メモリ使用量: {memory_usage:.1f}MB")
            except:
                self.update_perf_info(f"データ読み込み: {load_time:.2f}秒")
                
            # データ処理
            process_start = time.time()
            start_date = pd.to_datetime(self.start_date.get())
            end_date = pd.to_datetime(self.end_date.get())
            df_processed = process_dataframe_with_progress(
                df_original, start_date, end_date, self.progress_callback
            )
            process_time = time.time() - process_start
            
            if df_processed.empty:
                self.show_error("処理結果が空です")
                return
                
            # 出力ファイル保存
            self.update_status("結果を保存中...")
            output_filename = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                initialfile='fixed_asset_register_output.csv'
            )
            
            if output_filename:
                save_start = time.time()
                df_processed.to_csv(output_filename, index=False, encoding='utf-8-sig')
                save_time = time.time() - save_start
                
                total_time = time.time() - self.start_time
                rows_per_second = len(df_processed) / total_time if total_time > 0 else 0
                
                self.show_success(
                    f"処理が完了しました。\n\n" + 
                    f"📊 処理統計:\n" +
                    f"• 総処理時間: {total_time:.2f}秒\n" +
                    f"• データ処理: {process_time:.2f}秒\n" +
                    f"• ファイル保存: {save_time:.2f}秒\n" +
                    f"• 処理行数: {len(df_processed):,}行\n" +
                    f"• 処理速度: {rows_per_second:.0f}行/秒\n\n" +
                    f"💾 保存先: {output_filename}"
                )
            else:
                self.show_info("保存がキャンセルされました")
                
        except Exception as e:
            self.show_error(f"エラーが発生しました:\n{str(e)}")
            
        finally:
            self.processing_complete()
            
    def update_status(self, message):
        self.root.after(0, lambda: self.status_label.config(text=message))
        
    def update_perf_info(self, message):
        self.root.after(0, lambda: self.perf_label.config(text=message))
        
    def show_error(self, message):
        self.root.after(0, lambda: messagebox.showerror("エラー", message))
        
    def show_success(self, message):
        self.root.after(0, lambda: messagebox.showinfo("処理完了", message))
        
    def show_info(self, message):
        self.root.after(0, lambda: messagebox.showinfo("情報", message))
        
    def processing_complete(self):
        def complete_ui():
            self.progress.stop()
            self.progress.grid_remove()
            self.progress_percent_label.grid_remove()
            self.process_button.config(state='normal')
            self.status_label.config(text="")
            if hasattr(self, 'start_time'):
                elapsed = time.time() - self.start_time
                self.time_label.config(text=f"✅ 完了 (総処理時間: {elapsed:.2f}秒)")
        
        self.root.after(0, complete_ui)
        self.processing = False

if __name__ == "__main__":
    app = FixedAssetCalculatorGUI()
    app.root.mainloop()