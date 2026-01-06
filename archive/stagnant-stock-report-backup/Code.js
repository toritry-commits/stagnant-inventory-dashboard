/**
 * ============================================================================
 * 滞留在庫ダッシュボード 自動更新スクリプト
 * ============================================================================
 *
 * 【このスクリプトの目的】
 * BigQuery（Googleの大規模データ分析サービス）にある滞留在庫データを
 * スプレッドシートに自動で取り込み、減損対象の在庫を可視化します。
 *
 * 【主な機能】
 * 1. BigQueryから滞留在庫データを取得してスプレッドシートに書き込み
 *    ※取得時点で以下の条件で絞り込み（パフォーマンス向上）
 *    - 簿価累計 > 0（減損対象があるSKUのみ）
 *    - 除外フラグが「除外」を含まない
 * 2. 簿価累計が0になったSKUを「削除履歴」シートに自動保存
 * 3. 手入力データ（BP列以降）をSKU IDで紐づけて保持
 * 4. 商品画像の自動表示
 * 5. 表示モードの切り替え（期末1のみ / 期末1・2のみ / 全期間）
 * 6. 毎朝9時の自動更新
 *
 * 【処理の流れ（11ステップ）】
 * Step 1:  フィルターを解除
 * Step 2:  手入力データを一時保存
 * Step 3:  現在のデータを保存（削除検出用）
 * Step 4:  既存データをクリア
 * Step 5:  BigQueryからデータ取得・書き込み（絞り込み済み）
 * Step 6:  削除されたSKUを履歴シートに保存
 * Step 7:  手入力データを復元
 * Step 8:  期末1減損予定簿価で降順ソート
 * Step 9:  画像を設定
 * Step 10: フィルターを設定
 * Step 11: 表示モードを設定
 *
 * 【スプレッドシートの構成】
 * - A列: 商品画像（IMAGE関数）
 * - B〜BO列: BigQueryから取得したデータ（66カラム）
 * - BP列以降: 手入力データ（更新しても保持される）
 *
 * 【使い方】
 * 1. スプレッドシートを開く
 * 2. メニュー「滞留在庫レポート」から操作
 *    - 「今すぐ更新」: 手動でデータを更新
 *    - 「表示モード」: 表示する列を切り替え
 *    - 「毎朝9時トリガーを設定」: 自動更新を有効化
 *
 * 【注意事項】
 * - 1行目のヘッダーは変更されません（日本語ヘッダーを維持）
 * - 処理時間は約1〜3分（データ量による）
 * - 手入力データはSKU IDで紐づけて復元されます
 * - BigQueryで絞り込み済みのため、除外対象はシートに表示されません
 *
 * @author CLAS経理部
 * @version 1.2.0
 * @lastUpdated 2026-01-06
 */

// ============================================================================
// 設定値（プロジェクトに応じて変更する部分）
// ============================================================================

/**
 * BigQuery接続設定
 * - PROJECT_ID: Google CloudのプロジェクトID
 * - LOCATION: BigQueryのデータセットがあるリージョン
 */
const BIGQUERY_PROJECT_ID = 'clas-analytics';
const BIGQUERY_LOCATION = 'asia-northeast1';  // 東京リージョン

/**
 * スプレッドシートのシート名
 * - SHEET_NAME: メインのデータシート
 * - DELETE_HISTORY_SHEET_NAME: 削除されたSKUの履歴シート
 */
const SHEET_NAME = '滞留在庫レポート';
const DELETE_HISTORY_SHEET_NAME = '削除履歴';

// ============================================================================
// 列番号の定義
// ============================================================================

/**
 * スプレッドシートの列番号を定義
 * ※スプレッドシートの列は1から始まる（A=1, B=2, C=3...）
 *
 * 【列の構成】
 * A列(1): 商品画像
 * B列(2)〜BO列(67): BigQueryから取得するデータ
 * BP列(68)以降: 手入力データ
 */
const COLS = {
  // --- 基本列 ---
  IMAGE: 1,                        // A列: 商品画像（IMAGE関数で表示）
  DATA_START: 2,                   // B列: BigQueryデータの開始列
  DATA_END: 67,                    // BO列: BigQueryデータの終了列

  // --- 重要な列 ---
  IMAGE_URL: 2,                    // B列: 画像URL（BigQueryのimage_url列）
  EXCLUSION_FLAG: 3,               // C列: 除外フラグ（「除外」が入っている行は非表示）
  SKU_ID: 4,                       // D列: SKU ID（商品を識別する一意のID）

  // --- 集計列 ---
  TERM1_BOOK_VALUE: 51,            // AY列: 期末1の減損予定簿価（ソート基準）
  TERM2_BOOK_VALUE_CUMULATIVE: 56, // BD列: 期末2までの減損予定簿価累計（BigQuery絞り込み基準）

  // --- 手入力列 ---
  MANUAL_INPUT_START: 68           // BP列: 手入力データの開始列
};

/**
 * BigQueryから取得するデータのカラム数
 * B列(2)〜BO列(67) = 66カラム
 */
const BIGQUERY_DATA_COLS = COLS.DATA_END - COLS.DATA_START + 1;

// ============================================================================
// BigQueryクエリ
// ============================================================================

/**
 * BigQueryで実行するSQLクエリ
 *
 * 【クエリの説明】
 * - FROM: 滞留在庫レポートのテーブルから取得
 * - WHERE:
 *   1. 期末2までの簿価累計が0より大きい（減損対象がある）
 *   2. 除外フラグが「除外」を含まない（または空）
 *   ※これらの条件で絞り込むことで、スプレッドシートに書き込むデータ量を削減
 * - ORDER BY: SKU IDで並び替え（後でシート上でソートし直す）
 */
const STAGNANT_STOCK_QUERY = `
SELECT *
FROM \`clas-analytics.mart.stagnant_stock_report\`
WHERE term2_impairment_book_value_cumulative > 0
  AND (exclusion_flag IS NULL OR exclusion_flag NOT LIKE '%除外%')
ORDER BY sku_id ASC
`;

// ============================================================================
// 表示モードの設定
// ============================================================================

/**
 * 「期末1・2のみ」モードで非表示にする列番号のリスト
 *
 * 【非表示にする列の種類】
 * - term3/4/impairedのデータ列（直近の期末だけ見たいため）
 * - 合計・累計の一部（詳細が不要な場合）
 * - 滞留日数平均（通常は不要）
 * - 日付列（通常は不要）
 */
const TERM1_2_HIDE_COLUMNS = [
  // 在庫ID（term3/4/impaired）
  21, 22, 23,  // 商品の在庫ID
  26, 27, 28,  // 余りパーツの在庫ID

  // 数量（term3/4/impaired + 合計）
  31, 32, 33,  // 商品数量
  34,          // 商品合計
  37, 38, 39,  // 余りパーツ数量
  40,          // 余りパーツ合計
  41, 42,      // 合計（term1/2）
  43, 44, 45,  // 合計（term3/4/impaired）
  46,          // 総合計

  // 累計（全期間）
  47, 48,      // 累計（term1/2）
  49, 50,      // 累計（term3/4）

  // 簿価（term2/3/4 + 一部累計）
  52,          // 簿価（term2）- 期末2減損予定簿価
  53, 54,      // 簿価（term3/4）
  55,          // 簿価累計（term1）
  57, 58,      // 簿価累計（term3/4）

  // 平均簿価（term3/4）
  61, 62,

  // 滞留日数平均（通常は不要）
  63,          // BK列: 滞留日数平均切上

  // 日付（全期間）
  64, 65, 66, 67
];

/**
 * 「期末1のみ」モードで非表示にする列番号のリスト
 *
 * 【非表示にする列の種類】
 * TERM1_2_HIDE_COLUMNS に加えて、期末2関連の列も非表示にする
 * - T列(20): 期末2の商品在庫ID
 * - Y列(25): 期末2の余りパーツ在庫ID
 * - AD列(30): 期末2の商品数量
 * - AJ列(36): 期末2の余りパーツ数量
 * - BD列(56): 期末2までの減損予定簿価累計
 * - BH列(60): 期末2の平均簿価
 */
const TERM1_ONLY_HIDE_COLUMNS = [
  // TERM1_2_HIDE_COLUMNS の内容をすべて含む
  ...TERM1_2_HIDE_COLUMNS,

  // 期末2関連の列を追加で非表示
  20,          // T列: 期末2の商品在庫ID
  25,          // Y列: 期末2の余りパーツ在庫ID
  30,          // AD列: 期末2の商品数量
  36,          // AJ列: 期末2の余りパーツ数量
  56,          // BD列: 期末2までの減損予定簿価累計
  60           // BH列: 期末2の平均簿価
];

// ============================================================================
// メニュー設定
// スプレッドシートを開いたときに表示されるカスタムメニュー
// ============================================================================

/**
 * スプレッドシートを開いたときに自動実行される関数
 * カスタムメニューを作成してメニューバーに追加する
 *
 * 【メニュー構成】
 * 滞留在庫レポート
 * ├── 期末1の列だけ表示
 * ├── 期末1・2の列だけ表示
 * ├── すべての列を表示
 * ├── ─────────
 * └── データを今すぐ更新
 */
function onOpen() {
  const ui = SpreadsheetApp.getUi();

  ui.createMenu('滞留在庫レポート')
    .addItem('期末1の列だけ表示', 'setViewModeTerm1Only')
    .addItem('期末1・2の列だけ表示', 'setViewModeTerm1And2')
    .addItem('すべての列を表示', 'setViewModeFullPeriod')
    .addSeparator()
    .addItem('データを今すぐ更新', 'updateStagnantStockReport')
    .addToUi();
}

// ============================================================================
// メイン処理
// ============================================================================

/**
 * 滞留在庫レポートを更新する（手動実行用）
 *
 * 【処理内容】
 * 1. 画面右下に「処理中」のメッセージを表示
 * 2. executeUpdate()を呼び出してデータを更新
 * 3. 完了したら処理時間を表示
 *
 * 【呼び出し方】
 * メニュー「滞留在庫レポート」→「今すぐ更新」
 */
function updateStagnantStockReport() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  try {
    // 処理開始を通知（-1は「閉じるまで表示し続ける」の意味）
    ss.toast('滞留在庫レポートの更新を開始します...', '処理中', -1);

    // 処理時間を計測
    const startTime = new Date();
    const totalRows = executeUpdate(ss);
    const endTime = new Date();
    const elapsedSeconds = Math.round((endTime - startTime) / 1000);

    // 完了を通知
    ss.toast(`更新完了: ${totalRows}行を書き込みました（${elapsedSeconds}秒）`, '完了', 10);
    Logger.log(`滞留在庫レポート更新完了: ${totalRows}行, ${elapsedSeconds}秒`);

  } catch (error) {
    // エラーが発生した場合
    Logger.log(`エラー発生: ${error.message}`);
    Logger.log(`スタックトレース: ${error.stack}`);
    ss.toast(`エラー: ${error.message}`, 'エラー', 10);
    throw error;
  }
}

/**
 * 滞留在庫レポートを更新する（自動実行用）
 *
 * 【手動実行との違い】
 * - toast通知を表示しない（バックグラウンドで実行されるため画面がない）
 * - ログにのみ結果を出力
 *
 * 【呼び出し方】
 * 毎朝9時のトリガーから自動的に呼び出される
 */
function updateStagnantStockReportAuto() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  try {
    const startTime = new Date();
    const totalRows = executeUpdate(ss);
    const endTime = new Date();
    const elapsedSeconds = Math.round((endTime - startTime) / 1000);

    Logger.log(`[自動実行] 滞留在庫レポート更新完了: ${totalRows}行, ${elapsedSeconds}秒`);

  } catch (error) {
    Logger.log(`[自動実行] エラー発生: ${error.message}`);
    Logger.log(`スタックトレース: ${error.stack}`);
    throw error;
  }
}

// ============================================================================
// 更新処理の実行（メインロジック）
// ============================================================================

/**
 * データ更新のメイン処理
 * 11のステップでデータを更新する
 *
 * @param {SpreadsheetApp.Spreadsheet} ss - スプレッドシートオブジェクト
 * @returns {number} 書き込んだ行数
 */
function executeUpdate(ss) {
  // ----- 準備 -----
  // シートを取得（なければ作成）
  let sheet = ss.getSheetByName(SHEET_NAME);
  if (!sheet) {
    sheet = ss.insertSheet(SHEET_NAME);
    Logger.log(`シート「${SHEET_NAME}」を作成しました`);
  }

  // 現在のデータ範囲を取得
  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getLastColumn();

  // ----- Step 1: フィルターを解除 -----
  // フィルターがかかっていると一部の行が非表示になり、正しく処理できないため
  Logger.log('Step 1: フィルターを解除中...');
  removeFilter(sheet);

  // ----- Step 2: 手入力データを保存 -----
  // BP列以降に手入力されたデータをSKU IDと紐づけて一時保存
  // 更新後に同じSKU IDの行に復元する
  Logger.log('Step 2: BP列以降の手入力データを保存中...');
  const savedManualData = saveManualInputData(sheet, lastRow, lastCol);
  Logger.log(`${Object.keys(savedManualData.data).length}件のSKU IDに紐づくデータを保存しました`);

  // ----- Step 3: 現在のデータを保存（削除検出用） -----
  // 更新前と更新後を比較して、消えたSKUを検出するため
  Logger.log('Step 3: 現在のデータを保存中（削除検出用）...');
  const beforeData = saveCurrentSheetData(sheet, lastRow, lastCol);
  Logger.log(`${beforeData.skuIds.size}件のSKU IDを保存しました`);

  // ----- Step 4: 既存データをクリア -----
  // 新しいデータを書き込む前に、既存のデータを消す
  // ※1行目（ヘッダー）は触らない
  Logger.log('Step 4: 既存データをクリア中（B〜BO列、A列）...');
  clearExistingData(sheet, lastRow, lastCol);

  // ----- Step 5: BigQueryからデータ取得・書き込み -----
  // 簿価累計 > 0 のデータだけ取得
  Logger.log('Step 5: BigQueryクエリを実行中（簿価累計>0のみ取得）...');
  const totalRows = executeBigQueryJobWithStreaming(STAGNANT_STOCK_QUERY, sheet);

  // ----- Step 6: 削除されたSKUを履歴シートに保存 -----
  // 前回あったけど今回なくなったSKUを検出して履歴に保存
  Logger.log('Step 6: 削除されたSKUを検出・履歴保存中...');
  saveDeletedSkusToHistory(ss, sheet, beforeData, totalRows, lastCol);

  // ----- Step 7: 手入力データを復元 -----
  // Step 2で保存したデータを、新しいSKU IDの位置に復元
  Logger.log('Step 7: BP列以降の手入力データを復元中...');
  const restoredCount = restoreManualInputData(sheet, savedManualData, totalRows);
  Logger.log(`${restoredCount}件のデータを復元しました`);

  // ----- Step 8: データをソート -----
  // 期末1の減損予定簿価が大きい順に並び替え
  Logger.log('Step 8: AY列（期末1減損予定簿価）で降順ソート中...');
  sortByBookValueCumulative(sheet, totalRows, lastCol);

  // ----- Step 9: 画像を設定 -----
  // A列にIMAGE関数を設定（除外フラグがない行のみ）
  Logger.log('Step 9: A列にIMAGE関数を設定中...');
  if (totalRows > 0) {
    setImageFormulas(sheet, totalRows);
  }

  // ----- Step 10: フィルターを設定 -----
  // 全列にフィルターを設定（絞り込みができるように）
  // ※フィルター条件の自動設定は削除（BigQueryで既に絞り込み済みのため）
  Logger.log('Step 10: フィルターを設定中...');
  createFilter(sheet, totalRows);

  // ----- Step 11: 表示モードを設定 -----
  // デフォルトで「期末1のみ」モードを適用
  Logger.log('Step 11: 表示モードを「期末1のみ」に設定中...');
  applyViewModeTerm1Only(sheet);
  Logger.log('表示モードを設定しました');

  // 変更をスプレッドシートに反映
  SpreadsheetApp.flush();

  return totalRows;
}

// ============================================================================
// ヘルパー関数（executeUpdateから呼び出される補助関数）
// ============================================================================

/**
 * フィルターを解除する
 * @param {SpreadsheetApp.Sheet} sheet - シート
 */
function removeFilter(sheet) {
  const existingFilter = sheet.getFilter();
  if (existingFilter) {
    existingFilter.remove();
    Logger.log('フィルターを解除しました');
  } else {
    Logger.log('フィルターは設定されていませんでした');
  }
}

/**
 * 既存データをクリアする
 * @param {SpreadsheetApp.Sheet} sheet - シート
 * @param {number} lastRow - 最終行
 * @param {number} lastCol - 最終列
 */
function clearExistingData(sheet, lastRow, lastCol) {
  if (lastRow <= 1) return;

  // A列（IMAGE関数）をクリア
  sheet.getRange(2, COLS.IMAGE, lastRow - 1, 1).clearContent();

  // B〜BO列（BigQueryデータ）をクリア
  sheet.getRange(2, COLS.DATA_START, lastRow - 1, BIGQUERY_DATA_COLS).clearContent();

  // BP列以降もクリア（後で復元する）
  if (lastCol >= COLS.MANUAL_INPUT_START) {
    const manualColCount = lastCol - COLS.MANUAL_INPUT_START + 1;
    sheet.getRange(2, COLS.MANUAL_INPUT_START, lastRow - 1, manualColCount).clearContent();
  }

  Logger.log(`${lastRow - 1}行のデータをクリアしました`);
}

/**
 * 削除されたSKUを検出して履歴シートに保存
 * @param {SpreadsheetApp.Spreadsheet} ss - スプレッドシート
 * @param {SpreadsheetApp.Sheet} sheet - シート
 * @param {Object} beforeData - 更新前のデータ
 * @param {number} totalRows - 新しいデータの行数
 * @param {number} lastCol - 最終列
 */
function saveDeletedSkusToHistory(ss, sheet, beforeData, totalRows, lastCol) {
  if (totalRows <= 0 || beforeData.skuIds.size === 0) return;

  // 新しいSKU IDを取得
  const afterSkuIds = new Set();
  const newSkuData = sheet.getRange(2, COLS.SKU_ID, totalRows, 1).getValues();
  for (const row of newSkuData) {
    if (row[0]) afterSkuIds.add(row[0]);
  }

  // 削除されたSKUを検出
  const deletedSkuIds = detectDeletedSkus(beforeData.skuIds, afterSkuIds);
  Logger.log(`削除されたSKU: ${deletedSkuIds.length}件`);

  // 履歴シートに保存
  if (deletedSkuIds.length > 0) {
    saveToDeleteHistory(ss, sheet, deletedSkuIds, beforeData, lastCol);
  }
}

/**
 * データを期末1減損予定簿価で降順ソート
 * AY列（期末1の減損予定簿価）が大きい順に並び替える
 *
 * @param {SpreadsheetApp.Sheet} sheet - シート
 * @param {number} totalRows - データ行数
 * @param {number} lastCol - 最終列
 */
function sortByBookValueCumulative(sheet, totalRows, lastCol) {
  if (totalRows <= 0) return;

  const newLastCol = Math.max(lastCol, COLS.DATA_END);
  const sortRange = sheet.getRange(2, 1, totalRows, newLastCol);
  sortRange.sort({ column: COLS.TERM1_BOOK_VALUE, ascending: false });
  Logger.log('ソート完了（AY列: 期末1減損予定簿価で降順）');
}

/**
 * フィルターを作成
 * @param {SpreadsheetApp.Sheet} sheet - シート
 * @param {number} totalRows - データ行数
 */
function createFilter(sheet, totalRows) {
  if (totalRows <= 0) return;

  const filterLastCol = Math.max(sheet.getLastColumn(), COLS.DATA_END);
  const filterRange = sheet.getRange(1, 1, totalRows + 1, filterLastCol);
  filterRange.createFilter();
  Logger.log(`フィルターを設定しました（${filterLastCol}列まで）`);
}

// ============================================================================
// 手入力データの保存・復元
// BP列以降に入力されたデータをSKU IDと紐づけて保持する
// ============================================================================

/**
 * BP列以降の手入力データをSKU IDと紐づけて保存
 *
 * 【処理内容】
 * 1. D列（SKU ID）からBP列以降のデータを一括取得
 * 2. SKU IDをキーにしてデータを保存
 * 3. 空白行は保存しない（容量削減）
 *
 * @param {SpreadsheetApp.Sheet} sheet - シート
 * @param {number} lastRow - 最終行
 * @param {number} lastCol - 最終列
 * @returns {Object} { data: {skuId: [values...]}, colCount: number }
 */
function saveManualInputData(sheet, lastRow, lastCol) {
  const result = { data: {}, colCount: 0 };

  // BP列以降のデータがない場合は空で返す
  if (lastRow <= 1 || lastCol < COLS.MANUAL_INPUT_START) {
    return result;
  }

  // D列（SKU ID）から最終列まで一括取得（API呼び出し削減のため）
  const startCol = COLS.SKU_ID;
  const totalCols = lastCol - startCol + 1;
  const allData = sheet.getRange(2, startCol, lastRow - 1, totalCols).getValues();

  // BP列以降のカラム数を計算
  const manualColCount = lastCol - COLS.MANUAL_INPUT_START + 1;
  result.colCount = manualColCount;

  // インデックス計算
  const skuIdIndex = 0;  // D列が開始列なのでindex 0
  const manualStartIndex = COLS.MANUAL_INPUT_START - startCol;  // BP列の開始インデックス

  // SKU IDをキーにしてデータを保存
  for (let i = 0; i < allData.length; i++) {
    const skuId = allData[i][skuIdIndex];
    if (!skuId) continue;

    // BP列以降のデータを抽出
    const rowData = allData[i].slice(manualStartIndex);

    // データが1つでもあれば保存
    const hasData = rowData.some(cell => cell !== '' && cell !== null && cell !== undefined);
    if (hasData) {
      result.data[skuId] = rowData;
    }
  }

  return result;
}

/**
 * 手入力データをSKU IDで照合して復元
 *
 * @param {SpreadsheetApp.Sheet} sheet - シート
 * @param {Object} savedData - 保存データ
 * @param {number} totalRows - 新しいデータの行数
 * @returns {number} 復元した件数
 */
function restoreManualInputData(sheet, savedData, totalRows) {
  // 復元するデータがなければ終了
  if (totalRows <= 0 || Object.keys(savedData.data).length === 0 || savedData.colCount === 0) {
    return 0;
  }

  // 新しいSKU ID列を取得
  const newSkuIds = sheet.getRange(2, COLS.SKU_ID, totalRows, 1).getValues();

  // 復元データを作成
  const restoreData = [];
  let restoredCount = 0;

  for (let i = 0; i < newSkuIds.length; i++) {
    const skuId = newSkuIds[i][0];

    if (skuId && savedData.data[skuId]) {
      // 保存していたデータを復元
      restoreData.push(savedData.data[skuId]);
      restoredCount++;
    } else {
      // 該当するデータがない場合は空配列
      restoreData.push(new Array(savedData.colCount).fill(''));
    }
  }

  // BP列以降にデータを書き込み
  if (restoreData.length > 0) {
    sheet.getRange(2, COLS.MANUAL_INPUT_START, restoreData.length, savedData.colCount).setValues(restoreData);
  }

  return restoredCount;
}

// ============================================================================
// 削除履歴シートの処理
// 簿価累計が0になったSKU（=減損完了）を履歴として保存
// ============================================================================

/**
 * 現在のシートから全データを取得（削除検出用）
 *
 * @param {SpreadsheetApp.Sheet} sheet - シート
 * @param {number} lastRow - 最終行
 * @param {number} lastCol - 最終列
 * @returns {Object} { skuIds: Set, dataBySkuId: {skuId: [rowData]} }
 */
function saveCurrentSheetData(sheet, lastRow, lastCol) {
  const result = { skuIds: new Set(), dataBySkuId: {} };

  if (lastRow <= 1) {
    return result;
  }

  // B列〜最終列のデータを一括取得
  const dataColCount = lastCol - COLS.DATA_START + 1;
  const allData = sheet.getRange(2, COLS.DATA_START, lastRow - 1, dataColCount).getValues();

  // SKU ID列のインデックス（D列 = 4、B列起点なのでindex = 2）
  const skuIdIndex = COLS.SKU_ID - COLS.DATA_START;

  for (let i = 0; i < allData.length; i++) {
    const skuId = allData[i][skuIdIndex];
    if (skuId) {
      result.skuIds.add(skuId);
      result.dataBySkuId[skuId] = allData[i];
    }
  }

  return result;
}

/**
 * 削除されたSKUを検出
 *
 * @param {Set} beforeSkuIds - 更新前のSKU IDセット
 * @param {Set} afterSkuIds - 更新後のSKU IDセット
 * @returns {Array} 削除されたSKU IDの配列
 */
function detectDeletedSkus(beforeSkuIds, afterSkuIds) {
  const deletedSkuIds = [];

  for (const skuId of beforeSkuIds) {
    if (!afterSkuIds.has(skuId)) {
      deletedSkuIds.push(skuId);
    }
  }

  return deletedSkuIds;
}

/**
 * 削除履歴シートを取得または作成
 *
 * @param {SpreadsheetApp.Spreadsheet} ss - スプレッドシート
 * @param {SpreadsheetApp.Sheet} sourceSheet - 元シート（ヘッダーコピー用）
 * @returns {SpreadsheetApp.Sheet} 削除履歴シート
 */
function getOrCreateDeleteHistorySheet(ss, sourceSheet) {
  let historySheet = ss.getSheetByName(DELETE_HISTORY_SHEET_NAME);

  if (!historySheet) {
    // シートを新規作成
    historySheet = ss.insertSheet(DELETE_HISTORY_SHEET_NAME);
    Logger.log(`「${DELETE_HISTORY_SHEET_NAME}」シートを作成しました`);

    // 元シートからヘッダーをコピー（1行目）
    const sourceLastCol = sourceSheet.getLastColumn();
    if (sourceLastCol > 0) {
      const headerValues = sourceSheet.getRange(1, 1, 1, sourceLastCol).getValues();
      historySheet.getRange(1, 1, 1, sourceLastCol).setValues(headerValues);

      // 削除日時列のヘッダーを追加
      historySheet.getRange(1, sourceLastCol + 1).setValue('削除日時');
    }

    Logger.log('ヘッダーをコピーしました');
  }

  return historySheet;
}

/**
 * 削除されたSKUを履歴シートに保存
 *
 * @param {SpreadsheetApp.Spreadsheet} ss - スプレッドシート
 * @param {SpreadsheetApp.Sheet} sourceSheet - 元シート
 * @param {Array} deletedSkuIds - 削除されたSKU IDの配列
 * @param {Object} beforeData - 更新前のデータ
 * @param {number} sourceLastCol - 元シートの最終列
 */
function saveToDeleteHistory(ss, sourceSheet, deletedSkuIds, beforeData, sourceLastCol) {
  if (deletedSkuIds.length === 0) {
    Logger.log('削除されたSKUはありません');
    return;
  }

  // 削除履歴シートを取得または作成
  const historySheet = getOrCreateDeleteHistorySheet(ss, sourceSheet);
  const historyLastRow = historySheet.getLastRow();

  // 削除日時を作成
  const deleteDate = new Date();
  const deleteDateStr = Utilities.formatDate(deleteDate, 'Asia/Tokyo', 'yyyy-MM-dd HH:mm:ss');

  // 転記データを作成
  const rowsToAdd = [];

  for (const skuId of deletedSkuIds) {
    const rowData = beforeData.dataBySkuId[skuId];
    if (rowData) {
      // A列は空白（画像なし）、B列〜最終列はデータ、その次に削除日時
      const newRow = ['', ...rowData, deleteDateStr];
      rowsToAdd.push(newRow);
    }
  }

  // 履歴シートに追記
  if (rowsToAdd.length > 0) {
    const startRow = historyLastRow + 1;
    const numCols = rowsToAdd[0].length;
    historySheet.getRange(startRow, 1, rowsToAdd.length, numCols).setValues(rowsToAdd);
    Logger.log(`${rowsToAdd.length}件のSKUを削除履歴に保存しました`);
  }
}

// ============================================================================
// BigQuery連携
// 大量データを効率的に取得してスプレッドシートに書き込む
// ============================================================================

/**
 * BigQueryクエリを実行し、結果をスプレッドシートに書き込む
 *
 * 【処理の流れ】
 * 1. BigQueryにジョブを送信
 * 2. ジョブが完了するまで待機（最大5分）
 * 3. 結果をページ単位で取得してシートに書き込み
 *
 * 【ストリーミング書き込みの理由】
 * - BigQueryの結果は1000行ずつページングされる
 * - 全件をメモリに溜めると、大量データでメモリ不足になる
 * - ページごとに書き込むことでメモリを節約
 *
 * @param {string} query - SQLクエリ
 * @param {SpreadsheetApp.Sheet} sheet - 書き込み先シート
 * @returns {number} 書き込んだ総行数
 */
function executeBigQueryJobWithStreaming(query, sheet) {
  try {
    // ----- ジョブを作成 -----
    Logger.log('BigQueryジョブを作成中...');

    const job = {
      configuration: {
        query: {
          query: query,
          useLegacySql: false  // 標準SQLを使用
        }
      },
      jobReference: {
        projectId: BIGQUERY_PROJECT_ID,
        location: BIGQUERY_LOCATION
      }
    };

    const insertedJob = BigQuery.Jobs.insert(job, BIGQUERY_PROJECT_ID);
    const jobId = insertedJob.jobReference.jobId;
    Logger.log(`ジョブID: ${jobId}`);

    // ----- ジョブ完了まで待機 -----
    let jobStatus = insertedJob;
    let retryCount = 0;
    const maxRetries = 300;  // 最大5分（1秒 × 300回）

    while (jobStatus.status.state !== 'DONE' && retryCount < maxRetries) {
      // 10秒ごとに進捗をログ出力
      if (retryCount % 10 === 0) {
        Logger.log(`ジョブ実行中... 状態: ${jobStatus.status.state} (${retryCount}秒経過)`);
      }

      Utilities.sleep(1000);  // 1秒待機
      jobStatus = BigQuery.Jobs.get(BIGQUERY_PROJECT_ID, jobId, { location: BIGQUERY_LOCATION });
      retryCount++;
    }

    // タイムアウトチェック
    if (jobStatus.status.state !== 'DONE') {
      throw new Error('BigQueryジョブがタイムアウトしました（5分経過）');
    }

    // エラーチェック
    if (jobStatus.status.errorResult) {
      throw new Error(`BigQueryエラー: ${jobStatus.status.errorResult.message}`);
    }

    Logger.log('ジョブ完了。結果を取得・書き込み中...');

    // ----- 結果をページ単位で取得・書き込み -----
    let pageToken = null;
    let pageCount = 0;
    let currentRow = 2;  // 2行目から書き込み（1行目はヘッダー）
    let totalRows = 0;

    do {
      // 結果を取得
      const options = { location: BIGQUERY_LOCATION };
      if (pageToken) {
        options.pageToken = pageToken;
      }

      const queryResults = BigQuery.Jobs.getQueryResults(BIGQUERY_PROJECT_ID, jobId, options);

      // 最初のページで総行数をログ出力
      if (pageCount === 0) {
        Logger.log(`総行数: ${queryResults.totalRows}`);
      }

      // データがあれば書き込み
      if (queryResults.rows && queryResults.rows.length > 0) {
        const data = convertBigQueryRowsToValues(queryResults.rows);

        // B〜BO列にデータを書き込み
        sheet.getRange(currentRow, COLS.DATA_START, data.length, data[0].length).setValues(data);

        currentRow += data.length;
        totalRows += data.length;

        Logger.log(`ページ${pageCount + 1}書き込み完了。累計${totalRows}行`);
      }

      pageToken = queryResults.pageToken;
      pageCount++;

    } while (pageToken);

    Logger.log(`BigQueryデータ書き込み完了: 合計${totalRows}行, ${pageCount}ページ`);

    return totalRows;

  } catch (error) {
    Logger.log(`BigQuery実行エラー: ${error.message}`);
    throw error;
  }
}

/**
 * BigQueryの行データをスプレッドシート用の配列に変換
 *
 * @param {Array} rows - BigQueryの行データ
 * @returns {Array} 2次元配列
 */
function convertBigQueryRowsToValues(rows) {
  return rows.map(row => {
    return row.f.map(cell => {
      const value = cell.v;

      // null/undefinedは空文字に変換
      if (value === null || value === undefined) {
        return '';
      }

      // 真偽値の変換
      if (value === true || value === 'true') {
        return true;
      }
      if (value === false || value === 'false') {
        return false;
      }

      return value;
    });
  });
}

// ============================================================================
// 画像・フィルター設定
// ============================================================================

/**
 * A列にIMAGE関数を設定
 *
 * 【IMAGE関数について】
 * =IMAGE("URL", 1) でセル内に画像を表示できる
 * 第2引数の1は「セルに合わせてリサイズ」の意味
 *
 * 【補足】
 * 除外フラグのチェックは不要（BigQueryで除外済みのデータしか取得していないため）
 *
 * @param {SpreadsheetApp.Sheet} sheet - シート
 * @param {number} totalRows - データ行数
 */
function setImageFormulas(sheet, totalRows) {
  // B列（画像URL）を取得
  const imageUrls = sheet.getRange(2, COLS.IMAGE_URL, totalRows, 1).getValues();

  let imageCount = 0;
  const imageFormulas = [];

  for (let i = 0; i < totalRows; i++) {
    const imageUrl = imageUrls[i][0];

    // 画像URLがあれば設定（除外フラグはBigQueryで既に除外済み）
    if (imageUrl && String(imageUrl).startsWith('http')) {
      imageFormulas.push([`=IMAGE("${imageUrl}", 1)`]);
      imageCount++;
    } else {
      imageFormulas.push(['']);
    }
  }

  // A列にIMAGE関数を一括設定
  sheet.getRange(2, COLS.IMAGE, imageFormulas.length, 1).setFormulas(imageFormulas);

  Logger.log(`IMAGE関数設定: ${imageCount}件（全${totalRows}件中）`);
}

/**
 * 【注意】フィルター条件の自動設定は削除しました
 *
 * 以前はC列（除外フラグ）の「除外」を含む行を非表示にする条件を
 * 自動設定していましたが、BigQueryのWHERE句で除外済みのデータのみ
 * 取得するようにしたため、この処理は不要になりました。
 *
 * 必要に応じて、ユーザーが手動でフィルター条件を設定できます。
 */

// ============================================================================
// 表示モード切替
// 期末1のみ / 期末1・2のみ / 全期間 の3つのモードを切り替える
// ============================================================================

/**
 * 表示モードの定義
 * - name: 完了メッセージに表示するモード名
 * - hideColumns: 非表示にする列番号のリスト（nullの場合は全列表示）
 */
const VIEW_MODES = {
  TERM1_ONLY: {
    name: '期末1の列だけ表示',
    hideColumns: TERM1_ONLY_HIDE_COLUMNS
  },
  TERM1_AND_2: {
    name: '期末1・2の列だけ表示',
    hideColumns: TERM1_2_HIDE_COLUMNS
  },
  FULL_PERIOD: {
    name: 'すべての列を表示',
    hideColumns: null  // 全列表示
  }
};

/**
 * 表示モードを切り替える共通処理
 *
 * @param {Object} mode - VIEW_MODESのいずれか
 */
function switchViewMode(mode) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(SHEET_NAME);

  if (!sheet) {
    ss.toast('シートが見つかりません', 'エラー', 5);
    return;
  }

  ss.toast('表示モードを切り替え中...', '処理中', -1);

  try {
    applyViewMode(sheet, mode);
    ss.toast(`${mode.name}モードに切り替えました`, '完了', 5);
    Logger.log(`表示モード: ${mode.name}`);

  } catch (error) {
    Logger.log(`表示モード切替エラー: ${error.message}`);
    ss.toast(`エラー: ${error.message}`, 'エラー', 10);
  }
}

/**
 * 表示モードを適用する共通処理（内部処理用）
 *
 * @param {SpreadsheetApp.Sheet} sheet - シート
 * @param {Object} mode - VIEW_MODESのいずれか
 */
function applyViewMode(sheet, mode) {
  // まず全列を表示
  showAllColumns(sheet);

  // 非表示列が指定されていれば非表示にする
  if (mode.hideColumns) {
    hideColumnsList(sheet, mode.hideColumns);
  }
}

// ----- メニューから呼び出される関数 -----

/**
 * 「期末1のみ」モードに切り替え（メニューから呼び出し）
 * 期末1関連の列だけを表示し、期末2以降の列も非表示にする
 */
function setViewModeTerm1Only() {
  switchViewMode(VIEW_MODES.TERM1_ONLY);
}

/**
 * 「期末1・2のみ」モードに切り替え（メニューから呼び出し）
 * 期末1・2関連の列を表示し、期末3以降の列を非表示にする
 */
function setViewModeTerm1And2() {
  switchViewMode(VIEW_MODES.TERM1_AND_2);
}

/**
 * 「全期間」モードに切り替え（メニューから呼び出し）
 * すべての列を表示する
 */
function setViewModeFullPeriod() {
  switchViewMode(VIEW_MODES.FULL_PERIOD);
}

// ----- 内部処理用の適用関数（executeUpdateから呼び出し） -----

/**
 * 「期末1のみ」モードを適用（内部処理用）
 * @param {SpreadsheetApp.Sheet} sheet - シート
 */
function applyViewModeTerm1Only(sheet) {
  applyViewMode(sheet, VIEW_MODES.TERM1_ONLY);
}

/**
 * 「期末1・2のみ」モードを適用（内部処理用）
 * @param {SpreadsheetApp.Sheet} sheet - シート
 */
function applyViewModeTerm1And2(sheet) {
  applyViewMode(sheet, VIEW_MODES.TERM1_AND_2);
}

/**
 * 「全期間」モードを適用（内部処理用）
 * @param {SpreadsheetApp.Sheet} sheet - シート
 */
function applyViewModeFullPeriod(sheet) {
  applyViewMode(sheet, VIEW_MODES.FULL_PERIOD);
}

// ============================================================================
// 列表示/非表示ユーティリティ
// ============================================================================

/**
 * 指定した列を非表示にする（パフォーマンス最適化版）
 *
 * 【最適化のポイント】
 * 連続する列をグループ化して一括で非表示にする
 * 例: [21,22,23,26,27] → グループ1: 21-23（3列）、グループ2: 26-27（2列）
 * これにより、5回のAPI呼び出しが2回に削減される
 *
 * @param {SpreadsheetApp.Sheet} sheet - シート
 * @param {number[]} columns - 非表示にする列番号のリスト
 */
function hideColumnsList(sheet, columns) {
  // 重複を除去してソート
  const uniqueColumns = [...new Set(columns)].sort((a, b) => a - b);
  const maxCol = sheet.getMaxColumns();

  // 連続する列をグループ化
  const groups = [];
  let start = null;
  let prev = null;

  for (const col of uniqueColumns) {
    if (col < 1 || col > maxCol) continue;

    if (start === null) {
      // 新しいグループを開始
      start = col;
      prev = col;
    } else if (col === prev + 1) {
      // 連続している場合は継続
      prev = col;
    } else {
      // 連続が途切れたらグループを保存して新しいグループを開始
      groups.push({ start, count: prev - start + 1 });
      start = col;
      prev = col;
    }
  }

  // 最後のグループを保存
  if (start !== null) {
    groups.push({ start, count: prev - start + 1 });
  }

  // グループごとに一括非表示
  for (const group of groups) {
    sheet.hideColumns(group.start, group.count);
  }

  Logger.log(`列非表示: ${uniqueColumns.length}列を${groups.length}回のAPI呼び出しで処理`);
}

/**
 * 全列を表示する
 *
 * @param {SpreadsheetApp.Sheet} sheet - シート
 */
function showAllColumns(sheet) {
  const maxCol = sheet.getMaxColumns();
  if (maxCol > 0) {
    sheet.showColumns(1, maxCol);
  }
}

// ============================================================================
// トリガー管理
// 毎朝9時の自動実行を設定/解除する
// ※メニューには表示せず、スクリプトエディタから直接実行する
// ============================================================================

/**
 * 毎朝9時の自動実行トリガーを設定
 *
 * 【トリガーとは】
 * 指定した時刻に自動でスクリプトを実行する仕組み
 * 設定すると、毎朝9時にupdateStagnantStockReportAuto()が実行される
 *
 * 【実行方法】
 * スクリプトエディタで この関数を選択して実行ボタンをクリック
 */
function setupDailyTrigger() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  try {
    // 既存のトリガーを削除（重複防止）
    const triggers = ScriptApp.getProjectTriggers();
    let removed = 0;

    for (const trigger of triggers) {
      if (trigger.getHandlerFunction() === 'updateStagnantStockReportAuto') {
        ScriptApp.deleteTrigger(trigger);
        removed++;
      }
    }

    if (removed > 0) {
      Logger.log(`既存のトリガーを${removed}件削除しました`);
    }

    // 新しいトリガーを作成
    ScriptApp.newTrigger('updateStagnantStockReportAuto')
      .timeBased()
      .atHour(9)
      .everyDays(1)
      .inTimezone('Asia/Tokyo')
      .create();

    Logger.log('毎朝9時のトリガーを設定しました');
    ss.toast('毎朝9時の自動更新トリガーを設定しました', '設定完了', 5);

  } catch (error) {
    Logger.log(`トリガー設定エラー: ${error.message}`);
    ss.toast(`エラー: ${error.message}`, 'エラー', 10);
    throw error;
  }
}

/**
 * 自動実行トリガーを削除
 *
 * 【実行方法】
 * スクリプトエディタで この関数を選択して実行ボタンをクリック
 */
function removeDailyTrigger() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  try {
    const triggers = ScriptApp.getProjectTriggers();
    let removed = 0;

    for (const trigger of triggers) {
      if (trigger.getHandlerFunction() === 'updateStagnantStockReportAuto') {
        ScriptApp.deleteTrigger(trigger);
        removed++;
      }
    }

    if (removed > 0) {
      Logger.log(`トリガーを${removed}件削除しました`);
      ss.toast(`自動更新トリガーを${removed}件削除しました`, '削除完了', 5);
    } else {
      Logger.log('削除対象のトリガーはありませんでした');
      ss.toast('削除対象のトリガーはありませんでした', '情報', 5);
    }

  } catch (error) {
    Logger.log(`トリガー削除エラー: ${error.message}`);
    ss.toast(`エラー: ${error.message}`, 'エラー', 10);
    throw error;
  }
}
