// ============================================================================
// 非稼働SKUリスト出力 GAS
//   BigQuery table (clas-analytics.sandbox.assemblable_nonop_by_sku) を読み、
//   対象スプレッドシートに v2 SKUリストと同じデザインで書き込む。
//   画像は =HYPERLINK(修正済みCDN) の有効リンク。
// ============================================================================
var TARGET_SS_ID = '1luwTZexd-jXGTlrXF8K1pmIeeVRUsdI49PZnGe7id2o';
var SHEET_NAME   = 'SKUリスト(非稼働)';
var BQ_PROJECT   = '513151086449';   // 課金/実行プロジェクト
var BQ_TABLE     = 'clas-analytics.sandbox.assemblable_nonop_by_sku';
var AMARI_SHEET  = '余り在庫リスト';
var AMARI_TABLE  = 'clas-analytics.sandbox.amari_stock_list';

// 全SKU / SKU_ID 昇順 / 管理区分列は除外
function buildQuery_() {
  return 'SELECT * EXCEPT(`管理区分`) FROM `' + BQ_TABLE + '` ORDER BY `SKU_ID`';
}

// =HYPERLINK("url",...) 文字列から画像URLを取り出し =IMAGE(url) 数式に変換
function toImageFormula_(v) {
  if (!v) return '';
  var m = String(v).match(/"(https:\/\/[^"]+)"/);
  if (!m) return '';
  return '=IMAGE("' + m[1] + '", 4, 56, 56)';  // mode4: 高さ56 x 幅56 px
}

// ---- BigQuery REST でクエリ実行(ページング対応) ----
function runBq_(sql) {
  var token = ScriptApp.getOAuthToken();
  var base = 'https://bigquery.googleapis.com/bigquery/v2/projects/' + BQ_PROJECT;
  var res = UrlFetchApp.fetch(base + '/queries', {
    method: 'post', contentType: 'application/json',
    headers: { Authorization: 'Bearer ' + token },
    payload: JSON.stringify({ query: sql, useLegacySql: false, timeoutMs: 60000, maxResults: 10000 }),
    muteHttpExceptions: true
  });
  var data = JSON.parse(res.getContentText());
  if (data.error) throw new Error('BQ error: ' + JSON.stringify(data.error));
  var schema = data.schema.fields;
  var jobId = data.jobReference.jobId;
  var location = data.jobReference.location;
  var rows = data.rows || [];
  var complete = data.jobComplete;
  var pageToken = data.pageToken;
  // 完了待ち + ページング
  while (!complete || pageToken) {
    var url = base + '/queries/' + jobId + '?maxResults=10000&timeoutMs=60000&location=' + location +
      (pageToken ? '&pageToken=' + pageToken : '');
    var r = UrlFetchApp.fetch(url, { headers: { Authorization: 'Bearer ' + token }, muteHttpExceptions: true });
    var d = JSON.parse(r.getContentText());
    if (d.error) throw new Error('BQ page error: ' + JSON.stringify(d.error));
    complete = d.jobComplete;
    if (d.rows) rows = rows.concat(d.rows);
    pageToken = d.pageToken;
    if (complete && !pageToken) break;
  }
  return { schema: schema, rows: rows };
}

function isNumericType_(t) {
  return t === 'INTEGER' || t === 'INT64' || t === 'FLOAT' || t === 'FLOAT64' || t === 'NUMERIC' || t === 'BIGNUMERIC';
}

// ---- メイン: シートに出力 ----
function writeSkuList() {
  var ss = SpreadsheetApp.openById(TARGET_SS_ID);
  var result = runBq_(buildQuery_());
  var schema = result.schema;
  var cols = schema.map(function (f) { return f.name; });
  var numeric = schema.map(function (f) { return isNumericType_(f.type); });

  var imgCol = cols.indexOf('画像'); // 画像列 → =IMAGE に変換
  // 2D配列を構築(ヘッダ + データ)
  var values = [cols];
  result.rows.forEach(function (row) {
    var arr = row.f.map(function (cell, i) {
      var v = cell.v;
      if (i === imgCol) return toImageFormula_(v);
      if (v === null || v === undefined) return '';
      if (numeric[i]) { var n = Number(v); return isNaN(n) ? '' : n; }
      return v;
    });
    values.push(arr);
  });

  var nData = values.length - 1;           // ヘッダ除くデータ行数
  var dataRows = values.slice(1);
  var nCol = cols.length;

  // シート初期化
  var sh = ss.getSheetByName(SHEET_NAME);
  if (sh) sh.clear(); else sh = ss.insertSheet(SHEET_NAME);
  if (sh.getFilter()) sh.getFilter().remove();

  // レイアウト: 1行目=SUBTOTAL合計 / 2行目=ヘッダ / 3行目〜=データ
  var HEADER_ROW = 2, DATA_START = 3, DATA_END = DATA_START + nData - 1;
  sh.getRange(HEADER_ROW, 1, 1, nCol).setValues([cols]);
  if (nData > 0) sh.getRange(DATA_START, 1, nData, nCol).setValues(dataRows);

  // 1行目: パーツ数 / 案件紐づけ数 列に SUBTOTAL(9,...) (フィルタ絞込を反映した合計)
  var row1 = [];
  for (var c = 0; c < nCol; c++) {
    if (nData > 0 && (/パーツ数$/.test(cols[c]) || cols[c] === '貸出中数' || cols[c] === '総パーツ数')) {
      var L = colLetter_(c + 1);
      row1.push('=SUBTOTAL(9,' + L + DATA_START + ':' + L + DATA_END + ')');
    } else {
      row1.push('');
    }
  }
  row1[0] = '合計→';
  sh.getRange(1, 1, 1, nCol).setValues([row1]);

  // ---- 書式 ----
  var lastRow = Math.max(DATA_END, HEADER_ROW);
  sh.getRange(1, 1, lastRow, nCol).setVerticalAlignment('top');       // 上寄せ
  sh.getRange(1, 1, 1, nCol).setFontWeight('bold').setBackground('#FFF2CC'); // 1行目=合計
  sh.getRange(HEADER_ROW, 1, 1, nCol).setFontColor('#FFFFFF').setFontWeight('bold') // 2行目=ヘッダ
    .setBackground('#1F4E78').setHorizontalAlignment('center').setVerticalAlignment('middle').setWrap(true);
  sh.setFrozenRows(2);
  sh.setFrozenColumns(Math.min(3, nCol));
  if (imgCol >= 0 && nData > 0) sh.setRowHeights(DATA_START, nData, 60); // 画像行高
  // フィルタ(ヘッダ2行目 + データ)
  sh.getRange(HEADER_ROW, 1, nData + 1, nCol).createFilter();
  // 列幅
  var widths = { '画像': 70, 'SKU_ID': 70, '商品ID': 65, '商品IDリンク': 110,
    'シリーズ名': 170, '商品名': 170, '属性': 190, 'カテゴリ': 90, 'サプライヤー': 110,
    '構成パーツ_id': 110, '構成パーツ_名前': 160, '構成パーツ_数量': 90 };
  cols.forEach(function (c2, i) {
    var w = widths[c2]; if (!w) w = (c2.indexOf('ids') >= 0 ? 180 : 80);
    sh.setColumnWidth(i + 1, w);
  });
  // 余り(死蔵)=橙 / 案件紐づけ=緑 / ids折り返し (データ行のみ)
  if (nData > 0) cols.forEach(function (c2, i) {
    if (/(_余り|余りパーツ数)$/.test(c2)) sh.getRange(DATA_START, i + 1, nData, 1).setBackground('#F8CBAD');
    if (/(_案件紐づけ|案件紐づけパーツ数)$/.test(c2)) sh.getRange(DATA_START, i + 1, nData, 1).setBackground('#C6E0B4');
    if (/_(組み上げ|余り|案件紐づけ)$/.test(c2)) sh.getRange(DATA_START, i + 1, nData, 1).setWrap(true); // パーツごと点数(改行)
  });

  SpreadsheetApp.flush();
  return '出力完了: データ' + nData + '行 / ' + nCol + '列 (1行目=SUBTOTAL合計, 2行目=ヘッダ)';
}

// 列番号(1始まり) → A1列記号
function colLetter_(n) {
  var s = '';
  while (n > 0) { var m = (n - 1) % 26; s = String.fromCharCode(65 + m) + s; n = Math.floor((n - 1) / 26); }
  return s;
}

// ============================================================================
// 余り(組み上げ不能)在庫リストを出力: 1在庫=1行 / セル内改行なし
//   BQテーブル amari_stock_list を読み、新シート「余り在庫リスト」に書き込む。
//   clasp run writeAmariList で実行。
// ============================================================================
function writeAmariList() {
  var ss = SpreadsheetApp.openById(TARGET_SS_ID);
  var q = 'SELECT * FROM `' + AMARI_TABLE + '` ORDER BY `エリア`, `SKU_ID`, `在庫id`';
  var result = runBq_(q);
  var cols = result.schema.map(function (f) { return f.name; });
  var numeric = result.schema.map(function (f) { return isNumericType_(f.type); });
  var imgCol = cols.indexOf('画像');

  var values = [cols];
  result.rows.forEach(function (row) {
    values.push(row.f.map(function (cell, i) {
      var v = cell.v;
      if (i === imgCol) return toImageFormula_(v);
      if (v === null || v === undefined) return '';
      if (numeric[i]) { var n = Number(v); return isNaN(n) ? '' : n; }
      return v;
    }));
  });

  var sh = ss.getSheetByName(AMARI_SHEET);
  if (sh) sh.clear(); else sh = ss.insertSheet(AMARI_SHEET);
  if (sh.getFilter()) sh.getFilter().remove();
  var nRow = values.length, nCol = cols.length;
  sh.getRange(1, 1, nRow, nCol).setValues(values);

  // 書式: 1行目ヘッダ / データは1在庫1行・改行なし
  sh.getRange(1, 1, nRow, nCol).setVerticalAlignment('middle').setWrap(false);
  sh.getRange(1, 1, 1, nCol).setFontColor('#FFFFFF').setFontWeight('bold').setBackground('#1F4E78')
    .setHorizontalAlignment('center');
  sh.setFrozenRows(1);
  sh.setFrozenColumns(Math.min(2, nCol));
  if (imgCol >= 0 && nRow > 1) sh.setRowHeights(2, nRow - 1, 60); // 画像サムネイル用
  sh.getRange(1, 1, nRow, nCol).createFilter();
  var widths = { '画像': 70, '在庫id': 80, 'パーツid': 70, 'パーツ名': 180, 'エリア': 60, 'ステータス': 90,
    'SKU_ID': 70, '商品ID': 65, '商品IDリンク': 110, 'シリーズ名': 160, '商品名': 160, '属性': 180,
    'カテゴリ': 90, 'サプライヤー': 110 };
  cols.forEach(function (c, i) { sh.setColumnWidth(i + 1, widths[c] || 90); });

  SpreadsheetApp.flush();
  return '余り在庫リスト出力完了: ' + (nRow - 1) + '行 / ' + nCol + '列 → シート「' + AMARI_SHEET + '」';
}

// ============================================================================
// 余りパーツ「解消提案」レポートを出力: 1行 = SKU x エリア x 不足相方パーツ
//   BQテーブル leftover_reason_report (sql/leftover_reason_report.sql で作成) を読み、
//   シート「余りパーツ解消提案」に書き込む。
//   clasp run writeLeftoverReport で実行。
// ============================================================================
var LEFTOVER_SHEET = '余りパーツ解消提案';
var LEFTOVER_TABLE = 'clas-analytics.sandbox.leftover_reason_report';

function writeLeftoverReport() {
  var ss = SpreadsheetApp.openById(TARGET_SS_ID);
  // 余り在庫ID / 単価出所 / SKU合計必要金額 は詳細シートに出さない (SKU単位シート・並び順用)
  var q = 'SELECT * EXCEPT(`余り在庫ID`, `配送準備中案件ID`, `配送準備中案件名`, `配送準備中案件リンク`, `単価出所`, `SKU合計必要金額`) FROM `' + LEFTOVER_TABLE + '` ORDER BY ' +
    "CASE `エリア` WHEN '関東' THEN 1 WHEN '関西' THEN 2 ELSE 3 END, " +
    '`SKU合計必要金額` DESC NULLS LAST, `SKU_ID`, `相方パーツID`';
  var result = runBq_(q);
  var cols = result.schema.map(function (f) { return f.name; });
  var numeric = result.schema.map(function (f) { return isNumericType_(f.type); });
  var imgCol = cols.indexOf('画像'); // プレーンURL → =IMAGE に変換

  var values = [cols];
  result.rows.forEach(function (row) {
    values.push(row.f.map(function (cell, i) {
      var v = cell.v;
      if (i === imgCol) return v ? '=IMAGE("' + v + '", 4, 56, 56)' : '';
      if (v === null || v === undefined) return '';
      if (numeric[i]) { var n = Number(v); return isNaN(n) ? '' : n; }
      return v;
    }));
  });

  var sh = ss.getSheetByName(LEFTOVER_SHEET);
  if (sh) sh.clear(); else sh = ss.insertSheet(LEFTOVER_SHEET);
  if (sh.getFilter()) sh.getFilter().remove();
  var nRow = values.length, nCol = cols.length;
  sh.getRange(1, 1, nRow, nCol).setValues(values);

  // 書式: 1行目ヘッダ / 金額列はカンマ区切り / 内訳列は折り返し
  sh.getRange(1, 1, nRow, nCol).setVerticalAlignment('middle').setWrap(false);
  sh.getRange(1, 1, 1, nCol).setFontColor('#FFFFFF').setFontWeight('bold').setBackground('#1F4E78')
    .setHorizontalAlignment('center').setWrap(true);
  sh.setFrozenRows(1);
  sh.setFrozenColumns(Math.min(2, nCol));
  if (imgCol >= 0 && nRow > 1) sh.setRowHeights(2, nRow - 1, 60); // 画像サムネイル用
  sh.getRange(1, 1, nRow, nCol).createFilter();
  var widths = { '画像': 70, 'SKU_ID': 70, '商品ID': 65, '商品IDリンク': 110,
    'シリーズ名': 170, '商品名': 170, '属性': 190, 'カテゴリ': 90, 'サプライヤー': 110,
    'エリア': 60, '余りパーツ内訳': 260, '余り点数_貸出可能': 90,
    '追加で組める商品数': 90, '相方パーツID': 85, '相方パーツ名': 200,
    '必要点数_1商品あたり': 80, '相方の余り在庫': 85, '追加必要点数': 85,
    '単価': 90, '必要金額': 100, '相方の管理対象外内訳': 220 };
  cols.forEach(function (c, i) {
    sh.setColumnWidth(i + 1, widths[c] || 90);
    if (c === '余りパーツ内訳' || c === '相方の管理対象外内訳') {
      sh.getRange(2, i + 1, nRow - 1, 1).setWrap(true);
    }
    if (c === '単価' || c === '必要金額') {
      sh.getRange(2, i + 1, nRow - 1, 1).setNumberFormat('#,##0');
    }
    if (c === '必要金額') sh.getRange(2, i + 1, nRow - 1, 1).setBackground('#FFF2CC');
  });

  SpreadsheetApp.flush();
  return '余りパーツ解消提案 出力完了: ' + (nRow - 1) + '行 / ' + nCol + '列 → シート「' + LEFTOVER_SHEET + '」';
}

// ============================================================================
// 余りパーツ「解消提案」レポート SKU単位版: 1行 = SKU x エリア
//   相方パーツはセル内改行で1セルに集約。
//   BQテーブル leftover_reason_report_by_sku (leftover_reason_report_by_sku.sql) を読む。
//   clasp run writeLeftoverReportBySku で実行。
// ============================================================================
var LEFTOVER_SKU_SHEET = '余りパーツ解消提案_SKU単位';
var LEFTOVER_SKU_TABLE = 'clas-analytics.sandbox.leftover_reason_report_by_sku';

function writeLeftoverReportBySku() {
  var ss = SpreadsheetApp.openById(TARGET_SS_ID);
  var q = 'SELECT * FROM `' + LEFTOVER_SKU_TABLE + '` ORDER BY ' +
    "CASE `エリア` WHEN '関東' THEN 1 WHEN '関西' THEN 2 ELSE 3 END, " +
    '`必要金額合計` DESC NULLS LAST, `SKU_ID`';
  var result = runBq_(q);
  var cols = result.schema.map(function (f) { return f.name; });
  var numeric = result.schema.map(function (f) { return isNumericType_(f.type); });
  var imgCol = cols.indexOf('画像'); // プレーンURL → =IMAGE に変換

  var values = [cols];
  result.rows.forEach(function (row) {
    values.push(row.f.map(function (cell, i) {
      var v = cell.v;
      if (i === imgCol) return v ? '=IMAGE("' + v + '", 4, 56, 56)' : '';
      if (v === null || v === undefined) return '';
      if (numeric[i]) { var n = Number(v); return isNaN(n) ? '' : n; }
      return v;
    }));
  });

  var sh = ss.getSheetByName(LEFTOVER_SKU_SHEET);
  if (sh) sh.clear(); else sh = ss.insertSheet(LEFTOVER_SKU_SHEET);
  if (sh.getFilter()) sh.getFilter().remove();
  var nRow = values.length, nCol = cols.length;
  sh.getRange(1, 1, nRow, nCol).setValues(values);

  // 書式: 1行目ヘッダ / 金額列はカンマ区切り / 内訳列は折り返し
  sh.getRange(1, 1, nRow, nCol).setVerticalAlignment('middle').setWrap(false);
  sh.getRange(1, 1, 1, nCol).setFontColor('#FFFFFF').setFontWeight('bold').setBackground('#1F4E78')
    .setHorizontalAlignment('center').setWrap(true);
  sh.setFrozenRows(1);
  sh.setFrozenColumns(Math.min(2, nCol));
  if (imgCol >= 0 && nRow > 1) sh.setRowHeights(2, nRow - 1, 60); // 画像サムネイル用
  sh.getRange(1, 1, nRow, nCol).createFilter();
  var widths = { '画像': 70, 'SKU_ID': 70, '商品ID': 65, '商品IDリンク': 110,
    'シリーズ名': 170, '商品名': 170, '属性': 190, 'カテゴリ': 90, 'サプライヤー': 110,
    'エリア': 60, '余りパーツ内訳': 260, '余り点数_貸出可能': 90,
    '余り在庫ID': 90, '余り在庫IDリンク': 240,
    '案件ID': 80, '案件名': 220, '案件リンク': 260,
    '配送準備中案件ID': 80, '配送準備中案件名': 220, '配送準備中案件リンク': 260,
    '追加で組める商品数': 90, '相方パーツ内訳': 300, '追加必要点数合計': 90,
    '必要金額合計': 110, '相方の管理対象外内訳': 240 };
  cols.forEach(function (c, i) {
    sh.setColumnWidth(i + 1, widths[c] || 90);
    if (c === '余りパーツ内訳' || c === '相方パーツ内訳' || c === '相方の管理対象外内訳' ||
        c === '余り在庫ID' || c === '余り在庫IDリンク' ||
        c === '案件ID' || c === '案件名' || c === '案件リンク' ||
        c === '配送準備中案件ID' || c === '配送準備中案件名' || c === '配送準備中案件リンク') {
      sh.getRange(2, i + 1, nRow - 1, 1).setWrap(true);
    }
    if (c === '必要金額合計') {
      sh.getRange(2, i + 1, nRow - 1, 1).setNumberFormat('#,##0').setBackground('#FFF2CC');
    }
  });

  SpreadsheetApp.flush();
  return '余りパーツ解消提案(SKU単位) 出力完了: ' + (nRow - 1) + '行 / ' + nCol + '列 → シート「' + LEFTOVER_SKU_SHEET + '」';
}

// ============================================================================
// 解消提案ランキング: カテゴリ別 / サプライヤー別の SKU数 (ユニークSKU_ID数)
//   leftover_reason_report_by_sku から集計。1シートに2表を横並びで出力。
//   サプライヤーはカンマ区切り複数の場合に分解してそれぞれカウント。
//   clasp run writeLeftoverRankings で実行。
// ============================================================================
var LEFTOVER_RANK_SHEET = '解消提案_ランキング';

function writeLeftoverRankings() {
  var ss = SpreadsheetApp.openById(TARGET_SS_ID);
  var catQ = 'SELECT `カテゴリ`, COUNT(DISTINCT `SKU_ID`) AS sku_count FROM `' +
    LEFTOVER_SKU_TABLE + '` GROUP BY `カテゴリ` ORDER BY sku_count DESC, `カテゴリ`';
  var supQ = 'SELECT s AS `サプライヤー`, COUNT(DISTINCT `SKU_ID`) AS sku_count FROM `' +
    LEFTOVER_SKU_TABLE + '`, UNNEST(SPLIT(IFNULL(`サプライヤー`, \'不明\'), \',\')) AS s ' +
    'GROUP BY s ORDER BY sku_count DESC, s';
  var cat = runBq_(catQ);
  var sup = runBq_(supQ);

  var sh = ss.getSheetByName(LEFTOVER_RANK_SHEET);
  if (sh) sh.clear(); else sh = ss.insertSheet(LEFTOVER_RANK_SHEET);
  if (sh.getFilter()) sh.getFilter().remove();

  // A-C列: カテゴリランキング / E-G列: サプライヤーランキング (先頭列=順位)
  var catRows = [['順位', 'カテゴリ', 'SKU数']];
  cat.rows.forEach(function (row, i) {
    catRows.push([i + 1, row.f[0].v || '', Number(row.f[1].v)]);
  });
  var supRows = [['順位', 'サプライヤー', 'SKU数']];
  sup.rows.forEach(function (row, i) {
    supRows.push([i + 1, row.f[0].v || '', Number(row.f[1].v)]);
  });
  sh.getRange(1, 1, catRows.length, 3).setValues(catRows);
  sh.getRange(1, 5, supRows.length, 3).setValues(supRows);

  // 書式
  [1, 5].forEach(function (c) {
    sh.getRange(1, c, 1, 3).setFontColor('#FFFFFF').setFontWeight('bold')
      .setBackground('#1F4E78').setHorizontalAlignment('center');
  });
  sh.setFrozenRows(1);
  sh.setColumnWidth(1, 50); sh.setColumnWidth(2, 160); sh.setColumnWidth(3, 70);
  sh.setColumnWidth(4, 30);
  sh.setColumnWidth(5, 50); sh.setColumnWidth(6, 260); sh.setColumnWidth(7, 70);

  SpreadsheetApp.flush();
  return 'ランキング出力完了: カテゴリ' + (catRows.length - 1) + '件 / サプライヤー' +
    (supRows.length - 1) + '件 → シート「' + LEFTOVER_RANK_SHEET + '」';
}

// 既存デザイン検査用(参考)
function inspectSheet() {
  var ss = SpreadsheetApp.openById(TARGET_SS_ID);
  var out = { name: ss.getName(), sheets: [] };
  ss.getSheets().forEach(function (sh) {
    out.sheets.push({ sheet: sh.getName(), rows: sh.getLastRow(), cols: sh.getLastColumn() });
  });
  return JSON.stringify(out);
}
