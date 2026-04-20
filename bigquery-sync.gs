/**
 * BigQuery → スプレッドシート同期
 * gid=758403278 のシートに受注データを出力
 */

var PROJECT_ID = 'stellar-shape-491201-g8';
var BQ_QUERY = 'SELECT * FROM `rakuten.orders` ORDER BY orderDatetime DESC';

function syncBigQueryToSheet() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();

  // gid=758403278 のシートを探す、なければ作成
  var sheet = null;
  var sheets = ss.getSheets();
  for (var i = 0; i < sheets.length; i++) {
    if (sheets[i].getSheetId() === 758403278) {
      sheet = sheets[i];
      break;
    }
  }
  if (!sheet) {
    sheet = ss.insertSheet('BQ_受注データ');
    Logger.log('新しいシート「BQ_受注データ」を作成しました');
  }

  // BigQuery実行
  var request = {
    query: BQ_QUERY,
    useLegacySql: false,
  };

  var queryResults = BigQuery.Jobs.query(request, PROJECT_ID);
  var jobId = queryResults.jobReference.jobId;

  // 完了待ち
  var sleepMs = 500;
  while (!queryResults.jobComplete) {
    Utilities.sleep(sleepMs);
    sleepMs = Math.min(sleepMs * 2, 5000);
    queryResults = BigQuery.Jobs.getQueryResults(PROJECT_ID, jobId);
  }

  // ヘッダー取得
  var headers = [];
  for (var i = 0; i < queryResults.schema.fields.length; i++) {
    headers.push(queryResults.schema.fields[i].name);
  }

  // 全行取得（ページネーション対応）
  var allRows = [];
  var pageToken = null;

  // 最初のページ
  if (queryResults.rows) {
    for (var i = 0; i < queryResults.rows.length; i++) {
      var row = [];
      for (var j = 0; j < queryResults.rows[i].f.length; j++) {
        row.push(queryResults.rows[i].f[j].v || '');
      }
      allRows.push(row);
    }
    pageToken = queryResults.pageToken;
  }

  // 残りのページ
  while (pageToken) {
    var nextPage = BigQuery.Jobs.getQueryResults(PROJECT_ID, jobId, { pageToken: pageToken });
    if (nextPage.rows) {
      for (var i = 0; i < nextPage.rows.length; i++) {
        var row = [];
        for (var j = 0; j < nextPage.rows[i].f.length; j++) {
          row.push(nextPage.rows[i].f[j].v || '');
        }
        allRows.push(row);
      }
    }
    pageToken = nextPage.pageToken;
  }

  // シートクリア＆書き込み
  sheet.clearContents();

  if (allRows.length === 0) {
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    Logger.log('データなし（ヘッダーのみ出力）');
    return;
  }

  // ヘッダー + データ
  var output = [headers].concat(allRows);

  // 50000行ずつ書き込み（大量データ対策）
  var BATCH = 50000;
  for (var start = 0; start < output.length; start += BATCH) {
    var end = Math.min(start + BATCH, output.length);
    var batch = output.slice(start, end);
    sheet.getRange(start + 1, 1, batch.length, headers.length).setValues(batch);
  }

  // 書式設定
  sheet.getRange(1, 1, 1, headers.length).setFontWeight('bold').setBackground('#4472c4').setFontColor('white');
  sheet.setFrozenRows(1);

  Logger.log('同期完了: ' + allRows.length + '行');
  SpreadsheetApp.getActiveSpreadsheet().toast(allRows.length + '行をBigQueryから同期しました', 'BQ同期完了', 5);
}

// メニュー追加
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('BigQuery')
    .addItem('受注データ同期', 'syncBigQueryToSheet')
    .addToUi();
}
