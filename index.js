const puppeteer = require('puppeteer-core');
const chromium = require('@sparticuz/chromium');
const { google } = require('googleapis');
const { parse } = require('csv-parse/sync');
const AdmZip = require('adm-zip');
const iconv = require('iconv-lite');
const fs = require('fs');
const path = require('path');

// ============================
// 設定
// ============================
const SPREADSHEET_ID = '1V-CgRs9xpjbbaqb3OasgiCEYxfXP7_bpNsZFso-eiZI';
const RMS_LOGIN_URL = 'https://glogin.rms.rakuten.co.jp/?sp_id=1';
const DOWNLOAD_DIR = '/tmp/rpp_downloads';

// RPPレポートのカラム定義
const COLUMNS_ALL = [
  '日付', 'CTR(%)', 'クリック数(合計)', '実績額(合計)', 'CPC実績(合計)',
  '売上金額(合計720時間)', '売上件数(合計720時間)', 'CVR(合計720時間)(%)', 'ROAS(合計720時間)(%)', '注文獲得単価(合計720時間)',
];
const COLUMNS_ITEM = [
  '日付', '商品ページURL', '商品管理番号', '入札単価', 'CTR(%)', '商品CPC',
  'クリック数(合計)', '実績額(合計)', 'CPC実績(合計)',
  '売上金額(合計720時間)', '売上件数(合計720時間)', 'CVR(合計720時間)(%)', 'ROAS(合計720時間)(%)', '注文獲得単価(合計720時間)',
];
const COLUMNS_KW = [
  '日付', '商品ページURL', '商品管理番号', 'キーワード', 'CTR(%)', '目安CPC', 'キーワードCPC',
  'クリック数(合計)', '実績額(合計)', 'CPC実績(合計)',
  '売上金額(合計720時間)', '売上件数(合計720時間)', 'CVR(合計720時間)(%)', 'ROAS(合計720時間)(%)', '注文獲得単価(合計720時間)',
];

// ============================
// Cloud Function エントリポイント
// ============================
const functions = require('@google-cloud/functions-framework');
functions.http('fetchRppReport', async (req, res) => {
  let browser;
  try {
    const rmsUser = process.env.RMS_LOGIN_ID;
    const rmsPass = process.env.RMS_PASSWORD;
    const rakutenPass = process.env.RMS_2FA_PASSWORD;

    if (!rmsUser || !rmsPass) {
      throw new Error('RMS_LOGIN_ID / RMS_PASSWORD が未設定です');
    }

    if (!fs.existsSync(DOWNLOAD_DIR)) {
      fs.mkdirSync(DOWNLOAD_DIR, { recursive: true });
    }
    cleanupDir(DOWNLOAD_DIR);

    // ブラウザ起動
    chromium.setHeadlessMode = true;
    browser = await puppeteer.launch({
      args: chromium.args,
      defaultViewport: chromium.defaultViewport,
      executablePath: await chromium.executablePath(),
      headless: chromium.headless,
    });

    const page = await browser.newPage();

    // ダウンロード先設定
    const cdp = await page.createCDPSession();
    await cdp.send('Page.setDownloadBehavior', { behavior: 'allow', downloadPath: DOWNLOAD_DIR });

    // レスポンスインターセプター（CSV/ZIPキャプチャ）
    const capturedDownloads = [];
    page.on('response', async (response) => {
      try {
        const headers = response.headers();
        const contentDisp = headers['content-disposition'] || '';
        if ((contentDisp.includes('attachment') || contentDisp.includes('filename')) && response.ok()) {
          const buffer = await response.buffer();
          const fileName = contentDisp.match(/filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/)?.[1]?.replace(/['"]/g, '') || `report_${capturedDownloads.length}.bin`;
          // CSV/ZIPのみキャプチャ（画像・フォント等を除外）
          const lowerName = fileName.toLowerCase();
          const contentType = headers['content-type'] || '';
          // 画像・フォント等のバイナリファイルを除外
          const excludeExts = ['.png', '.jpg', '.jpeg', '.gif', '.webp', '.ico', '.svg', '.bmp',
                               '.woff', '.woff2', '.ttf', '.eot', '.js', '.css', '.html'];
          if (excludeExts.some(ext => lowerName.endsWith(ext))) {
            // skip
          } else if (contentType.includes('image/') || contentType.includes('font/')) {
            // skip
          } else if (lowerName.endsWith('.csv') || lowerName.endsWith('.zip') ||
              contentType.includes('csv') || contentType.includes('zip') || contentType.includes('octet-stream')) {
            console.log(`ダウンロードキャプチャ: ${fileName} (${buffer.length} bytes) content-type: ${contentType}`);
            capturedDownloads.push({ fileName, buffer, timestamp: Date.now() });
          }
        }
      } catch (e) { /* ignore */ }
    });

    // ============================================================
    // Step 0-4: ログイン
    // ============================================================
    await doLogin(page, rmsUser, rmsPass, rakutenPass);

    // ============================================================
    // Step 5: RPPレポートページへ遷移
    // ============================================================
    console.log('Step 5: RPPレポートページへ遷移...');
    await page.goto('https://ad.rms.rakuten.co.jp/rpp/reports', { waitUntil: 'networkidle2', timeout: 30000 });
    await new Promise(r => setTimeout(r, 5000));
    console.log('RPPページURL:', page.url());

    // ============================================================
    // Step 6: RPPチェックボックス設定 & ダウンロード
    // ============================================================
    console.log('Step 6: RPPレポート条件設定...');
    await setCheckboxes(page);

    // 日付入力フィールドの調査 & 全期間設定
    console.log('Step 6: 日付フィールド調査...');
    const dateFields = await page.evaluate(() => {
      const inputs = Array.from(document.querySelectorAll('input'));
      return inputs.filter(i => {
        const t = i.type?.toLowerCase();
        const id = (i.id || '').toLowerCase();
        const name = (i.name || '').toLowerCase();
        const ph = (i.placeholder || '').toLowerCase();
        return t === 'date' || t === 'text' && (id.includes('date') || name.includes('date') || ph.includes('年') || ph.includes('/') || /^\d{4}/.test(i.value));
      }).map(i => ({ id: i.id, name: i.name, type: i.type, value: i.value, placeholder: i.placeholder, className: i.className?.substring(0, 60) }));
    });
    console.log('日付フィールド:', JSON.stringify(dateFields));

    // 期間をできるだけ広く設定（開始日を2年前に）
    const twoYearsAgo = new Date();
    twoYearsAgo.setFullYear(twoYearsAgo.getFullYear() - 2);
    const startStr = twoYearsAgo.toISOString().slice(0, 10);
    const endStr = new Date().toISOString().slice(0, 10);
    console.log(`期間設定: ${startStr} 〜 ${endStr}`);

    await page.evaluate((start, end) => {
      // パターン1: input[type=date] or id/nameに'start','from','begin'等を含むフィールド
      const inputs = Array.from(document.querySelectorAll('input'));
      const dateInputs = inputs.filter(i => {
        const id = (i.id || '').toLowerCase();
        const name = (i.name || '').toLowerCase();
        return i.type === 'date' || id.includes('date') || name.includes('date');
      });

      // 開始日・終了日のペアを探す
      for (const input of dateInputs) {
        const id = (input.id || '').toLowerCase();
        const name = (input.name || '').toLowerCase();
        if (id.includes('start') || id.includes('from') || id.includes('begin') || name.includes('start') || name.includes('from')) {
          const nativeInputValueSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
          nativeInputValueSetter.call(input, start);
          input.dispatchEvent(new Event('input', { bubbles: true }));
          input.dispatchEvent(new Event('change', { bubbles: true }));
        }
        if (id.includes('end') || id.includes('to') || name.includes('end') || name.includes('to')) {
          const nativeInputValueSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
          nativeInputValueSetter.call(input, end);
          input.dispatchEvent(new Event('input', { bubbles: true }));
          input.dispatchEvent(new Event('change', { bubbles: true }));
        }
      }

      // パターン2: 日付ピッカーのセレクトボックス（年/月/日）
      const selects = Array.from(document.querySelectorAll('select'));
      for (const sel of selects) {
        const id = (sel.id || '').toLowerCase();
        if ((id.includes('start') || id.includes('from')) && id.includes('year')) {
          sel.value = start.split('-')[0];
          sel.dispatchEvent(new Event('change', { bubbles: true }));
        }
        if ((id.includes('start') || id.includes('from')) && id.includes('month')) {
          sel.value = String(parseInt(start.split('-')[1]));
          sel.dispatchEvent(new Event('change', { bubbles: true }));
        }
      }
    }, startStr, endStr);
    await new Promise(r => setTimeout(r, 2000));

    // 6a: rpp_all_raw
    console.log('Step 6a: rpp_all_raw ダウンロード...');
    await page.evaluate(() => { const r = document.querySelector('#rdReportTypeAllAds'); if (r) r.click(); });
    await new Promise(r => setTimeout(r, 1000));
    await page.evaluate(() => { const r = document.querySelector('#rdPeriodDay'); if (r) r.click(); });
    await new Promise(r => setTimeout(r, 2000));
    await clickButton(page, 'この条件でダウンロード');

    // 6b: rpp_商品_raw
    console.log('Step 6b: rpp_商品_raw ダウンロード...');
    await page.evaluate(() => { const r = document.querySelector('#rdPeriodMonth'); if (r) r.click(); });
    await new Promise(r => setTimeout(r, 2000));
    await clickButton(page, '全商品レポートダウンロード');

    // 6c: rpp_kw_raw
    console.log('Step 6c: rpp_kw_raw ダウンロード...');
    await clickButton(page, '全キーワードレポートダウンロード');

    // ============================================================
    // Step 7: RPPダウンロード履歴でCSV取得
    // ============================================================
    console.log('Step 7: ダウンロード履歴ページへ...');
    await page.goto('https://ad.rms.rakuten.co.jp/rpp/download', { waitUntil: 'networkidle2', timeout: 30000 });
    await new Promise(r => setTimeout(r, 5000));

    for (let retry = 0; retry < 12; retry++) {
      const status = await page.evaluate(() => {
        const t = document.body.innerText;
        return {
          item: t.includes('全商品レポートダウンロード') && t.includes('完了'),
          kw: t.includes('全キーワードレポートダウンロード') && t.includes('完了'),
          all: t.includes('この条件でダウンロード') && t.includes('完了'),
        };
      });
      console.log(`ダウンロード履歴確認 (${retry + 1}/12):`, JSON.stringify(status));

      if ((status.item && status.kw && status.all) || (status.item && status.kw && !status.all)) {
        const targets = status.all
          ? ['この条件でダウンロード', '全商品レポートダウンロード', '全キーワードレポートダウンロード']
          : ['全商品レポートダウンロード', '全キーワードレポートダウンロード'];
        for (const name of targets) {
          const clicked = await page.evaluate((target) => {
            const links = Array.from(document.querySelectorAll('a'));
            for (const link of links) {
              if (link.textContent.trim() !== 'ダウンロード') continue;
              const row = link.closest('tr') || link.parentElement?.parentElement;
              if (!row) continue;
              if (row.textContent.includes(target) && row.textContent.includes('完了')) {
                link.click();
                return true;
              }
            }
            return false;
          }, name);
          console.log(`${name} ダウンロードクリック: ${clicked}`);
          await new Promise(r => setTimeout(r, 5000));
        }
        break;
      }

      console.log(`レポート準備待ち... (${retry + 1}/12)`);
      await new Promise(r => setTimeout(r, 10000));
      await page.reload({ waitUntil: 'networkidle2' });
      await new Promise(r => setTimeout(r, 3000));
    }

    // ============================================================
    // Step 8: RPP CSV処理 & 書き込み
    // ============================================================
    const rppDownloadCount = capturedDownloads.length;
    await processRppDownloads(capturedDownloads.slice(0, rppDownloadCount));

    // ============================================================
    // Step 9: 広告プラットフォームのナビゲーション調査
    // ============================================================
    console.log('Step 9: 広告プラットフォームのナビゲーション調査...');

    // RPPページに戻ってサイドバーのリンクを調査
    await page.goto('https://ad.rms.rakuten.co.jp/rpp/reports', { waitUntil: 'networkidle2', timeout: 30000 });
    await new Promise(r => setTimeout(r, 3000));

    const navLinks = await page.evaluate(() => {
      return Array.from(document.querySelectorAll('a, [role="tab"], [role="menuitem"]')).map(a => ({
        text: (a.textContent || '').trim().substring(0, 80),
        href: a.href || a.getAttribute('href') || '',
      })).filter(l => l.text && !l.text.includes('\n'));
    });
    console.log('=== 広告ナビゲーションリンク ===');
    for (const l of navLinks.slice(0, 40)) {
      console.log(`  [${l.text}] → ${l.href}`);
    }

    // 確定済みURLマッピング（ナビゲーション調査で判明）
    // 検索連動型広告-エクスパンション → /rppexp/top
    // 楽天市場／楽天グループ広告 → /ec/top
    // 効果保証型広告（楽天CPA広告）→ /cpa/top
    // 運用型クーポン広告（クーポンアドバンス広告）→ /cpnadv/top
    // ターゲティングディスプレイ広告（TDA）→ /tda/top
    // ターゲティングディスプレイ広告-エクスパンション → /tdaexp/top

    // ============================================================
    // Step 10: 追加広告レポート取得
    // ============================================================
    const adReports = [
      {
        sheet: 'rpp-exp_raw',
        topUrl: 'https://ad.rms.rakuten.co.jp/rppexp/top',
        reportsUrl: 'https://ad.rms.rakuten.co.jp/rppexp/reports',
        download: 'この条件でダウンロード',
      },
      {
        sheet: '楽天市場広告_raw',
        topUrl: 'https://ad.rms.rakuten.co.jp/ec/top',
        reportsUrl: 'https://ad.rms.rakuten.co.jp/ec/reports',
        download: 'この条件でダウンロード',
      },
      {
        sheet: 'ca_raw',
        topUrl: 'https://ad.rms.rakuten.co.jp/cpnadv/top',
        reportsUrl: 'https://ad.rms.rakuten.co.jp/cpnadv/reports',
        download: 'この条件でダウンロード',
      },
      {
        sheet: 'tda_raw',
        topUrl: 'https://ad.rms.rakuten.co.jp/tda/top',
        reportsUrl: 'https://ad.rms.rakuten.co.jp/tda/reports',
        download: 'この条件でダウンロード',
      },
      {
        sheet: 'cpa_raw',
        topUrl: 'https://ad.rms.rakuten.co.jp/cpa/top',
        reportsUrl: 'https://ad.rms.rakuten.co.jp/cpa/reports',
        scrape: true,
      },
    ];

    const additionalResults = [];

    for (const report of adReports) {
      try {
        console.log(`\n=== Step 10: ${report.sheet} ===`);

        // ナビゲーション: まずtopページでセッション確立 → reportsページ
        let navigated = false;

        // Step 1: topページにアクセス（セッション確立）
        console.log(`${report.sheet}: ${report.topUrl} にアクセス...`);
        try {
          await page.goto(report.topUrl, { waitUntil: 'networkidle2', timeout: 20000 });
          await new Promise(r => setTimeout(r, 3000));
        } catch (e) {
          console.log(`${report.sheet}: topアクセス失敗: ${e.message}`);
          additionalResults.push({ sheet: report.sheet, status: 'nav_failed' });
          continue;
        }

        const topUrl = page.url();
        const topBody = await page.evaluate(() => document.body?.innerText?.substring(0, 500) || '');
        console.log(`${report.sheet}: top到達 URL=${topUrl}`);

        // 認証失敗チェック
        if (topBody.includes('Authentication Failed') || topUrl.includes('login')) {
          console.log(`${report.sheet}: 認証失敗、スキップ`);
          additionalResults.push({ sheet: report.sheet, status: 'auth_failed' });
          continue;
        }

        // 未利用チェック（topページ段階で早期検出）
        if (topUrl.includes('notfound') || topUrl.includes('onboard') ||
            topBody.includes('ご利用開始') || topBody.includes('申し込み')) {
          console.log(`${report.sheet}: 未利用の広告タイプ（${topUrl}）、スキップ`);
          additionalResults.push({ sheet: report.sheet, status: 'not_subscribed' });
          continue;
        }

        // Step 2: サイドバーの「パフォーマンスレポート」or「レポート」リンクをクリック
        const reportNavClicked = await page.evaluate(() => {
          // 優先順: パフォーマンスレポート > レポート
          for (const text of ['パフォーマンスレポート', 'レポート']) {
            for (const a of document.querySelectorAll('a')) {
              const t = a.textContent.trim();
              if (t === text || t === `パフォーマンス\nレポート`) {
                a.click();
                return text;
              }
            }
          }
          return null;
        });

        if (reportNavClicked) {
          console.log(`${report.sheet}: 「${reportNavClicked}」リンクをクリック`);
          await new Promise(r => setTimeout(r, 5000));
          navigated = true;
        }

        // サイドバーリンクがなかった場合、reportsURLへ直接遷移
        if (!navigated && report.reportsUrl) {
          console.log(`${report.sheet}: ${report.reportsUrl} に直接遷移...`);
          try {
            await page.goto(report.reportsUrl, { waitUntil: 'networkidle2', timeout: 15000 });
            await new Promise(r => setTimeout(r, 3000));
            const navUrl = page.url();
            // 認証失敗でリダイレクトされた場合はスキップ
            if (!navUrl.includes('login') && !navUrl.includes('Authentication')) {
              navigated = true;
            }
          } catch (e) {
            console.log(`${report.sheet}: reportsページ遷移失敗: ${e.message}`);
          }
        }

        if (!navigated) {
          console.log(`${report.sheet}: レポートページ到達失敗、スキップ`);
          additionalResults.push({ sheet: report.sheet, status: 'nav_failed' });
          continue;
        }

        const currentUrl = page.url();
        const bodyText = await page.evaluate(() => document.body?.innerText?.substring(0, 800) || '');
        console.log(`${report.sheet}: ページ到達 URL=${currentUrl}`);
        console.log(`${report.sheet}: ページ先頭: ${bodyText.substring(0, 300)}`);

        // ページ内のボタン一覧をログ出力
        const buttons = await page.evaluate(() => {
          return Array.from(document.querySelectorAll('button, a, input[type="submit"]')).map(el => {
            const t = (el.textContent || el.value || '').trim().replace(/\s+/g, ' ');
            return t.substring(0, 60);
          }).filter(t => t && t.length > 1);
        });
        console.log(`${report.sheet}: ページ内ボタン: ${buttons.join(' | ')}`);

        // 月ごとに表示を選択
        console.log(`${report.sheet}: 月ごとに表示を設定...`);
        await page.evaluate(() => {
          // radio button for monthly
          const radios = document.querySelectorAll('input[type="radio"]');
          for (const r of radios) {
            const label = r.parentElement?.textContent || r.nextSibling?.textContent || '';
            if (label.includes('月ごと')) { r.click(); return; }
          }
          // 別パターン: id指定
          const monthRadio = document.querySelector('#rdPeriodMonth, [value="month"], [value="monthly"]');
          if (monthRadio) monthRadio.click();
        });
        await new Promise(r => setTimeout(r, 2000));

        if (report.scrape) {
          // CPA: この条件で検索 → テーブルスクレイプ
          console.log(`${report.sheet}: この条件で検索...`);
          await clickButton(page, 'この条件で検索');
          await new Promise(r => setTimeout(r, 5000));

          // テーブルデータをスクレイプ
          const tableData = await page.evaluate(() => {
            const tables = document.querySelectorAll('table');
            for (const table of tables) {
              const rows = table.querySelectorAll('tr');
              if (rows.length < 2) continue;
              const headers = Array.from(rows[0].querySelectorAll('th, td')).map(c => c.textContent.trim());
              if (headers.length < 3) continue;
              const data = [];
              for (let i = 1; i < rows.length; i++) {
                const cells = Array.from(rows[i].querySelectorAll('td')).map(c => c.textContent.trim());
                if (cells.length > 0) data.push(cells);
              }
              return { headers, data };
            }
            // テーブルがない場合、div構造を試行
            return null;
          });

          if (tableData && tableData.data.length > 0) {
            console.log(`${report.sheet}: テーブルスクレイプ成功 ${tableData.data.length}行 x ${tableData.headers.length}列`);
            await writeRawToSheet(tableData.headers, tableData.data, report.sheet);
            additionalResults.push({ sheet: report.sheet, status: 'ok', rows: tableData.data.length });
          } else {
            console.log(`${report.sheet}: テーブルデータなし`);
            // ページ内容をログ
            const bodySnippet = await page.evaluate(() => document.body?.innerText?.substring(0, 500) || '');
            console.log(`${report.sheet}: ページ内容: ${bodySnippet}`);
            additionalResults.push({ sheet: report.sheet, status: 'no_data' });
          }
        } else {
          // ダウンロード型: この条件でダウンロード / ダウンロード
          const beforeCount = capturedDownloads.length;

          // ダウンロードボタンを試行（複数テキストパターン）
          const dlTexts = [report.download, 'この条件でダウンロード', 'ダウンロード'].filter(Boolean);
          let dlClicked = false;
          for (const text of dlTexts) {
            dlClicked = await clickButton(page, text);
            if (dlClicked) break;
          }

          if (!dlClicked) {
            console.log(`${report.sheet}: ダウンロードボタンなし`);
            additionalResults.push({ sheet: report.sheet, status: 'no_button' });
            continue;
          }

          // ダウンロード完了を待つ
          await new Promise(r => setTimeout(r, 5000));

          // キャプチャされたかチェック
          if (capturedDownloads.length > beforeCount) {
            const dl = capturedDownloads[capturedDownloads.length - 1];
            console.log(`${report.sheet}: ダウンロード即時取得 ${dl.fileName} (${dl.buffer.length} bytes)`);
            const records = processDownload(dl);
            if (records.length > 0) {
              const headers = Object.keys(records[0]);
              const rows = records.map(r => headers.map(h => r[h] || ''));
              await writeRawToSheet(headers, rows, report.sheet);
              additionalResults.push({ sheet: report.sheet, status: 'ok', rows: records.length });
            } else {
              additionalResults.push({ sheet: report.sheet, status: 'empty_csv' });
            }
          } else {
            // 非同期ダウンロード: ダウンロード履歴ページで完了を待つ
            console.log(`${report.sheet}: 即時ダウンロードなし、ダウンロード履歴で待機...`);

            // レポートページに戻って「ダウンロード履歴」リンクをクリック
            try {
              await page.goBack({ waitUntil: 'networkidle2', timeout: 10000 }).catch(() => {});
              await new Promise(r => setTimeout(r, 2000));

              // 「ダウンロード履歴」リンクをクリック
              const histLinkClicked = await page.evaluate(() => {
                for (const a of document.querySelectorAll('a')) {
                  if (a.textContent.trim() === 'ダウンロード履歴') {
                    a.click();
                    return true;
                  }
                }
                return false;
              });

              if (histLinkClicked) {
                console.log(`${report.sheet}: ダウンロード履歴リンクをクリック`);
                await new Promise(r => setTimeout(r, 5000));
              } else {
                // フォールバック: URLパターンでダウンロード履歴ページに遷移
                const dlPageUrl = currentUrl.replace(/\/(reports|performanceReport|performance_reports)(\/.*)?$/, '') + '/download';
                console.log(`${report.sheet}: ダウンロード履歴へ直接遷移: ${dlPageUrl}`);
                await page.goto(dlPageUrl, { waitUntil: 'networkidle2', timeout: 15000 });
                await new Promise(r => setTimeout(r, 3000));
              }

              const dlPageUrl = page.url();
              console.log(`${report.sheet}: ダウンロード履歴ページ URL=${dlPageUrl}`);

              // 最大8回リトライ（各10秒待ち = 約80秒）
              for (let retry = 0; retry < 8; retry++) {
                const histClicked = await page.evaluate(() => {
                  const links = Array.from(document.querySelectorAll('a'));
                  for (const link of links) {
                    if (link.textContent.trim() === 'ダウンロード') {
                      const row = link.closest('tr') || link.parentElement?.parentElement;
                      if (row && row.textContent.includes('完了')) {
                        link.click();
                        return 'clicked';
                      }
                      if (row && (row.textContent.includes('処理中') || row.textContent.includes('準備中'))) {
                        return 'pending';
                      }
                    }
                  }
                  return 'none';
                });

                console.log(`${report.sheet}: 履歴確認 (${retry + 1}/8): ${histClicked}`);

                if (histClicked === 'clicked') {
                  await new Promise(r => setTimeout(r, 5000));
                  break;
                } else if (histClicked === 'pending' || (histClicked === 'none' && retry < 4)) {
                  // pending: レポート生成中、none: まだ表示されていない可能性あり（最大4回まで）
                  await new Promise(r => setTimeout(r, 10000));
                  await page.reload({ waitUntil: 'networkidle2' });
                  await new Promise(r => setTimeout(r, 3000));
                } else {
                  break; // 4回試しても表示されない
                }
              }
            } catch (e) {
              console.log(`${report.sheet}: ダウンロード履歴エラー: ${e.message}`);
            }

            if (capturedDownloads.length > beforeCount) {
              const dl = capturedDownloads[capturedDownloads.length - 1];
              console.log(`${report.sheet}: 履歴経由で取得 ${dl.fileName} (${dl.buffer.length} bytes)`);
              const records = processDownload(dl);
              if (records.length > 0) {
                const headers = Object.keys(records[0]);
                const rows = records.map(r => headers.map(h => r[h] || ''));
                await writeRawToSheet(headers, rows, report.sheet);
                additionalResults.push({ sheet: report.sheet, status: 'ok', rows: records.length });
              } else {
                additionalResults.push({ sheet: report.sheet, status: 'empty_csv' });
              }
            } else {
              console.log(`${report.sheet}: ダウンロード取得失敗`);
              additionalResults.push({ sheet: report.sheet, status: 'download_failed' });
            }
          }
        }
      } catch (err) {
        console.error(`${report.sheet} エラー:`, err.message);
        additionalResults.push({ sheet: report.sheet, status: 'error', message: err.message });
      }
    }

    console.log('\n=== 追加レポート結果 ===');
    for (const r of additionalResults) {
      console.log(`  ${r.sheet}: ${r.status}${r.rows ? ` (${r.rows}件)` : ''}${r.message ? ` - ${r.message}` : ''}`);
    }

    cleanupDir(DOWNLOAD_DIR);

    const message = `レポート取得完了。追加レポート: ${JSON.stringify(additionalResults)}`;
    console.log(message);
    if (res) res.status(200).send(message);
    return message;
  } catch (error) {
    console.error('エラー:', error.message);
    if (res) res.status(500).send(`エラー: ${error.message}`);
    throw error;
  } finally {
    if (browser) await browser.close();
  }
});

// ============================
// RPP CSV処理
// ============================
async function processRppDownloads(downloads) {
  console.log('Step 8: RPP CSV解析 & スプレッドシート書き込み...');

  // ZIP展開 + Shift_JIS→UTF-8変換
  for (const dl of downloads) {
    if (dl.fileName.endsWith('.zip')) {
      const zip = new AdmZip(dl.buffer);
      for (const entry of zip.getEntries()) {
        if (entry.entryName.endsWith('.csv')) {
          const utf8 = iconv.decode(entry.getData(), 'Shift_JIS');
          const csvPath = path.join(DOWNLOAD_DIR, entry.entryName);
          fs.writeFileSync(csvPath, utf8, 'utf-8');
          console.log(`ZIP展開→CSV: ${csvPath}`);
        }
      }
    } else if (dl.fileName.endsWith('.csv')) {
      const utf8 = iconv.decode(dl.buffer, 'Shift_JIS');
      fs.writeFileSync(path.join(DOWNLOAD_DIR, dl.fileName), utf8, 'utf-8');
      console.log(`CSV保存: ${dl.fileName}`);
    }
  }

  const csvFiles = findAllCsv(DOWNLOAD_DIR);
  console.log(`CSVファイル数: ${csvFiles.length}`);
  let totalRecords = 0;

  for (const csvFile of csvFiles) {
    const fileName = path.basename(csvFile).toLowerCase();
    const content = fs.readFileSync(csvFile, 'utf-8');
    const records = parseRakutenCsv(content);
    if (records.length === 0) continue;

    let sheetName, columns;
    if (fileName.includes('item') || fileName.includes('shohin')) {
      sheetName = 'rpp_商品_raw';
      columns = COLUMNS_ITEM;
    } else if (fileName.includes('keyword') || fileName.includes('kw')) {
      sheetName = 'rpp_kw_raw';
      columns = COLUMNS_KW;
    } else {
      sheetName = 'rpp_all_raw';
      columns = COLUMNS_ALL;
    }

    const filtered = filterColumns(records, columns);
    console.log(`${sheetName}: ${filtered.length}件, カラム: ${columns.join(', ')}`);
    await writeToSheet(filtered, sheetName, columns);
    totalRecords += filtered.length;
  }

  cleanupDir(DOWNLOAD_DIR);
  console.log(`RPPレポート合計: ${totalRecords}件`);
}

// ============================
// ダウンロードデータ処理（ZIP/CSV → records）
// ============================
function processDownload(dl) {
  let csvContent = '';

  if (dl.fileName.endsWith('.zip')) {
    const zip = new AdmZip(dl.buffer);
    for (const entry of zip.getEntries()) {
      if (entry.entryName.endsWith('.csv')) {
        csvContent = iconv.decode(entry.getData(), 'Shift_JIS');
        break;
      }
    }
  } else if (dl.fileName.endsWith('.csv')) {
    // Shift_JISを試行、失敗したらUTF-8
    csvContent = iconv.decode(dl.buffer, 'Shift_JIS');
  } else {
    // 拡張子不明の場合もShift_JISを試行
    csvContent = iconv.decode(dl.buffer, 'Shift_JIS');
  }

  if (!csvContent) return [];
  return parseRakutenCsv(csvContent);
}

// ============================
// ログイン処理
// ============================
async function doLogin(page, rmsUser, rmsPass, rakutenPass) {
  const rakutenEmail = process.env.RMS_2FA_EMAIL;

  console.log('Step 0: 既存セッションのログアウト...');
  try {
    await page.goto('https://ad.rms.rakuten.co.jp/rpp/api/auth/logout', { waitUntil: 'networkidle2', timeout: 15000 });
  } catch (e) { /* OK */ }
  await new Promise(r => setTimeout(r, 2000));

  console.log('Step 1: RMSログインページへ...');
  await page.goto(RMS_LOGIN_URL, { waitUntil: 'networkidle2', timeout: 30000 });
  await page.waitForSelector('input[name="login_id"]', { timeout: 10000 });
  await page.$eval('input[name="login_id"]', el => { el.value = ''; el.focus(); });
  await page.type('input[name="login_id"]', rmsUser, { delay: 50 });
  await page.$eval('input[name="passwd"]', el => { el.value = ''; el.focus(); });
  await page.type('input[name="passwd"]', rmsPass, { delay: 50 });
  await Promise.all([
    page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 60000 }).catch(() => {}),
    page.keyboard.press('Enter'),
  ]);
  await new Promise(r => setTimeout(r, 3000));
  console.log('Step 1 完了。現在URL:', page.url());

  if (page.url().includes('login.account.rakuten.com') && rakutenEmail) {
    console.log('Step 2a: 楽天会員メールアドレス入力...');
    await page.waitForSelector('input[type="text"], input[type="email"]', { timeout: 10000 });
    const emailInput = await page.$('input[type="text"], input[type="email"]');
    if (emailInput) {
      await emailInput.click();
      await emailInput.type(rakutenEmail, { delay: 30 });
    }
    await page.keyboard.press('Enter');
    await page.waitForSelector('input[type="password"]', { timeout: 30000 });
    await new Promise(r => setTimeout(r, 1000));

    console.log('Step 2b: 楽天会員パスワード入力...');
    const passField = await page.$('input[type="password"]');
    if (passField && rakutenPass) {
      await passField.click();
      await passField.type(rakutenPass, { delay: 30 });
      await page.keyboard.press('Enter');
      await page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 15000 }).catch(() => {});
      for (let i = 0; i < 30; i++) {
        if (!page.url().includes('login.account.rakuten.com')) break;
        await new Promise(r => setTimeout(r, 2000));
      }
      await new Promise(r => setTimeout(r, 5000));
    }
    console.log('Step 2 完了。現在URL:', page.url());
  }

  const bodyText3 = await page.evaluate(() => document.body?.innerText?.substring(0, 500) || '');
  if (bodyText3.includes('お気をつけください') || bodyText3.includes('安全認証')) {
    console.log('Step 3: 注意事項ページ「次へ」クリック...');
    await page.evaluate(() => {
      for (const el of document.querySelectorAll('a, button, input')) {
        if ((el.textContent || el.value || '').includes('次へ')) { el.click(); return; }
      }
    });
    await page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {});
    await new Promise(r => setTimeout(r, 3000));
  }

  const bodyText4 = await page.evaluate(() => document.body?.innerText?.substring(0, 1000) || '');
  if (bodyText4.includes('遵守') || bodyText4.includes('RMSを利用します')) {
    console.log('Step 4: 同意ポップアップクリック...');
    await page.evaluate(() => {
      for (const el of document.querySelectorAll('a, button, input')) {
        if ((el.textContent || el.value || '').includes('RMSを利用します')) { el.click(); return; }
      }
    });
    await new Promise(r => setTimeout(r, 3000));
  }

  console.log('ログイン完了！');
}

// ============================
// チェックボックス設定（RPP用）
// ============================
async function setCheckboxes(page) {
  await page.evaluate(() => {
    const all = document.querySelector('#cbMetricsAll');
    if (all && !all.checked) all.click();
  });
  await new Promise(r => setTimeout(r, 500));
  await page.evaluate(() => {
    const cb = document.querySelector('#cb12H');
    if (cb && cb.checked) cb.click();
  });
  await new Promise(r => setTimeout(r, 500));
  await page.evaluate(() => {
    const cbNew = document.querySelector('#cbNewUsers');
    if (cbNew && cbNew.checked) cbNew.click();
    const cbExist = document.querySelector('#cbExistingUsers');
    if (cbExist && cbExist.checked) cbExist.click();
  });
  await new Promise(r => setTimeout(r, 500));
  console.log('チェックボックス設定完了（12時間OFF、新規/既存OFF、合計+720時間のみ）');
}

// ============================
// ボタンクリック
// ============================
async function clickButton(page, buttonText) {
  console.log(`${buttonText}をリクエスト...`);
  const clicked = await page.evaluate((text) => {
    for (const btn of document.querySelectorAll('button, a, input[type="submit"]')) {
      if (btn.textContent.trim() === text || btn.value === text) { btn.click(); return true; }
    }
    // 部分一致
    for (const btn of document.querySelectorAll('button, a, input[type="submit"]')) {
      if (btn.textContent.includes(text)) { btn.click(); return true; }
    }
    return false;
  }, buttonText);
  console.log(`${buttonText}: ${clicked ? 'クリック成功' : 'ボタン見つからず'}`);
  await new Promise(r => setTimeout(r, 3000));
  return clicked;
}

// ============================
// 楽天CSVパース
// ============================
function parseRakutenCsv(content) {
  let clean = content.replace(/^\uFEFF/, '');
  const lines = clean.split('\n');

  let headerIdx = -1;
  for (let i = 0; i < Math.min(20, lines.length); i++) {
    const line = lines[i];
    if (line.startsWith('実行日時') || line.startsWith('検索条件') || line.startsWith('集計') || line.includes('■■■')) continue;
    const commas = (line.match(/,/g) || []).length;
    if (commas >= 3) { headerIdx = i; break; }
  }

  if (headerIdx > 0) {
    console.log(`CSVメタデータ行スキップ: ${headerIdx}行`);
    clean = lines.slice(headerIdx).join('\n');
  }

  return parse(clean, { columns: true, skip_empty_lines: true, relax_column_count: true, trim: true });
}

// ============================
// カラムフィルタリング
// ============================
function filterColumns(records, targetColumns) {
  return records.map(record => {
    const filtered = {};
    for (const col of targetColumns) {
      filtered[col] = record[col] || '';
    }
    return filtered;
  });
}

// ============================
// Google Sheets書き込み（RPP用 - カラムフィルタ付き）
// ============================
async function writeToSheet(records, sheetName, columns) {
  const auth = new google.auth.GoogleAuth({ scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
  const sheets = google.sheets({ version: 'v4', auth });

  if (records.length === 0) return;

  const headers = columns;
  const rows = records.map(r => headers.map(h => r[h] || ''));
  const values = [headers, ...rows];

  try {
    const spreadsheet = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
    const exists = spreadsheet.data.sheets.some(s => s.properties.title === sheetName);
    if (!exists) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: { requests: [{ addSheet: { properties: { title: sheetName } } }] },
      });
    }
  } catch (e) {
    console.log('シート作成エラー:', e.message);
  }

  await sheets.spreadsheets.values.clear({ spreadsheetId: SPREADSHEET_ID, range: `${sheetName}!A:ZZ` });
  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: `${sheetName}!A1`,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values },
  });

  console.log(`${sheetName} 書き込み完了: ${records.length}件`);
}

// ============================
// Google Sheets書き込み（追加レポート用 - 全カラム）
// ============================
async function writeRawToSheet(headers, dataRows, sheetName) {
  const auth = new google.auth.GoogleAuth({ scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
  const sheets = google.sheets({ version: 'v4', auth });

  const values = [headers, ...dataRows];

  try {
    const spreadsheet = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
    const exists = spreadsheet.data.sheets.some(s => s.properties.title === sheetName);
    if (!exists) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: { requests: [{ addSheet: { properties: { title: sheetName } } }] },
      });
    }
  } catch (e) {
    console.log('シート作成エラー:', e.message);
  }

  await sheets.spreadsheets.values.clear({ spreadsheetId: SPREADSHEET_ID, range: `${sheetName}!A:ZZ` });
  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: `${sheetName}!A1`,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values },
  });

  console.log(`${sheetName} 書き込み完了: ${dataRows.length}件`);
}

// ============================
// ユーティリティ
// ============================
function findAllCsv(dir) {
  if (!fs.existsSync(dir)) return [];
  return fs.readdirSync(dir).filter(f => f.endsWith('.csv')).map(f => path.join(dir, f));
}

function cleanupDir(dir) {
  if (fs.existsSync(dir)) {
    fs.readdirSync(dir).forEach(f => fs.unlinkSync(path.join(dir, f)));
  }
}
