const puppeteer = require('puppeteer-core');
const chromium = require('@sparticuz/chromium');
const { google } = require('googleapis');
const { BigQuery } = require('@google-cloud/bigquery');
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
  // ダッシュボードモード
  if (req.query.mode === 'dashboard') {
    try {
      const html = await generateDashboardHtml();
      res.set('Content-Type', 'text/html; charset=utf-8');
      return res.status(200).send(html);
    } catch (err) {
      console.error('Dashboard error:', err);
      return res.status(500).send(`Dashboard error: ${err.message}`);
    }
  }

  // 受注データ取得モード（RMS WEB API）
  if (req.query.mode === 'fetch_orders') {
    try {
      const result = await fetchOrdersFromRmsApi(req.query);
      return res.status(200).json(result);
    } catch (err) {
      console.error('fetch_orders error:', err);
      return res.status(500).json({ error: err.message });
    }
  }

  let browser;
  try {
    // months_back: 何ヶ月前まで遡るか（デフォルト0=当月のみ）
    const monthsBack = parseInt(req.query.months_back || '0', 10);
    // months_skip: 最新からN ヶ月分をスキップ（分割実行用）
    const monthsSkip = parseInt(req.query.months_skip || '0', 10);
    // skip_ads=1 で広告レポート(Step10)をスキップ
    const skipAds = req.query.skip_ads === '1';
    // skip_rpp=1 でRPPをスキップし広告レポートのみ実行
    const skipRpp = req.query.skip_rpp === '1';
    // skip_datatool=1 でall_raw/all_item_rawをスキップ
    const skipDatatool = req.query.skip_datatool === '1';
    // skip_extra=1 でafi_raw/mail_raw/line_rawをスキップ
    const skipExtra = req.query.skip_extra === '1';
    // 個別スキップ: skip_afi=1, skip_mail=1, skip_line=1
    const skipAfi = req.query.skip_afi === '1';
    const skipMail = req.query.skip_mail === '1';
    const skipLine = req.query.skip_line === '1';
    // ad_only=1 で広告レポートをad_raw(楽天市場広告)のみに絞る
    const adOnly = req.query.ad_only === '1';
    // ad_targets=ad_raw,tda_raw で取得対象の広告シートをカンマ区切り指定（ad_onlyより優先）
    const adTargets = req.query.ad_targets ? req.query.ad_targets.split(',').map(s => s.trim()) : null;
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
    // Step 5-8: RPPレポート（skip_rpp=1でスキップ可能）
    // ============================================================
    if (skipRpp) {
      console.log('skip_rpp=1: RPPレポートをスキップ');
    }

    if (!skipRpp) {
    // ============================================================
    // Step 5: RPPレポートページへ遷移
    // ============================================================
    console.log('Step 5: RPPレポートページへ遷移...');
    await page.goto('https://ad.rms.rakuten.co.jp/rpp/reports', { waitUntil: 'networkidle2', timeout: 30000 });
    await new Promise(r => setTimeout(r, 2000));
    console.log('RPPページURL:', page.url());

    // ============================================================
    // Step 6: RPPチェックボックス設定 & ダウンロード
    // ============================================================
    console.log('Step 6: RPPレポート条件設定...');

    // ラジオボタンID一覧をログ出力
    const radioIds = await page.evaluate(() => {
      return Array.from(document.querySelectorAll('input[type="radio"]')).map(r => {
        const label = r.parentElement?.textContent?.trim()?.substring(0, 30) || '';
        return `${r.id || r.name || '(no-id)'}=${label}`;
      });
    });
    console.log('ラジオボタン一覧:', radioIds.join(' | '));

    await setCheckboxes(page);

    // 月ごとのループ用: 取得対象月のリストを生成
    // months_skip: 最新N月をスキップ（例: months_back=12&months_skip=5 → 12月前〜5月前）
    const targetMonths = [];
    for (let i = monthsBack; i >= monthsSkip; i--) {
      const d = new Date();
      d.setMonth(d.getMonth() - i);
      const year = d.getFullYear();
      const month = d.getMonth(); // 0-indexed
      const monthStr = `${year}-${String(month + 1).padStart(2, '0')}`; // YYYY-MM（月ごと用）
      const startStr = `${monthStr}-01`;
      const lastDay = new Date(year, month + 1, 0).getDate();
      const endStr = `${monthStr}-${String(lastDay).padStart(2, '0')}`;
      targetMonths.push({ label: `${year}年${month + 1}月`, monthStr, startStr, endStr });
    }
    console.log(`取得対象月: ${targetMonths.map(m => m.label).join(', ')}`);

    // 月ごとにRPPレポートをダウンロード
    for (const targetMonth of targetMonths) {
      console.log(`\n===== RPP ${targetMonth.label} 取得開始 =====`);

      // RPPレポートページに戻る（月ループ2回目以降）
      if (targetMonths.indexOf(targetMonth) > 0) {
        await page.goto('https://ad.rms.rakuten.co.jp/rpp/reports', { waitUntil: 'networkidle2', timeout: 30000 });
        await new Promise(r => setTimeout(r, 3000));
        await setCheckboxes(page);
      }

      // 6a: rpp_all_raw（すべての広告・日ごと）
      console.log(`Step 6a [${targetMonth.label}]: rpp_all_raw ダウンロード...`);
      await page.evaluate(() => { const r = document.querySelector('#rdReportTypeAllAds'); if (r) r.click(); });
      await new Promise(r => setTimeout(r, 1000));
      await page.evaluate(() => { const r = document.querySelector('#rdPeriodDay'); if (r) r.click(); });
      await new Promise(r => setTimeout(r, 2000));
      // 日ごと選択後に日付設定（YYYY-MM-DD）
      await setRppDateRange(page, targetMonth.startStr, targetMonth.endStr);
      await clickButton(page, 'この条件でダウンロード');

      // 6b: rpp_item_raw（商品別・月ごと）
      console.log(`Step 6b [${targetMonth.label}]: rpp_item_raw ダウンロード...`);
      const itemRadioClicked = await page.evaluate(() => {
        for (const id of ['#rdReportTypeItem', '#rdReportTypeProduct', '#rdReportTypeShohin']) {
          const r = document.querySelector(id); if (r) { r.click(); return id; }
        }
        for (const radio of document.querySelectorAll('input[type="radio"]')) {
          const label = radio.parentElement?.textContent || '';
          if (label.includes('商品別')) { radio.click(); return '商品別(label)'; }
        }
        return null;
      });
      console.log('商品別ラジオ:', itemRadioClicked);
      await new Promise(r => setTimeout(r, 1000));
      await page.evaluate(() => { const r = document.querySelector('#rdPeriodMonth'); if (r) r.click(); });
      await new Promise(r => setTimeout(r, 2000));
      // 月ごと選択後に日付設定（YYYY-MM）
      await setRppDateRange(page, targetMonth.monthStr, targetMonth.monthStr);
      await clickButton(page, '全商品レポートダウンロード');

      // 6c: rpp_kw_raw（キーワード別・月ごと）
      console.log(`Step 6c [${targetMonth.label}]: rpp_kw_raw ダウンロード...`);
      const kwRadioClicked = await page.evaluate(() => {
        for (const id of ['#rdReportTypeKeyword', '#rdReportTypeKw']) {
          const r = document.querySelector(id); if (r) { r.click(); return id; }
        }
        for (const radio of document.querySelectorAll('input[type="radio"]')) {
          const label = radio.parentElement?.textContent || '';
          if (label.includes('キーワード別')) { radio.click(); return 'キーワード別(label)'; }
        }
        return null;
      });
      console.log('キーワード別ラジオ:', kwRadioClicked);
      await new Promise(r => setTimeout(r, 1000));
      // 日別で取得（月別だと1商品1KWに集約されるため）
      await page.evaluate(() => { const r = document.querySelector('#rdPeriodDay'); if (r) r.click(); });
      await new Promise(r => setTimeout(r, 2000));
      await setRppDateRange(page, targetMonth.monthStr, targetMonth.monthStr);
      await clickButton(page, '全キーワードレポートダウンロード');

      // ダウンロード履歴でCSV取得
      console.log(`Step 7 [${targetMonth.label}]: ダウンロード履歴ページへ...`);
      await page.goto('https://ad.rms.rakuten.co.jp/rpp/download', { waitUntil: 'networkidle2', timeout: 30000 });
      await new Promise(r => setTimeout(r, 2000));

      for (let retry = 0; retry < 6; retry++) {
        const status = await page.evaluate(() => {
          const t = document.body.innerText;
          return {
            item: t.includes('全商品レポートダウンロード') && t.includes('完了'),
            kw: t.includes('全キーワードレポートダウンロード') && t.includes('完了'),
            all: t.includes('この条件でダウンロード') && t.includes('完了'),
          };
        });
        console.log(`ダウンロード履歴確認 [${targetMonth.label}] (${retry + 1}/6):`, JSON.stringify(status));

        if (status.item || status.kw || status.all) {
          const targets = [];
          if (status.all) targets.push('この条件でダウンロード');
          if (status.item) targets.push('全商品レポートダウンロード');
          if (status.kw) targets.push('全キーワードレポートダウンロード');
          console.log(`完了レポート: ${targets.join(', ')}`);
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
            await new Promise(r => setTimeout(r, 3000));
          }
          break;
        }

        console.log(`レポート準備待ち... (${retry + 1}/6)`);
        await new Promise(r => setTimeout(r, 3000));
        await page.reload({ waitUntil: 'networkidle2' });
        await new Promise(r => setTimeout(r, 3000));
      }
    } // end targetMonths loop

    // ============================================================
    // Step 8: RPP CSV処理 & 書き込み（全月分まとめて処理）
    // ============================================================
    const rppDownloadCount = capturedDownloads.length;
    await processRppDownloads(capturedDownloads.slice(0, rppDownloadCount));
    } // end if (!skipRpp)

    // ============================================================
    // Step 9以降: 広告レポート（skip_ads=1でスキップ可能）
    // ============================================================
    const additionalResults = [];
    if (skipAds) {
      console.log('skip_ads=1: 広告レポートをスキップ');
    }

    if (!skipAds) {
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
    // Step 10: 追加広告レポート取得（月ごとにループ）
    // ============================================================
    // 注意: 楽天市場広告(EC)は最後に処理（セッション互換性の問題があるため）
    const adReports = [
      {
        sheet: 'rpp-exp_raw',
        topUrl: 'https://ad.rms.rakuten.co.jp/rppexp/top',
        reportsUrl: 'https://ad.rms.rakuten.co.jp/rppexp/reports',
        downloadHistoryUrl: 'https://ad.rms.rakuten.co.jp/rppexp/download',
        unitLabel: null,
        download: 'この条件でダウンロード',
      },
      {
        sheet: 'ca_raw',
        topUrl: 'https://ad.rms.rakuten.co.jp/cpnadv/top',
        reportsUrl: 'https://ad.rms.rakuten.co.jp/cpnadv/reports',
        downloadHistoryUrl: 'https://ad.rms.rakuten.co.jp/cpnadv/download_history',
        unitLabel: '商品別',
        download: 'この条件でダウンロード',
      },
      {
        sheet: 'tda_raw',
        topUrl: 'https://ad.rms.rakuten.co.jp/tda/top',
        reportsUrl: 'https://ad.rms.rakuten.co.jp/tda/performanceReport',
        downloadHistoryUrl: null,
        unitLabel: 'キャンペーン',
        download: 'この条件でダウンロード',
      },
      {
        sheet: 'cpa_raw',
        topUrl: 'https://ad.rms.rakuten.co.jp/cpa/top',
        reportsUrl: 'https://ad.rms.rakuten.co.jp/cpa/reports',
        downloadHistoryUrl: null,
        unitLabel: null,
        download: 'この条件で検索',
        scrapeTable: true,
      },
      {
        sheet: 'ad_raw',
        topUrl: 'https://ad.rms.rakuten.co.jp/ec/top',
        reportsUrl: 'https://ad.rms.rakuten.co.jp/ec/performance_reports',
        downloadHistoryUrl: 'https://ad.rms.rakuten.co.jp/ec/download',
        unitLabel: '広告別',
        download: 'この条件でダウンロード',
      },
    ];

    // 広告レポート用: 月ごとのターゲットリスト生成（YYYY-MM形式）
    const adTargetMonths = [];
    for (let i = monthsBack; i >= 0; i--) {
      const d = new Date();
      d.setMonth(d.getMonth() - i);
      const year = d.getFullYear();
      const month = d.getMonth() + 1;
      const monthStr = `${year}-${String(month).padStart(2, '0')}`;
      const startStr = `${monthStr}-01`;
      const lastDay = new Date(year, month, 0).getDate();
      const endStr = `${monthStr}-${String(lastDay).padStart(2, '0')}`;
      adTargetMonths.push({ label: `${year}年${month}月`, monthStr, startStr, endStr });
    }
    console.log(`広告レポート取得対象月: ${adTargetMonths.map(m => m.label).join(', ')}`);

    // シートごとの累積レコード（全月分を溜めて最後に一括書き込み）
    const accumulatedRecords = {};
    for (const report of adReports) {
      accumulatedRecords[report.sheet] = [];
    }

    // ============================================================
    // 月ごとにループ: 各月で全プラットフォームをダウンロード
    // ============================================================
    for (const adMonth of adTargetMonths) {
      console.log(`\n========== 広告レポート ${adMonth.label} (${adMonth.monthStr}) ==========`);

      // Pass 1: 全プラットフォームのダウンロードを一括キック
      console.log(`\n=== Step 10 Pass 1 [${adMonth.label}]: ダウンロード一括キック ===`);
      const kickedReports = [];

      for (const report of adReports) {
        // ad_targets指定時は対象シートのみ実行、ad_only=1はad_rawのみ
        if (adTargets && !adTargets.includes(report.sheet)) {
          console.log(`${report.sheet}: ad_targets指定外スキップ`);
          continue;
        } else if (!adTargets && adOnly && report.sheet !== 'ad_raw') {
          console.log(`${report.sheet}: ad_only=1 スキップ`);
          continue;
        }
        try {
          console.log(`\n--- ${report.sheet} [${adMonth.label}]: ダウンロードキック ---`);

          // topページでセッション確立 → レポートページへ
          try {
            await page.goto(report.topUrl, { waitUntil: 'networkidle2', timeout: 15000 });
            await new Promise(r => setTimeout(r, 2000));
          } catch (e) {
            console.log(`${report.sheet}: topアクセス失敗: ${e.message}`);
            continue;
          }

          let topUrl = page.url();

          // system_error/auth_error時 → RMSメインメニュー経由でセッション復旧
          if (topUrl.includes('system_error') || topUrl.includes('auth=e')) {
            console.log(`${report.sheet}: セッションエラー検出、RMSメインメニュー経由で復旧中...`);
            try {
              await page.goto('https://mainmenu.rms.rakuten.co.jp/', { waitUntil: 'networkidle2', timeout: 15000 });
              await new Promise(r => setTimeout(r, 2000));
              const mmUrl = page.url();
              if (mmUrl.includes('login') || mmUrl.includes('glogin')) {
                await doLogin(page, rmsUser, rmsPass, rakutenPass, true);
                await page.goto('https://mainmenu.rms.rakuten.co.jp/', { waitUntil: 'networkidle2', timeout: 15000 });
                await new Promise(r => setTimeout(r, 2000));
              }
              // RMSメインメニューから広告プラットフォームリンクを経由
              const adLink = await page.evaluate(() => {
                for (const a of document.querySelectorAll('a')) {
                  if ((a.href || '').includes('ad.rms.rakuten.co.jp')) return a.href;
                }
                return null;
              });
              if (adLink) {
                console.log(`${report.sheet}: 広告リンク経由: ${adLink}`);
                await page.goto(adLink, { waitUntil: 'networkidle2', timeout: 15000 });
                await new Promise(r => setTimeout(r, 2000));
              }
              // 再度topページへ
              await page.goto(report.topUrl, { waitUntil: 'networkidle2', timeout: 15000 });
              await new Promise(r => setTimeout(r, 2000));
              topUrl = page.url();
              console.log(`${report.sheet}: 復旧後URL=${topUrl}`);
            } catch (e) {
              console.log(`${report.sheet}: セッション復旧失敗: ${e.message}`);
              continue;
            }
          }

          if (topUrl.includes('notfound') || topUrl.includes('onboard') || topUrl.includes('login') || topUrl.includes('system_error')) {
            console.log(`${report.sheet}: 利用不可またはログイン失敗、スキップ`);
            continue;
          }

          // サイドバーの「パフォーマンスレポート」or「レポート」リンクをクリック
          const reportNavClicked = await page.evaluate(() => {
            for (const text of ['パフォーマンスレポート', 'レポート']) {
              for (const a of document.querySelectorAll('a')) {
                const t = a.textContent.trim();
                if (t === text || t === `パフォーマンス\nレポート`) { a.click(); return text; }
              }
            }
            return null;
          });
          if (reportNavClicked) {
            await new Promise(r => setTimeout(r, 3000));
          } else if (report.reportsUrl) {
            await page.goto(report.reportsUrl, { waitUntil: 'networkidle2', timeout: 15000 });
            await new Promise(r => setTimeout(r, 2000));
          }

          console.log(`${report.sheet}: ページ到達 URL=${page.url()}`);

          // 集計単位ラジオボタンを選択
          if (report.unitLabel) {
            await page.evaluate((targetLabel) => {
              for (const r of document.querySelectorAll('input[type="radio"]')) {
                const label = r.parentElement?.textContent?.trim() || '';
                if (label.includes(targetLabel)) { r.click(); return; }
              }
            }, report.unitLabel);
            console.log(`${report.sheet}: 集計単位「${report.unitLabel}」設定`);
            await new Promise(r => setTimeout(r, 1000));
          }

          // 「月ごとに表示」ラジオボタンを選択
          await page.evaluate(() => {
            for (const r of document.querySelectorAll('input[type="radio"]')) {
              if ((r.parentElement?.textContent || '').includes('月ごと')) { r.click(); return; }
            }
          });
          await new Promise(r => setTimeout(r, 1000));

          // 集計期間を設定
          // 楽天市場広告(EC)はYYYY-MM-DD形式、他はYYYY-MM形式
          const isEcPlatform = report.sheet === 'ad_raw' || report.topUrl.includes('/ec/');
          const periodStart = isEcPlatform ? adMonth.startStr : adMonth.monthStr;
          const periodEnd = isEcPlatform ? adMonth.endStr : adMonth.monthStr;
          console.log(`${report.sheet}: 期間設定 ${periodStart} 〜 ${periodEnd}`);

          const monthPickerSet = await page.evaluate((startVal, endVal) => {
            const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;

            // 1. input[type="month"] を探す
            const monthInputs = Array.from(document.querySelectorAll('input[type="month"]'));
            if (monthInputs.length >= 2) {
              nativeSetter.call(monthInputs[0], startVal);
              monthInputs[0].dispatchEvent(new Event('input', { bubbles: true }));
              monthInputs[0].dispatchEvent(new Event('change', { bubbles: true }));
              nativeSetter.call(monthInputs[1], endVal);
              monthInputs[1].dispatchEvent(new Event('input', { bubbles: true }));
              monthInputs[1].dispatchEvent(new Event('change', { bubbles: true }));
              return 'month_input';
            }

            // 2. YYYY-MM-DD形式のテキスト入力を探す（EC用）
            const allTextInputs = Array.from(document.querySelectorAll('input[type="text"]'));
            const dateFullInputs = allTextInputs.filter(inp => {
              const v = inp.value || '';
              return v.match(/^\d{4}-\d{2}-\d{2}$/);
            });
            if (dateFullInputs.length >= 2) {
              nativeSetter.call(dateFullInputs[0], startVal);
              dateFullInputs[0].dispatchEvent(new Event('input', { bubbles: true }));
              dateFullInputs[0].dispatchEvent(new Event('change', { bubbles: true }));
              nativeSetter.call(dateFullInputs[1], endVal);
              dateFullInputs[1].dispatchEvent(new Event('input', { bubbles: true }));
              dateFullInputs[1].dispatchEvent(new Event('change', { bubbles: true }));
              return 'date_full_input';
            }

            // 3. YYYY-MM形式のテキスト入力を探す
            const dateMonthInputs = allTextInputs.filter(inp => {
              const v = inp.value || '';
              return v.match(/^\d{4}-\d{2}$/) && !v.match(/^\d{4}-\d{2}-\d{2}$/);
            });
            if (dateMonthInputs.length >= 2) {
              nativeSetter.call(dateMonthInputs[0], startVal);
              dateMonthInputs[0].dispatchEvent(new Event('input', { bubbles: true }));
              dateMonthInputs[0].dispatchEvent(new Event('change', { bubbles: true }));
              nativeSetter.call(dateMonthInputs[1], endVal);
              dateMonthInputs[1].dispatchEvent(new Event('input', { bubbles: true }));
              dateMonthInputs[1].dispatchEvent(new Event('change', { bubbles: true }));
              return 'text_input';
            }

            // 4. datepicker-input クラスを探す
            const dpInputs = Array.from(document.querySelectorAll('input.datepicker-input, input[placeholder*="Select"]'))
              .filter(inp => (inp.value || '').match(/\d{4}[-/]\d{2}/));
            if (dpInputs.length >= 2) {
              nativeSetter.call(dpInputs[0], startVal);
              dpInputs[0].dispatchEvent(new Event('input', { bubbles: true }));
              dpInputs[0].dispatchEvent(new Event('change', { bubbles: true }));
              nativeSetter.call(dpInputs[1], endVal);
              dpInputs[1].dispatchEvent(new Event('input', { bubbles: true }));
              dpInputs[1].dispatchEvent(new Event('change', { bubbles: true }));
              return 'datepicker_input';
            }

            return null;
          }, periodStart, periodEnd);

          if (!monthPickerSet) {
            // フォールバック: ページ上の全inputをログ出力
            const allInputInfo = await page.evaluate(() => {
              return Array.from(document.querySelectorAll('input')).map((inp, i) => ({
                idx: i, type: inp.type, name: inp.name || '', id: inp.id || '',
                cls: (inp.className || '').substring(0, 40),
                val: (inp.value || '').substring(0, 20),
                ph: (inp.placeholder || '').substring(0, 20),
                vis: inp.offsetParent !== null,
              }));
            });
            console.log(`${report.sheet}: 全input要素: ${JSON.stringify(allInputInfo.slice(0, 20))}`);

            // click+typeフォールバック: date系テキスト入力をすべて試す
            const dateInputs = await page.$$('input[type="text"]');
            const candidates = [];
            for (const inp of dateInputs) {
              const val = await inp.evaluate(el => el.value || '');
              if (val.match(/^\d{4}[-/]\d{2}([-/]\d{2})?$/)) {
                candidates.push(inp);
              }
            }
            console.log(`${report.sheet}: 日付入力候補: ${candidates.length}個`);

            if (candidates.length >= 2) {
              await candidates[0].click({ clickCount: 3 });
              await new Promise(r => setTimeout(r, 300));
              await candidates[0].type(periodStart, { delay: 50 });
              await page.keyboard.press('Tab');
              await new Promise(r => setTimeout(r, 500));
              await candidates[1].click({ clickCount: 3 });
              await new Promise(r => setTimeout(r, 300));
              await candidates[1].type(periodEnd, { delay: 50 });
              await page.keyboard.press('Tab');
              await new Promise(r => setTimeout(r, 500));
              console.log(`${report.sheet}: 期間設定（click+type）`);
            } else {
              console.log(`${report.sheet}: 日付フィールドが見つかりません - デフォルトで実行`);
            }
          } else {
            console.log(`${report.sheet}: 期間設定完了 (${monthPickerSet})`);
          }
          await new Promise(r => setTimeout(r, 1000));

          // CPA: テーブルスクレイピング（CSVダウンロードなし）
          if (report.scrapeTable) {
            const searchClicked = await clickButton(page, report.download || 'この条件で検索');
            console.log(`${report.sheet} [${adMonth.label}]: 検索ボタン ${searchClicked ? '成功' : '失敗'}`);
            if (searchClicked) {
              await page.waitForNetworkIdle({ idleTime: 2000, timeout: 15000 }).catch(() => {});
              await new Promise(r => setTimeout(r, 1000));
            }
            const tableRecords = await page.evaluate(() => {
              const tables = document.querySelectorAll('table');
              // 検索結果テーブルを探す（ヘッダーに「日付」を含むテーブル）
              let targetTable = null;
              for (const t of tables) {
                if (t.textContent.includes('日付')) { targetTable = t; break; }
              }
              if (!targetTable) return [];
              const allRows = Array.from(targetTable.querySelectorAll('tr'));
              if (allRows.length < 2) return [];
              const headerCells = Array.from(allRows[0].querySelectorAll('th, td'));
              const headers = headerCells.map(c => c.textContent.trim().replace(/\s+/g, ' '));
              const dataRows = allRows.slice(1);
              return dataRows.map(tr => {
                const cells = Array.from(tr.querySelectorAll('td, th'));
                if (cells.length === 0) return null;
                const values = cells.map(c => c.textContent.trim().replace(/\s+/g, ' '));
                if (values.every(v => !v)) return null;
                const record = {};
                headers.forEach((h, i) => { record[h] = (values[i] || '').replace(/\s*円\s*/g, '').replace(/,/g, ''); });
                return record;
              }).filter(Boolean);
            });
            console.log(`${report.sheet} [${adMonth.label}]: テーブルから${tableRecords.length}件取得`);
            if (tableRecords.length > 0) {
              accumulatedRecords[report.sheet].push(...tableRecords);
              console.log(`${report.sheet} [${adMonth.label}]: 累計${accumulatedRecords[report.sheet].length}件`);
            } else {
              console.log(`${report.sheet} [${adMonth.label}]: データなし（スキップ）`);
            }
            continue;
          }

          // ダウンロードキック（ネットワーク監視付き）
          cleanupDir(DOWNLOAD_DIR);
          const beforeKick = capturedDownloads.length;

          // ページ上の全ボタン情報をログ出力（デバッグ用）
          const allButtons = await page.evaluate(() => {
            return Array.from(document.querySelectorAll('button, a, input[type="submit"]')).map(el => {
              const t = (el.textContent || el.value || '').trim().replace(/\s+/g, ' ').substring(0, 60);
              const tag = el.tagName;
              const disabled = el.disabled || false;
              const vis = el.offsetParent !== null;
              return t ? `[${tag}${disabled ? ':disabled' : ''}${vis ? '' : ':hidden'}]${t}` : null;
            }).filter(Boolean);
          });
          console.log(`${report.sheet}: ページ上のボタン: ${allButtons.slice(0, 15).join(' | ')}`);

          // ダイアログ/確認ポップアップハンドラ
          let dialogHandled = false;
          const dialogHandler = async (dialog) => {
            console.log(`${report.sheet}: ダイアログ検出: ${dialog.type()} - ${dialog.message()}`);
            await dialog.accept();
            dialogHandled = true;
          };
          page.on('dialog', dialogHandler);

          // ネットワークリクエスト監視（ダウンロード関連APIを検出）
          const apiResponses = [];
          const responseHandler = async (response) => {
            const url = response.url();
            if (url.includes('download') || url.includes('report') || url.includes('csv') || url.includes('export')) {
              const status = response.status();
              const contentType = response.headers()['content-type'] || '';
              console.log(`${report.sheet}: APIレスポンス: ${status} ${url.substring(0, 120)} (${contentType})`);
              apiResponses.push({ url, status, contentType });
            }
          };
          page.on('response', responseHandler);

          const dlClicked = await clickButton(page, report.download || 'この条件でダウンロード');
          console.log(`${report.sheet}: ダウンロードキック ${dlClicked ? '成功' : '失敗'}`);

          // ボタンが見つからない場合、代替テキストで再試行
          if (!dlClicked) {
            const altTexts = ['ダウンロード', 'CSVダウンロード', 'レポートダウンロード', 'CSV出力'];
            for (const alt of altTexts) {
              const altClicked = await clickButton(page, alt);
              if (altClicked) {
                console.log(`${report.sheet}: 代替ボタン「${alt}」でキック成功`);
                break;
              }
            }
          }

          // レスポンス待ち
          const waitTime = isEcPlatform ? 10000 : 5000;
          await new Promise(r => setTimeout(r, waitTime));

          // ダイアログハンドラ解除
          page.off('dialog', dialogHandler);
          page.off('response', responseHandler);

          // ダウンロードキック後のページ状態確認
          const afterClickState = await page.evaluate(() => {
            const body = document.body?.innerText?.substring(0, 300) || '';
            const hasError = body.includes('エラー') || body.includes('error');
            const hasNoData = body.includes('データがありません') || body.includes('該当するデータ') || body.includes('0件');
            const hasSuccess = body.includes('ダウンロードを開始') || body.includes('作成中') || body.includes('受け付け');
            return { hasError, hasNoData, hasSuccess, snippet: body.substring(0, 200) };
          });
          if (afterClickState.hasError) console.log(`${report.sheet}: エラー検出: ${afterClickState.snippet}`);
          if (afterClickState.hasNoData) console.log(`${report.sheet}: データなし検出`);
          if (afterClickState.hasSuccess) console.log(`${report.sheet}: ダウンロード開始検出`);
          if (dialogHandled) console.log(`${report.sheet}: ダイアログ応答済み`);
          console.log(`${report.sheet}: API応答数: ${apiResponses.length}`);

          // 即時CSV返却チェック（レスポンスインターセプト）
          let gotCsv = false;
          if (capturedDownloads.length > beforeKick) {
            const dl = capturedDownloads[capturedDownloads.length - 1];
            console.log(`${report.sheet} [${adMonth.label}]: CSV即時取得! ${dl.fileName} (${dl.buffer.length} bytes)`);
            const records = processDownload(dl);
            if (records.length > 0) {
              accumulatedRecords[report.sheet].push(...records);
              console.log(`${report.sheet} [${adMonth.label}]: ${records.length}件取得 (累計${accumulatedRecords[report.sheet].length}件)`);
              gotCsv = true;
            }
          }

          // ファイルシステムフォールバック
          if (!gotCsv) {
            const files = fs.existsSync(DOWNLOAD_DIR) ? fs.readdirSync(DOWNLOAD_DIR).filter(f => f.endsWith('.csv') || f.endsWith('.zip')) : [];
            if (files.length > 0) {
              console.log(`${report.sheet}: ファイルシステムから取得: ${files.join(', ')}`);
              for (const f of files) {
                const buf = fs.readFileSync(path.join(DOWNLOAD_DIR, f));
                const records = processDownload({ fileName: f, buffer: buf });
                if (records.length > 0) {
                  accumulatedRecords[report.sheet].push(...records);
                  console.log(`${report.sheet} [${adMonth.label}]: FS ${records.length}件取得 (累計${accumulatedRecords[report.sheet].length}件)`);
                  gotCsv = true;
                  break;
                }
              }
            }
          }

          if (!gotCsv) {
            if (afterClickState.hasNoData) {
              console.log(`${report.sheet} [${adMonth.label}]: データなし（スキップ）`);
            } else if (isEcPlatform) {
              // ECはダウンロード履歴が壊れているのでスキップ
              console.log(`${report.sheet}: CSV即時取得失敗。EC履歴ページはシステムエラーのためスキップ`);
            } else {
              kickedReports.push(report);
            }
          }
        } catch (err) {
          console.error(`${report.sheet} [${adMonth.label}] キックエラー:`, err.message);
        }
      }

      // Pass 2: ダウンロード履歴から一括回収（この月分）
      if (kickedReports.length > 0) {
        console.log(`\n=== Step 10 Pass 2 [${adMonth.label}]: ${kickedReports.length}件のダウンロード回収 ===`);
        console.log('レポート生成待ち 10秒...');
        await new Promise(r => setTimeout(r, 3000));

        for (const report of kickedReports) {
          try {
            let historyUrl = report.downloadHistoryUrl;
            if (!historyUrl) {
              console.log(`${report.sheet}: ダウンロード履歴URLなし、スキップ`);
              continue;
            }

            console.log(`${report.sheet}: ダウンロード履歴 ${historyUrl} へ...`);
            await page.goto(historyUrl, { waitUntil: 'networkidle2', timeout: 15000 });
            await new Promise(r => setTimeout(r, 2000));

            const histUrl = page.url();
            console.log(`${report.sheet}: 履歴ページ到達 URL=${histUrl}`);

            if (histUrl.includes('notfound') || histUrl.includes('login')) {
              console.log(`${report.sheet}: 履歴ページなし`);
              continue;
            }

            // 「更新」ボタンをクリック（リフレッシュ）
            const refreshClicked = await page.evaluate(() => {
              for (const el of document.querySelectorAll('a, button')) {
                const t = (el.textContent || '').trim();
                if (t === '更新' || t === 'ダウンロード履歴更新') { el.click(); return true; }
              }
              return false;
            });
            if (refreshClicked) {
              console.log(`${report.sheet}: 履歴更新ボタンクリック`);
              await new Promise(r => setTimeout(r, 3000));
            }

            // 履歴テーブルの内容をデバッグ出力
            const histTableInfo = await page.evaluate(() => {
              const tables = document.querySelectorAll('table');
              const rows = [];
              for (const table of tables) {
                for (const tr of table.querySelectorAll('tr')) {
                  const cells = Array.from(tr.querySelectorAll('td, th')).map(c => c.textContent.trim().replace(/\s+/g, ' ').substring(0, 40));
                  if (cells.length > 0) rows.push(cells.join(' | '));
                }
              }
              return rows.slice(0, 10);
            });
            console.log(`${report.sheet}: 履歴テーブル: ${histTableInfo.length > 0 ? histTableInfo.join(' / ') : '(テーブルなし)'}`);

            let downloaded = false;
            const beforeDl = capturedDownloads.length;
            for (let retry = 0; retry < 2; retry++) {
              const result = await page.evaluate(() => {
                // 「完了」行の「ダウンロード」リンクを探す
                const links = Array.from(document.querySelectorAll('a'));
                for (const link of links) {
                  if (link.textContent.trim() === 'ダウンロード') {
                    const row = link.closest('tr') || link.parentElement?.parentElement;
                    if (row && row.textContent.includes('完了')) { link.click(); return 'clicked_complete'; }
                  }
                }
                // ダウンロードリンク（href含む）
                for (const link of links) {
                  const t = link.textContent.trim();
                  if (t === 'ダウンロード' && link.href && link.href.includes('download')) {
                    link.click(); return 'clicked_link';
                  }
                }
                const bodyText = document.body?.innerText || '';
                if (bodyText.includes('処理中') || bodyText.includes('準備中')) return 'pending';
                if (bodyText.includes('データがありません') || bodyText.includes('0件')) return 'no_data';
                return 'none';
              });
              console.log(`${report.sheet}: 履歴チェック (${retry + 1}/2): ${result}`);

              if (result.startsWith('clicked')) {
                await new Promise(r => setTimeout(r, 3000));
                downloaded = true;
                break;
              } else if (result === 'no_data' || result === 'none') {
                if (retry === 0) {
                  await new Promise(r => setTimeout(r, 4000));
                  await page.reload({ waitUntil: 'networkidle2' });
                  await new Promise(r => setTimeout(r, 2000));
                }
              } else if (result === 'pending') {
                await new Promise(r => setTimeout(r, 4000));
                await page.reload({ waitUntil: 'networkidle2' });
                await new Promise(r => setTimeout(r, 2000));
              }
            }

            let adRecords = [];
            if (capturedDownloads.length > beforeDl) {
              const dl = capturedDownloads[capturedDownloads.length - 1];
              console.log(`${report.sheet} [${adMonth.label}]: CSV取得 ${dl.fileName} (${dl.buffer.length} bytes)`);
              adRecords = processDownload(dl);
            } else {
              const files = fs.existsSync(DOWNLOAD_DIR) ? fs.readdirSync(DOWNLOAD_DIR).filter(f => f.endsWith('.csv') || f.endsWith('.zip')) : [];
              for (const f of files) {
                const buf = fs.readFileSync(path.join(DOWNLOAD_DIR, f));
                adRecords = processDownload({ fileName: f, buffer: buf });
                if (adRecords.length > 0) break;
              }
              cleanupDir(DOWNLOAD_DIR);
            }

            if (adRecords.length > 0) {
              accumulatedRecords[report.sheet].push(...adRecords);
              console.log(`${report.sheet} [${adMonth.label}]: ${adRecords.length}件取得 (累計${accumulatedRecords[report.sheet].length}件)`);
            } else {
              console.log(`${report.sheet} [${adMonth.label}]: 履歴からCSV取得できず`);
            }
          } catch (err) {
            console.error(`${report.sheet} [${adMonth.label}] 回収エラー:`, err.message);
          }
        }
      }
    } // end adTargetMonths loop

    // 全月分の累積データをシートに一括書き込み
    console.log('\n=== 広告レポート一括書き込み ===');
    for (const report of adReports) {
      const records = accumulatedRecords[report.sheet];
      if (records.length > 0) {
        const headers = Object.keys(records[0]);
        const rows = records.map(r => headers.map(h => r[h] || ''));
        const keyColumns = report.scrapeTable ? ['日付'] : undefined;
        await writeRawToSheet(headers, rows, report.sheet, keyColumns);
        additionalResults.push({ sheet: report.sheet, status: 'ok', rows: records.length });
      } else {
        additionalResults.push({ sheet: report.sheet, status: 'no_data' });
      }
    }

    console.log('\n=== 追加レポート結果 ===');
    for (const r of additionalResults) {
      console.log(`  ${r.sheet}: ${r.status}${r.rows ? ` (${r.rows}件)` : ''}${r.message ? ` - ${r.message}` : ''}`);
    }

    } // end if (!skipAds)

    // ============================================================
    // Step 11-12: RMSデータ分析 (datatool) - skip_datatool=1でスキップ可能
    // ============================================================
    if (skipDatatool) {
      console.log('skip_datatool=1: all_raw・all_item_rawをスキップ');
    }

    if (!skipDatatool) {

    // ============================================================
    // Step 11: RMSデータ分析 - 店舗データ(日次) → all_raw
    // ============================================================
    console.log('\n=== Step 11: all_raw (店舗データ日次) ===');
    try {
      // RMSメインメニューに遷移（datatoolはメインメニュー経由でのみアクセス可能）
      console.log('RMSメインメニューへ遷移...');
      await page.goto('https://mainmenu.rms.rakuten.co.jp/', { waitUntil: 'networkidle2', timeout: 30000 });
      await new Promise(r => setTimeout(r, 3000));
      const mainUrl = page.url();
      console.log('RMSメインメニューURL:', mainUrl);

      // ログインページにリダイレクトされた場合は再ログイン
      if (mainUrl.includes('glogin') || mainUrl.includes('login')) {
        console.log('RMSセッション切れ、再ログイン...');
        await doLogin(page, rmsUser, rmsPass, rakutenPass, true);
        await page.goto('https://mainmenu.rms.rakuten.co.jp/', { waitUntil: 'networkidle2', timeout: 30000 });
        await new Promise(r => setTimeout(r, 3000));
        console.log('再ログイン後メインメニューURL:', page.url());
      }

      // メインメニューのページ内容をログ
      const mainBody = await page.evaluate(() => document.body?.innerText?.substring(0, 800) || '');
      console.log('メインメニュー内容:', mainBody.substring(0, 300));

      // メインメニュー内の全リンクを取得してログ
      const allMenuLinks = await page.evaluate(() => {
        return Array.from(document.querySelectorAll('a')).map(a => ({
          text: (a.textContent || '').trim().substring(0, 60),
          href: a.href || ''
        })).filter(l => l.href && !l.href.startsWith('javascript'));
      });
      console.log(`メインメニューリンク数: ${allMenuLinks.length}`);
      // datatool関連リンクを抽出
      const dataLinks = allMenuLinks.filter(l =>
        l.href.includes('datatool') || l.text.includes('データ') || l.text.includes('アクセス') ||
        l.text.includes('売上') || l.text.includes('分析')
      );
      console.log('データ関連リンク:', JSON.stringify(dataLinks));

      // iframeも確認（RMSメインメニューはiframeを使う場合がある）
      const mainFrames = page.frames();
      if (mainFrames.length > 1) {
        console.log(`メインメニューフレーム数: ${mainFrames.length}`);
        for (const frame of mainFrames) {
          const fUrl = frame.url();
          if (fUrl !== 'about:blank' && fUrl !== mainUrl) {
            console.log(`  フレーム: ${fUrl}`);
            const frameLinks = await frame.evaluate(() => {
              return Array.from(document.querySelectorAll('a')).map(a => ({
                text: (a.textContent || '').trim().substring(0, 60),
                href: a.href || ''
              })).filter(l => l.href.includes('datatool') || (l.text && (l.text.includes('データ') || l.text.includes('分析'))));
            }).catch(() => []);
            if (frameLinks.length > 0) {
              console.log(`  フレーム内データリンク:`, JSON.stringify(frameLinks));
              dataLinks.push(...frameLinks);
            }
          }
        }
      }

      // datatoolへのリンクを使って遷移（セッショントークン付きURLが必要）
      let datatoolReached = false;
      const datatoolLink = dataLinks.find(l => l.href.includes('datatool'));
      if (datatoolLink) {
        console.log('datatoolリンク経由で遷移:', datatoolLink.href);
        await page.goto(datatoolLink.href, { waitUntil: 'networkidle2', timeout: 30000 });
        await new Promise(r => setTimeout(r, 2000));
        const dtUrl = page.url();
        console.log('datatool遷移後URL:', dtUrl);
        const dtBody = await page.evaluate(() => document.body?.innerText?.substring(0, 500) || '');
        console.log('datatool遷移後内容:', dtBody.substring(0, 300));

        if (dtUrl.includes('datatool') && !dtBody.includes('認証エラー')) {
          datatoolReached = true;
          // datatool内でデータダウンロードページへ遷移
          const ddLink = await page.evaluate(() => {
            for (const a of document.querySelectorAll('a')) {
              const t = (a.textContent || '').trim();
              const h = a.href || '';
              if (h.includes('datadownload') || t.includes('データダウンロード') || t.includes('売上データ')) {
                return { text: t.substring(0, 50), href: h };
              }
            }
            return null;
          });
          console.log('データダウンロードリンク:', JSON.stringify(ddLink));
          if (ddLink) {
            await page.goto(ddLink.href, { waitUntil: 'networkidle2', timeout: 30000 });
            await new Promise(r => setTimeout(r, 3000));
          }
        }
      }

      // datatoolリンクが見つからない場合、メインメニューからリンクをクリック（JavaScript navigation対応）
      if (!datatoolReached) {
        console.log('datatoolリンクなし、メインメニューからクリック遷移を試行...');
        const clickResult = await page.evaluate(() => {
          for (const a of document.querySelectorAll('a')) {
            const t = (a.textContent || '').trim();
            if (t.includes('データ分析') || t.includes('売上データ') || t.includes('データダウンロード')) {
              a.click();
              return t;
            }
          }
          return null;
        });
        console.log('メニュークリック結果:', clickResult);
        if (clickResult) {
          await page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 15000 }).catch(() => {});
          await new Promise(r => setTimeout(r, 3000));
          console.log('クリック遷移後URL:', page.url());
        }

        // それでもダメなら直接URL
        if (!page.url().includes('datatool')) {
          console.log('直接URLでdatatoolにアクセス...');
          await page.goto('https://datatool.rms.rakuten.co.jp/datadownload/', { waitUntil: 'networkidle2', timeout: 30000 });
          await new Promise(r => setTimeout(r, 3000));
        }
      }

      const ddUrl = page.url();
      console.log('データダウンロードページURL:', ddUrl);

      // Cookie確認
      const cookies = await page.cookies();
      const rmsCookies = cookies.filter(c => c.domain.includes('rms.rakuten') || c.domain.includes('datatool'));
      console.log(`RMS関連Cookie: ${rmsCookies.length}件 [${rmsCookies.map(c => c.name).join(', ')}]`);

      // ページ内容確認
      let ddBody = await page.evaluate(() => document.body?.innerText?.substring(0, 800) || '');
      console.log('データダウンロードページ内容:', ddBody.substring(0, 400));

      // HTML構造もログ
      const ddHtml = await page.evaluate(() => document.body?.innerHTML?.substring(0, 2000) || '');
      console.log('データダウンロードHTML:', ddHtml.substring(0, 600));

      // 認証エラーの場合
      if (ddUrl.includes('login_error') || ddUrl.includes('glogin') || ddBody.includes('認証エラー') || ddBody.includes('再度ログイン') || ddBody.includes('ログインし直してください') || ddBody.includes('サービス別権限設定')) {
        console.log('datatool認証エラー');
        additionalResults.push({ sheet: 'all_raw', status: 'auth_failed', message: 'datatool認証エラー - R-Loginのサービス別権限設定を確認してください' });
        additionalResults.push({ sheet: 'all_item_raw', status: 'auth_failed', message: 'datatool認証エラー' });
        throw new Error('datatool認証エラー - サービス別権限設定を確認');
      }

      // 全てのタブ・リンクをログ
      const allTabs = await page.evaluate(() => {
        return Array.from(document.querySelectorAll('a, button, [role="tab"], li.tab, .nav-item')).map(el => ({
          tag: el.tagName, text: (el.textContent || '').trim().substring(0, 50),
          href: el.href || '', cls: el.className?.substring?.(0, 50) || ''
        })).filter(el => el.text);
      });
      console.log('ページ内要素:', JSON.stringify(allTabs.slice(0, 30)));

      // 「店舗データ」タブ or リンクをクリック
      const shopDataClicked = await page.evaluate(() => {
        for (const el of document.querySelectorAll('a, button, li, div[role="tab"], span, [class*="tab"], [class*="nav"]')) {
          const t = (el.textContent || '').trim();
          if (t === '店舗データ' || t.includes('店舗データ')) {
            el.click();
            return t;
          }
        }
        return null;
      });
      console.log('店舗データクリック:', shopDataClicked);
      await new Promise(r => setTimeout(r, 3000));

      // 「日次」を選択
      const dailyClicked = await page.evaluate(() => {
        for (const el of document.querySelectorAll('input[type="radio"], a, button, li, div[role="tab"], label, span')) {
          const t = (el.textContent || el.parentElement?.textContent || '').trim();
          const label = el.closest('label')?.textContent || '';
          if (t === '日次' || label.includes('日次')) {
            el.click();
            return true;
          }
        }
        for (const sel of document.querySelectorAll('select')) {
          for (const opt of sel.options) {
            if (opt.text.includes('日次')) {
              sel.value = opt.value;
              sel.dispatchEvent(new Event('change', { bubbles: true }));
              return true;
            }
          }
        }
        return false;
      });
      console.log('日次選択:', dailyClicked);
      await new Promise(r => setTimeout(r, 2000));

      // 現在のフォーム状態をログ
      const formState = await page.evaluate(() => {
        return Array.from(document.querySelectorAll('input, select')).map(el => ({
          type: el.type, name: el.name, value: el.value?.substring(0, 30), id: el.id
        }));
      });
      console.log('フォーム状態:', JSON.stringify(formState));

      // 期間設定（datatoolの日付入力は "YYYY/MM/DD - YYYY/MM/DD" 形式のテキストフィールド）
      const twoYearsAgo = new Date();
      twoYearsAgo.setFullYear(twoYearsAgo.getFullYear() - 2);
      twoYearsAgo.setDate(1); // 月初
      const startStr = `${twoYearsAgo.getFullYear()}/${String(twoYearsAgo.getMonth() + 1).padStart(2, '0')}/01`;
      const now = new Date();
      const endStr = `${now.getFullYear()}/${String(now.getMonth() + 1).padStart(2, '0')}/${String(now.getDate()).padStart(2, '0')}`;
      const dateRangeStr = `${startStr} - ${endStr}`;
      console.log(`all_raw 期間設定: ${dateRangeStr}`);

      // 日次用の日付入力フィールドを探す（"YYYY/MM/DD - YYYY/MM/DD" 形式）
      const dateSet = await page.evaluate((newRange) => {
        for (const input of document.querySelectorAll('input[type="text"]')) {
          if (/\d{4}\/\d{2}\/\d{2}\s*-\s*\d{4}\/\d{2}\/\d{2}/.test(input.value)) {
            // React/Vueのsetterを使って値を設定
            const nativeInputValueSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
            nativeInputValueSetter.call(input, newRange);
            input.dispatchEvent(new Event('input', { bubbles: true }));
            input.dispatchEvent(new Event('change', { bubbles: true }));
            return { old: input.value, set: true };
          }
        }
        return { set: false };
      }, dateRangeStr);
      console.log('日付設定結果:', JSON.stringify(dateSet));

      // 設定が反映されたか確認、反映されない場合はクリック+キーボード入力
      if (!dateSet.set) {
        console.log('日付フィールドが見つからないため、手動入力を試行');
        const allTextInputs = await page.$$('input[type="text"]');
        for (const inp of allTextInputs) {
          const val = await inp.evaluate(el => el.value);
          if (/\d{4}\/\d{2}\/\d{2}/.test(val)) {
            await inp.click({ clickCount: 3 });
            await new Promise(r => setTimeout(r, 300));
            await page.keyboard.type(dateRangeStr, { delay: 30 });
            await page.keyboard.press('Enter');
            console.log('手動入力完了');
            break;
          }
        }
      }
      await new Promise(r => setTimeout(r, 2000));

      // ダウンロードディレクトリをクリア
      cleanupDir(DOWNLOAD_DIR);
      const beforeDl = capturedDownloads.length;

      // ダウンロードボタンをクリック
      let dlClicked = await clickButton(page, 'CSVダウンロード');
      if (!dlClicked) dlClicked = await clickButton(page, 'ダウンロード');
      if (!dlClicked) dlClicked = await clickButton(page, 'CSV出力');
      console.log('ダウンロードボタンクリック:', dlClicked);
      await new Promise(r => setTimeout(r, 3000));

      // ページ内容をログ
      ddBody = await page.evaluate(() => document.body?.innerText?.substring(0, 800) || '');
      console.log('ダウンロード後のページ:', ddBody.substring(0, 300));

      // レスポンスキャプチャ or ファイルシステムからCSV取得
      let allRawRecords = [];
      if (capturedDownloads.length > beforeDl) {
        const dl = capturedDownloads[capturedDownloads.length - 1];
        console.log(`all_raw レスポンスキャプチャ: ${dl.fileName} (${dl.buffer.length} bytes)`);
        allRawRecords = processDownload(dl);
      } else {
        const files = fs.existsSync(DOWNLOAD_DIR) ? fs.readdirSync(DOWNLOAD_DIR).filter(f => f.endsWith('.csv') || f.endsWith('.zip')) : [];
        console.log(`all_raw: レスポンスキャプチャなし、ファイルシステム確認: ${files.length}件 [${files.join(', ')}]`);
        for (const f of files) {
          const buf = fs.readFileSync(path.join(DOWNLOAD_DIR, f));
          console.log(`all_raw: ファイル取得 ${f} (${buf.length} bytes)`);
          allRawRecords = processDownload({ fileName: f, buffer: buf });
          if (allRawRecords.length > 0) break;
        }
      }

      if (allRawRecords.length > 0) {
        const headers = Object.keys(allRawRecords[0]);
        const rows = allRawRecords.map(r => headers.map(h => r[h] || ''));
        await writeRawToSheet(headers, rows, 'all_raw');
        additionalResults.push({ sheet: 'all_raw', status: 'ok', rows: allRawRecords.length });
      } else {
        console.log('all_raw: データ取得失敗');
        additionalResults.push({ sheet: 'all_raw', status: 'download_failed' });
      }
    } catch (err) {
      console.error('all_raw エラー:', err.message);
      if (!additionalResults.find(r => r.sheet === 'all_raw')) {
        additionalResults.push({ sheet: 'all_raw', status: 'error', message: err.message });
      }
    }

    // ============================================================
    // Step 12: RMSアクセス・流入分析 - 商品ページ月次CSV → all_item_raw
    // 商品ページ分析ページで月次選択→全商品CSV→ダイアログで全件→ダウンロード
    // monthsBack分の月をループして累積
    // ============================================================
    console.log('\n=== Step 12: all_item_raw (アクセス・流入分析) ===');
    try {
      // monthsBack を取得（RPPと同じ）
      const accMonthsBack = parseInt(req?.query?.months_back || '0', 10);
      let allAccRecords = [];

      // 商品ページ分析ページへ直接遷移（前テストで判明したURL）
      // まずメインメニュー経由でセッション確認後、access/itemに遷移
      await page.goto('https://datatool.rms.rakuten.co.jp/access/item', { waitUntil: 'networkidle2', timeout: 30000 });
      await new Promise(r => setTimeout(r, 3000));
      console.log('商品ページ分析URL:', page.url());

      // SPA レンダリング待ち（最大20秒）
      let spaReady = false;
      try {
        await page.waitForFunction(
          () => {
            const text = document.body?.innerText || '';
            return text.includes('商品ページ') || text.includes('CSV') || text.includes('月次');
          },
          { timeout: 20000 }
        );
        spaReady = true;
      } catch (e) {
        console.log('SPA待ちタイムアウト、ページ内容確認中...');
      }

      let targetFrame = page;
      const accBody = await page.evaluate(() => document.body?.innerText?.substring(0, 800) || '');
      console.log('商品ページ分析内容:', accBody.substring(0, 400));

      // コンテンツが空の場合、メインメニュー経由でクリック遷移
      if (!spaReady || !accBody.includes('商品ページ')) {
        console.log('直接遷移失敗、メインメニュー経由でクリック...');
        await page.goto('https://mainmenu.rms.rakuten.co.jp/', { waitUntil: 'networkidle2', timeout: 30000 });
        await new Promise(r => setTimeout(r, 2000));
        // アクセス・流入分析リンクをクリック（ページ遷移を待つ）
        await Promise.all([
          page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 30000 }).catch(() => {}),
          page.evaluate(() => {
            for (const a of document.querySelectorAll('a')) {
              const t = (a.textContent || '').trim();
              if (t.includes('アクセス・流入分析')) { a.click(); return; }
            }
          })
        ]);
        await new Promise(r => setTimeout(r, 3000));
        console.log('クリック後URL:', page.url());

        // SPA再待ち
        try {
          await page.waitForFunction(
            () => (document.body?.innerText || '').includes('商品ページ'),
            { timeout: 15000 }
          );
        } catch (e) {
          console.log('SPA再待ちタイムアウト');
        }

        // 商品ページタブをクリック
        const productPageClicked = await page.evaluate(() => {
          for (const el of document.querySelectorAll('a, [role="tab"], button, span')) {
            const t = (el.textContent || '').trim();
            if (t === '商品ページ') { el.click(); return t; }
          }
          return null;
        });
        console.log('商品ページタブクリック:', productPageClicked);
        await Promise.all([
          page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 15000 }).catch(() => {}),
          new Promise(r => setTimeout(r, 5000))
        ]);
        console.log('タブクリック後URL:', page.url());
      }

      // 月次ラジオボタン or タブを探してクリック
      const accBody2 = await page.evaluate(() => document.body?.innerText?.substring(0, 800) || '');
      console.log('現在のページ内容:', accBody2.substring(0, 300));

      // 「月次」をクリック
      const monthlyClicked = await page.evaluate(() => {
        // ラジオボタン
        for (const el of document.querySelectorAll('input[type="radio"]')) {
          const label = el.closest('label')?.textContent || el.parentElement?.textContent || '';
          if (label.includes('月次')) { el.click(); return 'radio'; }
        }
        // テキスト要素
        for (const el of document.querySelectorAll('label, span, div, button, a')) {
          const t = (el.textContent || '').trim();
          if (t === '月次') { el.click(); return 'text'; }
        }
        return false;
      });
      console.log('月次選択:', monthlyClicked);
      await new Promise(r => setTimeout(r, 3000));

      // 月ごとにダウンロード（accMonthsBack月前〜当月）
      for (let i = accMonthsBack; i >= 0; i--) {
        const d = new Date();
        d.setMonth(d.getMonth() - i);
        const monthLabel = `${d.getFullYear()}/${String(d.getMonth() + 1).padStart(2, '0')}`;
        const monthRange = `${monthLabel} - ${monthLabel}`;
        console.log(`all_item_raw [${monthLabel}]: 期間設定...`);

        // 月次日付入力フィールドを探して設定（"YYYY/MM - YYYY/MM" 形式）
        const dateSetResult = await targetFrame.evaluate((newRange) => {
          for (const input of document.querySelectorAll('input[type="text"]')) {
            if (/\d{4}\/\d{2}\s*-\s*\d{4}\/\d{2}/.test(input.value)) {
              const nativeInputValueSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
              nativeInputValueSetter.call(input, newRange);
              input.dispatchEvent(new Event('input', { bubbles: true }));
              input.dispatchEvent(new Event('change', { bubbles: true }));
              return { old: input.value, set: true };
            }
          }
          return { set: false };
        }, monthRange);
        console.log(`all_item_raw [${monthLabel}]: 日付設定:`, JSON.stringify(dateSetResult));

        // フォールバック: テキスト入力で設定
        if (!dateSetResult.set) {
          const allTextInputs = await targetFrame.$$('input[type="text"]');
          for (const inp of allTextInputs) {
            const val = await inp.evaluate(el => el.value);
            if (/\d{4}\/\d{2}/.test(val)) {
              await inp.click({ clickCount: 3 });
              await new Promise(r => setTimeout(r, 300));
              await page.keyboard.type(monthRange, { delay: 30 });
              await page.keyboard.press('Enter');
              console.log(`all_item_raw [${monthLabel}]: 手動入力で設定`);
              break;
            }
          }
        }
        await new Promise(r => setTimeout(r, 2000));

        // ダウンロードディレクトリをクリア
        cleanupDir(DOWNLOAD_DIR);
        const beforeDl2 = capturedDownloads.length;

        // 「全商品CSV」ボタンをクリック（ダイアログを開く）
        let csvBtnClicked = await page.evaluate(() => {
          for (const el of document.querySelectorAll('button, a, span')) {
            const t = (el.textContent || '').trim();
            if (t.includes('全商品CSV') || t.includes('全商品 CSV')) {
              el.click();
              return t;
            }
          }
          return null;
        });
        console.log(`all_item_raw [${monthLabel}]: 全商品CSVクリック:`, csvBtnClicked);
        await new Promise(r => setTimeout(r, 2000));

        // CSVダウンロードダイアログ: 「全件」ラジオボタンを選択
        const allItemsSet = await page.evaluate(() => {
          for (const el of document.querySelectorAll('input[type="radio"]')) {
            const label = el.closest('label')?.textContent || el.parentElement?.textContent || '';
            if (label.includes('全件')) {
              el.click();
              return true;
            }
          }
          return false;
        });
        console.log(`all_item_raw [${monthLabel}]: 全件選択:`, allItemsSet);
        await new Promise(r => setTimeout(r, 1000));

        // ダイアログ内「ダウンロード」ボタンをクリック（モーダル内のbuttonのみ）
        const dlBtnClicked = await page.evaluate(() => {
          // モーダル/ダイアログコンテナを探す
          const modals = document.querySelectorAll('[class*="modal"], [class*="dialog"], [class*="Modal"], [class*="Dialog"], [role="dialog"], [class*="overlay"], [class*="Overlay"], [class*="popup"], [class*="Popup"]');
          // モーダル内のボタンを優先
          for (const modal of modals) {
            for (const btn of modal.querySelectorAll('button, a')) {
              const t = (btn.textContent || '').trim();
              if (t === 'ダウンロード' || (t.includes('ダウンロード') && !t.includes('キャンセル') && !t.includes('全商品') && !t.includes('データ'))) {
                btn.click();
                return `modal: ${t}`;
              }
            }
          }
          // モーダルが見つからない場合、buttonタグのみで探す（aタグの「6 データダウンロード」を除外）
          for (const btn of document.querySelectorAll('button')) {
            const t = (btn.textContent || '').trim();
            if (t === 'ダウンロード' || (t.includes('ダウンロード') && t.length < 10 && !t.includes('キャンセル') && !t.includes('全商品'))) {
              btn.click();
              return `button: ${t}`;
            }
          }
          return null;
        });
        console.log(`all_item_raw [${monthLabel}]: ダウンロードクリック:`, dlBtnClicked);
        await new Promise(r => setTimeout(r, 3000));

        // レスポンスキャプチャ or ファイルシステムからCSV取得
        let monthRecords = [];
        if (capturedDownloads.length > beforeDl2) {
          const dl = capturedDownloads[capturedDownloads.length - 1];
          console.log(`all_item_raw [${monthLabel}]: レスポンスキャプチャ: ${dl.fileName} (${dl.buffer.length} bytes)`);
          monthRecords = processDownload(dl);
        } else {
          const files = fs.existsSync(DOWNLOAD_DIR) ? fs.readdirSync(DOWNLOAD_DIR).filter(f => f.endsWith('.csv') || f.endsWith('.zip')) : [];
          console.log(`all_item_raw [${monthLabel}]: ファイル確認: ${files.length}件 [${files.join(', ')}]`);
          for (const f of files) {
            const buf = fs.readFileSync(path.join(DOWNLOAD_DIR, f));
            monthRecords = processDownload({ fileName: f, buffer: buf });
            if (monthRecords.length > 0) break;
          }
        }

        if (monthRecords.length > 0) {
          // 取得月を先頭カラムとして追加（"YYYY年MM月" 形式）
          const monthJp = `${d.getFullYear()}年${String(d.getMonth() + 1).padStart(2, '0')}月`;
          for (const rec of monthRecords) {
            rec['取得月'] = monthJp;
          }
          allAccRecords.push(...monthRecords);
          console.log(`all_item_raw [${monthLabel}]: ${monthRecords.length}件取得`);
        } else {
          console.log(`all_item_raw [${monthLabel}]: データなし`);
        }
      }

      if (allAccRecords.length > 0) {
        // 取得月を先���に、残りはCSV元順
        const allKeys = Object.keys(allAccRecords[0]);
        const headers = ['取得月', ...allKeys.filter(k => k !== '取得月')];
        const rows = allAccRecords.map(r => headers.map(h => r[h] || ''));
        await writeRawToSheet(headers, rows, 'all_item_raw', ['\u53D6\u5F97\u6708', '\u5546\u54C1\u7BA1\u7406\u756A\u53F7']);
        additionalResults.push({ sheet: 'all_item_raw', status: 'ok', rows: allAccRecords.length });
      } else {
        console.log('all_item_raw: データ取得失敗');
        additionalResults.push({ sheet: 'all_item_raw', status: 'download_failed' });
      }
    } catch (err) {
      console.error('all_item_raw エラー:', err.message);
      additionalResults.push({ sheet: 'all_item_raw', status: 'error', message: err.message });
    }

    cleanupDir(DOWNLOAD_DIR);

    } // end if (!skipDatatool)

    // ============================================================
    // Step 13-15: 追加レポート (afi_raw / mail_raw / line_raw)
    // skip_extra=1 でスキップ可能
    // ============================================================
    if (skipExtra) {
      console.log('skip_extra=1: afi_raw/mail_raw/line_rawをスキップ');
    }

    if (!skipExtra) {

    // ============================================================
    // Step 13: アフィリエイト成果レポート → afi_raw
    // アフィリエイト > 成果レポート > 成果速報 注文一覧 > CSV
    // ============================================================
    console.log('\n=== Step 13: afi_raw (アフィリエイト成果レポート) ===');
    if (skipAfi) {
      console.log('skip_afi=1: afi_rawをスキップ');
    } else try {
      const afiMonthsBack = parseInt(req?.query?.months_back || '0', 10);
      let allAfiRecords = [];

      // メインメニューへ遷移してリンク構造を取得
      await page.goto('https://mainmenu.rms.rakuten.co.jp/', { waitUntil: 'networkidle2', timeout: 30000 });
      await new Promise(r => setTimeout(r, 2000));

      // メインメニューの全リンクからアフィリエイト関連のhrefを取得
      const afiLinks = await page.evaluate(() => {
        const links = [];
        for (const a of document.querySelectorAll('a[href]')) {
          const t = (a.textContent || '').trim();
          const h = a.href;
          if (t.includes('\u30A2\u30D5\u30A3\u30EA\u30A8\u30A4\u30C8') || h.includes('affiliate') || h.includes('afi')) {
            links.push({ text: t.substring(0, 60), href: h });
          }
        }
        return links;
      });
      console.log('アフィリエイト関連リンク:', JSON.stringify(afiLinks));

      // アフィリエイトページへ直接遷移（hrefから最適なリンクを選択）
      let afiUrl = null;
      for (const link of afiLinks) {
        // サブメニューの「アフィリエイト」項目のhrefを優先
        if (link.href && !link.href.includes('javascript:') && link.href !== 'https://mainmenu.rms.rakuten.co.jp/') {
          afiUrl = link.href;
          break;
        }
      }

      if (afiUrl) {
        console.log('アフィリエイトURL:', afiUrl);
        await page.goto(afiUrl, { waitUntil: 'networkidle2', timeout: 30000 });
      } else {
        // フォールバック: サイドメニューのカテゴリを展開してからリンクを取得
        console.log('直接リンクなし。メニュー展開を試行...');
        await page.evaluate(() => {
          // 「広告・アフィリエイト・楽天大学」カテゴリをクリックして展開
          for (const el of document.querySelectorAll('a, div, span, li')) {
            const t = (el.textContent || '').trim();
            if (t.includes('\u5E83\u544A\u30FB\u30A2\u30D5\u30A3\u30EA\u30A8\u30A4\u30C8')) { el.click(); return; }
          }
        });
        await new Promise(r => setTimeout(r, 1500));
        // 展開後のリンクを再取得
        const expandedLinks = await page.evaluate(() => {
          const links = [];
          for (const a of document.querySelectorAll('a[href]')) {
            const t = (a.textContent || '').trim();
            if (t === '\u30A2\u30D5\u30A3\u30EA\u30A8\u30A4\u30C8' || (t.includes('\u30A2\u30D5\u30A3\u30EA\u30A8\u30A4\u30C8') && !t.includes('\u5E83\u544A'))) {
              links.push({ text: t, href: a.href });
            }
          }
          return links;
        });
        console.log('展開後リンク:', JSON.stringify(expandedLinks));
        for (const link of expandedLinks) {
          if (link.href && !link.href.includes('javascript:')) {
            afiUrl = link.href;
            break;
          }
        }
        if (afiUrl) {
          await page.goto(afiUrl, { waitUntil: 'networkidle2', timeout: 30000 });
        }
      }
      await new Promise(r => setTimeout(r, 3000));

      // 新しいタブが開いた場合はそちらに切り替え
      let allPages = await browser.pages();
      let afiPage = allPages[allPages.length - 1];
      console.log('アフィリエイトページURL:', afiPage.url());

      // ページ内容と全リンクをログ
      const afiBody = await afiPage.evaluate(() => document.body?.innerText?.substring(0, 800) || '');
      console.log('アフィリエイトページ内容:', afiBody.substring(0, 500));

      // 成果速報－注文一覧ページへ直接遷移
      console.log('\u6210\u679C\u901F\u5831\u2015\u6CE8\u6587\u4E00\u89A7\u3078\u76F4\u63A5\u9077\u79FB: https://afl.rms.rakuten.co.jp/report#pending');
      await afiPage.goto('https://afl.rms.rakuten.co.jp/report#pending', { waitUntil: 'networkidle2', timeout: 30000 });
      await new Promise(r => setTimeout(r, 3000));

      // ハッシュナビゲーション対応：クリックで「成果速報－注文一覧」タブに切替
      const orderTabClicked = await afiPage.evaluate(() => {
        for (const a of document.querySelectorAll('a[href]')) {
          const t = (a.textContent || '').trim();
          if (t === '\u6210\u679C\u901F\u5831\uFF0D\u6CE8\u6587\u4E00\u89A7' || t === '\u6210\u679C\u901F\u5831\u2015\u6CE8\u6587\u4E00\u89A7') {
            a.click();
            return t;
          }
        }
        // フォールバック: 部分マッチ
        for (const a of document.querySelectorAll('a[href]')) {
          const t = (a.textContent || '').trim();
          if (t.includes('\u6CE8\u6587\u4E00\u89A7') && t.includes('\u6210\u679C\u901F\u5831')) {
            a.click();
            return t;
          }
        }
        return null;
      });
      console.log('\u6CE8\u6587\u4E00\u89A7\u30BF\u30D6\u30AF\u30EA\u30C3\u30AF:', orderTabClicked);
      await new Promise(r => setTimeout(r, 3000));

      // タブ切り替えチェック
      allPages = await browser.pages();
      afiPage = allPages[allPages.length - 1];
      console.log('\u6CE8\u6587\u4E00\u89A7\u30DA\u30FC\u30B8URL:', afiPage.url());

      // ページ内容をログ
      const reportBody = await afiPage.evaluate(() => document.body?.innerText?.substring(0, 1000) || '');
      console.log('\u6CE8\u6587\u4E00\u89A7\u30DA\u30FC\u30B8\u5185\u5BB9:', reportBody.substring(0, 600));

      // ページ上の全ボタン・リンクをログ
      const allAfiButtons = await afiPage.evaluate(() => {
        const btns = [];
        for (const el of document.querySelectorAll('button, a, input[type="submit"], input[type="button"]')) {
          const t = (el.textContent || el.value || '').trim().substring(0, 50);
          if (t) btns.push({ text: t, tag: el.tagName, id: el.id || '', cls: (el.className || '').substring(0, 30) });
        }
        return btns;
      });
      console.log('afi全ボタン:', JSON.stringify(allAfiButtons));

      // CSVダウンロードAPI URLを特定
      const csvApiBase = await afiPage.evaluate(() => {
        // 最初のcsvリンク（成果速報-注文一覧）のhrefを取得
        for (const a of document.querySelectorAll('a')) {
          if ((a.textContent || '').trim().toLowerCase() === 'csv') {
            return a.href || '';
          }
        }
        return '';
      });
      console.log('afi_raw CSVダウンロードAPI:', csvApiBase);
      // csvApiBase例: https://afl.rms.rakuten.co.jp/api/report/download/pending?format=csv&date=

      // 月ごとにCSV APIを直接呼び出し
      for (let i = afiMonthsBack; i >= monthsSkip; i--) {
        const d = new Date();
        d.setMonth(d.getMonth() - i);
        const year = d.getFullYear();
        const month = d.getMonth() + 1;
        const monthLabel = `${year}/${String(month).padStart(2, '0')}`;
        const monthValue = `${year}-${String(month).padStart(2, '0')}`;
        console.log(`afi_raw [${monthLabel}]: CSV API直接呼び出し...`);

        // API URLにdate=YYYY-MMを付けて直接ナビゲート（セッションcookieを利用）
        const csvUrl = csvApiBase ? csvApiBase.replace(/date=$/, `date=${monthValue}`) :
          `https://afl.rms.rakuten.co.jp/api/report/download/pending?format=csv&date=${monthValue}`;
        console.log(`afi_raw [${monthLabel}]: URL: ${csvUrl}`);

        // ダウンロードディレクトリをクリーン
        // CSV APIを直接呼び出してダウンロード
        // CSV APIをfetch()で直接取得
        const csvContent = await afiPage.evaluate(async (url) => {
          try {
            const resp = await fetch(url, { credentials: 'include' });
            if (!resp.ok) return { error: `HTTP ${resp.status}`, url };
            const buf = await resp.arrayBuffer();
            const bytes = new Uint8Array(buf);
            let binary = '';
            for (let j = 0; j < bytes.length; j++) binary += String.fromCharCode(bytes[j]);
            return { base64: btoa(binary), size: bytes.length };
          } catch (e) {
            return { error: e.message, url };
          }
        }, csvUrl);

        // CSV取���
        let afiRecords = [];
        if (csvContent.error) {
          console.log(`afi_raw [${monthLabel}]: CSV fetch error: ${csvContent.error}`);
        } else if (csvContent.base64) {
          console.log(`afi_raw [${monthLabel}]: CSV fetch ok size=${csvContent.size}`);
          const buf = Buffer.from(csvContent.base64, 'base64');
          afiRecords = processDownload({ fileName: 'afi.csv', buffer: buf });
        }

        if (afiRecords.length > 0) {
          allAfiRecords.push(...afiRecords);
          console.log(`afi_raw [${monthLabel}]: ${afiRecords.length}件取得`);
        } else {
          console.log(`afi_raw [${monthLabel}]: データなし`);
        }
      }

      if (allAfiRecords.length > 0) {
        const headers = Object.keys(allAfiRecords[0]);
        const rows = allAfiRecords.map(r => headers.map(h => r[h] || ''));
        await writeRawToSheet(headers, rows, 'afi_raw', ['受注番号']);
        additionalResults.push({ sheet: 'afi_raw', status: 'ok', rows: allAfiRecords.length });
      } else {
        console.log('afi_raw: データなし');
        additionalResults.push({ sheet: 'afi_raw', status: 'no_data' });
      }
    } catch (err) {
      console.error('afi_raw エラー:', err.message);
      additionalResults.push({ sheet: 'afi_raw', status: 'error', message: err.message });
    }

    cleanupDir(DOWNLOAD_DIR);

    // ============================================================
    // Step 14: メルマガ月次主要指標 → mail_raw
    // メルマガ配信 > 月次主要指標 > CSVダウンロード
    // ============================================================
    console.log('\n=== Step 14: mail_raw (メルマガ月次主要指標) ===');
    if (skipMail) {
      console.log('skip_mail=1: mail_rawをスキップ');
    } else try {
      const mailMonthsBack = parseInt(req?.query?.months_back || '0', 10);
      let allMailRecords = [];

      // メインメニューへ遷移
      await page.goto('https://mainmenu.rms.rakuten.co.jp/', { waitUntil: 'networkidle2', timeout: 30000 });
      await new Promise(r => setTimeout(r, 2000));

      // メール・SNS関連のリンクhrefを取得
      const mailLinks = await page.evaluate(() => {
        const links = [];
        for (const a of document.querySelectorAll('a[href]')) {
          const t = (a.textContent || '').trim();
          const h = a.href;
          if (t.includes('\u30E1\u30EB\u30DE\u30AC') || t.includes('R-Mail') || t.includes('\u30E1\u30FC\u30EB\u914D\u4FE1') || h.includes('r-mail') || h.includes('mail')) {
            links.push({ text: t.substring(0, 60), href: h });
          }
        }
        return links;
      });
      console.log('メルマガ関連リンク:', JSON.stringify(mailLinks));

      // メルマガページへ直接遷移
      let mailUrl = null;
      for (const link of mailLinks) {
        if (link.href && !link.href.includes('javascript:') && link.href !== 'https://mainmenu.rms.rakuten.co.jp/') {
          mailUrl = link.href;
          break;
        }
      }

      if (!mailUrl) {
        // メニュー展開: 「メール・SNSマーケティング」カテゴリ
        console.log('直接リンクなし。メニュー展開を試行...');
        await page.evaluate(() => {
          for (const el of document.querySelectorAll('a, div, span, li')) {
            const t = (el.textContent || '').trim();
            if (t.includes('\u30E1\u30FC\u30EB\u30FBSNS')) { el.click(); return; }
          }
        });
        await new Promise(r => setTimeout(r, 1500));
        const expandedLinks = await page.evaluate(() => {
          const links = [];
          for (const a of document.querySelectorAll('a[href]')) {
            const t = (a.textContent || '').trim();
            if (t.includes('\u30E1\u30EB\u30DE\u30AC') || t.includes('R-Mail')) {
              links.push({ text: t, href: a.href });
            }
          }
          return links;
        });
        console.log('展開後リンク:', JSON.stringify(expandedLinks));
        for (const link of expandedLinks) {
          if (link.href && !link.href.includes('javascript:')) { mailUrl = link.href; break; }
        }
      }

      if (mailUrl) {
        console.log('メルマガURL:', mailUrl);
        await page.goto(mailUrl, { waitUntil: 'networkidle2', timeout: 30000 });
      }
      await new Promise(r => setTimeout(r, 3000));

      // 最新タブに切り替え
      let mailPages = await browser.pages();
      let mailPage = mailPages[mailPages.length - 1];
      console.log('メルマガページURL:', mailPage.url());

      // ページ内容と全リンクを確認
      let mailBody = await mailPage.evaluate(() => document.body?.innerText?.substring(0, 800) || '');
      console.log('メルマガページ内容:', mailBody.substring(0, 500));

      // 月次主要指標関連のリンクを探す
      const mailPageLinks = await mailPage.evaluate(() => {
        const links = [];
        for (const a of document.querySelectorAll('a[href]')) {
          const t = (a.textContent || '').trim();
          if (t.includes('\u6708\u6B21') || t.includes('\u4E3B\u8981\u6307\u6A19') || t.includes('CSV') || t.includes('\u30EC\u30DD\u30FC\u30C8')) {
            links.push({ text: t.substring(0, 60), href: a.href });
          }
        }
        return links;
      });
      console.log('メルマガページリンク:', JSON.stringify(mailPageLinks));

      // 月次主要指標ページへ遷移
      let monthlyUrl = null;
      for (const link of mailPageLinks) {
        if (link.text.includes('\u6708\u6B21\u4E3B\u8981\u6307\u6A19') && link.href && !link.href.includes('javascript:')) {
          monthlyUrl = link.href;
          break;
        }
      }
      if (monthlyUrl) {
        console.log('月次主要指標URL:', monthlyUrl);
        await mailPage.goto(monthlyUrl, { waitUntil: 'networkidle2', timeout: 30000 });
      } else {
        // クリックで遷移を試みる
        const monthlyTabClicked = await mailPage.evaluate(() => {
          for (const el of document.querySelectorAll('a, button, [role="tab"], li, span')) {
            const t = (el.textContent || '').trim();
            if (t.includes('\u6708\u6B21\u4E3B\u8981\u6307\u6A19')) { el.click(); return t; }
          }
          return null;
        });
        console.log('月次主要指標タブ:', monthlyTabClicked);
      }
      await new Promise(r => setTimeout(r, 3000));

      // 月ごとにCSVダウンロード
      for (let i = mailMonthsBack; i >= monthsSkip; i--) {
        const d = new Date();
        d.setMonth(d.getMonth() - i);
        const monthLabel = `${d.getFullYear()}/${String(d.getMonth() + 1).padStart(2, '0')}`;
        const yearMonth = `${d.getFullYear()}\u5E74${d.getMonth() + 1}\u6708`;
        const monthJp = `${d.getFullYear()}\u5E74${String(d.getMonth() + 1).padStart(2, '0')}\u6708`;
        console.log(`mail_raw [${monthLabel}]: 期間設定...`);

        // 表示月を設定（"2026年4月" 形式の入力フィールドまたはセレクタ）
        const monthSet = await mailPage.evaluate((ym, ymFull) => {
          // input[type="month"]
          for (const inp of document.querySelectorAll('input[type="month"]')) {
            const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
            nativeSetter.call(inp, ymFull);
            inp.dispatchEvent(new Event('input', { bubbles: true }));
            inp.dispatchEvent(new Event('change', { bubbles: true }));
            return `month_input: ${inp.value}`;
          }
          // テキスト入力で年月
          for (const inp of document.querySelectorAll('input[type="text"]')) {
            if (/\d{4}\u5E74\d{1,2}\u6708/.test(inp.value) || /\d{4}\/\d{2}/.test(inp.value)) {
              const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
              nativeSetter.call(inp, ym);
              inp.dispatchEvent(new Event('input', { bubbles: true }));
              inp.dispatchEvent(new Event('change', { bubbles: true }));
              return `text_input: ${inp.value}`;
            }
          }
          return 'not_found';
        }, yearMonth, `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`);
        console.log(`mail_raw [${monthLabel}]: 月設定: ${monthSet}`);

        // 「変更」ボタンクリック
        await mailPage.evaluate(() => {
          for (const btn of document.querySelectorAll('button, input[type="submit"], a')) {
            const t = (btn.textContent || btn.value || '').trim();
            if (t === '\u5909\u66F4' || t.includes('\u5909\u66F4')) { btn.click(); return; }
          }
        });
        await new Promise(r => setTimeout(r, 3000));

        // CSVダウンロード
        cleanupDir(DOWNLOAD_DIR);
        const beforeDl = capturedDownloads.length;

        const csvClicked = await mailPage.evaluate(() => {
          for (const el of document.querySelectorAll('button, a')) {
            const t = (el.textContent || '').trim();
            if (t === 'CSV\u30C0\u30A6\u30F3\u30ED\u30FC\u30C9' || t.includes('CSV')) { el.click(); return t; }
          }
          return null;
        });
        console.log(`mail_raw [${monthLabel}]: CSVクリック: ${csvClicked}`);
        await new Promise(r => setTimeout(r, 4000));

        // CSV取得
        let mailRecords = [];
        if (capturedDownloads.length > beforeDl) {
          const dl = capturedDownloads[capturedDownloads.length - 1];
          mailRecords = processDownload(dl);
        } else {
          const files = fs.existsSync(DOWNLOAD_DIR) ? fs.readdirSync(DOWNLOAD_DIR).filter(f => f.endsWith('.csv')) : [];
          for (const f of files) {
            const buf = fs.readFileSync(path.join(DOWNLOAD_DIR, f));
            mailRecords = processDownload({ fileName: f, buffer: buf });
            if (mailRecords.length > 0) break;
          }
        }

        if (mailRecords.length > 0) {
          allMailRecords.push(...mailRecords);
          console.log(`mail_raw [${monthLabel}]: ${mailRecords.length}件取得`);
        } else {
          console.log(`mail_raw [${monthLabel}]: データなし`);
        }
      }

      if (allMailRecords.length > 0) {
        const headers = Object.keys(allMailRecords[0]);
        const rows = allMailRecords.map(r => headers.map(h => r[h] || ''));
        await writeRawToSheet(headers, rows, 'mail_raw');
        additionalResults.push({ sheet: 'mail_raw', status: 'ok', rows: allMailRecords.length });
      } else {
        console.log('mail_raw: データなし');
        additionalResults.push({ sheet: 'mail_raw', status: 'no_data' });
      }
    } catch (err) {
      console.error('mail_raw エラー:', err.message);
      additionalResults.push({ sheet: 'mail_raw', status: 'error', message: err.message });
    }

    cleanupDir(DOWNLOAD_DIR);

    // ============================================================
    // Step 15: LINE公式アカウント for R-SNS メッセージ分析 → line_raw
    // LINE公式アカウント for R-SNS > パフォーマンスレポート > メッセージ分析 > CSVダウンロード
    // 月ごとにループして各月のCSVを取得
    // ============================================================
    console.log('\n=== Step 15: line_raw (LINE R-SNS メッセージ分析) ===');
    if (skipLine) {
      console.log('skip_line=1: line_rawをスキップ');
    } else try {
      const lineMonthsBack = parseInt(req?.query?.months_back || '0', 10);
      let allLineRecords = [];

      // メインメニューへ
      await page.goto('https://mainmenu.rms.rakuten.co.jp/', { waitUntil: 'networkidle2', timeout: 30000 });
      await new Promise(r => setTimeout(r, 2000));

      // R-SNS / LINE関連リンクのhrefを取得
      const lineLinks = await page.evaluate(() => {
        const links = [];
        for (const a of document.querySelectorAll('a[href]')) {
          const t = (a.textContent || '').trim();
          const h = a.href;
          if (t.includes('R-SNS') || t.includes('LINE') || h.includes('r-sns') || h.includes('line') || h.includes('sns')) {
            links.push({ text: t.substring(0, 60), href: h });
          }
        }
        return links;
      });
      console.log('LINE関連リンク:', JSON.stringify(lineLinks));

      // LINE公式アカウント for R-SNSページへ直接遷移
      let lineUrl = null;
      for (const link of lineLinks) {
        if (link.href && link.href.includes('sns.rms.rakuten.co.jp/home')) {
          lineUrl = link.href;
          break;
        }
      }
      if (!lineUrl) {
        for (const link of lineLinks) {
          if (link.href && !link.href.includes('javascript:') && link.href !== 'https://mainmenu.rms.rakuten.co.jp/' && (link.text.includes('LINE') || link.href.includes('sns'))) {
            lineUrl = link.href;
            break;
          }
        }
      }

      if (!lineUrl) {
        console.log('直接リンクなし。メニュー展開を試行...');
        await page.evaluate(() => {
          for (const el of document.querySelectorAll('a, div, span, li')) {
            const t = (el.textContent || '').trim();
            if (t.includes('メール・SNS') || t.includes('SNSマーケティング')) { el.click(); return; }
          }
        });
        await new Promise(r => setTimeout(r, 1500));
        const expandedLinks = await page.evaluate(() => {
          const links = [];
          for (const a of document.querySelectorAll('a[href]')) {
            const t = (a.textContent || '').trim();
            if (t.includes('R-SNS') || t.includes('LINE')) {
              links.push({ text: t, href: a.href });
            }
          }
          return links;
        });
        console.log('展開後リンク:', JSON.stringify(expandedLinks));
        for (const link of expandedLinks) {
          if (link.href && !link.href.includes('javascript:')) { lineUrl = link.href; break; }
        }
      }

      if (lineUrl) {
        console.log('LINE URL:', lineUrl);
        await page.goto(lineUrl, { waitUntil: 'networkidle2', timeout: 30000 });
      }
      await new Promise(r => setTimeout(r, 3000));

      // 最新タブに切り替え
      let linePages = await browser.pages();
      let linePage = linePages[linePages.length - 1];
      console.log('LINEページURL:', linePage.url());

      // パフォーマンスレポートへ遷移
      const perfLink = await linePage.evaluate(() => {
        for (const a of document.querySelectorAll('a[href]')) {
          const t = (a.textContent || '').trim();
          if (t === 'パフォーマンスレポート') return { text: t, href: a.href };
        }
        return null;
      });
      console.log('パフォーマンスレポートリンク:', JSON.stringify(perfLink));

      if (perfLink && perfLink.href && !perfLink.href.includes('javascript:')) {
        await linePage.goto(perfLink.href, { waitUntil: 'networkidle2', timeout: 30000 });
      } else {
        await linePage.evaluate(() => {
          for (const el of document.querySelectorAll('a, button, span, div')) {
            const t = (el.textContent || '').trim();
            if (t === 'パフォーマンスレポート') { el.click(); return; }
          }
        });
      }
      await new Promise(r => setTimeout(r, 3000));
      linePages = await browser.pages();
      linePage = linePages[linePages.length - 1];
      console.log('パフォーマンスレポートURL:', linePage.url());

      // 「メッセージ分析」タブへ遷移
      const msgAnalysisLink = await linePage.evaluate(() => {
        for (const a of document.querySelectorAll('a[href]')) {
          const t = (a.textContent || '').trim();
          if (t === 'メッセージ分析') return { text: t, href: a.href };
        }
        return null;
      });
      console.log('メッセージ分析リンク:', JSON.stringify(msgAnalysisLink));

      if (msgAnalysisLink && msgAnalysisLink.href && !msgAnalysisLink.href.includes('javascript:')) {
        await linePage.goto(msgAnalysisLink.href, { waitUntil: 'networkidle2', timeout: 30000 });
      } else {
        await linePage.evaluate(() => {
          for (const el of document.querySelectorAll('a, button')) {
            const t = (el.textContent || '').trim();
            if (t === 'メッセージ分析') { el.click(); return; }
          }
        });
      }
      await new Promise(r => setTimeout(r, 3000));
      linePages = await browser.pages();
      linePage = linePages[linePages.length - 1];
      console.log('メッセージ分析URL:', linePage.url());

      // ページ内容を確認
      const lineReportBody = await linePage.evaluate(() => document.body?.innerText?.substring(0, 1000) || '');
      console.log('メッセージ分析内容:', lineReportBody.substring(0, 600));

      // ページ構造を診断（API URL、daterangepicker構造）
      const linePageStructure = await linePage.evaluate(() => {
        const result = {};
        // 全inputの情報
        result.inputs = Array.from(document.querySelectorAll('input')).map((inp, idx) => ({
          idx, type: inp.type, name: inp.name, id: inp.id, value: (inp.value || '').substring(0, 50),
          cls: (inp.className || '').substring(0, 60), readonly: inp.readOnly
        }));
        // CSVダウンロードリンク
        result.csvLinks = Array.from(document.querySelectorAll('a, button')).filter(el => {
          const t = (el.textContent || '').trim().toLowerCase();
          return t.includes('csv') || t.includes('download');
        }).map(el => ({
          tag: el.tagName, text: (el.textContent || '').trim().substring(0, 50),
          href: el.href || '', onclick: el.getAttribute('onclick') || '',
          outerHTML: el.outerHTML.substring(0, 200)
        }));
        // daterangepicker構造
        const drpContainer = document.querySelector('.daterangepicker');
        if (drpContainer) {
          result.drpHTML = drpContainer.innerHTML.substring(0, 500);
          result.drpVisible = drpContainer.style.display !== 'none';
        }
        // URL情報
        result.url = window.location.href;
        result.search = window.location.search;
        return result;
      });
      console.log('line_raw ページ構造:', JSON.stringify(linePageStructure));

      // 月ごとにループしてCSVダウンロード
      for (let i = lineMonthsBack; i >= monthsSkip; i--) {
        const d = new Date();
        d.setMonth(d.getMonth() - i);
        const year = d.getFullYear();
        const month = d.getMonth() + 1;
        const startDate = `${year}/${String(month).padStart(2, '0')}/01`;
        const today = new Date();
        const lastDay = (year === today.getFullYear() && month === today.getMonth() + 1)
          ? today.getDate() - 1  // 当月は昨日まで
          : new Date(year, month, 0).getDate();
        if (lastDay < 1) { console.log(`line_raw [${year}年${month}月]: skip (today is 1st)`); continue; }
        const endDate = `${year}/${String(month).padStart(2, '0')}/${String(lastDay).padStart(2, '0')}`;
        const monthLabel = `${year}年${month}月`;
        console.log(`line_raw [${monthLabel}]: 期間 ${startDate} ~ ${endDate}`);

        // 日付範囲ピッカー対応（1つのinputに「YYYY/MM/DD - YYYY/MM/DD」形式）
        // 1. 期間入力フィールドをクリックしてdaterangepickerを開く
        const rangeInputIdx = await linePage.evaluate(() => {
          const inputs = Array.from(document.querySelectorAll('input'));
          for (let idx = 0; idx < inputs.length; idx++) {
            const v = inputs[idx].value || '';
            if (v.match(/\d{4}\/\d{2}\/\d{2}\s*-\s*\d{4}\/\d{2}\/\d{2}/)) return idx;
          }
          return -1;
        });
        console.log(`line_raw [${monthLabel}]: 範囲input idx: ${rangeInputIdx}`);

        if (rangeInputIdx >= 0) {
          // 1. 日付入力欄の値を書き換え
          const rangeValue = `${startDate} - ${endDate}`;
          await linePage.evaluate((idx, val) => {
            const inp = document.querySelectorAll('input')[idx];
            const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
            nativeSetter.call(inp, val);
            inp.dispatchEvent(new Event('input', { bubbles: true }));
            inp.dispatchEvent(new Event('change', { bubbles: true }));
          }, rangeInputIdx, rangeValue);
          console.log(`line_raw [${monthLabel}]: input set to ${rangeValue}`);

          // 2. 「絞り込み」ボタンをクリック
          const filterClicked = await linePage.evaluate(() => {
            const btns = Array.from(document.querySelectorAll('button, input[type="button"], input[type="submit"], a'));
            for (const btn of btns) {
              const t = (btn.textContent || btn.value || '').trim();
              if (t === '絞り込み') { btn.click(); return true; }
            }
            return false;
          });
          console.log(`line_raw [${monthLabel}]: filter clicked: ${filterClicked}`);

          if (filterClicked) {
            // ネットワークが安定するのを待つ
            await linePage.waitForNetworkIdle({ idleTime: 2000, timeout: 15000 }).catch(() => {});
            await new Promise(r => setTimeout(r, 1000));
          }

          // 3. CSVダウンロードAPIでデータ取得（絞り込み後のセッション状態を使用）
          const startDateParam = startDate.replace(/\//g, '');
          const endDateParam = endDate.replace(/\//g, '');
          const csvApiUrl = `https://sns.rms.rakuten.co.jp/api/line-content-api/performance/download?startDate=${startDateParam}&endDate=${endDateParam}`;
          console.log(`line_raw [${monthLabel}]: API URL: ${csvApiUrl}`);

          const csvContent = await linePage.evaluate(async (url) => {
            try {
              const resp = await fetch(url, { credentials: 'include' });
              if (!resp.ok) return { error: `HTTP ${resp.status}`, url };
              const buf = await resp.arrayBuffer();
              const bytes = new Uint8Array(buf);
              let binary = '';
              for (let j = 0; j < bytes.length; j++) binary += String.fromCharCode(bytes[j]);
              return { base64: btoa(binary), size: bytes.length };
            } catch (e) {
              return { error: e.message, url };
            }
          }, csvApiUrl);

          let monthRecords = [];
          if (csvContent.error) {
            console.log(`line_raw [${monthLabel}]: CSV fetch error: ${csvContent.error}`);
          } else if (csvContent.base64) {
            console.log(`line_raw [${monthLabel}]: CSV fetch ok size=${csvContent.size}`);
            const buf = Buffer.from(csvContent.base64, 'base64');
            monthRecords = processDownload({ fileName: 'line.csv', buffer: buf });
          }

          if (monthRecords.length > 0) {
            allLineRecords.push(...monthRecords);
            console.log(`line_raw [${monthLabel}]: ${monthRecords.length}件取得`);
          } else {
            console.log(`line_raw [${monthLabel}]: データなし`);
          }
        }
      }

      if (allLineRecords.length > 0) {
        console.log(`line_raw: 合計 ${allLineRecords.length}件, ヘッダー: ${Object.keys(allLineRecords[0]).slice(0, 5).join(', ')}`);
        const headers = Object.keys(allLineRecords[0]);
        const rows = allLineRecords.map(r => headers.map(h => r[h] || ''));
        await writeRawToSheet(headers, rows, 'line_raw', ['ID']);
        additionalResults.push({ sheet: 'line_raw', status: 'ok', rows: allLineRecords.length });
      } else {
        console.log('line_raw: データなし');
        additionalResults.push({ sheet: 'line_raw', status: 'no_data' });
      }
    } catch (err) {
      console.error('line_raw エラー:', err.message);
      additionalResults.push({ sheet: 'line_raw', status: 'error', message: err.message });
    }

    cleanupDir(DOWNLOAD_DIR);

    } // end if (!skipExtra)

    const message = `レポート取得完了。全レポート: ${JSON.stringify(additionalResults)}`;
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

  // レスポンスキャプチャからZIP/CSV展開
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
      // バッファがUTF-8かShift_JISか判定して変換
      const asUtf8 = dl.buffer.toString('utf-8');
      let utf8;
      if (asUtf8.includes('\ufffd') || (!asUtf8.includes(',') && asUtf8.length > 100)) {
        // UTF-8として不正 → Shift_JISとしてデコード
        utf8 = iconv.decode(dl.buffer, 'Shift_JIS');
        console.log(`CSV保存(Shift_JIS→UTF-8): conv_${dl.fileName}`);
      } else {
        // 既にUTF-8
        utf8 = asUtf8;
        console.log(`CSV保存(UTF-8そのまま): conv_${dl.fileName}`);
      }
      fs.writeFileSync(path.join(DOWNLOAD_DIR, `conv_${dl.fileName}`), utf8, 'utf-8');
    }
  }

  // ファイルシステムに直接落ちたファイルもShift_JIS→UTF-8変換
  const rawFiles = fs.existsSync(DOWNLOAD_DIR) ? fs.readdirSync(DOWNLOAD_DIR) : [];
  for (const f of rawFiles) {
    const fp = path.join(DOWNLOAD_DIR, f);
    if (f.endsWith('.zip')) {
      try {
        const zip = new AdmZip(fp);
        for (const entry of zip.getEntries()) {
          if (entry.entryName.endsWith('.csv')) {
            const utf8 = iconv.decode(entry.getData(), 'Shift_JIS');
            const csvPath = path.join(DOWNLOAD_DIR, `fs_${entry.entryName}`);
            fs.writeFileSync(csvPath, utf8, 'utf-8');
            console.log(`ファイルシステムZIP展開→CSV: ${csvPath}`);
          }
        }
        fs.unlinkSync(fp);
      } catch (e) { console.log(`ZIP展開エラー: ${f} - ${e.message}`); }
    } else if (f.endsWith('.csv') && !f.startsWith('fs_') && !f.startsWith('conv_')) {
      // UTF-8でなければ変換
      try {
        const buf = fs.readFileSync(fp);
        const content = buf.toString('utf-8');
        if (content.includes('\ufffd') || !content.includes(',')) {
          const utf8 = iconv.decode(buf, 'Shift_JIS');
          fs.writeFileSync(fp, utf8, 'utf-8');
          console.log(`ファイルシステムCSV変換: ${f}`);
        }
      } catch (e) { /* OK */ }
    }
  }

  const csvFiles = findAllCsv(DOWNLOAD_DIR);
  console.log(`CSVファイル数: ${csvFiles.length} [${csvFiles.map(f => path.basename(f)).join(', ')}]`);
  let totalRecords = 0;

  // シートごとにレコードを集約（全月分をまとめてクリア→一括書き込み）
  const rppData = { 'rpp_all_raw': [], 'rpp_item_raw': [], 'rpp_kw_raw': [] };

  for (const csvFile of csvFiles) {
    const fileName = path.basename(csvFile).toLowerCase();
    const content = fs.readFileSync(csvFile, 'utf-8');
    const records = parseRakutenCsv(content);
    if (records.length === 0) continue;

    let sheetName;
    if (fileName.includes('item') || fileName.includes('shohin')) {
      sheetName = 'rpp_item_raw';
    } else if (fileName.includes('keyword') || fileName.includes('kw')) {
      sheetName = 'rpp_kw_raw';
    } else {
      sheetName = 'rpp_all_raw';
    }

    rppData[sheetName].push(...records);
    totalRecords += records.length;
  }

  // 各RPPシートをクリア→全データ一括書き込み（writeRawToSheet使用）
  for (const [sheetName, records] of Object.entries(rppData)) {
    if (records.length === 0) {
      console.log(`${sheetName}: データなし`);
      continue;
    }
    const headers = Object.keys(records[0]);
    const rows = records.map(r => headers.map(h => r[h] || ''));
    console.log(`${sheetName}: ${records.length}件, カラム: ${headers.join(', ')}`);
    await writeRawToSheet(headers, rows, sheetName);
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
    // UTF-8かShift_JISか判定
    const asUtf8 = dl.buffer.toString('utf-8');
    if (asUtf8.includes('\ufffd') || (!asUtf8.includes(',') && asUtf8.length > 100)) {
      csvContent = iconv.decode(dl.buffer, 'Shift_JIS');
    } else {
      csvContent = asUtf8;
    }
  } else {
    // 拡張子不明の場合もUTF-8判定
    const asUtf8 = dl.buffer.toString('utf-8');
    if (asUtf8.includes('\ufffd') || (!asUtf8.includes(',') && asUtf8.length > 100)) {
      csvContent = iconv.decode(dl.buffer, 'Shift_JIS');
    } else {
      csvContent = asUtf8;
    }
  }

  if (!csvContent) return [];
  return parseRakutenCsv(csvContent);
}

// ============================
// ログイン処理
// ============================
async function doLogin(page, rmsUser, rmsPass, rakutenPass, skipLogout = false) {
  const rakutenEmail = process.env.RMS_2FA_EMAIL;

  if (!skipLogout) {
    console.log('Step 0: 既存セッションのログアウト...');
    try {
      await page.goto('https://ad.rms.rakuten.co.jp/rpp/api/auth/logout', { waitUntil: 'networkidle2', timeout: 15000 });
    } catch (e) { /* OK */ }
    await new Promise(r => setTimeout(r, 2000));
  } else {
    console.log('Step 0: ログアウトスキップ（セッション復旧モード）');
  }

  console.log('Step 1: RMSログインページへ...');
  await page.goto(RMS_LOGIN_URL, { waitUntil: 'networkidle2', timeout: 30000 });
  await new Promise(r => setTimeout(r, 2000));

  // ページ内容を確認して新旧ログイン画面を判別
  const loginPageContent = await page.evaluate(() => document.body?.innerText?.substring(0, 500) || '');
  console.log('Step 1 ページ内容:', loginPageContent.substring(0, 300));
  const currentLoginUrl = page.url();
  console.log('Step 1 現在URL:', currentLoginUrl);

  // 2026-03-18以降: 新RMSログイン画面
  // login_id + passwd を入力し、「楽天会員ログインへ」ボタン（button type=submit）で送信
  {
    console.log('Step 1: 新RMSログイン画面');

    // R-Login ID と パスワードを入力（純粋なキーボード操作: click→3xclick全選択→type）
    console.log('Step 1: 認証情報 ID長さ:', rmsUser?.length, 'PW長さ:', rmsPass?.length);
    // login_id: トリプルクリックで全選択してからタイプ
    await page.click('input[name="login_id"]');
    await page.click('input[name="login_id"]', { clickCount: 3 });
    await page.keyboard.type(rmsUser, { delay: 30 });
    // passwd: トリプルクリックで全選択してからタイプ
    await page.click('input[name="passwd"]');
    await page.click('input[name="passwd"]', { clickCount: 3 });
    await page.keyboard.type(rmsPass, { delay: 30 });
    // 入力確認
    const filledValues = await page.evaluate(() => ({
      login_id: document.querySelector('input[name="login_id"]')?.value || '',
      passwd_len: document.querySelector('input[name="passwd"]')?.value?.length || 0,
      passwd_first2: document.querySelector('input[name="passwd"]')?.value?.substring(0, 2) || '',
    }));
    console.log('Step 1: 入力値:', JSON.stringify(filledValues));

    // 「楽天会員ログインへ」ボタン（button type=submit）をクリック
    const btnInfo = await page.evaluate(() => {
      const buttons = Array.from(document.querySelectorAll('button'));
      return buttons.map(b => ({ text: b.textContent?.trim()?.substring(0, 30), type: b.type, cls: b.className }));
    });
    console.log('Step 1: ボタン一覧:', JSON.stringify(btnInfo));

    await Promise.all([
      page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 60000 }).catch((e) => {
        console.log('Step 1: ナビゲーションタイムアウト:', e.message?.substring(0, 80));
      }),
      page.evaluate(() => {
        const buttons = Array.from(document.querySelectorAll('button'));
        const submitBtn = buttons.find(b => b.type === 'submit');
        if (submitBtn) { submitBtn.click(); return 'submit'; }
        const primary = document.querySelector('.rf-button-primary');
        if (primary) { primary.click(); return 'primary'; }
        if (buttons.length > 0) { buttons[0].click(); return 'first'; }
        return 'none';
      }),
    ]);
    await new Promise(r => setTimeout(r, 3000));

    const afterBtnUrl = page.url();
    console.log('Step 1: フォーム送信後URL:', afterBtnUrl);
    // エラーメッセージを詳しくキャプチャ
    const afterBtnDetails = await page.evaluate(() => {
      const body = document.body?.innerText?.substring(0, 1000) || '';
      const errors = Array.from(document.querySelectorAll('.rf-alert, .error, .alert, [role="alert"], .rf-error, .rf-form-error')).map(el => el.textContent?.trim());
      const forms = Array.from(document.querySelectorAll('form')).map(f => f.action);
      return { body: body, errors, forms, title: document.title };
    });
    console.log('Step 1: 送信後タイトル:', afterBtnDetails.title);
    console.log('Step 1: 送信後エラー:', JSON.stringify(afterBtnDetails.errors));
    console.log('Step 1: 送信後フォーム:', JSON.stringify(afterBtnDetails.forms));
    console.log('Step 1: 送信後内容:', afterBtnDetails.body.substring(0, 500));
  }

  // 以下は旧コード互換のelseブロック開始（到達しないが残す）
  if (false) {
    console.log('Step 1: 旧ログインフロー（到達不可）');

    // 全input要素をログ出力
    const gloginInputs = await page.evaluate(() => {
      return Array.from(document.querySelectorAll('input')).map(inp => ({
        type: inp.type, name: inp.name || '', id: inp.id || '',
        ph: (inp.placeholder || '').substring(0, 30), vis: inp.offsetParent !== null,
        val: (inp.value || '').substring(0, 20),
      }));
    });
    console.log('glogin inputs:', JSON.stringify(gloginInputs));

    // R-Login IDフィールドを探して入力
    const rLoginFilled = await page.evaluate((loginId) => {
      // テキスト入力フィールドを探す（hidden以外）
      const inputs = Array.from(document.querySelectorAll('input'));
      const textInputs = inputs.filter(inp =>
        (inp.type === 'text' || inp.type === 'email' || inp.type === '') &&
        inp.offsetParent !== null
      );
      if (textInputs.length > 0) {
        textInputs[0].value = loginId;
        textInputs[0].dispatchEvent(new Event('input', { bubbles: true }));
        textInputs[0].dispatchEvent(new Event('change', { bubbles: true }));
        return `filled: ${textInputs[0].name || textInputs[0].id || 'input[0]'}`;
      }
      // name属性で探す
      for (const inp of inputs) {
        if (inp.name && (inp.name.includes('login') || inp.name.includes('id') || inp.name === 'u')) {
          inp.value = loginId;
          inp.dispatchEvent(new Event('input', { bubbles: true }));
          inp.dispatchEvent(new Event('change', { bubbles: true }));
          return `filled: ${inp.name}`;
        }
      }
      return null;
    }, rmsUser);
    console.log('Step 1: R-Login ID入力:', rLoginFilled || '入力フィールドなし');

    // 「RMSにログインする」ボタンをクリック
    const loginBtnClicked = await page.evaluate(() => {
      for (const el of document.querySelectorAll('button, input[type="submit"], a')) {
        const t = (el.textContent || el.value || '').trim();
        if (t.includes('RMSにログインする') || t.includes('ログインする') || t === 'ログイン') {
          el.click();
          return t;
        }
      }
      // formのsubmitを試す
      const form = document.querySelector('form');
      if (form) { form.submit(); return 'form.submit'; }
      return null;
    });
    console.log('Step 1: ログインボタン:', loginBtnClicked || 'なし');

    if (loginBtnClicked) {
      await page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 15000 }).catch(() => {});
    }
    await new Promise(r => setTimeout(r, 3000));
    console.log('Step 1: 遷移後URL:', page.url());

    // ページ内容を確認
    const afterLoginContent = await page.evaluate(() => document.body?.innerText?.substring(0, 300) || '');
    console.log('Step 1: 遷移後ページ内容:', afterLoginContent.substring(0, 200));
  }

  const step1Url = page.url();
  console.log('Step 1 完了。現在URL:', step1Url);

  // 楽天会員ログイン画面の処理
  if (page.url().includes('login.account.rakuten.com') || page.url().includes('grp02.id.rakuten.co.jp')) {
    // session upgradeの場合はパスワード直接入力（メールアドレス入力なし）
    const isSessionUpgrade = page.url().includes('/session/upgrade') || page.url().includes('#/sign_in/password');
    const hasPasswordField = await page.$('input[type="password"]');

    if (isSessionUpgrade && hasPasswordField) {
      console.log('Step 2: セッションアップグレード（パスワード直接入力）...');
      await hasPasswordField.click();
      await hasPasswordField.type(rakutenPass, { delay: 30 });
      await page.keyboard.press('Enter');
      await page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 15000 }).catch(() => {});
      for (let i = 0; i < 30; i++) {
        if (!page.url().includes('login.account.rakuten.com') && !page.url().includes('grp02.id.rakuten.co.jp')) break;
        await new Promise(r => setTimeout(r, 2000));
      }
      await new Promise(r => setTimeout(r, 3000));
    } else {
      // メールアドレス入力（RMS_2FA_EMAIL または RMS_LOGIN_ID をフォールバック）
      const loginEmail = rakutenEmail || process.env.RMS_LOGIN_ID;
      console.log('Step 2a: 楽天会員ログイン（ユーザー入力）...', loginEmail ? '(メール設定あり)' : '(メール未設定)');

      // ページ内容をログ
      const rkLoginContent = await page.evaluate(() => document.body?.innerText?.substring(0, 300) || '');
      console.log('楽天ログインページ内容:', rkLoginContent.substring(0, 200));

      // 全input要素をログ
      const rkInputs = await page.evaluate(() => {
        return Array.from(document.querySelectorAll('input')).map(inp => ({
          type: inp.type, name: inp.name || '', id: inp.id || '',
          ph: (inp.placeholder || '').substring(0, 30), vis: inp.offsetParent !== null,
        }));
      });
      console.log('楽天ログインinputs:', JSON.stringify(rkInputs.slice(0, 10)));

      if (loginEmail) {
        try {
          await page.waitForSelector('input[type="text"], input[type="email"], input[name="u"], input[name="username"]', { timeout: 10000 });
          const emailInput = await page.$('input[type="text"], input[type="email"], input[name="u"], input[name="username"]');
          if (emailInput) {
            await emailInput.click({ clickCount: 3 });
            await emailInput.type(loginEmail, { delay: 30 });
          }
          // ログインボタンをクリック（Enter代わり）
          const loginBtn2a = await page.evaluate(() => {
            const btns = Array.from(document.querySelectorAll('button, input[type="submit"]'));
            for (const b of btns) {
              const txt = (b.textContent || b.value || '').trim();
              if (txt.includes('次へ') || txt.includes('ログイン') || txt.includes('Sign in') || txt.includes('Next') || b.type === 'submit') {
                b.click();
                return txt || b.type;
              }
            }
            return null;
          });
          console.log('Step 2a: ボタンクリック:', loginBtn2a);
          if (!loginBtn2a) await page.keyboard.press('Enter');
          await new Promise(r => setTimeout(r, 3000));
          console.log('Step 2a: メール送信後URL:', page.url());
          const after2aContent = await page.evaluate(() => document.body?.innerText?.substring(0, 300) || '');
          console.log('Step 2a: メール送信後内容:', after2aContent.substring(0, 200));

          // パスワード入力を待つ
          let passField = await page.$('input[type="password"]');
          if (!passField) {
            await page.waitForSelector('input[type="password"]', { timeout: 15000 }).catch(() => {});
            passField = await page.$('input[type="password"]');
          }
          await new Promise(r => setTimeout(r, 1000));

          console.log('Step 2b: 楽天会員パスワード入力...', 'PW長さ:', rakutenPass?.length, 'field:', !!passField);
          if (passField && rakutenPass) {
            await passField.click();
            await passField.type(rakutenPass, { delay: 30 });
            // ログインボタンをクリック
            const loginBtn2b = await page.evaluate(() => {
              const btns = Array.from(document.querySelectorAll('button, input[type="submit"]'));
              for (const b of btns) {
                const txt = (b.textContent || b.value || '').trim();
                if (txt.includes('次へ') || txt.includes('ログイン') || txt.includes('Sign in') || txt.includes('送信') || b.type === 'submit') {
                  b.click();
                  return txt || b.type;
                }
              }
              return null;
            });
            console.log('Step 2b: ボタンクリック:', loginBtn2b);
            if (!loginBtn2b) await page.keyboard.press('Enter');
            // SPA対応: URLが変わるまで待つ
            for (let i = 0; i < 30; i++) {
              await new Promise(r => setTimeout(r, 2000));
              const curUrl = page.url();
              if (!curUrl.includes('login.account.rakuten.com') && !curUrl.includes('grp02.id.rakuten.co.jp')) {
                console.log('Step 2b: ログイン成功、遷移先:', curUrl);
                break;
              }
              if (i === 5) {
                const err2b = await page.evaluate(() => {
                  const errs = document.querySelectorAll('.error, .alert, [role="alert"], .c-text-danger');
                  return Array.from(errs).map(e => e.textContent?.trim()).filter(Boolean);
                });
                console.log('Step 2b: 10秒後エラー:', JSON.stringify(err2b));
                console.log('Step 2b: 10秒後URL:', page.url());
                console.log('Step 2b: 10秒後内容:', (await page.evaluate(() => document.body?.innerText?.substring(0, 300) || '')).substring(0, 200));
              }
            }
            await new Promise(r => setTimeout(r, 3000));
          }
        } catch (e) {
          console.log('楽天ログインエラー:', e.message);
        }
      } else {
        console.log('Step 2: 楽天会員メールアドレス未設定（RMS_2FA_EMAIL）、ログイン不可');
      }
    }
    console.log('Step 2 完了。現在URL:', page.url());
  }

  // gloginに留まっている場合 → gloginの内容を確認してから処理
  if (page.url().includes('glogin.rms.rakuten.co.jp')) {
    const gloginContent = await page.evaluate(() => document.body?.innerText?.substring(0, 500) || '');
    console.log('Step 3: glogin内容:', gloginContent.substring(0, 300));
    const gloginHtml = await page.evaluate(() => {
      const btns = Array.from(document.querySelectorAll('button, a, input[type="submit"]'));
      return btns.map(b => ({ tag: b.tagName, text: (b.textContent || b.value || '').trim().substring(0, 40), href: b.href || '' }));
    });
    console.log('Step 3: gloginボタン/リンク:', JSON.stringify(gloginHtml.slice(0, 10)));
    // gloginにリダイレクトが必要なボタンがあればクリック（最大3回ループ: 次へ→遵守確認→RMS利用）
    for (let gloginStep = 0; gloginStep < 3; gloginStep++) {
      if (!page.url().includes('glogin.rms.rakuten.co.jp')) break;
      const clickedGlogin = await page.evaluate(() => {
        const btns = Array.from(document.querySelectorAll('button, a, input[type="submit"]'));
        for (const b of btns) {
          const txt = (b.textContent || b.value || '').trim();
          if (txt.includes('次へ') || txt.includes('RMSを利用します') || txt.includes('遵守') || txt.includes('メインメニュー') || txt.includes('続ける') || txt.includes('進む')) {
            b.click();
            return txt;
          }
        }
        return null;
      });
      if (clickedGlogin) {
        console.log(`Step 3-${gloginStep + 1}: gloginボタンクリック:`, clickedGlogin);
        await page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 15000 }).catch(() => {});
        await new Promise(r => setTimeout(r, 3000));
        console.log(`Step 3-${gloginStep + 1}: 遷移後URL:`, page.url());
      } else {
        console.log(`Step 3-${gloginStep + 1}: クリック可能なボタンなし`);
        break;
      }
    }
    // まだgloginならmainmenuへ
    if (page.url().includes('glogin.rms.rakuten.co.jp')) {
      console.log('Step 3: gloginに留まっている → RMSメインメニューへ遷移...');
      try {
        await page.goto('https://mainmenu.rms.rakuten.co.jp/', { waitUntil: 'networkidle2', timeout: 15000 });
        await new Promise(r => setTimeout(r, 2000));
        console.log('RMSメインメニューURL:', page.url());
      } catch (e) {
        console.log('RMSメインメニュー遷移失敗:', e.message);
      }
    }
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
// RPP日付範囲設定（ラジオ選択後に呼ぶ）
// dateStrは日ごと=YYYY-MM-DD、月ごと=YYYY-MM
async function setRppDateRange(page, startStr, endStr) {
  console.log(`RPP期間設定: ${startStr} 〜 ${endStr}`);

  // まずReactのnativeSetterで値を設定（inputのtype="month"やtype="text"に対応）
  const setResult = await page.evaluate((s, e) => {
    // 全inputからdatepicker系を探す
    const inputs = Array.from(document.querySelectorAll('input'));
    const dateInputs = inputs.filter(inp => {
      const cls = inp.className || '';
      const ph = inp.placeholder || '';
      const val = inp.value || '';
      return cls.includes('datepicker') || ph.includes('Select') ||
             val.match(/^\d{4}-\d{2}(-\d{2})?$/) ||
             inp.type === 'month';
    });

    if (dateInputs.length < 2) return `found_${dateInputs.length}_inputs`;

    const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
    // 開始
    nativeSetter.call(dateInputs[0], s);
    dateInputs[0].dispatchEvent(new Event('input', { bubbles: true }));
    dateInputs[0].dispatchEvent(new Event('change', { bubbles: true }));
    // 終了
    nativeSetter.call(dateInputs[1], e);
    dateInputs[1].dispatchEvent(new Event('input', { bubbles: true }));
    dateInputs[1].dispatchEvent(new Event('change', { bubbles: true }));
    return `set_ok: [${dateInputs[0].value}] ~ [${dateInputs[1].value}]`;
  }, startStr, endStr);
  console.log(`RPP期間設定結果: ${setResult}`);

  // フォールバック: clickCount:3 + type で上書き
  if (setResult.includes('found_')) {
    const datepickers = await page.$$('input.datepicker-input, input[placeholder*="Select"]');
    if (datepickers.length >= 2) {
      for (let idx = 0; idx < 2; idx++) {
        const val = idx === 0 ? startStr : endStr;
        await datepickers[idx].click({ clickCount: 3 });
        await new Promise(r => setTimeout(r, 300));
        await datepickers[idx].type(val, { delay: 50 });
        await page.keyboard.press('Enter');
        await new Promise(r => setTimeout(r, 500));
      }
      console.log('RPP期間設定: フォールバック入力完了');
    }
  }
  await new Promise(r => setTimeout(r, 1000));
}

async function setCheckboxes(page) {
  // 全チェックボックスをONにする（12時間、新規、既存、合計、720時間すべて取得）
  await page.evaluate(() => {
    const ids = ['#cbMetricsAll', '#cb12H', '#cbNewUsers', '#cbExistingUsers'];
    for (const id of ids) {
      const cb = document.querySelector(id);
      if (cb && !cb.checked) cb.click();
    }
  });
  await new Promise(r => setTimeout(r, 500));
  console.log('チェックボックス設定完了（全項目ON: 合計/12時間/新規/既存/720時間）');
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

  // ヘッダー行を検出: 日本語カラム名が含まれ、かつ3つ以上のカンマがある行
  // メタデータ行（ステータスコード表など）はスキップ
  let headerIdx = -1;
  const headerPatterns = [
    '成果発生日時', '日付', '商品管理番号', '商品名', 'クリック数',
    '実績額', '売上金額', 'CVR', 'ROAS', 'CTR', '注文番号',
    '年月', '区分', 'デバイス', 'メール種別', 'ジャンル',
  ];

  for (let i = 0; i < Math.min(30, lines.length); i++) {
    const line = lines[i].replace(/"/g, '');
    const commas = (line.match(/,/g) || []).length;
    if (commas < 3) continue;
    // ヘッダー行は日本語のカラム名を含む
    if (headerPatterns.some(p => line.includes(p))) {
      headerIdx = i;
      break;
    }
  }

  // ヘッダーパターンが見つからない場合、最初の3+カンマ行をヘッダーとして使用
  if (headerIdx < 0) {
    for (let i = 0; i < Math.min(20, lines.length); i++) {
      const line = lines[i];
      if (line.startsWith('実行日時') || line.startsWith('検索条件') || line.startsWith('集計') || line.includes('■■■')) continue;
      const commas = (line.match(/,/g) || []).length;
      if (commas >= 3) { headerIdx = i; break; }
    }
  }

  if (headerIdx > 0) {
    console.log(`CSVメタデータ行スキップ: ${headerIdx}行`);
    clean = lines.slice(headerIdx).join('\n');
  }

  // ヘッダー直後にフィールドID行がある場合はスキップ（例: date,item_mng_id,...）
  let cleanLines = clean.split('\n');
  if (cleanLines.length >= 2) {
    const secondLine = cleanLines[1].replace(/"/g, '').trim();
    // 英字のみのフィールドID行を検出
    if (secondLine && /^[a-z_,]+$/.test(secondLine.replace(/\s/g, ''))) {
      console.log('CSVフィールドID行スキップ');
      cleanLines = [cleanLines[0], ...cleanLines.slice(2)];
      clean = cleanLines.join('\n');
    }
  }

  // 2行ヘッダー検出（LINE CSVなど: カテゴリ行＋詳細行）
  // 1行目に多数の空カラムがあり、2行目に詳細ヘッダーがある場合
  if (cleanLines.length >= 3) {
    const row1Cols = cleanLines[0].split(',');
    const row2Cols = cleanLines[1].split(',');
    const row1Empties = row1Cols.filter(c => !c.replace(/"/g, '').trim()).length;
    // 1行目の半数以上が空 かつ 2行目にデータがある → 2行ヘッダー
    if (row1Cols.length > 10 && row1Empties > row1Cols.length * 0.5 && row2Cols.length >= row1Cols.length) {
      console.log('CSV 2行ヘッダー検出: カテゴリ行と詳細行をマージ');
      // カテゴリを前方展開（空セルは直前のカテゴリを継承）
      let lastCat = '';
      const mergedHeaders = [];
      for (let j = 0; j < Math.max(row1Cols.length, row2Cols.length); j++) {
        const cat = (row1Cols[j] || '').replace(/"/g, '').trim();
        const detail = (row2Cols[j] || '').replace(/"/g, '').trim();
        if (cat) lastCat = cat;
        if (j === 0) {
          mergedHeaders.push(cat || detail);
        } else if (detail) {
          mergedHeaders.push(lastCat ? `${lastCat}_${detail}` : detail);
        } else {
          mergedHeaders.push(lastCat || `col${j}`);
        }
      }
      // マージしたヘッダー＋データ行でCSV再構築
      clean = [mergedHeaders.join(','), ...cleanLines.slice(2)].join('\n');
    }
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
// RPPシート用: 累積書き込み（日付重複は除外して追記）
async function writeToSheet(records, sheetName, columns) {
  const auth = new google.auth.GoogleAuth({ scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
  const sheets = google.sheets({ version: 'v4', auth });

  if (records.length === 0) return;

  const headers = columns;

  // シート存在確認・作成
  try {
    const spreadsheet = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
    const exists = spreadsheet.data.sheets.some(s => s.properties.title === sheetName);
    if (!exists) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: { requests: [{ addSheet: { properties: { title: sheetName } } }] },
      });
      // 新規シート: ヘッダー書き込み
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `${sheetName}!A1`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [headers] },
      });
    }
  } catch (e) {
    console.log('シート作成エラー:', e.message);
  }

  // 既存データから日付+識別キーのセットを取得（重複除外用）
  let existingKeys = new Set();
  try {
    const existing = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: `${sheetName}!A:D`,
    });
    const existingRows = existing.data.values || [];
    // ヘッダーがなければ書き込み
    if (existingRows.length === 0) {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `${sheetName}!A1`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [headers] },
      });
    }
    // 日付 + 商品管理番号(あれば) + キーワード(あれば) をキーにする
    for (let i = 1; i < existingRows.length; i++) {
      const row = existingRows[i];
      const key = (row || []).slice(0, 4).join('|');
      existingKeys.add(key);
    }
    console.log(`${sheetName}: 既存データ ${existingRows.length - 1}行`);
  } catch (e) {
    console.log(`${sheetName}: 既存データ読み込みエラー: ${e.message}`);
  }

  // 新規レコードのみフィルタ
  const newRows = records
    .map(r => headers.map(h => r[h] || ''))
    .filter(row => !existingKeys.has(row.slice(0, 4).join('|')));

  if (newRows.length === 0) {
    console.log(`${sheetName}: 新規データなし（全${records.length}件が既存）`);
    return;
  }

  // 追記（append）
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${sheetName}!A1`,
    valueInputOption: 'USER_ENTERED',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values: newRows },
  });

  console.log(`${sheetName} 追記完了: 新規${newRows.length}件 (スキップ${records.length - newRows.length}件)`);
}

// ============================
// Google Sheets書き込み（追加レポート用 - 全カラム、累積追記）
// ============================
async function writeRawToSheet(headers, dataRows, sheetName, keyColumnNames) {
  const auth = new google.auth.GoogleAuth({ scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
  const sheets = google.sheets({ version: 'v4', auth });

  if (dataRows.length === 0) return;

  // キーカラムのインデックスを特定（指定がなければ先頭4列）
  let keyIndices;
  if (keyColumnNames && keyColumnNames.length > 0) {
    keyIndices = keyColumnNames.map(name => headers.indexOf(name)).filter(i => i >= 0);
    console.log(`${sheetName} キーカラム: ${keyColumnNames.join(', ')} → indices: ${keyIndices.join(', ')}`);
  }
  const makeKey = (row) => {
    if (keyIndices && keyIndices.length > 0) {
      return keyIndices.map(i => row[i] || '').join('|');
    }
    return row.slice(0, 4).join('|');
  };

  // シート存在確認・作成
  let sheetId = null;
  try {
    const spreadsheet = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
    const sheet = spreadsheet.data.sheets.find(s => s.properties.title === sheetName);
    if (sheet) {
      sheetId = sheet.properties.sheetId;
    } else {
      const addRes = await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: { requests: [{ addSheet: { properties: { title: sheetName } } }] },
      });
      sheetId = addRes.data.replies[0].addSheet.properties.sheetId;
    }
  } catch (e) {
    console.log('シート作成エラー:', e.message);
  }

  // 既存データを全列取得
  let existingData = new Map(); // key → row
  try {
    const existing = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: `${sheetName}!A:ZZ`,
    });
    const existingRows = existing.data.values || [];
    for (let i = 1; i < existingRows.length; i++) {
      const row = existingRows[i] || [];
      if (row.length === 0) continue;
      existingData.set(makeKey(row), row);
    }
  } catch (e) {
    console.log('既存データ取得エラー:', e.message);
  }

  // 新データをマージ（同キーは上書き）
  let newCount = 0;
  let updateCount = 0;
  for (const row of dataRows) {
    const key = makeKey(row);
    if (existingData.has(key)) {
      updateCount++;
    } else {
      newCount++;
    }
    existingData.set(key, row); // 上書き or 新規追加
  }

  // ソート（取得月 or 日付列）
  const sortIdx = headers.indexOf('取得月') >= 0 ? headers.indexOf('取得月') : (headers.indexOf('日付') >= 0 ? headers.indexOf('日付') : 0);
  const mergedRows = Array.from(existingData.values()).sort((a, b) => {
    const va = a[sortIdx] || '';
    const vb = b[sortIdx] || '';
    return va < vb ? -1 : va > vb ? 1 : 0;
  });

  // クリア → ヘッダー + 全データ書き込み
  await sheets.spreadsheets.values.clear({ spreadsheetId: SPREADSHEET_ID, range: `${sheetName}!A:ZZ` });
  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: `${sheetName}!A1`,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values: [headers, ...mergedRows] },
  });

  console.log(`${sheetName} 書き込み完了: 合計${mergedRows.length}件 (新規${newCount}件, 上書き${updateCount}件, ソート済み)`);
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

// ============================
// シートデータ読み取り（名前ベース → sheetIdフォールバック）
// ============================
async function readSheetData(sheets, sheetName) {
  // まずスプレッドシート情報を取得してsheetIdを特定
  const spreadsheet = await sheets.spreadsheets.get({
    spreadsheetId: SPREADSHEET_ID,
    fields: 'sheets.properties,dataSources(dataSourceId,spec)',
  });
  const sheetMeta = spreadsheet.data.sheets.find(s => s.properties.title === sheetName);
  if (!sheetMeta) throw new Error(`Sheet "${sheetName}" not found`);
  const sheetId = sheetMeta.properties.sheetId;
  const maxRow = sheetMeta.properties.gridProperties?.rowCount || 50000;
  const maxCol = sheetMeta.properties.gridProperties?.columnCount || 26;

  // DATA_SOURCEシート検出 → BigQuery直接読み取り
  const dsProps = sheetMeta.properties?.dataSourceSheetProperties;
  if (dsProps) {
    console.log(`readSheetData(${sheetName}): DATA_SOURCE sheet detected, trying BigQuery direct read`);
    try {
      const bqResult = await readFromBigQuery(spreadsheet.data, dsProps, sheetName);
      if (bqResult) return bqResult;
    } catch (e) {
      console.log(`readSheetData(${sheetName}): BigQuery direct read failed: ${e.message?.substring(0, 120)}`);
    }
    // BigQuery失敗時はgridDataフォールバック（500行制限あり）
    console.log(`readSheetData(${sheetName}): falling back to gridData for DATA_SOURCE sheet`);
  }

  // gridRangeで直接データ取得
  try {
    const res = await sheets.spreadsheets.values.batchGet({
      spreadsheetId: SPREADSHEET_ID,
      ranges: [`gid=${sheetId}`],
    });
    if (res.data.valueRanges?.[0]?.values) return res;
  } catch (e) {
    console.log(`readSheetData(${sheetName}): batchGet gid failed: ${e.message?.substring(0, 80)}`);
  }

  // INDIRECT的にシート名指定
  const colLetter = maxCol <= 26 ? String.fromCharCode(64 + maxCol) : 'AZ';
  const rangeVariants = [
    `${sheetName}!A1:${colLetter}${maxRow}`,
    `'${sheetName}'!A1:${colLetter}${maxRow}`,
    `${sheetName}`,
  ];
  for (const range of rangeVariants) {
    try {
      console.log(`readSheetData(${sheetName}): trying range: ${range}`);
      const res = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range });
      return res;
    } catch (e) {
      console.log(`readSheetData(${sheetName}): range "${range}" failed: ${e.message?.substring(0, 80)}`);
    }
  }

  // 最終手段: spreadsheets.get with includeGridData
  console.log(`readSheetData(${sheetName}): falling back to gridData (maxRow=${maxRow})`);
  let fullData;

  // フォールバック: 通常のgridData取得
  const gridRanges = [`${sheetName}`, `'${sheetName}'`];
  for (const gr of gridRanges) {
    try {
      fullData = await sheets.spreadsheets.get({
        spreadsheetId: SPREADSHEET_ID,
        includeGridData: true,
        ranges: [gr],
        fields: 'sheets.properties,sheets.data.rowData.values(formattedValue,effectiveValue)',
      });
      if (fullData.data.sheets?.[0]?.data?.[0]?.rowData?.length > 0) {
        console.log(`readSheetData(${sheetName}): gridData range "${gr}" got ${fullData.data.sheets[0].data[0].rowData.length} rows`);
        break;
      }
    } catch (e) {
      console.log(`readSheetData(${sheetName}): gridData range "${gr}" failed: ${e.message?.substring(0, 80)}`);
    }
  }

  if (!fullData?.data?.sheets) return { data: { values: [] } };

  // 対象シートを探す
  const sheetInfo = fullData.data.sheets?.find(s => s.properties?.title === sheetName) || fullData.data.sheets?.[0];
  const gridData = sheetInfo?.data?.[0];
  if (!gridData?.rowData) return { data: { values: [] } };

  const dataRows = gridData.rowData.map(row =>
    (row.values || []).map(cell => {
      if (cell.formattedValue) return cell.formattedValue;
      if (cell.effectiveValue?.stringValue) return cell.effectiveValue.stringValue;
      if (cell.effectiveValue?.numberValue != null) return String(cell.effectiveValue.numberValue);
      return '';
    })
  );

  // DATA_SOURCEシートはヘッダー行がない → dataSourceSheetPropertiesからカラム名を取得
  const dsPropsFallback = sheetInfo?.properties?.dataSourceSheetProperties;
  if (dsPropsFallback?.columns) {
    const headerRow = dsPropsFallback.columns.map(c => c.reference?.name || '');
    console.log(`readSheetData(${sheetName}): DATA_SOURCE headers (gridData fallback): ${headerRow.slice(0, 8).join(',')}`);
    return { data: { values: [headerRow, ...dataRows] } };
  }

  console.log(`readSheetData(${sheetName}): gridData success: ${dataRows.length} rows`);
  return { data: { values: dataRows } };
}

// BigQuery直接読み取り（DATA_SOURCEシート用）
async function readFromBigQuery(spreadsheetData, dsProps, sheetName) {
  // dataSourceSheetPropertiesからデータソースIDを取得
  const dataSourceId = dsProps.dataSourceId;
  if (!dataSourceId) {
    console.log(`readFromBigQuery(${sheetName}): no dataSourceId found`);
    return null;
  }

  // スプレッドシートのdataSourcesからBigQuery接続情報を取得
  const dataSources = spreadsheetData.dataSources || [];
  const ds = dataSources.find(d => d.dataSourceId === dataSourceId);
  if (!ds) {
    console.log(`readFromBigQuery(${sheetName}): dataSource ${dataSourceId} not found in spreadsheet dataSources`);
    // dataSourcesが取得できない場合、カラム名からテーブル推定を試みる
    return await readFromBigQueryByTableName(dsProps, sheetName);
  }

  // デバッグ: データソース構造をログ出力
  console.log(`readFromBigQuery(${sheetName}): dataSource keys=${Object.keys(ds).join(',')}`);
  if (ds.spec) console.log(`readFromBigQuery(${sheetName}): spec keys=${Object.keys(ds.spec).join(',')}`);
  console.log(`readFromBigQuery(${sheetName}): ds.spec=${JSON.stringify(ds.spec || {}).substring(0, 300)}`);

  // Sheets APIは spec.bigQuery（bigQuerySpecではない）
  const spec = ds.spec?.bigQuery || ds.spec?.bigQuerySpec;
  if (!spec) {
    console.log(`readFromBigQuery(${sheetName}): no bigQuery spec in dataSource, trying table name fallback`);
    return await readFromBigQueryByTableName(dsProps, sheetName);
  }

  const projectId = spec.projectId || 'stellar-shape-491201-g8';
  const tableSpec = spec.tableSpec;
  const querySpec = spec.querySpec;

  let query;
  if (tableSpec) {
    const tableProject = tableSpec.tableProjectId || projectId;
    const dataset = tableSpec.datasetId;
    const table = tableSpec.tableId;
    console.log(`readFromBigQuery(${sheetName}): table=${tableProject}.${dataset}.${table}`);
    query = `SELECT * FROM \`${tableProject}.${dataset}.${table}\``;
  } else if (querySpec?.rawQuery) {
    query = querySpec.rawQuery;
    console.log(`readFromBigQuery(${sheetName}): using custom query (${query.substring(0, 100)}...)`);
  } else {
    console.log(`readFromBigQuery(${sheetName}): no tableSpec or querySpec found`);
    return null;
  }

  const bigquery = new BigQuery({ projectId });
  let rows;
  try {
    [rows] = await bigquery.query({ query });
  } catch (e) {
    // リージョン指定なしで失敗した場合、USを試す
    console.log(`readFromBigQuery(${sheetName}): auto-location failed, trying US: ${e.message?.substring(0, 80)}`);
    [rows] = await bigquery.query({ query, location: 'US' });
  }
  console.log(`readFromBigQuery(${sheetName}): BigQuery returned ${rows.length} rows`);

  if (rows.length === 0) return { data: { values: [] } };

  const headers = Object.keys(rows[0]);
  const values = [headers, ...rows.map(row => headers.map(h => {
    const v = row[h];
    if (v === null || v === undefined) return '';
    if (v instanceof Date) return v.toISOString();
    if (typeof v === 'object' && v.value) return String(v.value); // BigQuery NUMERIC/BIGNUMERIC
    return String(v);
  }))];

  console.log(`readFromBigQuery(${sheetName}): success - ${headers.length} cols, ${rows.length} rows, headers: ${headers.slice(0, 8).join(',')}`);
  return { data: { values } };
}

// テーブル名推定によるBigQuery読み取りフォールバック
async function readFromBigQueryByTableName(dsProps, sheetName) {
  // カラム名からテーブルを特定できない場合、シート名をテーブル名として使用
  const projectId = 'stellar-shape-491201-g8';
  const bigquery = new BigQuery({ projectId });

  // データセット一覧から該当テーブルを検索
  try {
    const [datasets] = await bigquery.getDatasets();
    for (const dataset of datasets) {
      try {
        const [tables] = await dataset.getTables();
        const matchTable = tables.find(t => t.id === sheetName || t.id === sheetName.replace(/_raw$/, ''));
        if (matchTable) {
          const fullTable = `${projectId}.${dataset.id}.${matchTable.id}`;
          console.log(`readFromBigQueryByTableName(${sheetName}): found table ${fullTable}`);
          const query = `SELECT * FROM \`${fullTable}\``;
          const [rows] = await bigquery.query({ query, location: 'US' });
          console.log(`readFromBigQueryByTableName(${sheetName}): ${rows.length} rows`);

          if (rows.length === 0) return { data: { values: [] } };
          const headers = Object.keys(rows[0]);
          const values = [headers, ...rows.map(row => headers.map(h => {
            const v = row[h];
            if (v === null || v === undefined) return '';
            if (v instanceof Date) return v.toISOString();
            if (typeof v === 'object' && v.value) return String(v.value);
            return String(v);
          }))];
          return { data: { values } };
        }
      } catch (e) { /* skip dataset */ }
    }
  } catch (e) {
    console.log(`readFromBigQueryByTableName(${sheetName}): dataset scan failed: ${e.message?.substring(0, 80)}`);
  }
  return null;
}

// ============================
// RMS WEB API 受注データ取得 → BigQuery投入
// ============================
async function fetchOrdersFromRmsApi(query) {
  const serviceSecret = process.env.RMS_SERVICE_SECRET;
  const licenseKey = process.env.RMS_LICENSE_KEY;
  if (!serviceSecret || !licenseKey) throw new Error('RMS_SERVICE_SECRET / RMS_LICENSE_KEY not set');

  const https = require('https');
  const projectId = 'stellar-shape-491201-g8';
  const datasetId = 'rakuten';
  const tableId = 'orders';

  // 期間パラメータ: from=YYYY-MM-DD, to=YYYY-MM-DD (デフォルト過去6ヶ月)
  const now = new Date();
  const defaultFrom = new Date(now);
  defaultFrom.setMonth(defaultFrom.getMonth() - 6);
  const fromDate = query.from || defaultFrom.toISOString().substring(0, 10);
  const toDate = query.to || now.toISOString().substring(0, 10);

  console.log(`fetchOrdersFromRmsApi: ${fromDate} 〜 ${toDate}`);

  // RMS WEB API認証（ESA: Encoded ServiceSecret and LicenseKey Authorization）
  const authStr = Buffer.from(`${serviceSecret}:${licenseKey}`).toString('base64');

  // 受注検索API
  async function searchOrders(startDate, endDate, page) {
    const url = 'https://api.rms.rakuten.co.jp/es/2.0/order/searchOrder/';
    const body = JSON.stringify({
      dateType: 1, // 注文日
      startDatetime: startDate + 'T00:00:00+0900',
      endDatetime: endDate + 'T23:59:59+0900',
      PaginationRequestModel: { requestRecordsAmount: 1000, requestPage: page, SortModelList: [{ sortColumn: 1, sortDirection: 1 }] }
    });

    return new Promise((resolve, reject) => {
      const req = https.request(url, {
        method: 'POST',
        headers: {
          'Authorization': `ESA ${authStr}`,
          'Content-Type': 'application/json; charset=utf-8',
        },
      }, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => {
          try { resolve(JSON.parse(data)); } catch (e) { reject(new Error(`JSON parse error: ${data.substring(0, 200)}`)); }
        });
      });
      req.on('error', reject);
      req.write(body);
      req.end();
    });
  }

  // 受注詳細取得API
  async function getOrderDetail(orderNumbers) {
    const url = 'https://api.rms.rakuten.co.jp/es/2.0/order/getOrder/';
    const body = JSON.stringify({ orderNumberList: orderNumbers, version: 7 });

    return new Promise((resolve, reject) => {
      const req = https.request(url, {
        method: 'POST',
        headers: {
          'Authorization': `ESA ${authStr}`,
          'Content-Type': 'application/json; charset=utf-8',
        },
      }, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => {
          try { resolve(JSON.parse(data)); } catch (e) { reject(new Error(`JSON parse error: ${data.substring(0, 200)}`)); }
        });
      });
      req.on('error', reject);
      req.write(body);
      req.end();
    });
  }

  // 全注文番号を取得
  const allOrderNumbers = [];
  let page = 1;
  let totalPages = 1;

  // 30日ごとに分割してリクエスト（API制限対応）
  const fromDt = new Date(fromDate + 'T00:00:00+0900');
  const toDt = new Date(toDate + 'T00:00:00+0900');
  const chunks = [];
  let chunkStart = new Date(fromDt);
  while (chunkStart < toDt) {
    const chunkEnd = new Date(chunkStart);
    chunkEnd.setDate(chunkEnd.getDate() + 29);
    if (chunkEnd > toDt) chunkEnd.setTime(toDt.getTime());
    chunks.push({
      start: chunkStart.toISOString().substring(0, 10),
      end: chunkEnd.toISOString().substring(0, 10),
    });
    chunkStart = new Date(chunkEnd);
    chunkStart.setDate(chunkStart.getDate() + 1);
  }

  console.log(`fetchOrdersFromRmsApi: ${chunks.length} date chunks`);

  for (const chunk of chunks) {
    page = 1;
    totalPages = 1;
    while (page <= totalPages) {
      console.log(`fetchOrdersFromRmsApi: searchOrder ${chunk.start}〜${chunk.end} page ${page}`);
      const result = await searchOrders(chunk.start, chunk.end, page);
      if (result.MessageModelList) {
        const msg = result.MessageModelList.map(m => `${m.messageType}:${m.messageCode}:${m.message}`).join('; ');
        console.log(`fetchOrdersFromRmsApi: API message: ${msg}`);
        if (result.MessageModelList.some(m => m.messageType === 'ERROR')) {
          throw new Error(`RMS API error: ${msg}`);
        }
      }
      const orderNums = result.orderNumberList || [];
      allOrderNumbers.push(...orderNums);
      totalPages = result.PaginationResponseModel?.totalPages || 1;
      console.log(`fetchOrdersFromRmsApi: page ${page}/${totalPages}, got ${orderNums.length} orders`);
      page++;
    }
  }

  console.log(`fetchOrdersFromRmsApi: total ${allOrderNumbers.length} order numbers`);
  if (allOrderNumbers.length === 0) return { status: 'ok', orders: 0 };

  // 注文詳細を100件ずつ取得（レート制限対応で間隔あけ）
  const allOrders = [];
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  for (let i = 0; i < allOrderNumbers.length; i += 100) {
    const batch = allOrderNumbers.slice(i, i + 100);
    const batchNum = Math.floor(i / 100) + 1;
    const totalBatches = Math.ceil(allOrderNumbers.length / 100);
    console.log(`fetchOrdersFromRmsApi: getOrder batch ${batchNum}/${totalBatches} (${batch.length} orders)`);
    try {
      const detail = await getOrderDetail(batch);
      if (detail.OrderModelList) {
        allOrders.push(...detail.OrderModelList);
      } else {
        console.log(`fetchOrdersFromRmsApi: batch ${batchNum} no OrderModelList, keys=${Object.keys(detail).join(',')}`);
        if (detail.MessageModelList) {
          console.log(`fetchOrdersFromRmsApi: batch ${batchNum} msg=${detail.MessageModelList.map(m => m.messageType + ':' + m.message).join(';').substring(0, 200)}`);
        }
      }
    } catch (e) {
      console.log(`fetchOrdersFromRmsApi: batch ${batchNum} error: ${e.message?.substring(0, 100)}`);
    }
    // レート制限対応: 1秒待機
    if (i + 100 < allOrderNumbers.length) await sleep(1000);
  }

  console.log(`fetchOrdersFromRmsApi: got ${allOrders.length} order details`);

  // BigQueryに投入（既存データとマージ）
  const bigquery = new BigQuery({ projectId });

  // フラット化: 注文×商品行
  const flatRows = [];
  allOrders.forEach(order => {
    const base = {
      orderNumber: order.orderNumber || '',
      orderDatetime: order.orderDatetime || '',
      orderProgress: order.orderProgress || 0,
      subStatusId: order.subStatusId || '',
      subStatusName: order.subStatusName || '',
      orderType: order.orderType || 0,
      totalPrice: order.totalPrice || 0,
      goodsPrice: order.goodsPrice || 0,
      goodsTax: order.goodsTax || 0,
      postagePrice: order.postagePrice || 0,
      deliveryPrice: order.deliveryPrice || 0,
      totalCouponDiscount: order.totalCouponDiscount || 0,
      ordererEmailAddress: order.OrdererModel?.emailAddress || '',
      ordererPrefecture: order.OrdererModel?.prefecture || '',
      ordererSex: order.OrdererModel?.sex || '',
    };
    const items = order.PackageModelList?.flatMap(p => p.ItemModelList || []) || [];
    if (items.length === 0) {
      flatRows.push(base);
    } else {
      items.forEach(item => {
        flatRows.push({
          ...base,
          itemName: item.itemName || '',
          itemNumber: item.itemNumber || '',
          manageNumber: item.manageNumber || '',
          units: item.units || 1,
          goodsPrice: item.price || base.goodsPrice,
          itemId: item.itemId || '',
        });
      });
    }
  });

  console.log(`fetchOrdersFromRmsApi: ${flatRows.length} flat rows to insert`);

  // テーブル存在確認・作成
  const dataset = bigquery.dataset(datasetId);
  const table = dataset.table(tableId);
  const [exists] = await table.exists();
  if (!exists) {
    console.log(`fetchOrdersFromRmsApi: creating table ${datasetId}.${tableId}`);
    await dataset.createTable(tableId, { schema: { fields: Object.keys(flatRows[0]).map(k => ({ name: k, type: typeof flatRows[0][k] === 'number' ? 'FLOAT64' : 'STRING' })) } });
  }

  // 重複除去: 既存のorderNumberを取得して差分のみ挿入
  let existingOrderNumbers = new Set();
  try {
    const [existingRows] = await bigquery.query({
      query: `SELECT DISTINCT orderNumber FROM \`${projectId}.${datasetId}.${tableId}\``,
    });
    existingRows.forEach(r => existingOrderNumbers.add(r.orderNumber));
    console.log(`fetchOrdersFromRmsApi: ${existingOrderNumbers.size} existing orders in BigQuery`);
  } catch (e) {
    console.log(`fetchOrdersFromRmsApi: could not read existing orders: ${e.message?.substring(0, 80)}`);
  }

  const newRows = flatRows.filter(r => !existingOrderNumbers.has(r.orderNumber));
  console.log(`fetchOrdersFromRmsApi: ${newRows.length} new rows to insert`);

  if (newRows.length > 0) {
    // 500行ずつバッチ挿入
    for (let i = 0; i < newRows.length; i += 500) {
      const batch = newRows.slice(i, i + 500);
      await table.insert(batch);
      console.log(`fetchOrdersFromRmsApi: inserted batch ${Math.floor(i / 500) + 1} (${batch.length} rows)`);
    }
  }

  return {
    status: 'ok',
    period: `${fromDate} 〜 ${toDate}`,
    totalOrderNumbers: allOrderNumbers.length,
    totalDetails: allOrders.length,
    flatRows: flatRows.length,
    newRows: newRows.length,
    existingRows: existingOrderNumbers.size,
  };
}

// ============================
// 楽天イベントカレンダー取得
// ============================
async function fetchRakutenEvents() {
  const https = require('https');

  function fetchPage(url) {
    return new Promise((resolve) => {
      https.get(url, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => {
          try {
            const match = data.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/);
            if (!match) { resolve([]); return; }
            const nextData = JSON.parse(match[1]);
            const events = nextData?.props?.pageProps?.data?.eventSearchResponse?.data?.event_searches || [];
            resolve(events.map(e => ({
              id: e.event_id,
              title: e.cal_event?.event_title || '',
              startDate: e.view_start_date || '',
              endDate: e.view_end_date || '',
            })));
          } catch (e) { resolve([]); }
        });
      }).on('error', () => resolve([]));
    });
  }

  // 過去12ヶ月＋今月＋来月の範囲で月別取得
  const now = new Date();
  const allEvents = [];
  const seen = new Set();
  const months = [];
  for (let offset = -12; offset <= 1; offset++) {
    const d = new Date(now.getFullYear(), now.getMonth() + offset, 1);
    months.push(`${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`);
  }

  for (const ym of months) {
    const [y, m] = ym.split('-');
    const url = `https://calendar.rakuten.co.jp/search/evt?ec=1600&y=${y}&m=${parseInt(m)}`;
    const events = await fetchPage(url);
    events.forEach(e => {
      const key = e.id || (e.title + e.startDate);
      if (!seen.has(key)) { seen.add(key); allEvents.push(e); }
    });
  }

  console.log(`fetchRakutenEvents: ${allEvents.length} events from ${months[0]} to ${months[months.length - 1]}`);
  return allEvents;
}

// ============================
// ダッシュボード HTML生成
// ============================
async function generateDashboardHtml() {
  const auth = new google.auth.GoogleAuth({ scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
  const sheets = google.sheets({ version: 'v4', auth });

  // ── ヘルパー ──
  const num = (v) => { const n = parseFloat(String(v || '').replace(/[,，円%％￥]/g, '')); return isNaN(n) ? 0 : n; };
  const safe = s => String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');

  // Flexible column name matching - handles full-width chars, partial matches
  const findCol = (headers, ...patterns) => {
    for (const p of patterns) {
      const idx = headers.findIndex(h => h && h.includes(p));
      if (idx >= 0) return idx;
    }
    return -1;
  };

  const getVal = (headers, row, ...patterns) => {
    const idx = findCol(headers, ...patterns);
    return idx >= 0 ? (row[idx] || '') : '';
  };

  const getNum = (headers, row, ...patterns) => num(getVal(headers, row, ...patterns));

  const toMap = (headers, row) => {
    const m = {};
    headers.forEach((h, i) => { m[h] = row[i] || ''; });
    return m;
  };

  const parseSheet = (res) => {
    const rows = (res && res.data && res.data.values) ? res.data.values : [];
    if (rows.length < 2) return { headers: [], data: [], raw: [] };
    const headers = rows[0].map(h => String(h || '').trim());
    const data = rows.slice(1).map(r => toMap(headers, r));
    return { headers, data, raw: rows.slice(1) };
  };

  // ── シート並列読み込み ──
  const readSheet = async (name) => {
    try {
      const res = await readSheetData(sheets, name);
      return parseSheet(res);
    } catch (e) {
      console.log(`Dashboard: failed to read ${name}: ${e.message?.substring(0, 120)}`);
      return { headers: [], data: [], raw: [] };
    }
  };

  const [allRaw, rppAllRaw, rppItemRaw, rppKwRaw, tdaRaw, adRaw, cpaRaw, mailRaw, lineRaw, afiRaw, allItemRaw, orderRaw, masterRaw] = await Promise.all([
    readSheet('all_raw'),
    readSheet('rpp_all_raw'),
    readSheet('rpp_item_raw'),
    readSheet('rpp_kw_raw'),
    readSheet('tda_raw'),
    readSheet('ad_raw'),
    readSheet('cpa_raw'),
    readSheet('mail_raw'),
    readSheet('line_raw'),
    readSheet('afi_raw'),
    readSheet('all_item_raw'),
    readSheet('order_raw'),
    readSheet('master'),
  ]);

  // order_rawデバッグ
  console.log(`Dashboard order_raw: headers=${orderRaw.headers.length}, data=${orderRaw.data.length}, first5headers=${orderRaw.headers.slice(0, 5).join(',')}`);
  if (orderRaw.data.length > 0) {
    const sample = orderRaw.data[0];
    console.log(`Dashboard order_raw sample keys: ${Object.keys(sample).slice(0, 10).join(',')}`);
    console.log(`Dashboard order_raw sample email: ${sample.ordererEmailAddress || sample['メールアドレス'] || 'NONE'}`);
  }

  // ── 日付パース ──
  const now = new Date();
  const dateStr = `${now.getFullYear()}/${now.getMonth() + 1}/${now.getDate()} ${now.getHours()}:${String(now.getMinutes()).padStart(2, '0')}`;

  const parseDate = (d) => {
    if (!d) return null;
    const s = String(d).replace(/\//g, '-').replace(/年/g, '-').replace(/月/g, '-').replace(/日/g, '').trim();
    const m = s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/);
    return m ? new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3])) : null;
  };
  const toYM = (d) => {
    const dt = parseDate(d);
    if (dt) return `${dt.getFullYear()}-${String(dt.getMonth() + 1).padStart(2, '0')}`;
    // 「2026年3月」「2026-03」など日付なしの年月形式に対応
    if (!d) return '';
    const s = String(d).replace(/\//g, '-').replace(/年/g, '-').replace(/月/g, '').trim();
    const m2 = s.match(/^(\d{4})-(\d{1,2})$/);
    if (m2) return `${m2[1]}-${String(m2[2]).padStart(2, '0')}`;
    return '';
  };
  const toDateStr = (d) => {
    const dt = parseDate(d);
    return dt ? `${dt.getFullYear()}-${String(dt.getMonth() + 1).padStart(2, '0')}-${String(dt.getDate()).padStart(2, '0')}` : '';
  };
  const ymToLabel = (ym) => {
    if (!ym) return '';
    const [y, m] = ym.split('-');
    return `${y}年${parseInt(m)}月`;
  };

  // ── 全シートから月一覧を収集 ──
  const allMonths = new Set();
  const extractMonths = (data, ...dateKeys) => {
    data.forEach(r => {
      for (const k of dateKeys) {
        const v = r[k];
        if (v) { const ym = toYM(v); if (ym) { allMonths.add(ym); break; } }
      }
    });
  };
  extractMonths(allRaw.data, '日付');
  extractMonths(rppAllRaw.data, '日付');
  extractMonths(tdaRaw.data, '日付');
  extractMonths(adRaw.data, '日付');
  extractMonths(lineRaw.data, '配信日時');
  extractMonths(afiRaw.data, '成果発生日時');
  // all_item_raw uses 取得月 which may be YYYY年MM月 format
  allItemRaw.data.forEach(r => {
    const v = r['取得月'] || r['日付'] || '';
    const ym = toYM(v);
    if (ym) allMonths.add(ym);
  });
  // order_raw
  extractMonths(orderRaw.data, 'orderDatetime', '注文日時', '日付');

  const sortedMonths = [...allMonths].sort().reverse();

  // ── RPP column matching with flexible names ──
  const rppAllH = rppAllRaw.headers;
  const rppSpendKey = rppAllH.find(h => h && (h === '実績額(合計)' || h === '実績額')) || rppAllH.find(h => h && h.includes('実績額') && !h.includes('割引後')) || '実績額';
  const rppSalesKey = rppAllH.find(h => h && h.includes('売上金額') && h.includes('720')) || rppAllH.find(h => h && h.includes('売上金額')) || '売上金額';
  const rppClicksKey = rppAllH.find(h => h && (h === 'クリック数(合計)' || h === 'クリック数')) || rppAllH.find(h => h && h.includes('クリック数')) || 'クリック数';
  const rppOrdersKey = rppAllH.find(h => h && h.includes('売上件数') && h.includes('720')) || rppAllH.find(h => h && h.includes('売上件数')) || '売上件数';

  // TDA column matching
  const tdaH = tdaRaw.headers;
  const tdaSpendKey = tdaH.find(h => h && h.includes('実績額')) || '実績額(円)';
  const tdaSalesKey = tdaH.find(h => h && h.includes('売上金額')) || '売上金額';
  const tdaClicksKey = tdaH.find(h => h && h.includes('クリック数')) || 'クリック数';
  const tdaImpKey = tdaH.find(h => h && h.includes('インプレッション')) || 'ビューアブルインプレッション数';
  const tdaOrdersKey = tdaH.find(h => h && (h.includes('売上件数') && !h.includes('新規') && !h.includes('既存'))) || '売上件数';
  const tdaNewSalesKey = tdaH.find(h => h && h.includes('新規顧客_売上金額')) || '新規顧客_売上金額(円)';
  const tdaExistSalesKey = tdaH.find(h => h && h.includes('既存顧客_売上金額')) || '既存顧客_売上金額(円)';

  // ad_raw column matching
  const adH = adRaw.headers;
  const adSpendKey = adH.find(h => h && h.includes('広告費') && !h.includes('日割')) || '広告費(円)';
  const adSalesKey = adH.find(h => h && h.includes('売上金額')) || '売上金額(クロスデバイス含む)(円)';
  const adClicksKey = adH.find(h => h && h.includes('クリック数')) || 'クリック数';
  const adOrdersKey = adH.find(h => h && h.includes('売上件数')) || '売上件数(クロスデバイス含む)';
  const adNewCustKey = adH.find(h => h && h.includes('新規顧客獲得')) || '新規顧客獲得数';
  const adNewSalesKey = adH.find(h => h && h.includes('新規顧客売上金額')) || '新規顧客売上金額(円)';

  // RPP item column matching
  const rppItemH = rppItemRaw.headers;
  const rppItemSpendKey = rppItemH.find(h => h && (h === '実績額(合計)' || h === '実績額')) || rppItemH.find(h => h && h.includes('実績額') && !h.includes('新規') && !h.includes('既存')) || '実績額';
  const rppItemSalesKey = rppItemH.find(h => h && h.includes('売上金額') && h.includes('720')) || rppItemH.find(h => h && h.includes('売上金額')) || '売上金額';
  const rppItemClicksKey = rppItemH.find(h => h && (h === 'クリック数(合計)' || h === 'クリック数')) || rppItemH.find(h => h && h.includes('クリック数') && !h.includes('新規') && !h.includes('既存')) || 'クリック数';
  const rppItemOrdersKey = rppItemH.find(h => h && h.includes('売上件数') && h.includes('720')) || rppItemH.find(h => h && h.includes('売上件数')) || '売上件数';

  // RPP kw column matching
  const rppKwH = rppKwRaw.headers;
  const rppKwSpendKey = rppKwH.find(h => h && (h === '実績額(合計)' || h === '実績額')) || rppKwH.find(h => h && h.includes('実績額') && !h.includes('新規') && !h.includes('既存')) || '実績額';
  const rppKwSalesKey = rppKwH.find(h => h && h.includes('売上金額') && h.includes('720')) || rppKwH.find(h => h && h.includes('売上金額')) || '売上金額';
  const rppKwClicksKey = rppKwH.find(h => h && (h === 'クリック数(合計)' || h === 'クリック数')) || rppKwH.find(h => h && h.includes('クリック数') && !h.includes('新規') && !h.includes('既存')) || 'クリック数';
  const rppKwOrdersKey = rppKwH.find(h => h && h.includes('売上件数') && h.includes('720')) || rppKwH.find(h => h && h.includes('売上件数')) || '売上件数';

  // ── Build structured data for JSON embedding ──
  // all_raw - filter デバイス=すべて
  const allData = allRaw.data.filter(r => {
    const dev = (r['デバイス'] || '').trim();
    return dev === 'すべて' || dev === '全て' || dev === 'ALL' || dev === '';
  });

  // Build per-month all_raw data (日付で重複排除 - 後の行を優先)
  const allByMonth = {};
  const allDateSeen = {};
  allData.forEach(r => {
    const ym = toYM(r['日付']);
    if (!ym) return;
    const dateKey = toDateStr(r['日付']);
    if (!allByMonth[ym]) allByMonth[ym] = [];
    const row = {
      date: dateKey,
      sales: num(r['売上金額']),
      orders: num(r['売上件数']),
      access: num(r['アクセス人数']),
      cvr: num(r['転換率']),
      newBuyers: num(r['新規購入者数']),
      repeatBuyers: num(r['リピート購入者数']),
      uu: num(r['ユニークユーザー数']),
      couponShop: num(r['クーポン値引額（店舗）']),
      couponRakuten: num(r['クーポン値引額（楽天）']),
      benchmark: num(r['月商別平均値（月商100万～999万） 売上金額'] || r[allRaw.headers.find(h => h && h.includes('月商別平均')) || '__none__']),
      subgenreAvg: num(r['サブジャンルTOP10平均 売上金額'] || r[allRaw.headers.find(h => h && h.includes('サブジャンルTOP10')) || '__none__']),
    };
    if (dateKey && allDateSeen[dateKey] !== undefined) {
      // 重複→上書き（後の行が最新データ）
      allByMonth[ym][allDateSeen[dateKey]] = row;
    } else {
      allDateSeen[dateKey] = allByMonth[ym].length;
      allByMonth[ym].push(row);
    }
  });

  // RPP all data by month (日付で重複排除)
  const rppByMonth = {};
  const rppDateSeen = {};
  rppAllRaw.data.forEach(r => {
    const ym = toYM(r['日付']);
    if (!ym) return;
    if (!rppByMonth[ym]) rppByMonth[ym] = [];
    const dateKey = toDateStr(r['日付']);
    const row = {
      date: dateKey,
      spend: num(r[rppSpendKey]),
      sales: num(r[rppSalesKey]),
      clicks: num(r[rppClicksKey]),
      orders: num(r[rppOrdersKey]),
    };
    if (dateKey && rppDateSeen[dateKey] !== undefined) {
      rppByMonth[ym][rppDateSeen[dateKey]] = row;
    } else {
      rppDateSeen[dateKey] = rppByMonth[ym].length;
      rppByMonth[ym].push(row);
    }
  });

  // RPP item data by month
  const rppItemByMonth = {};
  rppItemRaw.data.forEach(r => {
    const ym = toYM(r['日付']);
    if (!ym) return;
    if (!rppItemByMonth[ym]) rppItemByMonth[ym] = [];
    rppItemByMonth[ym].push({
      date: toDateStr(r['日付']),
      manageNum: (r['商品管理番号'] || '').trim(),
      name: r['商品名'] || r['商品管理番号'] || '不明',
      spend: num(r[rppItemSpendKey]),
      sales: num(r[rppItemSalesKey]),
      clicks: num(r[rppItemClicksKey]),
      orders: num(r[rppItemOrdersKey]),
    });
  });

  // RPP kw data by month
  const rppKwByMonth = {};
  rppKwRaw.data.forEach(r => {
    const ym = toYM(r['日付']);
    if (!ym) return;
    if (!rppKwByMonth[ym]) rppKwByMonth[ym] = [];
    rppKwByMonth[ym].push({
      date: toDateStr(r['日付']),
      kw: r['キーワード'] || '不明',
      name: r['商品名'] || r['商品管理番号'] || '',
      spend: num(r[rppKwSpendKey]),
      sales: num(r[rppKwSalesKey]),
      clicks: num(r[rppKwClicksKey]),
      orders: num(r[rppKwOrdersKey]),
    });
  });

  // TDA data by month
  const tdaByMonth = {};
  tdaRaw.data.forEach(r => {
    const ym = toYM(r['日付']);
    if (!ym) return;
    if (!tdaByMonth[ym]) tdaByMonth[ym] = [];
    tdaByMonth[ym].push({
      date: toDateStr(r['日付']),
      campaign: r['キャンペーン名'] || r['キャンペーンID'] || '不明',
      spend: num(r[tdaSpendKey]),
      sales: num(r[tdaSalesKey]),
      clicks: num(r[tdaClicksKey]),
      imps: num(r[tdaImpKey]),
      orders: num(r[tdaOrdersKey]),
      newSales: num(r[tdaNewSalesKey]),
      existSales: num(r[tdaExistSalesKey]),
    });
  });

  // ad_raw data by month
  const adByMonth = {};
  adRaw.data.forEach(r => {
    const ym = toYM(r['日付']);
    if (!ym) return;
    if (!adByMonth[ym]) adByMonth[ym] = [];
    adByMonth[ym].push({
      date: toDateStr(r['日付']),
      product: r['広告商品名'] || '不明',
      type: r['枠種別'] || '',
      device: r['デバイス'] || '',
      spend: num(r[adSpendKey]),
      sales: num(r[adSalesKey]),
      clicks: num(r[adClicksKey]),
      orders: num(r[adOrdersKey]),
      newCust: num(r[adNewCustKey]),
      newSales: num(r[adNewSalesKey]),
    });
  });

  // CPA data by month
  const cpaByMonth = {};
  cpaRaw.data.forEach(r => {
    const ym = toYM(r['日付']);
    if (!ym) return;
    if (!cpaByMonth[ym]) cpaByMonth[ym] = [];
    cpaByMonth[ym].push({
      date: toDateStr(r['日付']),
      spend: num(r['ご請求額']),
      sales: num(r['効果保証型広告（楽天CPA広告）経由の売上']),
      rate: r['料率'] || '',
      status: r['確定／未確定'] || '',
    });
  });

  // LINE data by month
  const lineByMonth = {};
  lineRaw.data.forEach(r => {
    const ym = toYM(r['配信日時']);
    if (!ym) return;
    if (!lineByMonth[ym]) lineByMonth[ym] = [];
    lineByMonth[ym].push({
      date: toDateStr(r['配信日時']),
      title: r['タイトル'] || '',
      type: r['種別'] || '',
      sent: num(r['配信通数']),
      opened: num(r['開封数']),
      openRate: r['開封率'] || '',
      visitors: num(r['訪問者数']),
      conversions: num(r['転換数']),
      cvrStr: r['転換率'] || '',
      unitPrice: num(r['客単価']),
      sales: num(r['売上']),
      salesPerSend: num(r['売上/通']),
    });
  });

  // Affiliate data by month
  const afiByMonth = {};
  afiRaw.data.forEach(r => {
    const ym = toYM(r['成果発生日時']);
    if (!ym) return;
    if (!afiByMonth[ym]) afiByMonth[ym] = [];
    afiByMonth[ym].push({
      date: toDateStr(r['成果発生日時']),
      product: r['商品名'] || r['商品管理番号'] || '',
      manageNum: r['商品管理番号'] || '',
      sales: num(r['売上金額']),
      reward: num(r['成果報酬']),
      rateStr: r['料率'] || '',
      status: r['ステータス'] || '',
    });
  });

  // Mail data (monthly)
  const mailData = mailRaw.data.map(r => ({ ...r }));

  // all_item_raw by month
  const allItemByMonth = {};
  console.log(`Dashboard all_item_raw: headers=${allItemRaw.headers.join(',')}, rows=${allItemRaw.data.length}`);
  if (allItemRaw.data.length > 0) {
    const s = allItemRaw.data[0];
    console.log(`Dashboard all_item_raw sample: 商品管理番号=${s['商品管理番号']}, 商品名=${s['商品名']}, 売上=${s['売上']}, 売上金額=${s['売上金額']}, アクセス人数=${s['アクセス人数']}, 売上件数=${s['売上件数']}`);
  }
  allItemRaw.data.forEach(r => {
    const ymRaw = r['取得月'] || r['日付'] || '';
    const ym = toYM(ymRaw);
    if (!ym) return;
    if (!allItemByMonth[ym]) allItemByMonth[ym] = [];
    const aiSalesVal = r['売上'] || r['売上金額'] || r[allItemRaw.headers.find(h => h && (h === '売上' || h === '売上金額')) || '__none__'] || 0;
    const aiOrdersVal = r['売上件数'] || r[allItemRaw.headers.find(h => h && h.includes('売上件数')) || '__none__'] || 0;
    const aiAccessVal = r['アクセス人数'] || r[allItemRaw.headers.find(h => h && h.includes('アクセス')) || '__none__'] || 0;
    const aiCvrVal = r['転換率'] || r[allItemRaw.headers.find(h => h && h.includes('転換率')) || '__none__'] || 0;
    const aiUnitPriceVal = r['客単価'] || r[allItemRaw.headers.find(h => h && h.includes('客単価')) || '__none__'] || 0;
    allItemByMonth[ym].push({
      manageNum: (r['商品管理番号'] || '').trim(),
      name: r['商品名'] || r['商品管理番号'] || '不明',
      genre: (r['ジャンル'] || '').trim(),
      access: num(aiAccessVal),
      sales: num(aiSalesVal),
      orders: num(aiOrdersVal),
      cvr: num(aiCvrVal),
      unitPrice: num(aiUnitPriceVal),
    });
  });

  // master sheet - 商品管理番号→商品名マップ
  const masterProducts = {};
  masterRaw.data.forEach(r => {
    const mn = r['商品管理番号'] || '';
    const name = r['商品名'] || '';
    if (mn) masterProducts[mn] = name || mn;
  });
  console.log(`Dashboard master: ${Object.keys(masterProducts).length} products loaded`);

  // order_raw - build structured data
  const orderRows = orderRaw.data;
  const orderH = orderRaw.headers;
  const orders = [];
  orderRows.forEach(r => {
    const email = r['ordererEmailAddress'] || r[orderH.find(h => h && h.includes('Email')) || '__none__'] || r['メールアドレス'] || '';
    const orderNum = r['orderNumber'] || r[orderH.find(h => h && h.includes('orderNumber')) || '__none__'] || r['注文番号'] || '';
    const dateRaw = r['orderDatetime'] || r[orderH.find(h => h && h.includes('orderDatetime')) || '__none__'] || r['注文日時'] || r['日付'] || '';
    const totalPrice = num(r['totalPrice'] || r['合計金額'] || 0);
    const goodsPrice = num(r['goodsPrice'] || r['商品金額'] || 0);
    const price = totalPrice || goodsPrice;
    const itemName = r['itemName'] || r['商品名'] || '';
    const units = num(r['units'] || r['個数'] || 1);
    const manageNum = r['manageNumber'] || r['商品管理番号'] || '';
    const pref = r['ordererPrefecture'] || r['都道府県'] || '';
    const sex = r['ordererSex'] || '';
    const couponDiscount = num(r['totalCouponDiscount'] || 0);
    orders.push({ email, orderNum, date: dateRaw, price, itemName, units, manageNum, pref, sex, couponDiscount });
  });

  // Customer purchase history
  const customerOrders = {};
  const ordersByNumber = {};
  orders.forEach(r => {
    if (!r.email) return;
    if (!customerOrders[r.email]) customerOrders[r.email] = [];
    customerOrders[r.email].push(r);
    if (r.orderNum) {
      if (!ordersByNumber[r.orderNum]) ordersByNumber[r.orderNum] = { email: r.email, date: r.date, price: 0, items: [], units: 0, manageNums: [] };
      ordersByNumber[r.orderNum].price += r.price;
      ordersByNumber[r.orderNum].items.push(r.itemName);
      ordersByNumber[r.orderNum].manageNums.push(r.manageNum || r.itemName);
      ordersByNumber[r.orderNum].units += r.units;
    }
  });

  // First order date per customer
  const customerFirstOrder = {};
  Object.entries(customerOrders).forEach(([email, ords]) => {
    const sorted = [...ords].sort((a, b) => String(a.date).localeCompare(String(b.date)));
    customerFirstOrder[email] = sorted[0]?.date || '';
  });

  // Purchase count per customer
  const customerPurchaseCounts = {};
  Object.entries(customerOrders).forEach(([email, ords]) => {
    const uniqueNums = [...new Set(ords.map(o => o.orderNum).filter(Boolean))];
    customerPurchaseCounts[email] = uniqueNums.length || 1;
  });

  // Purchase distribution
  const purchaseDistribution = {};
  Object.values(customerPurchaseCounts).forEach(cnt => {
    const bucket = cnt >= 10 ? '10+' : String(cnt);
    purchaseDistribution[bucket] = (purchaseDistribution[bucket] || 0) + 1;
  });
  const purchaseDistRows = Object.entries(purchaseDistribution)
    .sort((a, b) => (a[0] === '10+' ? 999 : Number(a[0])) - (b[0] === '10+' ? 999 : Number(b[0])))
    .map(([cnt, customers]) => ({ cnt, customers }));

  const totalCustomers = Object.keys(customerPurchaseCounts).length;
  const firstTimers = Object.values(customerPurchaseCounts).filter(c => c === 1).length;
  const repeaters = totalCustomers - firstTimers;
  const f2Rate = totalCustomers > 0 ? (repeaters / totalCustomers * 100) : 0;

  // Monthly new/repeat
  const monthlyNR = {};
  orders.forEach(r => {
    if (!r.email || !r.date) return;
    const ym = toYM(r.date);
    if (!ym) return;
    if (!monthlyNR[ym]) monthlyNR[ym] = { newCust: new Set(), repeatCust: new Set() };
    const firstYM = toYM(customerFirstOrder[r.email]);
    if (firstYM === ym) monthlyNR[ym].newCust.add(r.email);
    else monthlyNR[ym].repeatCust.add(r.email);
  });
  const monthlyNRData = Object.entries(monthlyNR)
    .map(([ym, v]) => ({ month: ym, newCust: v.newCust.size, repeatCust: v.repeatCust.size }))
    .sort((a, b) => a.month.localeCompare(b.month));

  // Basket analysis
  const uniqueOrdersList = Object.values(ordersByNumber);
  const avgUnitsPerOrder = uniqueOrdersList.length > 0 ? uniqueOrdersList.reduce((s, o) => s + o.units, 0) / uniqueOrdersList.length : 0;
  const avgOrderPrice = uniqueOrdersList.length > 0 ? uniqueOrdersList.reduce((s, o) => s + o.price, 0) / uniqueOrdersList.length : 0;
  const multiItemOrders = uniqueOrdersList.filter(o => [...new Set(o.manageNums.filter(Boolean))].length > 1).length;
  const crossSellRate = uniqueOrdersList.length > 0 ? (multiItemOrders / uniqueOrdersList.length * 100) : 0;

  // Cross-purchase pairs
  const pairCounts = {};
  uniqueOrdersList.forEach(o => {
    const items = [...new Set(o.manageNums.filter(Boolean))];
    if (items.length < 2) return;
    items.sort();
    for (let i = 0; i < items.length; i++) {
      for (let j = i + 1; j < items.length; j++) {
        const key = `${items[i]}|||${items[j]}`;
        pairCounts[key] = (pairCounts[key] || 0) + 1;
      }
    }
  });
  const topPairs = Object.entries(pairCounts).sort((a, b) => b[1] - a[1]).slice(0, 20).map(([pair, count]) => {
    const [a, b] = pair.split('|||');
    return { a, b, count };
  });

  // Units per order distribution
  const unitsDistribution = {};
  uniqueOrdersList.forEach(o => {
    const bucket = o.units >= 5 ? '5+' : String(o.units);
    unitsDistribution[bucket] = (unitsDistribution[bucket] || 0) + 1;
  });
  const unitsDistRows = Object.entries(unitsDistribution)
    .sort((a, b) => (a[0] === '5+' ? 999 : Number(a[0])) - (b[0] === '5+' ? 999 : Number(b[0])))
    .map(([units, count]) => ({ units, count }));

  // Purchase count price
  const purchaseCountPrice = {};
  Object.entries(customerOrders).forEach(([email, ords]) => {
    const cnt = customerPurchaseCounts[email] || 1;
    const bucket = cnt >= 10 ? '10+' : String(cnt);
    const totalSpend = ords.reduce((s, o) => s + o.price, 0);
    if (!purchaseCountPrice[bucket]) purchaseCountPrice[bucket] = { total: 0, count: 0 };
    purchaseCountPrice[bucket].total += totalSpend;
    purchaseCountPrice[bucket].count += 1;
  });
  const purchaseCountPriceRows = Object.entries(purchaseCountPrice)
    .sort((a, b) => (a[0] === '10+' ? 999 : Number(a[0])) - (b[0] === '10+' ? 999 : Number(b[0])))
    .map(([cnt, v]) => ({ cnt, avgPrice: v.count > 0 ? Math.round(v.total / v.count) : 0, customers: v.count }));

  // First item LTV
  const firstItemLTV = {};
  Object.entries(customerOrders).forEach(([email, ords]) => {
    const sorted = [...ords].sort((a, b) => String(a.date).localeCompare(String(b.date)));
    const firstItem = sorted[0]?.manageNum || sorted[0]?.itemName || '不明';
    const totalSpend = sorted.reduce((s, o) => s + o.price, 0);
    const orderCount = customerPurchaseCounts[email] || 1;
    if (!firstItemLTV[firstItem]) firstItemLTV[firstItem] = { item: firstItem, totalLTV: 0, count: 0, repeatCount: 0 };
    firstItemLTV[firstItem].totalLTV += totalSpend;
    firstItemLTV[firstItem].count += 1;
    if (orderCount >= 2) firstItemLTV[firstItem].repeatCount += 1;
  });
  const firstItemLTVRows = Object.values(firstItemLTV)
    .map(r => ({
      item: r.item,
      count: r.count,
      avgLTV: r.count > 0 ? Math.round(r.totalLTV / r.count) : 0,
      f2Rate: r.count > 0 ? Math.round(r.repeatCount / r.count * 1000) / 10 : 0,
    }))
    .sort((a, b) => b.avgLTV - a.avgLTV)
    .slice(0, 30);

  // Repeat item analysis - which products get repeat-purchased
  const repeatItemAnalysis = {};
  Object.entries(customerOrders).forEach(([email, ords]) => {
    const itemCounts = {};
    ords.forEach(o => {
      const key = o.manageNum || o.itemName || '不明';
      itemCounts[key] = (itemCounts[key] || 0) + 1;
    });
    Object.entries(itemCounts).forEach(([item, cnt]) => {
      if (!repeatItemAnalysis[item]) repeatItemAnalysis[item] = { item, totalBuyers: 0, repeatBuyers: 0, totalPurchases: 0 };
      repeatItemAnalysis[item].totalBuyers++;
      repeatItemAnalysis[item].totalPurchases += cnt;
      if (cnt >= 2) repeatItemAnalysis[item].repeatBuyers++;
    });
  });
  const repeatItemRows = Object.values(repeatItemAnalysis)
    .map(r => ({ ...r, repeatRate: r.totalBuyers > 0 ? Math.round(r.repeatBuyers / r.totalBuyers * 1000) / 10 : 0 }))
    .sort((a, b) => b.repeatBuyers - a.repeatBuyers)
    .slice(0, 30);

  // Entry item F2 analysis (same as firstItemLTV but sorted by f2Rate)
  const entryItemF2Rows = Object.values(firstItemLTV)
    .map(r => ({
      item: r.item,
      count: r.count,
      avgLTV: r.count > 0 ? Math.round(r.totalLTV / r.count) : 0,
      f2Rate: r.count > 0 ? Math.round(r.repeatCount / r.count * 1000) / 10 : 0,
      repeatCount: r.repeatCount,
    }))
    .filter(r => r.count >= 2) // at least 2 customers
    .sort((a, b) => b.f2Rate - a.f2Rate)
    .slice(0, 30);

  // Per-product co-purchase map for basket selector
  const coProductMap = {};
  const productList = new Set();
  uniqueOrdersList.forEach(o => {
    const items = [...new Set(o.manageNums.filter(Boolean))];
    items.forEach(item => {
      productList.add(item);
      if (!coProductMap[item]) coProductMap[item] = {};
      items.forEach(other => {
        if (other !== item) {
          coProductMap[item][other] = (coProductMap[item][other] || 0) + 1;
        }
      });
    });
  });
  const coProductData = {};
  Object.entries(coProductMap).forEach(([item, others]) => {
    coProductData[item] = Object.entries(others)
      .map(([other, count]) => ({ item: other, count }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 20);
  });
  const basketProducts = [...productList].sort();

  // Mail data - parse with proper columns
  // mail_raw columns: 年月, 区分, デバイス, メール種別, 保有数, 配信回数, 送信数, 開封数, 開封率(%), 送客数, 送客率(%), クリック数, お気に入り登録数, お気に入り登録率(%), 転換数, 転換率(%), 売上, 売上/通
  const mailH = mailRaw.headers;
  const mailDateKey = mailH.find(h => h && (h.includes('年月') || h.includes('配信日') || h.includes('日時') || h.includes('日付'))) || '年月';
  const mailTypeKey = mailH.find(h => h && h.includes('メール種別')) || 'メール種別';
  const mailDeviceKey = mailH.find(h => h && h.includes('デバイス')) || 'デバイス';
  const mailCategoryKey = mailH.find(h => h && h.includes('区分')) || '区分';
  const mailSentCountKey = mailH.find(h => h && h.includes('配信回数')) || '配信回数';
  const mailSentKey = mailH.find(h => h && (h === '送信数' || h.includes('送信数'))) || '送信数';
  const mailOpenKey = mailH.find(h => h && h.includes('開封数')) || '開封数';
  const mailOpenRateKey = mailH.find(h => h && h.includes('開封率')) || '開封率(%)';
  const mailClickKey = mailH.find(h => h && h.includes('クリック数')) || 'クリック数';
  const mailSalesKey = mailH.find(h => h && h === '売上') || mailH.find(h => h && h.includes('売上') && !h.includes('/')) || '売上';
  const mailSalesPerKey = mailH.find(h => h && h.includes('売上/通')) || '売上/通';
  const mailConvKey = mailH.find(h => h && h.includes('転換数')) || '転換数';
  const mailConvRateKey = mailH.find(h => h && h.includes('転換率')) || '転換率(%)';
  // Aggregate by 年月 (全体/全デバイス or sum)
  const mailByYM = {};
  mailRaw.data.forEach(r => {
    const ymRaw = r[mailDateKey] || '';
    const ym = toYM(ymRaw);
    if (!ym) return;
    const device = (r[mailDeviceKey] || '').trim();
    const category = (r[mailCategoryKey] || '').trim();
    // 全体行のみ集計（デバイス=全体, 区分=自店舗）
    if (device !== '全体' && device !== '') return;
    if (category && category !== '自店舗') return;
    const mailType = (r[mailTypeKey] || '').trim();
    if (!mailByYM[ym]) mailByYM[ym] = { sentCount: 0, sent: 0, opened: 0, clicks: 0, sales: 0, conversions: 0, types: [] };
    mailByYM[ym].sentCount += num(r[mailSentCountKey]);
    mailByYM[ym].sent += num(r[mailSentKey]);
    mailByYM[ym].opened += num(r[mailOpenKey]);
    mailByYM[ym].clicks += num(r[mailClickKey]);
    mailByYM[ym].sales += num(r[mailSalesKey]);
    mailByYM[ym].conversions += num(r[mailConvKey]);
    if (mailType) mailByYM[ym].types.push(mailType);
  });
  const mailParsed = Object.entries(mailByYM).map(([ym, v]) => ({
    date: ym,
    subject: [...new Set(v.types)].join(', ') || '全体',
    sent: v.sent,
    opened: v.opened,
    openRate: v.sent > 0 ? (v.opened / v.sent * 100).toFixed(1) + '%' : '-',
    clicks: v.clicks,
    clickRate: v.sent > 0 ? (v.clicks / v.sent * 100).toFixed(2) + '%' : '-',
    sales: v.sales,
    orders: v.conversions,
    sentCount: v.sentCount,
  })).filter(r => r.sent > 0 || r.sales > 0).sort((a, b) => String(a.date).localeCompare(String(b.date)));

  // Affiliate by rate
  const afiByRate = {};
  afiRaw.data.forEach(r => {
    const rate = r['料率'] || '不明';
    if (!afiByRate[rate]) afiByRate[rate] = { rate, sales: 0, reward: 0, count: 0 };
    afiByRate[rate].sales += num(r['売上金額']);
    afiByRate[rate].reward += num(r['成果報酬']);
    afiByRate[rate].count += 1;
  });
  const afiByRateRows = Object.values(afiByRate).sort((a, b) => b.sales - a.sales);

  // ── 楽天イベントカレンダー取得 → シートに蓄積保存 ──
  let rakutenEvents = [];
  try {
    // 1. シートの既存データを読み込み
    let existingEvents = [];
    try {
      const evtSheet = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: 'event_calendar!A2:D5000',
      });
      const evtRows = evtSheet.data.values || [];
      existingEvents = evtRows.map(r => ({
        id: r[0] || '', title: r[1] || '', startDate: r[2] || '', endDate: r[3] || ''
      }));
      console.log(`Dashboard: ${existingEvents.length} existing events in sheet`);
    } catch (e2) {
      console.log(`Dashboard: event sheet read: ${e2.message}`);
    }

    // 2. 楽天カレンダーから新規取得
    const freshEvents = await fetchRakutenEvents();
    console.log(`Dashboard: ${freshEvents.length} Rakuten events fetched from web`);

    // 3. マージ（既存 + 新規、ID or タイトル+日付で重複除外）
    const seen = new Set();
    const merged = [];
    [...existingEvents, ...freshEvents].forEach(e => {
      const key = e.id ? String(e.id) : (e.title + '|' + e.startDate);
      if (seen.has(key)) return;
      seen.add(key);
      merged.push(e);
    });
    // 開始日でソート
    merged.sort((a, b) => (a.startDate || '').localeCompare(b.startDate || ''));

    // 4. シートに全データ書き込み（ヘッダー + マージ済みデータ）
    const evtHeaders = ['イベントID', 'イベント名', '開始日', '終了日'];
    const evtValues = [evtHeaders, ...merged.map(e => [
      e.id || '', e.title || '', (e.startDate || '').substring(0, 10), (e.endDate || '').substring(0, 10)
    ])];
    try {
      // 既存データをクリアしてから書き込み
      await sheets.spreadsheets.values.clear({
        spreadsheetId: SPREADSHEET_ID,
        range: 'event_calendar!A:D',
      });
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: 'event_calendar!A1',
        valueInputOption: 'RAW',
        requestBody: { values: evtValues },
      });
      console.log(`Dashboard: event_calendar sheet updated with ${merged.length} events (${merged.length - existingEvents.length} new)`);
    } catch (e2) {
      console.log(`Dashboard: event_calendar sheet write error: ${e2.message}`);
    }

    rakutenEvents = merged;
  } catch (e) {
    console.log(`Dashboard: event fetch error: ${e.message}`);
  }

  // ── Build the master JSON payload ──
  const dashboardData = {
    updatedAt: dateStr,
    months: sortedMonths,
    monthLabels: sortedMonths.reduce((acc, ym) => { acc[ym] = ymToLabel(ym); return acc; }, {}),
    allByMonth,
    rppByMonth,
    rppItemByMonth,
    rppKwByMonth,
    tdaByMonth,
    adByMonth,
    cpaByMonth,
    lineByMonth,
    afiByMonth,
    mailData,
    allItemByMonth,
    masterProducts,
    hasOrders: orders.length > 0,
    // Compact order data for client-side filtering (email, item, date, orderNum)
    orderItems: orders.map(r => ({ e: r.email, i: r.manageNum || r.itemName, d: r.date, n: r.orderNum, p: r.price })),
    repeatAnalysis: {
      totalCustomers,
      firstTimers,
      repeaters,
      f2Rate: Math.round(f2Rate * 100) / 100,
      purchaseDistRows,
      monthlyNR: monthlyNRData,
      repeatItemRows,
      entryItemF2Rows,
    },
    basketAnalysis: {
      totalOrders: uniqueOrdersList.length,
      avgUnitsPerOrder: Math.round(avgUnitsPerOrder * 10) / 10,
      avgOrderPrice: Math.round(avgOrderPrice),
      crossSellRate: Math.round(crossSellRate * 100) / 100,
      topPairs,
      unitsDistRows,
      coProductData,
      basketProducts,
    },
    ltvAnalysis: {
      purchaseCountPrice: purchaseCountPriceRows,
      firstItemLTV: firstItemLTVRows,
    },
    mailParsed,
    afiByRateRows,
    rakutenEvents,
  };

  const dataJson = JSON.stringify(dashboardData);

  // ══════════════════════════════════════════════
  // HTML
  // ══════════════════════════════════════════════
  return `<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>楽天市場アナリティクス</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js"><\/script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-annotation@3.0.1/dist/chartjs-plugin-annotation.min.js"><\/script>
<style>
:root {
  --c-primary: #1a3a5c;
  --c-primary-light: #2d5f8a;
  --c-primary-bg: rgba(26,58,92,0.06);
  --c-bg: #f3f5f9;
  --c-surface: #ffffff;
  --c-border: #e8eaed;
  --c-text: #1a1a2e;
  --c-text-secondary: #5f6368;
  --c-text-muted: #9aa0a6;
  --c-success: #0d904f;
  --c-danger: #d32f2f;
  --c-blue: #1a73e8;
  --c-orange: #e8710a;
  --c-purple: #7b1fa2;
  --shadow-sm: 0 1px 3px rgba(0,0,0,0.08);
  --shadow-md: 0 2px 8px rgba(0,0,0,0.1);
  --shadow-lg: 0 4px 16px rgba(0,0,0,0.12);
  --radius: 10px;
  --radius-sm: 6px;
}
*, *::before, *::after { margin: 0; padding: 0; box-sizing: border-box; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Hiragino Sans', 'Noto Sans JP', sans-serif;
  background: var(--c-bg); color: var(--c-text); line-height: 1.5; font-size: 14px;
}
.app-header {
  background: var(--c-primary); color: #fff; padding: 0 24px;
  position: sticky; top: 0; z-index: 100;
  box-shadow: var(--shadow-md);
}
.header-inner {
  max-width: 1440px; margin: 0 auto;
  display: flex; align-items: center; justify-content: space-between;
  height: 56px;
}
.header-title { font-size: 18px; font-weight: 700; letter-spacing: 1.5px; font-family: 'Helvetica Neue', Arial, 'Hiragino Kaku Gothic ProN', sans-serif; text-transform: uppercase; }
.header-meta { font-size: 12px; opacity: 0.85; }
.tab-bar {
  background: var(--c-surface); border-bottom: 1px solid var(--c-border);
  position: sticky; top: 56px; z-index: 99;
  overflow-x: auto; white-space: nowrap;
  -webkit-overflow-scrolling: touch;
  scrollbar-width: none;
}
.tab-bar::-webkit-scrollbar { display: none; }
.tab-bar-inner {
  max-width: 1440px; margin: 0 auto; padding: 0 16px;
  display: flex; gap: 0;
}
.main-tab {
  padding: 12px 20px; cursor: pointer; font-size: 13px; font-weight: 500;
  color: var(--c-text-secondary); border-bottom: 3px solid transparent;
  transition: all 0.2s; user-select: none; flex-shrink: 0;
}
.main-tab:hover { color: var(--c-primary); background: var(--c-primary-bg); }
.main-tab.active { color: var(--c-primary); border-bottom-color: var(--c-primary); font-weight: 600; }
.filter-bar {
  background: var(--c-surface); border-bottom: 1px solid var(--c-border);
  position: sticky; top: 97px; z-index: 98; padding: 10px 24px;
}
.filter-bar-inner {
  max-width: 1440px; margin: 0 auto;
  display: flex; align-items: center; gap: 16px; flex-wrap: wrap;
}
.filter-group { display: flex; align-items: center; gap: 8px; }
.filter-label { font-size: 12px; color: var(--c-text-secondary); font-weight: 500; }
.filter-select {
  padding: 6px 12px; border: 1px solid var(--c-border); border-radius: var(--radius-sm);
  font-size: 13px; color: var(--c-text); background: #fff; cursor: pointer;
  outline: none; transition: border-color 0.2s;
}
.filter-select:focus { border-color: var(--c-primary); }
.compare-toggle {
  display: flex; border: 1px solid var(--c-border); border-radius: var(--radius-sm); overflow: hidden;
}
.compare-btn {
  padding: 5px 14px; font-size: 12px; cursor: pointer; border: none;
  background: #fff; color: var(--c-text-secondary); transition: all 0.2s;
}
.compare-btn.active { background: var(--c-primary); color: #fff; }
.main-content { max-width: 1440px; margin: 0 auto; padding: 20px 24px; }
@keyframes fadeInUp { from { opacity: 0; transform: translateY(12px); } to { opacity: 1; transform: translateY(0); } }
@keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } }
@keyframes countUp { from { opacity: 0; transform: scale(0.9); } to { opacity: 1; transform: scale(1); } }
.tab-panel { display: none; }
.tab-panel.active { display: block; animation: fadeIn 0.3s ease-out; }
.sub-panel { animation: fadeIn 0.25s ease-out; }
.panel-title { font-size: 18px; font-weight: 700; margin-bottom: 16px; color: var(--c-text); }
.cards-grid {
  display: grid; grid-template-columns: repeat(5, 1fr);
  gap: 14px; margin-bottom: 20px;
}
.metric-card {
  background: var(--c-surface); border-radius: var(--radius);
  padding: 18px 20px; box-shadow: var(--shadow-sm);
  border: 1px solid var(--c-border); transition: all 0.25s cubic-bezier(0.4,0,0.2,1); cursor: default;
  animation: fadeInUp 0.4s ease-out backwards;
}
.metric-card:nth-child(1) { animation-delay: 0s; }
.metric-card:nth-child(2) { animation-delay: 0.05s; }
.metric-card:nth-child(3) { animation-delay: 0.1s; }
.metric-card:nth-child(4) { animation-delay: 0.15s; }
.metric-card:nth-child(5) { animation-delay: 0.2s; }
.metric-card:hover { box-shadow: var(--shadow-md); transform: translateY(-2px); }
.metric-label { font-size: 11px; color: var(--c-text-muted); text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px; }
.metric-value { font-size: 24px; font-weight: 700; color: var(--c-primary); line-height: 1.2; }
.metric-sub { font-size: 11px; color: var(--c-text-secondary); margin-top: 4px; }
.metric-change { font-size: 12px; font-weight: 600; margin-top: 4px; }
.metric-change.up { color: var(--c-success); }
.metric-change.down { color: var(--c-danger); }
.metric-change.flat { color: var(--c-text-muted); }
.section-box {
  background: var(--c-surface); border-radius: var(--radius);
  padding: 20px 24px; margin-bottom: 20px;
  box-shadow: var(--shadow-sm); border: 1px solid var(--c-border);
  animation: fadeInUp 0.4s ease-out backwards;
  transition: box-shadow 0.2s;
}
.section-box:hover { box-shadow: var(--shadow-md); }
.section-title {
  font-size: 15px; font-weight: 600; margin-bottom: 14px;
  padding-bottom: 8px; border-bottom: 2px solid var(--c-primary);
  display: inline-block;
}
.sub-title { font-size: 13px; font-weight: 600; color: var(--c-text-secondary); margin: 16px 0 10px; }
.chart-wrap { position: relative; margin: 16px 0; }
.chart-md { height: 320px; }
.chart-sm { height: 260px; }
.chart-xs { height: 200px; }
.grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
.grid-3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 20px; }
.table-wrap { overflow-x: auto; margin: 8px 0; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
thead th {
  background: #f8f9fa; color: var(--c-text-secondary); padding: 10px 12px;
  text-align: right; white-space: nowrap; font-weight: 600; font-size: 11px;
  text-transform: uppercase; letter-spacing: 0.3px;
  border-bottom: 2px solid var(--c-border); position: sticky; top: 0;
  cursor: pointer; user-select: none; transition: background 0.15s;
}
thead th:hover { background: #eef0f2; }
thead th:first-child { text-align: left; }
thead th { cursor: pointer; user-select: none; }
thead th { cursor: pointer; user-select: none; }
thead th .sort-icon { font-size: 10px; margin-left: 4px; opacity: 0.3; }
thead th.sorted .sort-icon { opacity: 1; color: var(--c-primary); }
thead th.sorted-asc .sort-icon::after { content: '▲'; }
thead th.sorted-desc .sort-icon::after { content: '▼'; }
thead th:not(.sorted) .sort-icon::after { content: '⇅'; }
tbody td {
  padding: 9px 12px; text-align: right; border-bottom: 1px solid #f0f0f0;
  white-space: nowrap; font-variant-numeric: tabular-nums;
}
tbody td:first-child {
  text-align: left; font-weight: 500; max-width: 320px;
  overflow: hidden; text-overflow: ellipsis;
}
tbody tr:hover { background: #fafbfc; }
tbody tr:nth-child(even) { background: #fcfcfd; }
tbody tr:nth-child(even):hover { background: #f5f6f8; }
.no-data {
  text-align: center; padding: 40px 20px; color: var(--c-text-muted);
  font-size: 14px; font-style: italic;
}
.sub-tabs { display: flex; gap: 0; margin-bottom: 14px; border-bottom: 1px solid var(--c-border); }
.sub-tab {
  padding: 8px 16px; cursor: pointer; font-size: 12px; font-weight: 500;
  color: var(--c-text-muted); border-bottom: 2px solid transparent;
  margin-bottom: -1px; transition: all 0.2s;
}
.sub-tab:hover { color: var(--c-primary); }
.sub-tab.active { color: var(--c-primary); border-bottom-color: var(--c-primary); }
.sub-panel { display: none; }
.sub-panel.active { display: block; }
.search-box {
  padding: 8px 12px; border: 1px solid var(--c-border); border-radius: var(--radius-sm);
  font-size: 13px; width: 260px; outline: none; transition: border-color 0.2s;
}
.search-box:focus { border-color: var(--c-primary); }
.kpi-row { display: flex; gap: 16px; flex-wrap: wrap; margin: 12px 0 16px; }
.kpi-item {
  flex: 1; min-width: 130px; text-align: center; padding: 14px 12px;
  background: #f8f9fa; border-radius: var(--radius-sm); border: 1px solid var(--c-border);
}
.kpi-label { font-size: 11px; color: var(--c-text-muted); margin-bottom: 4px; }
.kpi-val { font-size: 20px; font-weight: 700; color: var(--c-primary); }
.badge {
  display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 600;
}
.badge-success { background: #e8f5e9; color: var(--c-success); }
.badge-danger { background: #ffebee; color: var(--c-danger); }
.badge-neutral { background: #f5f5f5; color: var(--c-text-muted); }
@media (max-width: 768px) {
  .main-content { padding: 12px; }
  .section-box { padding: 14px 16px; }
  .cards-grid { grid-template-columns: repeat(2, 1fr); gap: 10px; }
  .metric-value { font-size: 20px; }
  .grid-2, .grid-3 { grid-template-columns: 1fr; }
  .chart-md { height: 240px; }
  .chart-sm { height: 200px; }
  table { font-size: 11px; }
  thead th, tbody td { padding: 6px 8px; }
  .filter-bar-inner { gap: 8px; }
  .search-box { width: 100%; }
  .header-inner { padding: 0 12px; height: 48px; }
  .tab-bar { top: 48px; }
  .filter-bar { top: 89px; }
}
</style>
</head>
<body>

<header class="app-header" onclick="location.reload()" style="cursor:pointer">
  <div class="header-inner">
    <div style="display:flex;flex-direction:column;align-items:center;gap:2px">
      <img src="../cards_logo.png" alt="CARDS" style="height:28px;filter:brightness(0) invert(1)" onerror="this.style.display='none'">
      <div style="font-size:12px;color:#fff;letter-spacing:1px;font-weight:500">楽天市場アナリティクス</div>
    </div>
    <div class="header-meta">百福堂（ひゃくふくどう）｜更新: ${safe(dateStr)}</div>
  </div>
</header>

<nav class="tab-bar">
  <div class="tab-bar-inner">
    <div class="main-tab active" data-tab="tab-sales">売上サマリ</div>
    <div class="main-tab" data-tab="tab-product">商品別分析</div>
    <div class="main-tab" data-tab="tab-ads">広告分析</div>
    <div class="main-tab" data-tab="tab-acq">CRM分析</div>
    <div class="main-tab" data-tab="tab-customer">顧客分析</div>
  </div>
</nav>

<div class="filter-bar">
  <div class="filter-bar-inner">
    <div class="filter-group">
      <span class="filter-label">期間</span>
      <select id="periodType" class="filter-select" style="display:none">
        <option value="day" selected>日</option>
      </select>
      <select id="monthFilter" class="filter-select" style="display:none"></select>
      <input type="date" id="dayFilterFrom" class="filter-select" style="width:auto">
      <span id="dayFilterSep"> 〜 </span>
      <input type="date" id="dayFilterTo" class="filter-select" style="width:auto">
    </div>
    <div class="filter-group">
      <span class="filter-label">比較</span>
      <div class="compare-toggle">
        <button class="compare-btn active" data-compare="mom">前月比</button>
        <button class="compare-btn" data-compare="yoy">前年同月比</button>
      </div>
    </div>
  </div>
</div>

<main class="main-content">

<!-- Tab 1: 売上サマリ -->
<div class="tab-panel active" id="tab-sales">
  <div class="panel-title">売上サマリ</div>
  <div id="salesCards" class="cards-grid"></div>
  <div class="grid-2" style="grid-template-columns:2fr 1fr">
    <div class="section-box">
      <div class="section-title">売上KPIツリー</div>
      <div id="salesTreeWrap" style="overflow-x:auto"></div>
    </div>
    <div class="section-box">
      <div class="section-title">新規/リピート購入比率</div>
      <div class="chart-wrap chart-sm"><canvas id="chartNewRepeatPie"></canvas></div>
    </div>
  </div>
  <div class="section-box">
    <div class="section-title">日別売上（RPP経由 / 広告外）</div>
    <div class="chart-wrap chart-md"><canvas id="chartDailyRppSplit"></canvas></div>
  </div>
</div>

<!-- Tab: 商品別分析 -->
<div class="tab-panel" id="tab-product">
  <div class="panel-title">商品別分析</div>
  <div id="productTableWrap" style="overflow-x:auto"></div>
  <div class="section-box" style="margin-top:20px">
    <div class="section-title">月別推移</div>
    <div style="display:flex;gap:12px;align-items:center;margin-bottom:14px;flex-wrap:wrap">
      <span class="filter-label">指標</span>
      <select id="productMetricSelect" class="filter-select" style="width:auto">
        <option value="sales" selected>売上</option>
        <option value="orders">件数</option>
        <option value="access">アクセス</option>
        <option value="cvr">転換率</option>
        <option value="unitPrice">客単価</option>
        <option value="rppSpend">RPP費用</option>
        <option value="rppSales">RPP売上</option>
        <option value="rppRoas">RPP ROAS</option>
      </select>
    </div>
    <div id="productMonthlyTableWrap" style="overflow-x:auto"></div>
  </div>
  <div class="section-box" style="margin-top:20px">
    <div class="section-title">カテゴリ別実績</div>
    <div id="categoryTableWrap"></div>
  </div>
  <div class="section-box" style="margin-top:20px">
    <div class="section-title">カテゴリ別 売上推移</div>
    <div class="chart-wrap chart-md"><canvas id="chartCategoryTrend"></canvas></div>
  </div>
</div>

<!-- Tab 2: 広告分析 -->
<div class="tab-panel" id="tab-ads">
  <div class="panel-title">広告分析</div>
  <div id="adCards" class="cards-grid"></div>
  <div class="section-box">
    <div class="sub-tabs" id="adSubTabs">
      <div class="sub-tab active" data-subtab="ad-rpp">RPP広告</div>
      <div class="sub-tab" data-subtab="ad-rakuten">楽天広告</div>
      <div class="sub-tab" data-subtab="ad-tda">TDA広告</div>
      <div class="sub-tab" data-subtab="ad-cpa">CPA広告</div>
      <div class="sub-tab" data-subtab="ad-afi">アフィリエイト</div>
    </div>
    <div class="sub-panel active" id="ad-rpp">
      <div class="sub-tabs" id="rppSubTabs" style="margin-bottom:12px;border-bottom:1px solid #e0e0e0">
        <div class="sub-tab active" data-subtab="rpp-all">全体</div>
        <div class="sub-tab" data-subtab="rpp-item">商品入札</div>
        <div class="sub-tab" data-subtab="rpp-kw">KW入札</div>
      </div>
      <div class="sub-panel active" id="rpp-all">
        <div id="rppKpiRow" class="kpi-row"></div>
      </div>
      <div class="sub-panel" id="rpp-item">
        <div id="rppItemTableWrap"></div>
      </div>
      <div class="sub-panel" id="rpp-kw">
        <div id="rppKwTableWrap"></div>
      </div>
    </div>
    <div class="sub-panel" id="ad-rakuten">
      <div id="adRakutenKpiRow" class="kpi-row"></div>
      <div id="adRakutenTableWrap"></div>
    </div>
    <div class="sub-panel" id="ad-tda">
      <div id="tdaKpiRow" class="kpi-row"></div>
      <div id="tdaTableWrap"></div>
    </div>
    <div class="sub-panel" id="ad-cpa">
      <div id="cpaKpiRow" class="kpi-row"></div>
    </div>
    <div class="sub-panel" id="ad-afi">
      <div id="afiKpiRow" class="kpi-row"></div>
      <div class="section-title" style="margin-bottom:8px">料率別集計</div>
      <div class="grid-2">
        <div id="afiByRateTableWrap"></div>
        <div class="section-box"><div class="chart-wrap chart-sm"><canvas id="chartAfiRatePie"></canvas></div></div>
      </div>
      <div style="margin-top:16px"><div class="section-title">商品別集計</div></div>
      <div id="afiByProductTableWrap"></div>
    </div>
  </div>
</div>

<!-- Tab 3: CRM分析 -->
<div class="tab-panel" id="tab-acq">
  <div class="panel-title">CRM分析</div>
  <div class="section-box">
    <div class="sub-tabs" id="acqSubTabs">
      <div class="sub-tab active" data-subtab="acq-mail">メルマガ</div>
      <div class="sub-tab" data-subtab="acq-line">LINE</div>
    </div>
    <div class="sub-panel active" id="acq-mail">
      <div id="mailKpiRow" class="kpi-row"></div>
      <div class="sub-title" style="margin-top:20px">配信分析</div>
      <div id="mailTableWrap"></div>
    </div>
    <div class="sub-panel" id="acq-line">
      <div class="sub-title">全体分析</div>
      <div id="lineKpiRow" class="kpi-row"></div>
      <div class="grid-2">
        <div class="chart-wrap chart-sm"><canvas id="chartLineTrend"></canvas></div>
        <div class="chart-wrap chart-sm"><canvas id="chartLinePerf"></canvas></div>
      </div>
      <div class="sub-title" style="margin-top:20px">メッセージ別分析</div>
      <div id="lineTableWrap"></div>
    </div>
  </div>
</div>

<!-- Tab 4: 顧客分析 -->
<div class="tab-panel" id="tab-customer">
  <div class="panel-title">顧客分析</div>
  <div class="section-box">
    <div class="sub-tabs">
      <div class="sub-tab active" data-subtab="cust-repeat">リピート</div>
      <div class="sub-tab" data-subtab="cust-basket">バスケット</div>
      <div class="sub-tab" data-subtab="cust-ltv">LTV</div>
      <div class="sub-tab" data-subtab="cust-rfm">RFM</div>
      <div class="sub-tab" data-subtab="cust-entrance">エントランス</div>
      <div class="sub-tab" data-subtab="cust-timing">タイミング</div>
    </div>
    <div class="sub-panel active" id="cust-repeat">
      <div style="margin-bottom:14px;display:flex;gap:12px;align-items:center">
        <span class="filter-label">商品フィルタ</span>
        <select id="repeatProductFilter" class="filter-select" style="width:auto;min-width:200px;max-width:400px">
          <option value="">全商品</option>
        </select>
      </div>
      <div id="repeatCards" class="cards-grid"></div>
      <div class="section-box">
        <div class="section-title">月別 新規/リピート客数・比率</div>
        <div class="chart-wrap" style="height:320px"><canvas id="chartMonthlyNR"></canvas></div>
        <div id="monthlyNRTable" style="margin-top:12px"></div>
      </div>
      <div class="grid-2">
        <div class="section-box">
          <div class="section-title">購入回数分布</div>
          <div class="chart-wrap chart-sm"><canvas id="chartPurchaseDist"></canvas></div>
        </div>
        <div class="section-box">
          <div class="section-title">月別リピート率推移</div>
          <div class="chart-wrap chart-sm"><canvas id="chartRepeatRateTrend"></canvas></div>
        </div>
      </div>
      <div class="grid-2">
        <div class="section-box">
          <div class="section-title">リピート購入商品ランキング<span style="font-size:11px;font-weight:400;color:#888;margin-left:8px">（累計）</span></div>
          <div id="repeatItemTableWrap"></div>
        </div>
        <div class="section-box">
          <div class="section-title">入口商品別 F2転換率<span style="font-size:11px;font-weight:400;color:#888;margin-left:8px">（累計）</span></div>
          <div id="entryItemF2TableWrap"></div>
        </div>
      </div>
    </div>
    <div class="sub-panel" id="cust-basket">
      <div id="basketCards" class="cards-grid"></div>
      <div class="grid-2">
        <div class="section-box">
          <div class="section-title">購入点数分布</div>
          <div class="chart-wrap chart-sm"><canvas id="chartUnitsDist"></canvas></div>
        </div>
        <div class="section-box">
          <div class="section-title">同時購入パターン TOP10</div>
          <div id="basketPairsTable"></div>
        </div>
      </div>
      <div class="section-box">
        <div class="section-title">商品別 同時購入分析</div>
        <div style="margin-bottom:14px">
          <select id="basketProductSelect" class="filter-select" style="width:auto;min-width:300px;max-width:100%">
            <option value="">商品を選択してください</option>
          </select>
        </div>
        <div id="coProductTableWrap"></div>
      </div>
    </div>
    <div class="sub-panel" id="cust-ltv">
      <div style="margin-bottom:14px;display:flex;gap:12px;align-items:center">
        <span class="filter-label">商品フィルタ</span>
        <select id="ltvProductFilter" class="filter-select" style="width:auto;min-width:200px;max-width:400px">
          <option value="">全商品</option>
        </select>
      </div>
      <div class="section-box">
        <div class="section-title">初回購入商品別 LTV・F2転換</div>
        <div id="ltvFirstItemTable"></div>
      </div>
      <div class="section-box">
        <div class="section-title">購入回数別 累計金額</div>
        <div id="ltvCountPriceTable"></div>
      </div>
    </div>
    <div class="sub-panel" id="cust-rfm">
      <div id="rfmCards" class="cards-grid"></div>
      <div class="section-box">
        <div class="section-title">RFMセグメント分布</div>
        <div class="grid-2">
          <div class="chart-wrap" style="height:350px"><canvas id="chartRfmHeatmap"></canvas></div>
          <div id="rfmSegmentTable"></div>
        </div>
      </div>
      <div class="section-box">
        <div class="section-title">セグメント別 購入商品比率</div>
        <div style="margin-bottom:10px;display:flex;gap:10px;align-items:center">
          <span class="filter-label">セグメント</span>
          <select id="rfmSegmentFilter" class="filter-select" style="width:auto;min-width:200px">
            <option value="">全セグメント</option>
          </select>
        </div>
        <div id="rfmProductTable"></div>
        <div class="chart-wrap chart-md" style="margin-top:16px"><canvas id="chartSegmentProduct"></canvas></div>
      </div>
    </div>
    <div class="sub-panel" id="cust-entrance">
      <div id="entranceCards" class="cards-grid"></div>
      <div class="section-box">
        <div class="section-title">入口商品別 F2転換・LTV</div>
        <div id="entranceItemTable"></div>
      </div>
      <div class="grid-2">
        <div class="section-box">
          <div class="section-title">入口商品別 F2転換率</div>
          <div class="chart-wrap chart-sm"><canvas id="chartEntranceF2"></canvas></div>
        </div>
        <div class="section-box">
          <div class="section-title">入口商品別 平均LTV</div>
          <div class="chart-wrap chart-sm"><canvas id="chartEntranceLTV"></canvas></div>
        </div>
      </div>
      <div class="section-box">
        <div class="section-title">2回目購入商品（入口商品別）</div>
        <div style="margin-bottom:10px;display:flex;gap:10px;align-items:center">
          <span class="filter-label">入口商品</span>
          <select id="entranceItemFilter" class="filter-select" style="width:auto;min-width:200px;max-width:400px">
            <option value="">選択してください</option>
          </select>
        </div>
        <div id="entrance2ndTable"></div>
      </div>
    </div>
    <div class="sub-panel" id="cust-timing">
      <div id="timingCards" class="cards-grid"></div>
      <div class="grid-2">
        <div class="section-box">
          <div class="section-title">初回→2回目購入 経過日数分布</div>
          <div class="chart-wrap chart-sm"><canvas id="chartF2Days"></canvas></div>
        </div>
        <div class="section-box">
          <div class="section-title">曜日別 注文数</div>
          <div class="chart-wrap chart-sm"><canvas id="chartDayOfWeek"></canvas></div>
        </div>
      </div>
      <div class="section-box">
        <div class="section-title">月別 注文数推移</div>
        <div class="chart-wrap" style="height:280px"><canvas id="chartMonthlyOrders"></canvas></div>
      </div>
    </div>
  </div>
</div>

</main>

<script>
// ── Data ──
const D = ${dataJson};

// ── Utility ──
const yen = v => '\\u00a5' + Number(v||0).toLocaleString();
const pct = v => Number(v||0).toFixed(2) + '%';
const pct1 = v => Number(v||0).toFixed(1) + '%';
const comma = v => Number(v||0).toLocaleString();
const safe = s => {
  const d = document.createElement('div');
  d.textContent = String(s||'');
  return d.innerHTML;
};

// ── State ──
let currentMonth = 'all';
let compareMode = 'mom'; // mom, yoy
let periodType = 'day'; // always day (date range)
let dayFrom = null;
let dayTo = null;
const chartInstances = {};

function destroyChart(id) {
  if (chartInstances[id]) { chartInstances[id].destroy(); delete chartInstances[id]; }
}

// ── Month filter population ──
const monthSelect = document.getElementById('monthFilter');
D.months.forEach(ym => {
  const opt = document.createElement('option');
  opt.value = ym; opt.textContent = D.monthLabels[ym] || ym;
  monthSelect.appendChild(opt);
});
// Default to latest month
if (D.months.length > 0) monthSelect.value = D.months[D.months.length - 1];

// ── Compare month calculation ──
function getCompareMonth(ym, mode) {
  if (!ym || ym === 'all') return null;
  const [y, m] = ym.split('-').map(Number);
  if (mode === 'mom') {
    const pm = m === 1 ? 12 : m - 1;
    const py = m === 1 ? y - 1 : y;
    return py + '-' + String(pm).padStart(2, '0');
  }
  if (mode === 'yoy') {
    return (y - 1) + '-' + String(m).padStart(2, '0');
  }
  return null;
}

function calcChange(current, previous) {
  if (!previous || previous === 0) return null;
  return ((current - previous) / Math.abs(previous)) * 100;
}

function changeHtml(changeVal) {
  if (changeVal === null || changeVal === undefined) return '';
  const cls = changeVal > 0.5 ? 'up' : changeVal < -0.5 ? 'down' : 'flat';
  const sign = changeVal > 0 ? '+' : '';
  return '<div class="metric-change ' + cls + '">' + sign + changeVal.toFixed(1) + '%</div>';
}

// ── Data getters ──
// shiftedRange: if provided, use this {from, to} instead of dayFrom/dayTo
function getMonthData(dataByMonth, ym, shiftedRange) {
  if (periodType === 'day' && (shiftedRange || (dayFrom && dayTo))) {
    const from = shiftedRange ? shiftedRange.from : dayFrom;
    const to = shiftedRange ? shiftedRange.to : dayTo;
    const fromYm = from.substring(0, 7);
    const toYm = to.substring(0, 7);
    const all = [];
    Object.entries(dataByMonth).forEach(([m, arr]) => {
      arr.forEach(r => {
        const d = (r.date || '').replace(/\\//g, '-');
        if (d) {
          // 日付がある場合は日付でフィルタ
          if (d >= from && d <= to) all.push(r);
        } else {
          // 日付がない月次集計データは月キーで判定
          if (m >= fromYm && m <= toYm) all.push(r);
        }
      });
    });
    return all;
  }
  if (ym === 'all') {
    const all = [];
    Object.values(dataByMonth).forEach(arr => all.push(...arr));
    return all;
  }
  return dataByMonth[ym] || [];
}

// Get shifted date range for comparison (mom = prev month, yoy = prev year)
function getCompareRange(mode) {
  if (!dayFrom || !dayTo) return null;
  function shiftDate(dateStr, months) {
    const d = new Date(dateStr + 'T00:00:00');
    d.setMonth(d.getMonth() + months);
    return d.toISOString().substring(0, 10);
  }
  if (mode === 'mom') return { from: shiftDate(dayFrom, -1), to: shiftDate(dayTo, -1) };
  if (mode === 'yoy') return { from: shiftDate(dayFrom, -12), to: shiftDate(dayTo, -12) };
  return null;
}

function sumField(arr, field) {
  return arr.reduce((s, r) => s + (Number(r[field]) || 0), 0);
}

function avgField(arr, field) {
  const vals = arr.filter(r => Number(r[field]) > 0);
  return vals.length ? vals.reduce((s, r) => s + Number(r[field]), 0) / vals.length : 0;
}

// ── Table builder ──
function buildTable(containerId, columns, rows, opts = {}) {
  const el = document.getElementById(containerId);
  if (!el) return;
  if (!rows || rows.length === 0) {
    el.innerHTML = '<div class="no-data">データなし</div>';
    return;
  }
  const limit = opts.limit || 100;
  const displayed = rows.slice(0, limit);
  let html = '<div class="table-wrap"><table><thead><tr>';
  columns.forEach((c, ci) => {
    html += '<th data-col="' + ci + '" data-key="' + (c.key||'') + '"><span>' + safe(c.label) + '</span><span class="sort-icon"></span></th>';
  });
  html += '</tr></thead><tbody>';
  displayed.forEach(row => {
    html += '<tr>';
    columns.forEach(c => {
      const v = row[c.key];
      const formatted = c.fmt ? c.fmt(v, row) : safe(v);
      html += '<td>' + formatted + '</td>';
    });
    html += '</tr>';
  });
  html += '</tbody></table></div>';
  el.innerHTML = html;

  // Sortable headers
  el.querySelectorAll('thead th').forEach(th => {
    th.addEventListener('click', function() {
      const key = this.dataset.key;
      const col = columns.find(c => c.key === key);
      if (!col) return;
      const asc = this.classList.contains('sorted-asc');
      el.querySelectorAll('th').forEach(t => { t.classList.remove('sorted', 'sorted-asc', 'sorted-desc'); });
      this.classList.add('sorted', asc ? 'sorted-desc' : 'sorted-asc');
      const sorted = [...rows].sort((a, b) => {
        let va = a[key], vb = b[key];
        if (typeof va === 'number' && typeof vb === 'number') return asc ? vb - va : va - vb;
        va = Number(va); vb = Number(vb);
        if (!isNaN(va) && !isNaN(vb)) return asc ? vb - va : va - vb;
        return asc ? String(b[key]||'').localeCompare(String(a[key]||'')) : String(a[key]||'').localeCompare(String(b[key]||''));
      });
      buildTable(containerId, columns, sorted, opts);
    });
  });
}

// ── Render functions ──

function renderSalesTab() {
  const data = getMonthData(D.allByMonth, currentMonth);
  const cmpYm = getCompareMonth(currentMonth, compareMode);
  const cmpRange = getCompareRange(compareMode);
  const cmpData = cmpRange ? getMonthData(D.allByMonth, cmpYm, cmpRange) : (cmpYm ? getMonthData(D.allByMonth, cmpYm) : []);

  const totalSales = sumField(data, 'sales');
  const totalOrders = sumField(data, 'orders');
  const totalAccess = sumField(data, 'access');
  const avgCvr = totalAccess > 0 ? (totalOrders / totalAccess * 100) : 0;
  const avgPrice = totalOrders > 0 ? totalSales / totalOrders : 0;
  const totalNew = sumField(data, 'newBuyers');
  const totalRepeat = sumField(data, 'repeatBuyers');

  const cTotalSales = cmpData.length ? sumField(cmpData, 'sales') : null;
  const cTotalOrders = cmpData.length ? sumField(cmpData, 'orders') : null;
  const cTotalAccess = cmpData.length ? sumField(cmpData, 'access') : null;
  const cAvgCvr = cmpData.length && sumField(cmpData, 'access') > 0 ? (sumField(cmpData, 'orders') / sumField(cmpData, 'access') * 100) : null;
  const cAvgPrice = cmpData.length && sumField(cmpData, 'orders') > 0 ? sumField(cmpData, 'sales') / sumField(cmpData, 'orders') : null;

  const cards = [
    { label: '売上', value: yen(totalSales), change: calcChange(totalSales, cTotalSales) },
    { label: '売上件数', value: comma(totalOrders), change: calcChange(totalOrders, cTotalOrders) },
    { label: 'アクセス人数', value: comma(totalAccess), change: calcChange(totalAccess, cTotalAccess) },
    { label: '転換率', value: pct(avgCvr), change: cAvgCvr !== null ? calcChange(avgCvr, cAvgCvr) : null },
    { label: '客単価', value: yen(Math.round(avgPrice)), change: calcChange(avgPrice, cAvgPrice) },
  ];
  document.getElementById('salesCards').innerHTML = cards.map(c =>
    '<div class="metric-card"><div class="metric-label">' + c.label + '</div><div class="metric-value">' + c.value + '</div>' + (c.change !== undefined && c.change !== null ? changeHtml(c.change) : '') + '</div>'
  ).join('');

  // Daily chart
  const sorted = [...data].sort((a, b) => String(a.date).localeCompare(String(b.date)));
  const labels = sorted.map(r => {
    const d = r.date.replace(/\\//g, '-').replace(/-/g, '/');
    const parts = d.split('/');
    return parts.length >= 3 ? parts[1] + '/' + parts[2] : r.date;
  });
  const salesArr = sorted.map(r => r.sales);
  const accessArr = sorted.map(r => r.access);
  const ordersArr = sorted.map(r => r.orders);

  // New/Repeat pie chart
  destroyChart('chartNewRepeatPie');
  if (totalNew > 0 || totalRepeat > 0) {
    chartInstances['chartNewRepeatPie'] = new Chart(document.getElementById('chartNewRepeatPie'), {
      type: 'doughnut',
      data: {
        labels: ['新規購入者', 'リピート購入者'],
        datasets: [{ data: [totalNew, totalRepeat], backgroundColor: ['#1a3a5c', '#e8710a'] }]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { position: 'bottom' },
          tooltip: { callbacks: { label: ctx => ctx.label + ': ' + comma(ctx.raw) + '人 (' + (totalNew + totalRepeat > 0 ? (ctx.raw / (totalNew + totalRepeat) * 100).toFixed(1) : 0) + '%)' } }
        }
      }
    });
  }

  // ── 売上KPIツリー ──
  const rppData = getMonthData(D.rppByMonth, currentMonth);
  const rppSales = sumField(rppData, 'sales');
  const rppClicks = sumField(rppData, 'clicks');
  const rppOrders = sumField(rppData, 'orders');
  const nonRppSales = totalSales - rppSales;
  const nonRppAccess = totalAccess - rppClicks;
  const nonRppOrders = totalOrders - rppOrders;
  const rppCvr = rppClicks > 0 ? (rppOrders / rppClicks * 100) : 0;
  const rppUnitPrice = rppOrders > 0 ? Math.round(rppSales / rppOrders) : 0;
  const nonRppCvr = nonRppAccess > 0 ? (nonRppOrders / nonRppAccess * 100) : 0;
  const nonRppUnitPrice = nonRppOrders > 0 ? Math.round(nonRppSales / nonRppOrders) : 0;

  // 前月・前年比
  const prevMonthYm = getCompareMonth(currentMonth, 'mom');
  const prevYearYm = getCompareMonth(currentMonth, 'yoy');
  const prevMonthRange = getCompareRange('mom');
  const prevYearRange = getCompareRange('yoy');
  function getTreeCompare(ym, range) {
    if (!ym) return null;
    const d = getMonthData(D.allByMonth, ym, range);
    const r = getMonthData(D.rppByMonth, ym, range);
    const s = sumField(d, 'sales'), o = sumField(d, 'orders'), a = sumField(d, 'access');
    const rs = sumField(r, 'sales'), rc = sumField(r, 'clicks'), ro = sumField(r, 'orders');
    return {
      sales: s, rppSales: rs, nonRppSales: s - rs,
      rppClicks: rc, rppOrders: ro, rppCvr: rc > 0 ? (ro/rc*100) : 0, rppUnitPrice: ro > 0 ? Math.round(rs/ro) : 0,
      nonRppAccess: a - rc, nonRppOrders: o - ro, nonRppCvr: (a-rc) > 0 ? ((o-ro)/(a-rc)*100) : 0, nonRppUnitPrice: (o-ro) > 0 ? Math.round((s-rs)/(o-ro)) : 0,
      access: a, cvr: a > 0 ? (o/a*100) : 0, unitPrice: o > 0 ? Math.round(s/o) : 0,
    };
  }
  const prevM = getTreeCompare(prevMonthYm, prevMonthRange);
  const prevY = getTreeCompare(prevYearYm, prevYearRange);

  function treeRatio(cur, prev) {
    if (!prev || prev === 0) return '-';
    return (cur / prev * 100).toFixed(1) + '%';
  }
  function treeBox(title, value, prevMVal, prevYVal, color) {
    const bg = color || 'var(--c-primary)';
    return '<div style="background:' + bg + ';color:#fff;border-radius:8px;padding:10px 16px;text-align:center;min-width:130px">' +
      '<div style="font-size:11px;opacity:0.85">' + title + '</div>' +
      '<div style="font-size:18px;font-weight:700;margin:4px 0">' + value + '</div>' +
      '<div style="font-size:10px;opacity:0.8">前月比 ' + (prevMVal || '-') + '</div>' +
      '<div style="font-size:10px;opacity:0.8">前年比 ' + (prevYVal || '-') + '</div>' +
    '</div>';
  }

  const monthLabel = D.monthLabels[currentMonth] || currentMonth;
  const treeHtml = '<div style="display:flex;flex-direction:column;align-items:center;gap:4px">' +
    // Level 1: 月売上
    '<div style="display:flex;justify-content:center">' +
    treeBox(monthLabel + '売上', yen(totalSales), treeRatio(totalSales, prevM?.sales), treeRatio(totalSales, prevY?.sales)) +
    '</div>' +
    '<div style="width:2px;height:16px;background:#1a3a5c;margin:0 auto"></div>' +
    // Level 2: RPP / 非RPP
    '<div style="display:flex;justify-content:center;gap:40px;position:relative">' +
    '<div style="position:absolute;top:0;left:25%;right:25%;height:1px;background:#1a3a5c"></div>' +
    treeBox('RPP広告売上', yen(rppSales), treeRatio(rppSales, prevM?.rppSales), treeRatio(rppSales, prevY?.rppSales)) +
    treeBox('非RPP広告売上', yen(nonRppSales), treeRatio(nonRppSales, prevM?.nonRppSales), treeRatio(nonRppSales, prevY?.nonRppSales)) +
    '</div>' +
    '<div style="display:flex;justify-content:center;gap:8px;position:relative">' +
    // Level 3 left: RPP breakdown
    treeBox('アクセス数', comma(rppClicks), treeRatio(rppClicks, prevM?.rppClicks), treeRatio(rppClicks, prevY?.rppClicks)) +
    treeBox('転換率', pct(rppCvr), treeRatio(rppCvr, prevM?.rppCvr), treeRatio(rppCvr, prevY?.rppCvr)) +
    treeBox('客単価', yen(rppUnitPrice), treeRatio(rppUnitPrice, prevM?.rppUnitPrice), treeRatio(rppUnitPrice, prevY?.rppUnitPrice)) +
    // Level 3 right: non-RPP breakdown
    treeBox('アクセス人数', comma(nonRppAccess), treeRatio(nonRppAccess, prevM?.nonRppAccess), treeRatio(nonRppAccess, prevY?.nonRppAccess)) +
    treeBox('転換率', pct(nonRppCvr), treeRatio(nonRppCvr, prevM?.nonRppCvr), treeRatio(nonRppCvr, prevY?.nonRppCvr)) +
    treeBox('客単価', yen(nonRppUnitPrice), treeRatio(nonRppUnitPrice, prevM?.nonRppUnitPrice), treeRatio(nonRppUnitPrice, prevY?.nonRppUnitPrice)) +
    '</div></div>';
  document.getElementById('salesTreeWrap').innerHTML = treeHtml;

  // ── 日別 RPP経由/広告外 積み上げチャート ──
  destroyChart('chartDailyRppSplit');
  const rppSorted = [...rppData].sort((a, b) => String(a.date).localeCompare(String(b.date)));
  // Build daily map: date -> { rppSales, totalSales }
  const dailyMap = {};
  sorted.forEach(r => {
    const d = r.date.replace(/\\//g, '-');
    dailyMap[d] = { total: r.sales, rpp: 0 };
  });
  rppSorted.forEach(r => {
    const d = String(r.date).replace(/\\//g, '-');
    if (dailyMap[d]) dailyMap[d].rpp = r.sales;
    else dailyMap[d] = { total: r.sales, rpp: r.sales };
  });
  const dailyDates = Object.keys(dailyMap).sort();
  if (dailyDates.length > 0) {
    const dayLabels = dailyDates.map(d => { const p = d.split(/[-\\/]/); return p.length >= 3 ? parseInt(p[2]) : d; });
    const rppDaySales = dailyDates.map(d => dailyMap[d].rpp);
    const nonRppDaySales = dailyDates.map(d => Math.max(0, dailyMap[d].total - dailyMap[d].rpp));

    // イベントアノテーション生成（主要イベントのみ: マラソン/スーパーSALE/ワンダフルデー）
    const eventAnnotations = {};
    const evtColorMap = { 'マラソン': 'rgba(255,87,34,0.13)', 'スーパーSALE': 'rgba(234,67,53,0.15)', 'ワンダフルデー': 'rgba(76,175,80,0.12)' };
    const majorEvents = (D.rakutenEvents || []).filter(evt => {
      const t = evt.title || '';
      return t.includes('マラソン') || t.includes('スーパーSALE') || t.includes('ワンダフルデー');
    });
    let evtIdx = 0;
    majorEvents.forEach(evt => {
      const eStart = evt.startDate ? evt.startDate.replace(/\\//g, '-').substring(0, 10) : '';
      const eEnd = evt.endDate ? evt.endDate.replace(/\\//g, '-').substring(0, 10) : '';
      if (!eStart) return;
      const si = dailyDates.findIndex(d => d >= eStart);
      const eiEnd = eEnd ? dailyDates.findIndex(d => d > eEnd) : si + 1;
      if (si < 0) return;
      const t = evt.title || '';
      const color = t.includes('スーパーSALE') ? evtColorMap['スーパーSALE'] : t.includes('マラソン') ? evtColorMap['マラソン'] : evtColorMap['ワンダフルデー'];
      const shortName = t.includes('スーパーSALE') ? 'SS' : t.includes('マラソン') ? 'マラソン' : 'WD';
      eventAnnotations['evt' + evtIdx++] = {
        type: 'box', xMin: si - 0.5, xMax: (eiEnd < 0 ? dailyDates.length : eiEnd) - 0.5,
        backgroundColor: color, borderWidth: 0,
        label: { display: true, content: shortName, position: 'start', font: { size: 9, weight: 'bold' }, color: '#c62828' }
      };
    });

    chartInstances['chartDailyRppSplit'] = new Chart(document.getElementById('chartDailyRppSplit'), {
      type: 'bar',
      data: {
        labels: dayLabels,
        datasets: [
          { label: 'RPP経由売上金額', data: rppDaySales, backgroundColor: 'rgba(66,133,244,0.7)', stack: 'a' },
          { label: '広告外売上金額', data: nonRppDaySales, backgroundColor: 'rgba(234,179,8,0.7)', stack: 'a' },
        ]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { position: 'bottom' },
          tooltip: { mode: 'index', callbacks: { label: ctx => ctx.dataset.label + ': ' + yen(ctx.raw), footer: items => '合計: ' + yen(items.reduce((s, i) => s + i.raw, 0)) } },
          annotation: { annotations: eventAnnotations },
        },
        scales: {
          x: { stacked: true },
          y: { stacked: true, ticks: { callback: v => v >= 10000 ? (v/10000).toFixed(0) + '万' : comma(v) } }
        }
      }
    });
  }


}

function aggregateProductMonth(months) {
  const agg = {};
  months.forEach(ym => {
    (D.allItemByMonth[ym] || []).forEach(r => {
      const mn = (r.manageNum || '').trim();
      if (!mn) return;
      if (!agg[mn]) agg[mn] = { manageNum: mn, sales: 0, orders: 0, access: 0, rppSpend: 0, rppSales: 0, rppClicks: 0, rppOrders: 0 };
      agg[mn].sales += r.sales || 0;
      agg[mn].orders += r.orders || 0;
      agg[mn].access += r.access || 0;
    });
    (D.rppItemByMonth[ym] || []).forEach(r => {
      const mn = (r.manageNum || '').trim();
      if (!mn) return;
      if (!agg[mn]) agg[mn] = { manageNum: mn, sales: 0, orders: 0, access: 0, rppSpend: 0, rppSales: 0, rppClicks: 0, rppOrders: 0 };
      agg[mn].rppSpend += r.spend || 0;
      agg[mn].rppSales += r.sales || 0;
      agg[mn].rppClicks += r.clicks || 0;
      agg[mn].rppOrders += r.orders || 0;
    });
  });
  return agg;
}

function renderProductTab() {
  const wrap = document.getElementById('productTableWrap');
  const prodMonth = document.getElementById('monthFilter').value || 'all';

  // 2025-10以降のデータのみ
  const validMonths = D.months.filter(ym => ym >= '2025-10');
  const targetMonths = prodMonth === 'all' ? validMonths : (validMonths.includes(prodMonth) ? [prodMonth] : []);

  if (targetMonths.length === 0) {
    wrap.innerHTML = '<div class="no-data">該当月のデータがありません</div>';
    return;
  }

  // 商品管理番号ごとに集計
  const productAgg = aggregateProductMonth(targetMonths);

  // 前月比・前年同月比（月指定時のみ）
  let prevMonthAgg = null, prevYearAgg = null;
  const hasCmp = prodMonth !== 'all' && prodMonth;
  if (hasCmp) {
    const [y, m] = prodMonth.split('-').map(Number);
    const pmYm = m === 1 ? (y - 1) + '-12' : y + '-' + String(m - 1).padStart(2, '0');
    const pyYm = (y - 1) + '-' + String(m).padStart(2, '0');
    if (D.months.includes(pmYm)) prevMonthAgg = aggregateProductMonth([pmYm]);
    if (D.months.includes(pyYm)) prevYearAgg = aggregateProductMonth([pyYm]);
  }

  const rows = Object.values(productAgg).map(p => {
    const cvr = p.access > 0 ? (p.orders / p.access * 100) : 0;
    const unitPrice = p.orders > 0 ? Math.round(p.sales / p.orders) : 0;
    const rppRoas = p.rppSpend > 0 ? (p.rppSales / p.rppSpend * 100) : 0;
    const rppCvr = p.rppClicks > 0 ? (p.rppOrders / p.rppClicks * 100) : 0;
    const rppCpc = p.rppClicks > 0 ? Math.round(p.rppSpend / p.rppClicks) : 0;
    return { ...p, cvr, unitPrice, rppRoas, rppCvr, rppCpc };
  });

  // 売上降順ソート
  rows.sort((a, b) => b.sales - a.sales);

  const totalSales = rows.reduce((s, r) => s + r.sales, 0);

  function cmpHtml(cur, prev) {
    if (!prev || prev === 0) return '-';
    const ch = ((cur - prev) / Math.abs(prev) * 100);
    const cls = ch > 0.5 ? 'color:var(--c-success)' : ch < -0.5 ? 'color:var(--c-danger)' : '';
    return '<span style="' + cls + '">' + (ch > 0 ? '+' : '') + ch.toFixed(1) + '%</span>';
  }

  const cols = [
    { label: '商品管理番号', key: 'manageNum', align: 'left', fmt: v => v, sticky: true },
    { label: '売上', key: 'sales', fmt: v => yen(v) },
    { label: '売上構成比', key: '_salesRatio', fmt: (v, r) => totalSales > 0 ? (r.sales / totalSales * 100).toFixed(1) + '%' : '-' },
    { label: '件数', key: 'orders', fmt: v => comma(v) },
    { label: 'アクセス', key: 'access', fmt: v => comma(v) },
    { label: '転換率', key: 'cvr', fmt: v => v.toFixed(2) + '%' },
    { label: '客単価', key: 'unitPrice', fmt: v => yen(v) },
    { label: 'RPP費用', key: 'rppSpend', fmt: v => yen(v) },
    { label: 'RPP売上', key: 'rppSales', fmt: v => yen(v) },
    { label: 'RPP ROAS', key: 'rppRoas', fmt: v => v.toFixed(0) + '%' },
    { label: 'RPP CVR', key: 'rppCvr', fmt: v => v.toFixed(2) + '%' },
    { label: 'RPP CPC', key: 'rppCpc', fmt: v => yen(v) },
  ];

  // 前月比・前年同月比カラム追加（月指定時のみ）
  if (hasCmp && (prevMonthAgg || prevYearAgg)) {
    if (prevMonthAgg) cols.push({ label: '前月比(売上)', key: '_momSales', align: 'right' });
    if (prevYearAgg) cols.push({ label: '前年同月比(売上)', key: '_yoySales', align: 'right' });
  }

  let html = '<table style="font-size:12px;width:100%"><thead><tr>';
  cols.forEach(c => {
    const stickyStyle = c.sticky ? 'position:sticky;left:0;background:#f8f9fa;z-index:1;' : '';
    html += '<th style="' + stickyStyle + 'text-align:' + (c.align || 'right') + ';cursor:pointer;white-space:nowrap" data-sort-key="' + c.key + '">' + c.label + '</th>';
  });
  html += '</tr></thead><tbody>';

  if (rows.length === 0) {
    html += '<tr><td colspan="' + cols.length + '" style="text-align:center;padding:20px">データなし</td></tr>';
  } else {
    rows.forEach(r => {
      html += '<tr>';
      cols.forEach(c => {
        const stickyStyle = c.sticky ? 'position:sticky;left:0;background:#fff;z-index:1;' : '';
        let val;
        if (c.key === '_salesRatio') val = c.fmt(0, r);
        else if (c.key === '_momSales') val = cmpHtml(r.sales, prevMonthAgg?.[r.manageNum]?.sales || 0);
        else if (c.key === '_yoySales') val = cmpHtml(r.sales, prevYearAgg?.[r.manageNum]?.sales || 0);
        else val = c.fmt(r[c.key]);
        html += '<td style="' + stickyStyle + 'text-align:' + (c.align || 'right') + '">' + val + '</td>';
      });
      html += '</tr>';
    });
  }
  html += '</tbody></table>';
  wrap.innerHTML = html;

  // ソート機能
  wrap.querySelectorAll('th[data-sort-key]').forEach(th => {
    th.addEventListener('click', function() {
      const key = this.dataset.sortKey;
      const asc = this.dataset.sortDir === 'asc';
      wrap.querySelectorAll('th').forEach(t => delete t.dataset.sortDir);
      this.dataset.sortDir = asc ? 'desc' : 'asc';
      const sorted = [...rows].sort((a, b) => {
        let va, vb;
        if (key === '_salesRatio') { va = a.sales; vb = b.sales; }
        else if (key === '_momSales') { va = prevMonthAgg?.[a.manageNum]?.sales ? a.sales / prevMonthAgg[a.manageNum].sales : 0; vb = prevMonthAgg?.[b.manageNum]?.sales ? b.sales / prevMonthAgg[b.manageNum].sales : 0; }
        else if (key === '_yoySales') { va = prevYearAgg?.[a.manageNum]?.sales ? a.sales / prevYearAgg[a.manageNum].sales : 0; vb = prevYearAgg?.[b.manageNum]?.sales ? b.sales / prevYearAgg[b.manageNum].sales : 0; }
        else { va = typeof a[key] === 'string' ? a[key] : a[key] || 0; vb = typeof b[key] === 'string' ? b[key] : b[key] || 0; }
        if (typeof va === 'string') return asc ? vb.localeCompare(va) : va.localeCompare(vb);
        return asc ? vb - va : va - vb;
      });
      const tbody = wrap.querySelector('tbody');
      tbody.innerHTML = '';
      sorted.forEach(r => {
        let tr = '<tr>';
        cols.forEach(c => {
          const stickyStyle = c.sticky ? 'position:sticky;left:0;background:#fff;z-index:1;' : '';
          let val;
          if (c.key === '_salesRatio') val = c.fmt(0, r);
          else if (c.key === '_momSales') val = cmpHtml(r.sales, prevMonthAgg?.[r.manageNum]?.sales || 0);
          else if (c.key === '_yoySales') val = cmpHtml(r.sales, prevYearAgg?.[r.manageNum]?.sales || 0);
          else val = c.fmt(r[c.key]);
          tr += '<td style="' + stickyStyle + 'text-align:' + (c.align || 'right') + '">' + val + '</td>';
        });
        tr += '</tr>';
        tbody.innerHTML += tr;
      });
    });
  });

  // 月別推移テーブル
  renderProductMonthlyTable();
  // カテゴリ別実績
  renderCategoryTable();
}

function renderCategoryTable() {
  const wrap = document.getElementById('categoryTableWrap');
  const prodMonth = document.getElementById('monthFilter').value || 'all';
  const validMonths = D.months.filter(ym => ym >= '2025-10');
  const targetMonths = prodMonth === 'all' ? validMonths : (validMonths.includes(prodMonth) ? [prodMonth] : []);

  // ジャンル別に集約
  const catAgg = {};
  targetMonths.forEach(ym => {
    (D.allItemByMonth[ym] || []).forEach(r => {
      const genre = r.genre || '未分類';
      if (!catAgg[genre]) catAgg[genre] = { genre, sales: 0, orders: 0, access: 0, products: new Set() };
      catAgg[genre].sales += r.sales || 0;
      catAgg[genre].orders += r.orders || 0;
      catAgg[genre].access += r.access || 0;
      if (r.manageNum) catAgg[genre].products.add(r.manageNum);
    });
  });

  const rows = Object.values(catAgg).map(c => ({
    genre: c.genre, sales: c.sales, orders: c.orders, access: c.access,
    productCount: c.products.size,
    cvr: c.access > 0 ? (c.orders / c.access * 100) : 0,
    unitPrice: c.orders > 0 ? c.sales / c.orders : 0,
  })).sort((a, b) => b.sales - a.sales);

  const totalSales = rows.reduce((s, r) => s + r.sales, 0);

  const catRows = rows.map(r => ({
    ...r,
    share: totalSales > 0 ? (r.sales / totalSales * 100) : 0,
  }));

  buildTable('categoryTableWrap', [
    { key: 'genre', label: 'カテゴリ', fmt: v => safe(v) },
    { key: 'productCount', label: '商品数', fmt: v => comma(v) },
    { key: 'sales', label: '売上', fmt: v => yen(v) },
    { key: 'share', label: '売上構成比', fmt: v => v.toFixed(1) + '%' },
    { key: 'orders', label: '件数', fmt: v => comma(v) },
    { key: 'access', label: 'アクセス', fmt: v => comma(v) },
    { key: 'cvr', label: '転換率', fmt: v => v.toFixed(2) + '%' },
    { key: 'unitPrice', label: '客単価', fmt: v => yen(Math.round(v)) },
  ], catRows);

  // カテゴリ別売上推移チャート
  renderCategoryTrendChart(rows.slice(0, 8).map(r => r.genre));
}

function renderCategoryTrendChart(topGenres) {
  destroyChart('chartCategoryTrend');
  const validMonths = D.months.filter(ym => ym >= '2025-10').sort();
  if (validMonths.length === 0 || topGenres.length === 0) return;

  const colors = ['#4285f4','#ea4335','#fbbc04','#34a853','#ff6d01','#46bdc6','#7b1fa2','#c2185b'];
  const datasets = topGenres.map((genre, gi) => {
    const data = validMonths.map(ym => {
      let sales = 0;
      (D.allItemByMonth[ym] || []).forEach(r => {
        if ((r.genre || '未分類') === genre) sales += r.sales || 0;
      });
      return sales;
    });
    return { label: genre.substring(0, 20), data, borderColor: colors[gi % colors.length], backgroundColor: colors[gi % colors.length] + '22', fill: false, tension: 0.3 };
  });

  chartInstances['chartCategoryTrend'] = new Chart(document.getElementById('chartCategoryTrend'), {
    type: 'line',
    data: { labels: validMonths.map(ym => D.monthLabels[ym] || ym), datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'bottom' } },
      scales: { y: { ticks: { callback: v => v >= 10000 ? (v/10000).toFixed(0) + '万' : comma(v) } } }
    }
  });
}

function renderProductMonthlyTable() {
  const mWrap = document.getElementById('productMonthlyTableWrap');
  if (!mWrap) return;
  const metric = document.getElementById('productMetricSelect')?.value || 'sales';

  const validMonths = D.months.filter(ym => ym >= '2025-10').slice().reverse(); // chronological
  if (validMonths.length === 0) { mWrap.innerHTML = '<div class="no-data">データなし</div>'; return; }

  // 各月 + 前月/前年同月のデータ
  const allNeededMonths = new Set(validMonths);
  validMonths.forEach(ym => {
    const [y, m] = ym.split('-').map(Number);
    const pm = m === 1 ? (y - 1) + '-12' : y + '-' + String(m - 1).padStart(2, '0');
    const py = (y - 1) + '-' + String(m).padStart(2, '0');
    allNeededMonths.add(pm);
    allNeededMonths.add(py);
  });
  const monthAggs = {};
  [...allNeededMonths].forEach(ym => { if (D.months.includes(ym)) monthAggs[ym] = aggregateProductMonth([ym]); });

  const allMn = new Set();
  validMonths.forEach(ym => { if (monthAggs[ym]) Object.keys(monthAggs[ym]).forEach(mn => allMn.add(mn)); });

  const fmtMap = {
    sales: v => yen(v), orders: v => comma(v), access: v => comma(v),
    cvr: v => v.toFixed(2) + '%', unitPrice: v => yen(v),
    rppSpend: v => yen(v), rppSales: v => yen(v), rppRoas: v => v.toFixed(0) + '%',
  };
  const fmt = fmtMap[metric] || (v => String(v));

  const getVal = (agg, mn) => {
    const p = agg?.[mn];
    if (!p) return 0;
    if (metric === 'cvr') return p.access > 0 ? (p.orders / p.access * 100) : 0;
    if (metric === 'unitPrice') return p.orders > 0 ? Math.round(p.sales / p.orders) : 0;
    if (metric === 'rppRoas') return p.rppSpend > 0 ? (p.rppSales / p.rppSpend * 100) : 0;
    return p[metric] || 0;
  };

  function chgHtml(cur, prev) {
    if (!prev || prev === 0) return '<span style="color:#999">-</span>';
    const ch = ((cur - prev) / Math.abs(prev) * 100);
    const cls = ch > 0.5 ? 'color:var(--c-success)' : ch < -0.5 ? 'color:var(--c-danger)' : '';
    return '<span style="font-size:10px;' + cls + '">' + (ch > 0 ? '+' : '') + ch.toFixed(0) + '%</span>';
  }

  // 前月/前年同月YM算出
  function getPrevYm(ym) {
    const [y, m] = ym.split('-').map(Number);
    return m === 1 ? (y - 1) + '-12' : y + '-' + String(m - 1).padStart(2, '0');
  }
  function getYoyYm(ym) {
    const [y, m] = ym.split('-').map(Number);
    return (y - 1) + '-' + String(m).padStart(2, '0');
  }

  const mnList = [...allMn].map(mn => {
    const total = validMonths.reduce((s, ym) => s + getVal(monthAggs[ym], mn), 0);
    return { mn, total };
  }).sort((a, b) => b.total - a.total).map(x => x.mn);

  // ヘッダー: 各月に値+前月比+前年同月比
  let html = '<table style="font-size:12px;width:100%;border-collapse:collapse"><thead><tr>';
  html += '<th rowspan="2" style="text-align:left;position:sticky;left:0;background:#f8f9fa;z-index:2;cursor:pointer;white-space:nowrap;border-bottom:2px solid #ddd" data-msort-key="mn">商品管理番号</th>';
  validMonths.forEach(ym => {
    html += '<th colspan="3" style="text-align:center;border-bottom:1px solid #eee;white-space:nowrap">' + (D.monthLabels[ym] || ym) + '</th>';
  });
  html += '<th rowspan="2" style="cursor:pointer;white-space:nowrap;border-bottom:2px solid #ddd" data-msort-key="total">合計</th>';
  html += '</tr><tr>';
  validMonths.forEach(ym => {
    html += '<th style="cursor:pointer;font-size:10px;white-space:nowrap;border-bottom:2px solid #ddd" data-msort-key="' + ym + '">値</th>';
    html += '<th style="font-size:10px;white-space:nowrap;border-bottom:2px solid #ddd">前月比</th>';
    html += '<th style="font-size:10px;white-space:nowrap;border-bottom:2px solid #ddd">前年比</th>';
  });
  html += '</tr></thead><tbody>';

  function renderRow(mn) {
    let tr = '<tr><td style="text-align:left;position:sticky;left:0;background:#fff;z-index:1;white-space:nowrap">' + mn + '</td>';
    validMonths.forEach(ym => {
      const v = getVal(monthAggs[ym], mn);
      const pm = getPrevYm(ym);
      const py = getYoyYm(ym);
      const pv = monthAggs[pm] ? getVal(monthAggs[pm], mn) : 0;
      const yv = monthAggs[py] ? getVal(monthAggs[py], mn) : 0;
      tr += '<td style="text-align:right">' + fmt(v) + '</td>';
      tr += '<td style="text-align:right">' + chgHtml(v, pv) + '</td>';
      tr += '<td style="text-align:right">' + chgHtml(v, yv) + '</td>';
    });
    const total = validMonths.reduce((s, ym) => s + getVal(monthAggs[ym], mn), 0);
    tr += '<td style="text-align:right;font-weight:600">' + fmt(total) + '</td></tr>';
    return tr;
  }

  mnList.forEach(mn => { html += renderRow(mn); });
  html += '</tbody></table>';
  mWrap.innerHTML = html;

  // ソート
  mWrap.querySelectorAll('th[data-msort-key]').forEach(th => {
    th.addEventListener('click', function() {
      const key = this.dataset.msortKey;
      const asc = this.dataset.sortDir === 'asc';
      mWrap.querySelectorAll('th').forEach(t => delete t.dataset.sortDir);
      this.dataset.sortDir = asc ? 'desc' : 'asc';
      const sorted = [...mnList].sort((a, b) => {
        if (key === 'mn') return asc ? b.localeCompare(a) : a.localeCompare(b);
        if (key === 'total') {
          const va = validMonths.reduce((s, ym) => s + getVal(monthAggs[ym], a), 0);
          const vb = validMonths.reduce((s, ym) => s + getVal(monthAggs[ym], b), 0);
          return asc ? vb - va : va - vb;
        }
        return asc ? getVal(monthAggs[key], b) - getVal(monthAggs[key], a) : getVal(monthAggs[key], a) - getVal(monthAggs[key], b);
      });
      const tbody = mWrap.querySelector('tbody');
      tbody.innerHTML = '';
      sorted.forEach(mn => { tbody.innerHTML += renderRow(mn); });
    });
  });
}

function getAllMonthData(dataByMonth) {
  // 月次集計データ（日付なし）は全期間を返す
  const all = [];
  Object.values(dataByMonth || {}).forEach(arr => all.push(...arr));
  return all;
}
function renderAdsTab() {
  const adMonth = document.getElementById('monthFilter').value || 'all';
  const rppData = getMonthData(D.rppByMonth, adMonth);
  const tdaData = getMonthData(D.tdaByMonth, adMonth);
  const adData = getMonthData(D.adByMonth, adMonth);
  const cpaData = getMonthData(D.cpaByMonth || {}, adMonth);
  const rppItemData = getMonthData(D.rppItemByMonth, adMonth);
  // rpp_kw_raw: 月フィルター（getMonthData使用で日付範囲対応）
  const rppKwData = getMonthData(D.rppKwByMonth, adMonth);

  const cmpYm = getCompareMonth(adMonth, compareMode);

  const rppSpend = sumField(rppData, 'spend');
  const rppSales = sumField(rppData, 'sales');
  const rppClicks = sumField(rppData, 'clicks');
  const rppOrders = sumField(rppData, 'orders');
  const tdaSpend = sumField(tdaData, 'spend');
  const tdaSales = sumField(tdaData, 'sales');
  const tdaClicks = sumField(tdaData, 'clicks');
  const adSpend = sumField(adData, 'spend');
  const adSales = sumField(adData, 'sales');
  const adClicks = sumField(adData, 'clicks');
  const cpaSpend = sumField(cpaData, 'spend');
  const cpaSales = sumField(cpaData, 'sales');

  const totalSpend = rppSpend + tdaSpend + adSpend + cpaSpend;
  const totalSales = rppSales + tdaSales + adSales + cpaSales;
  const totalRoas = totalSpend > 0 ? (totalSales / totalSpend * 100) : 0;

  // Compare
  let cTotalSpend = null, cTotalSales = null;
  const adCmpRange = getCompareRange(compareMode);
  if (cmpYm) {
    const cr = getMonthData(D.rppByMonth, cmpYm, adCmpRange);
    const ct = getMonthData(D.tdaByMonth, cmpYm, adCmpRange);
    const ca = getMonthData(D.adByMonth, cmpYm, adCmpRange);
    const cc = getMonthData(D.cpaByMonth || {}, cmpYm, adCmpRange);
    cTotalSpend = sumField(cr,'spend') + sumField(ct,'spend') + sumField(ca,'spend') + sumField(cc,'spend');
    cTotalSales = sumField(cr,'sales') + sumField(ct,'sales') + sumField(ca,'sales') + sumField(cc,'sales');
  }

  // TACOS = 広告費 / 全体売上
  const storeSalesData = getMonthData(D.allByMonth, adMonth);
  const storeTotalSales = sumField(storeSalesData, 'sales');
  const tacos = storeTotalSales > 0 ? (totalSpend / storeTotalSales * 100) : 0;

  document.getElementById('adCards').innerHTML = [
    { label: '広告費合計', value: yen(totalSpend), change: calcChange(totalSpend, cTotalSpend) },
    { label: '広告経由売上', value: yen(totalSales), change: calcChange(totalSales, cTotalSales) },
    { label: '全体ROAS', value: totalRoas.toFixed(0) + '%' },
    { label: 'TACOS', value: tacos.toFixed(1) + '%', sub: '広告費/売上' },
  ].map(c =>
    '<div class="metric-card"><div class="metric-label">' + c.label + '</div><div class="metric-value">' + c.value + '</div>' +
    (c.sub ? '<div class="metric-sub">' + c.sub + '</div>' : '') +
    (c.change !== undefined && c.change !== null ? changeHtml(c.change) : '') + '</div>'
  ).join('');

  // RPP KPIs - 全体/商品入札(全体−KW)/KW入札 の3分解
  const rppKwSpendTotal = sumField(rppKwData, 'spend');
  const rppKwSalesTotal = sumField(rppKwData, 'sales');
  const rppKwClicksTotal = sumField(rppKwData, 'clicks');
  const rppKwOrdersTotal = sumField(rppKwData, 'orders');
  const rppItemOnlySpend = rppSpend - rppKwSpendTotal;
  const rppItemOnlySales = rppSales - rppKwSalesTotal;
  const rppItemOnlyClicks = rppClicks - rppKwClicksTotal;
  const rppItemOnlyOrders = rppOrders - rppKwOrdersTotal;

  const rppCvr = rppClicks > 0 ? (rppOrders / rppClicks * 100) : 0;
  const rppRoas = rppSpend > 0 ? (rppSales / rppSpend * 100) : 0;
  const rppCpc = rppClicks > 0 ? rppSpend / rppClicks : 0;

  const makeRow = (label, spend, sales, clicks, orders, ratio) => {
    const roas = spend > 0 ? (sales / spend * 100).toFixed(0) : 0;
    const cvr = clicks > 0 ? (orders / clicks * 100).toFixed(2) : '0.00';
    const cpc = clicks > 0 ? Math.round(spend / clicks) : 0;
    const ratioStr = ratio !== null ? ratio.toFixed(1) + '%' : '-';
    return '<tr><td style="text-align:left;font-weight:600">' + label + '</td><td>' + yen(spend) + '</td><td>' + ratioStr + '</td><td>' + yen(sales) + '</td><td>' + comma(clicks) + '</td><td>' + comma(orders) + '</td><td>' + yen(cpc) + '</td><td>' + cvr + '%</td><td>' + roas + '%</td></tr>';
  };
  const itemRatio = rppSpend > 0 ? (rppItemOnlySpend / rppSpend * 100) : 0;
  const kwRatio = rppSpend > 0 ? (rppKwSpendTotal / rppSpend * 100) : 0;
  document.getElementById('rppKpiRow').innerHTML =
    '<div class="table-wrap"><table style="table-layout:fixed;width:100%"><colgroup><col style="width:9%"><col style="width:13%"><col style="width:8%"><col style="width:15%"><col style="width:11%"><col style="width:9%"><col style="width:9%"><col style="width:9%"><col style="width:12%"></colgroup><thead><tr><th style="text-align:left">区分</th><th>費用</th><th>費用比率</th><th>売上</th><th>クリック</th><th>件数</th><th>CPC</th><th>CVR</th><th>ROAS</th></tr></thead><tbody>' +
    makeRow('全体', rppSpend, rppSales, rppClicks, rppOrders, null) +
    makeRow('商品入札', rppItemOnlySpend, rppItemOnlySales, rppItemOnlyClicks, rppItemOnlyOrders, itemRatio) +
    makeRow('KW入札', rppKwSpendTotal, rppKwSalesTotal, rppKwClicksTotal, rppKwOrdersTotal, kwRatio) +
    '</tbody></table></div>';

  // RPP Item table
  const rppItemAgg = {};
  rppItemData.forEach(r => {
    const k = r.name;
    if (!rppItemAgg[k]) rppItemAgg[k] = { name: k, spend: 0, sales: 0, clicks: 0, orders: 0 };
    rppItemAgg[k].spend += r.spend; rppItemAgg[k].sales += r.sales;
    rppItemAgg[k].clicks += r.clicks; rppItemAgg[k].orders += r.orders;
  });
  const rppItems = Object.values(rppItemAgg).map(r => ({
    ...r,
    roas: r.spend > 0 ? Math.round(r.sales / r.spend * 100) : 0,
    cvr: r.clicks > 0 ? Math.round(r.orders / r.clicks * 10000) / 100 : 0,
    cpc: r.clicks > 0 ? Math.round(r.spend / r.clicks) : 0,
  })).sort((a, b) => b.sales - a.sales);
  buildTable('rppItemTableWrap', [
    { key: 'name', label: '商品名', fmt: v => safe(String(v).substring(0, 50)) },
    { key: 'spend', label: '費用', fmt: v => yen(v) },
    { key: 'sales', label: '売上', fmt: v => yen(v) },
    { key: 'clicks', label: 'クリック', fmt: v => comma(v) },
    { key: 'cpc', label: 'CPC', fmt: v => yen(v) },
    { key: 'roas', label: 'ROAS', fmt: v => v + '%' },
    { key: 'cvr', label: 'CVR', fmt: v => pct(v) },
  ], rppItems, { limit: 50 });

  // RPP KW table - 商品ごとにKWをグループ表示
  const rppKwByProd = {};
  rppKwData.forEach(r => {
    const prod = r.name || r.manageNum || '不明';
    if (!rppKwByProd[prod]) rppKwByProd[prod] = { name: prod, totalSpend: 0, totalSales: 0, totalClicks: 0, totalOrders: 0, kws: {} };
    rppKwByProd[prod].totalSpend += r.spend; rppKwByProd[prod].totalSales += r.sales;
    rppKwByProd[prod].totalClicks += r.clicks; rppKwByProd[prod].totalOrders += r.orders;
    const kw = r.kw || '不明';
    if (!rppKwByProd[prod].kws[kw]) rppKwByProd[prod].kws[kw] = { kw, spend: 0, sales: 0, clicks: 0, orders: 0 };
    rppKwByProd[prod].kws[kw].spend += r.spend; rppKwByProd[prod].kws[kw].sales += r.sales;
    rppKwByProd[prod].kws[kw].clicks += r.clicks; rppKwByProd[prod].kws[kw].orders += r.orders;
  });
  const rppKwRows = [];
  Object.values(rppKwByProd).sort((a, b) => b.totalSpend - a.totalSpend).forEach(prod => {
    // Product header row
    rppKwRows.push({ isHeader: true, name: prod.name, kw: '', spend: prod.totalSpend, sales: prod.totalSales, clicks: prod.totalClicks, orders: prod.totalOrders,
      roas: prod.totalSpend > 0 ? Math.round(prod.totalSales / prod.totalSpend * 100) : 0,
      cvr: prod.totalClicks > 0 ? Math.round(prod.totalOrders / prod.totalClicks * 10000) / 100 : 0,
      cpc: prod.totalClicks > 0 ? Math.round(prod.totalSpend / prod.totalClicks) : 0 });
    // KW rows sorted by spend
    Object.values(prod.kws).sort((a, b) => b.spend - a.spend).forEach(kw => {
      rppKwRows.push({ isHeader: false, name: '', kw: kw.kw, spend: kw.spend, sales: kw.sales, clicks: kw.clicks, orders: kw.orders,
        roas: kw.spend > 0 ? Math.round(kw.sales / kw.spend * 100) : 0,
        cvr: kw.clicks > 0 ? Math.round(kw.orders / kw.clicks * 10000) / 100 : 0,
        cpc: kw.clicks > 0 ? Math.round(kw.spend / kw.clicks) : 0 });
    });
  });
  // KW table with sortable columns
  const kwFlatRows = rppKwRows.map(r => ({
    label: r.isHeader ? r.name : '　' + r.kw,
    spend: r.spend, sales: r.sales, clicks: r.clicks, orders: r.orders,
    cpc: r.cpc, cvr: r.cvr, roas: r.roas, _isHeader: r.isHeader,
  }));
  buildTable('rppKwTableWrap', [
    { key: 'label', label: '商品 / キーワード', fmt: (v, row) => row._isHeader ? '<b>' + safe(String(v).substring(0, 45)) + '</b>' : '<span style="padding-left:16px;color:#555">' + safe(v) + '</span>' },
    { key: 'spend', label: '費用', fmt: v => yen(v) },
    { key: 'sales', label: '売上', fmt: v => yen(v) },
    { key: 'clicks', label: 'クリック', fmt: v => comma(v) },
    { key: 'orders', label: '件数', fmt: v => comma(v) },
    { key: 'cpc', label: 'CPC', fmt: v => yen(v) },
    { key: 'cvr', label: 'CVR', fmt: v => pct(v) },
    { key: 'roas', label: 'ROAS', fmt: v => v + '%' },
  ], kwFlatRows, { limit: 200 });

  // TDA
  const tdaTotalOrders = sumField(tdaData, 'orders');
  const tdaTotalImps = sumField(tdaData, 'imps');
  document.getElementById('tdaKpiRow').innerHTML = [
    { label: '費用', value: yen(tdaSpend) },
    { label: '売上', value: yen(tdaSales) },
    { label: 'ROAS', value: (tdaSpend > 0 ? (tdaSales/tdaSpend*100).toFixed(0) : 0) + '%' },
    { label: 'Vimp', value: comma(tdaTotalImps) },
    { label: 'クリック', value: comma(tdaClicks) },
  ].map(k => '<div class="kpi-item"><div class="kpi-label">' + k.label + '</div><div class="kpi-val">' + k.value + '</div></div>').join('');

  const tdaCampAgg = {};
  tdaData.forEach(r => {
    const k = r.campaign;
    if (!tdaCampAgg[k]) tdaCampAgg[k] = { campaign: k, spend: 0, sales: 0, clicks: 0, imps: 0, orders: 0, newSales: 0, existSales: 0 };
    tdaCampAgg[k].spend += r.spend; tdaCampAgg[k].sales += r.sales;
    tdaCampAgg[k].clicks += r.clicks; tdaCampAgg[k].imps += r.imps;
    tdaCampAgg[k].orders += r.orders; tdaCampAgg[k].newSales += r.newSales;
    tdaCampAgg[k].existSales += r.existSales;
  });
  const tdaCampaigns = Object.values(tdaCampAgg).map(r => ({
    ...r,
    roas: r.spend > 0 ? Math.round(r.sales / r.spend * 100) : 0,
    ctr: r.imps > 0 ? Math.round(r.clicks / r.imps * 10000) / 100 : 0,
  })).sort((a, b) => b.sales - a.sales);
  buildTable('tdaTableWrap', [
    { key: 'campaign', label: 'キャンペーン', fmt: v => safe(String(v).substring(0, 45)) },
    { key: 'spend', label: '費用', fmt: v => yen(v) },
    { key: 'sales', label: '売上', fmt: v => yen(v) },
    { key: 'imps', label: 'Vimp', fmt: v => comma(v) },
    { key: 'clicks', label: 'クリック', fmt: v => comma(v) },
    { key: 'roas', label: 'ROAS', fmt: v => v + '%' },
    { key: 'ctr', label: 'CTR', fmt: v => pct(v) },
  ], tdaCampaigns, { limit: 30 });

  // ad_raw
  const adTotalOrders = sumField(adData, 'orders');
  document.getElementById('adRakutenKpiRow').innerHTML = [
    { label: '広告費', value: yen(adSpend) },
    { label: '売上', value: yen(adSales) },
    { label: 'ROAS', value: (adSpend > 0 ? (adSales/adSpend*100).toFixed(0) : 0) + '%' },
    { label: 'クリック', value: comma(adClicks) },
    { label: '件数', value: comma(adTotalOrders) },
  ].map(k => '<div class="kpi-item"><div class="kpi-label">' + k.label + '</div><div class="kpi-val">' + k.value + '</div></div>').join('');

  const adProdAgg = {};
  adData.forEach(r => {
    const k = r.product;
    if (!adProdAgg[k]) adProdAgg[k] = { product: k, spend: 0, sales: 0, clicks: 0, orders: 0, newCust: 0, newSales: 0 };
    adProdAgg[k].spend += r.spend; adProdAgg[k].sales += r.sales;
    adProdAgg[k].clicks += r.clicks; adProdAgg[k].orders += r.orders;
    adProdAgg[k].newCust += r.newCust; adProdAgg[k].newSales += r.newSales;
  });
  const adProducts = Object.values(adProdAgg).map(r => ({
    ...r,
    roas: r.spend > 0 ? Math.round(r.sales / r.spend * 100) : 0,
  })).sort((a, b) => b.sales - a.sales);
  buildTable('adRakutenTableWrap', [
    { key: 'product', label: '広告商品名', fmt: v => safe(String(v).substring(0, 45)) },
    { key: 'spend', label: '広告費', fmt: v => yen(v) },
    { key: 'sales', label: '売上', fmt: v => yen(v) },
    { key: 'clicks', label: 'クリック', fmt: v => comma(v) },
    { key: 'roas', label: 'ROAS', fmt: v => v + '%' },
    { key: 'newCust', label: '新規獲得', fmt: v => comma(v) },
  ], adProducts, { limit: 30 });

  // CPA広告
  document.getElementById('cpaKpiRow').innerHTML = [
    { label: '請求額', value: yen(cpaSpend) },
    { label: '経由売上', value: yen(cpaSales) },
    { label: 'ROAS', value: (cpaSpend > 0 ? (cpaSales/cpaSpend*100).toFixed(0) : 0) + '%' },
  ].map(k => '<div class="kpi-item"><div class="kpi-label">' + k.label + '</div><div class="kpi-val">' + k.value + '</div></div>').join('');

  // アフィリエイト
  const afiData = getMonthData(D.afiByMonth, adMonth);
  const afiTotalSales = sumField(afiData, 'sales');
  const afiTotalReward = sumField(afiData, 'reward');
  document.getElementById('afiKpiRow').innerHTML = [
    { label: '成果件数', value: comma(afiData.length) },
    { label: '売上金額', value: yen(afiTotalSales) },
    { label: '成果報酬', value: yen(afiTotalReward) },
    { label: '報酬率', value: afiTotalSales > 0 ? pct(afiTotalReward / afiTotalSales * 100) : '0%' },
  ].map(k => '<div class="kpi-item"><div class="kpi-label">' + k.label + '</div><div class="kpi-val">' + k.value + '</div></div>').join('');

  // Affiliate by product
  const afiAgg = {};
  afiData.forEach(r => {
    const k = r.product || r.manageNum || '不明';
    if (!afiAgg[k]) afiAgg[k] = { product: k, sales: 0, reward: 0, count: 0 };
    afiAgg[k].sales += r.sales; afiAgg[k].reward += r.reward; afiAgg[k].count += 1;
  });
  const afiProducts = Object.values(afiAgg).sort((a, b) => b.sales - a.sales);
  buildTable('afiByProductTableWrap', [
    { key: 'product', label: '商品名', fmt: v => safe(String(v).substring(0, 45)) },
    { key: 'count', label: '件数', fmt: v => comma(v) },
    { key: 'sales', label: '売上', fmt: v => yen(v) },
    { key: 'reward', label: '報酬', fmt: v => yen(v) },
  ], afiProducts, { limit: 30 });

  // Affiliate by rate with ratio
  const afiRateData = (D.afiByRateRows || []).slice();
  const afiRateTotalSales = afiRateData.reduce((s, r) => s + (r.sales || 0), 0);
  const afiRateWithRatio = afiRateData.map(r => ({ ...r, salesRatio: afiRateTotalSales > 0 ? (r.sales / afiRateTotalSales * 100) : 0 }));
  buildTable('afiByRateTableWrap', [
    { key: 'rate', label: '料率', fmt: v => safe(v) },
    { key: 'count', label: '件数', fmt: v => comma(v) },
    { key: 'sales', label: '売上', fmt: v => yen(v) },
    { key: 'salesRatio', label: '売上構成比', fmt: v => v.toFixed(1) + '%' },
    { key: 'reward', label: '報酬', fmt: v => yen(v) },
  ], afiRateWithRatio, { limit: 20 });

  // Affiliate rate pie chart
  destroyChart('chartAfiRatePie');
  const pieColors = ['#1a3a5c','#e8734a','#f5b041','#2ecc71','#3498db','#9b59b6','#e74c3c','#1abc9c','#34495e','#f39c12'];
  if (afiRateWithRatio.length > 0) {
    chartInstances['chartAfiRatePie'] = new Chart(document.getElementById('chartAfiRatePie'), {
      type: 'doughnut',
      data: {
        labels: afiRateWithRatio.map(r => r.rate),
        datasets: [{ data: afiRateWithRatio.map(r => r.sales), backgroundColor: afiRateWithRatio.map((_, i) => pieColors[i % pieColors.length]) }]
      },
      options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'right', labels: { boxWidth: 12, font: { size: 11 } } }, tooltip: { callbacks: { label: function(ctx) { return ctx.label + ': ' + yen(ctx.raw) + ' (' + (afiRateTotalSales > 0 ? (ctx.raw / afiRateTotalSales * 100).toFixed(1) : 0) + '%)'; } } } } }
    });
  }
}

function renderAcqTab() {
  const crmMonth = document.getElementById('monthFilter').value || 'all';
  const lineData = getMonthData(D.lineByMonth, crmMonth);

  // LINE KPIs
  const lineTotalSent = sumField(lineData, 'sent');
  const lineTotalSales = sumField(lineData, 'sales');
  const lineTotalConversions = sumField(lineData, 'conversions');
  const lineTotalVisitors = sumField(lineData, 'visitors');
  const lineTotalOpened = sumField(lineData, 'opened');
  const lineAvgOpenRate = lineTotalSent > 0 ? (lineTotalOpened / lineTotalSent * 100) : 0;
  const lineAvgCvr = lineTotalVisitors > 0 ? (lineTotalConversions / lineTotalVisitors * 100) : 0;
  const lineSalesPerSend = lineTotalSent > 0 ? lineTotalSales / lineTotalSent : 0;
  document.getElementById('lineKpiRow').innerHTML = [
    { label: '配信数', value: comma(lineData.length) + '回' },
    { label: '配信通数', value: comma(lineTotalSent) },
    { label: '平均開封率', value: pct1(lineAvgOpenRate) },
    { label: '訪問者数', value: comma(lineTotalVisitors) },
    { label: '転換率', value: pct(lineAvgCvr) },
    { label: '売上合計', value: yen(lineTotalSales) },
    { label: '売上/通', value: yen(Math.round(lineSalesPerSend * 10) / 10) },
  ].map(k => '<div class="kpi-item"><div class="kpi-label">' + k.label + '</div><div class="kpi-val">' + k.value + '</div></div>').join('');

  // LINE trend chart
  destroyChart('chartLineTrend');
  const lineSorted = [...lineData].sort((a, b) => String(a.date).localeCompare(String(b.date)));
  if (lineSorted.length > 0) {
    chartInstances['chartLineTrend'] = new Chart(document.getElementById('chartLineTrend'), {
      type: 'bar',
      data: {
        labels: lineSorted.map(r => { const d = String(r.date).substring(0, 10); return d; }),
        datasets: [
          { label: '売上', data: lineSorted.map(r => r.sales), backgroundColor: 'rgba(26,58,92,0.6)', yAxisID: 'y', order: 1 },
          { label: '訪問者', data: lineSorted.map(r => r.visitors), type: 'line', borderColor: '#06b6d4', yAxisID: 'y1', tension: 0.3, pointRadius: 2, order: 0 },
        ]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { position: 'top' }, title: { display: true, text: '配信別 売上・訪問者推移' } },
        scales: {
          y: { position: 'left', ticks: { callback: v => v >= 10000 ? (v/10000).toFixed(0) + '万' : comma(v) } },
          y1: { position: 'right', grid: { drawOnChartArea: false } }
        }
      }
    });
  }

  // LINE performance chart (open rate vs cvr)
  destroyChart('chartLinePerf');
  if (lineSorted.length > 0) {
    chartInstances['chartLinePerf'] = new Chart(document.getElementById('chartLinePerf'), {
      type: 'scatter',
      data: {
        datasets: [{
          label: '開封率 vs 転換率',
          data: lineSorted.map(r => ({
            x: r.sent > 0 ? (r.opened / r.sent * 100) : 0,
            y: r.visitors > 0 ? (r.conversions / r.visitors * 100) : 0,
            title: r.title,
          })),
          backgroundColor: 'rgba(26,58,92,0.6)',
          pointRadius: 6, pointHoverRadius: 8,
        }]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          title: { display: true, text: '開封率 vs 転換率' },
          tooltip: { callbacks: { label: ctx => (ctx.raw.title || '') + ' 開封:' + ctx.raw.x.toFixed(1) + '% CVR:' + ctx.raw.y.toFixed(1) + '%' } }
        },
        scales: {
          x: { title: { display: true, text: '開封率(%)' } },
          y: { title: { display: true, text: '転換率(%)' }, beginAtZero: true }
        }
      }
    });
  }

  // LINE message table
  const lineForTable = [...lineData].sort((a, b) => String(b.date).localeCompare(String(a.date)));
  buildTable('lineTableWrap', [
    { key: 'date', label: '配信日時', fmt: v => safe(String(v).substring(0, 16)) },
    { key: 'title', label: 'タイトル', fmt: v => safe(String(v).substring(0, 35)) },
    { key: 'sent', label: '配信通数', fmt: v => comma(v) },
    { key: 'openRate', label: '開封率', fmt: v => safe(v) },
    { key: 'visitors', label: '訪問者', fmt: v => comma(v) },
    { key: 'conversions', label: '転換数', fmt: v => comma(v) },
    { key: 'sales', label: '売上', fmt: v => yen(v) },
    { key: 'salesPerSend', label: '売上/通', fmt: v => yen(Math.round(v * 10) / 10) },
  ], lineForTable, { limit: 50 });

  // Mail KPIs + table - 月フィルター適用
  const allMailData = D.mailParsed || [];
  const mailFiltered = allMailData.filter(r => r.date === crmMonth);
  const mailData = mailFiltered.length > 0 ? mailFiltered : allMailData;
  const mailTotalSent = sumField(mailData, 'sent');
  const mailTotalOpened = sumField(mailData, 'opened');
  const mailTotalClicks = sumField(mailData, 'clicks');
  const mailTotalSales = sumField(mailData, 'sales');
  const mailTotalOrders = sumField(mailData, 'orders');
  document.getElementById('mailKpiRow').innerHTML = [
    { label: '配信数', value: comma(mailTotalSent) },
    { label: '開封数', value: comma(mailTotalOpened) },
    { label: '開封率', value: mailTotalSent > 0 ? pct1(mailTotalOpened / mailTotalSent * 100) : '-' },
    { label: 'クリック数', value: comma(mailTotalClicks) },
    { label: '売上', value: yen(mailTotalSales) },
    { label: '転換数', value: comma(mailTotalOrders) },
  ].map(k => '<div class="kpi-item"><div class="kpi-label">' + k.label + '</div><div class="kpi-val">' + k.value + '</div></div>').join('');

  const mailSorted = [...mailData].sort((a, b) => String(b.date).localeCompare(String(a.date)));
  buildTable('mailTableWrap', [
    { key: 'date', label: '年月', fmt: v => { const ym = String(v); return D.monthLabels[ym] || ym; } },
    { key: 'sent', label: '配信数', fmt: v => comma(v) },
    { key: 'opened', label: '開封数', fmt: v => comma(v) },
    { key: 'openRate', label: '開封率', fmt: v => safe(v) },
    { key: 'clicks', label: 'クリック', fmt: v => comma(v) },
    { key: 'sales', label: '売上', fmt: v => yen(v) },
    { key: 'orders', label: '転換数', fmt: v => comma(v) },
  ], mailSorted, { limit: 50 });
}

function renderRepeatTab() {
  if (!D.hasOrders) {
    document.getElementById('repeatCards').innerHTML = '<div class="no-data">受注データなし（order_rawの読み取りに失敗した可能性があります）</div>';
    return;
  }

  // Product filter
  const rpSel = document.getElementById('repeatProductFilter');
  if (rpSel.options.length <= 1 && D.orderItems) {
    const items = new Set();
    D.orderItems.forEach(r => { if (r.i) items.add(r.i); });
    [...items].sort().forEach(p => {
      const opt = document.createElement('option');
      opt.value = p; opt.textContent = p;
      rpSel.appendChild(opt);
    });
  }
  const filterItem = rpSel ? rpSel.value : '';

  // Filter orders by product if selected
  const oi = D.orderItems || [];
  const filtered = filterItem ? oi.filter(r => r.i === filterItem) : oi;

  // Compute repeat analysis from filtered data
  const custOrders = {};
  filtered.forEach(r => {
    if (!r.e) return;
    if (!custOrders[r.e]) custOrders[r.e] = [];
    custOrders[r.e].push(r);
  });
  const custFirst = {};
  Object.entries(custOrders).forEach(([email, ords]) => {
    const sorted = [...ords].sort((a, b) => String(a.d).localeCompare(String(b.d)));
    custFirst[email] = sorted[0]?.d || '';
  });
  const custCounts = {};
  Object.entries(custOrders).forEach(([email, ords]) => {
    const uniqueNums = [...new Set(ords.map(o => o.n).filter(Boolean))];
    custCounts[email] = uniqueNums.length || 1;
  });

  const totalCust = Object.keys(custCounts).length;
  const firstTimersCnt = Object.values(custCounts).filter(c => c === 1).length;
  const repeatersCnt = totalCust - firstTimersCnt;
  const f2RateVal = totalCust > 0 ? (repeatersCnt / totalCust * 100) : 0;

  document.getElementById('repeatCards').innerHTML = [
    { label: '総顧客数', value: comma(totalCust) },
    { label: 'F1（初回購入）', value: comma(totalCust) },
    { label: 'F2（2回目購入）', value: comma(repeatersCnt) },
    { label: 'F2転換率', value: pct(f2RateVal), sub: 'F2÷F1' },
  ].map(c => '<div class="metric-card"><div class="metric-label">' + c.label + '</div><div class="metric-value">' + c.value + '</div>' + (c.sub ? '<div style="font-size:11px;color:#888;margin-top:2px">' + c.sub + '</div>' : '') + '</div>').join('');

  // Monthly NR - order_rawから算出
  const toYmClient = d => { if (!d) return ''; const s = String(d).replace(/\\//g, '-'); const m2 = s.match(/^(\\d{4})-(\\d{1,2})/); return m2 ? m2[1] + '-' + m2[2].padStart(2, '0') : ''; };
  // 全order_rawから顧客の初回購入月を算出（フィルタ前のデータで計算）
  const allCustFirst = {};
  (D.orderItems || []).forEach(r => {
    if (!r.e || !r.d) return;
    if (!allCustFirst[r.e] || r.d < allCustFirst[r.e]) allCustFirst[r.e] = r.d;
  });
  const monthlyNR = {};
  filtered.forEach(r => {
    if (!r.e || !r.d) return;
    const ym = toYmClient(r.d);
    if (!ym) return;
    if (!monthlyNR[ym]) monthlyNR[ym] = { newC: new Set(), repC: new Set() };
    const firstYM = toYmClient(allCustFirst[r.e]);
    if (firstYM === ym) monthlyNR[ym].newC.add(r.e);
    else monthlyNR[ym].repC.add(r.e);
  });
  const nr = Object.entries(monthlyNR).map(([m, v]) => ({ month: m, newCust: v.newC.size, repeatCust: v.repC.size })).sort((a, b) => a.month.localeCompare(b.month));

  destroyChart('chartMonthlyNR');
  destroyChart('chartRepeatRateTrend');
  if (nr.length > 0) {
    const nrLabels = nr.map(r => D.monthLabels[r.month] || r.month);
    const nrTotals = nr.map(r => r.newCust + r.repeatCust);
    const nrRepeatRates = nr.map(r => { const t = r.newCust + r.repeatCust; return t > 0 ? Math.round(r.repeatCust / t * 1000) / 10 : 0; });

    chartInstances['chartMonthlyNR'] = new Chart(document.getElementById('chartMonthlyNR'), {
      type: 'bar',
      data: {
        labels: nrLabels,
        datasets: [
          { label: '新規', data: nr.map(r => r.newCust), backgroundColor: '#1a3a5c', stack: 'a', yAxisID: 'y' },
          { label: 'リピート', data: nr.map(r => r.repeatCust), backgroundColor: '#FF9800', stack: 'a', yAxisID: 'y' },
          { label: 'リピート率', data: nrRepeatRates, type: 'line', borderColor: '#e53935', backgroundColor: 'transparent', pointRadius: 4, pointBackgroundColor: '#e53935', tension: 0.3, yAxisID: 'y1' },
        ]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { position: 'top' },
          tooltip: { callbacks: { label: ctx => { if (ctx.dataset.label === 'リピート率') return 'リピート率: ' + ctx.raw + '%'; return ctx.dataset.label + ': ' + comma(ctx.raw) + '人'; } } }
        },
        scales: {
          x: { stacked: true },
          y: { stacked: true, title: { display: true, text: '顧客数' } },
          y1: { position: 'right', min: 0, max: 100, grid: { drawOnChartArea: false }, title: { display: true, text: 'リピート率(%)' }, ticks: { callback: v => v + '%' } }
        }
      }
    });

    // 月別テーブル（ソート対応）
    const nrTableRows = nr.map((r, i) => {
      const total = r.newCust + r.repeatCust;
      const rate = total > 0 ? Math.round(r.repeatCust / total * 1000) / 10 : 0;
      return { month: nrLabels[i], monthSort: r.month, newCust: r.newCust, repeatCust: r.repeatCust, total, rate };
    });
    buildTable('monthlyNRTable', [
      { key: 'month', label: '月', fmt: v => v },
      { key: 'newCust', label: '新規', fmt: v => comma(v) },
      { key: 'repeatCust', label: 'リピート', fmt: v => comma(v) },
      { key: 'total', label: '合計', fmt: v => comma(v) },
      { key: 'rate', label: 'リピート率', fmt: v => '<span style="font-weight:600">' + v.toFixed(1) + '%</span>' },
    ], nrTableRows);

    // リピート率推移折れ線チャート
    chartInstances['chartRepeatRateTrend'] = new Chart(document.getElementById('chartRepeatRateTrend'), {
      type: 'line',
      data: {
        labels: nrLabels,
        datasets: [{ label: 'リピート率', data: nrRepeatRates, borderColor: '#e53935', backgroundColor: 'rgba(229,57,53,0.1)', fill: true, pointRadius: 4, pointBackgroundColor: '#e53935', tension: 0.3 }]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { display: false }, tooltip: { callbacks: { label: ctx => 'リピート率: ' + ctx.raw + '%' } } },
        scales: { y: { min: 0, max: 100, ticks: { callback: v => v + '%' } } }
      }
    });
  }

  // Purchase distribution
  const purchaseDist = {};
  Object.values(custCounts).forEach(cnt => {
    const bucket = cnt >= 10 ? '10+' : String(cnt);
    purchaseDist[bucket] = (purchaseDist[bucket] || 0) + 1;
  });
  const purchaseDistRows = Object.entries(purchaseDist)
    .sort((a, b) => (a[0] === '10+' ? 999 : Number(a[0])) - (b[0] === '10+' ? 999 : Number(b[0])))
    .map(([cnt, customers]) => ({ cnt, customers }));

  destroyChart('chartPurchaseDist');
  if (purchaseDistRows.length > 0) {
    chartInstances['chartPurchaseDist'] = new Chart(document.getElementById('chartPurchaseDist'), {
      type: 'bar',
      data: {
        labels: purchaseDistRows.map(r => r.cnt + '回'),
        datasets: [{ label: '顧客数', data: purchaseDistRows.map(r => r.customers), backgroundColor: '#1a73e8' }]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: { y: { beginAtZero: true } }
      }
    });
  }

  // Repeat item ranking (use precomputed for all, recompute for filtered)
  if (!filterItem) {
    buildTable('repeatItemTableWrap', [
      { key: 'item', label: '商品', fmt: v => safe(String(v).substring(0, 40)) },
      { key: 'totalBuyers', label: '購入者数', fmt: v => comma(v) },
      { key: 'repeatBuyers', label: 'リピーター数', fmt: v => comma(v) },
      { key: 'repeatRate', label: 'リピート率', fmt: v => pct1(v) },
      { key: 'totalPurchases', label: '総購入回数', fmt: v => comma(v) },
    ], D.repeatAnalysis.repeatItemRows || [], { limit: 30 });

    buildTable('entryItemF2TableWrap', [
      { key: 'item', label: '入口商品', fmt: v => safe(String(v).substring(0, 40)) },
      { key: 'count', label: '初回購入者数', fmt: v => comma(v) },
      { key: 'repeatCount', label: 'F2転換数', fmt: v => comma(v) },
      { key: 'f2Rate', label: 'F2転換率', fmt: v => '<span class="badge ' + (v >= 20 ? 'badge-success' : v >= 10 ? 'badge-neutral' : 'badge-danger') + '">' + pct1(v) + '</span>' },
      { key: 'avgLTV', label: '平均LTV', fmt: v => yen(v) },
    ], D.repeatAnalysis.entryItemF2Rows || [], { limit: 30 });
  } else {
    // For filtered product: show which other products this product's buyers also buy
    const buyerEmails = Object.keys(custOrders);
    const otherItems = {};
    D.orderItems.forEach(r => {
      if (!buyerEmails.includes(r.e) || r.i === filterItem || !r.i) return;
      if (!otherItems[r.i]) otherItems[r.i] = { item: r.i, count: 0, buyers: new Set() };
      otherItems[r.i].count++;
      otherItems[r.i].buyers.add(r.e);
    });
    const otherRows = Object.values(otherItems).map(r => ({ item: r.item, buyers: r.buyers.size, count: r.count })).sort((a, b) => b.buyers - a.buyers).slice(0, 30);
    buildTable('repeatItemTableWrap', [
      { key: 'item', label: '同一顧客が購入した他商品', fmt: v => safe(String(v).substring(0, 40)) },
      { key: 'buyers', label: '重複顧客数', fmt: v => comma(v) },
      { key: 'count', label: '購入回数', fmt: v => comma(v) },
    ], otherRows, { limit: 30 });

    document.getElementById('entryItemF2TableWrap').innerHTML = '';
  }
}

function renderBasketTab() {
  if (!D.hasOrders) {
    document.getElementById('basketCards').innerHTML = '<div class="no-data">受注データなし</div>';
    return;
  }
  const ba = D.basketAnalysis;
  document.getElementById('basketCards').innerHTML = [
    { label: '総注文数', value: comma(ba.totalOrders) },
    { label: '平均購入点数/注文', value: ba.avgUnitsPerOrder.toFixed(1) },
    { label: '平均注文単価', value: yen(ba.avgOrderPrice) },
    { label: 'クロスセル率', value: pct(ba.crossSellRate) },
  ].map(c => '<div class="metric-card"><div class="metric-label">' + c.label + '</div><div class="metric-value">' + c.value + '</div></div>').join('');

  // Units distribution chart
  destroyChart('chartUnitsDist');
  if (ba.unitsDistRows.length > 0) {
    chartInstances['chartUnitsDist'] = new Chart(document.getElementById('chartUnitsDist'), {
      type: 'bar',
      data: {
        labels: ba.unitsDistRows.map(r => r.units + '点'),
        datasets: [{ label: '注文数', data: ba.unitsDistRows.map(r => r.count), backgroundColor: '#7b1fa2' }]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: { y: { beginAtZero: true } }
      }
    });
  }

  // Pairs table
  buildTable('basketPairsTable', [
    { key: 'a', label: '商品A', fmt: v => safe(String(v).substring(0, 30)) },
    { key: 'b', label: '商品B', fmt: v => safe(String(v).substring(0, 30)) },
    { key: 'count', label: '回数', fmt: v => comma(v) },
  ], ba.topPairs, { limit: 20 });

  // Product selector for co-purchase
  const sel = document.getElementById('basketProductSelect');
  if (sel.options.length <= 1 && ba.basketProducts) {
    ba.basketProducts.forEach(p => {
      const opt = document.createElement('option');
      opt.value = p; opt.textContent = p;
      sel.appendChild(opt);
    });
  }
  renderCoProducts();
}

function renderLTVTab() {
  if (!D.hasOrders) {
    document.getElementById('cust-ltv').querySelectorAll('.section-box').forEach(el => {
      el.innerHTML = '<div class="no-data">受注データなし</div>';
    });
    return;
  }

  // Product filter
  const ltvSel = document.getElementById('ltvProductFilter');
  if (ltvSel.options.length <= 1 && D.orderItems) {
    const items = new Set();
    D.orderItems.forEach(r => { if (r.i) items.add(r.i); });
    [...items].sort().forEach(p => {
      const opt = document.createElement('option');
      opt.value = p; opt.textContent = p;
      ltvSel.appendChild(opt);
    });
  }
  const filterItem = ltvSel ? ltvSel.value : '';

  const oi = D.orderItems || [];
  const toYmClient = d => { if (!d) return ''; const s = String(d).replace(/\\//g, '-'); const m = s.match(/^(\\d{4})-(\\d{1,2})/); return m ? m[1] + '-' + m[2].padStart(2, '0') : ''; };

  // If filtered, recompute LTV data for customers who bought that product
  let countPriceData, firstItemData;

  if (filterItem) {
    // Find customers who bought this item
    const targetBuyers = new Set();
    oi.forEach(r => { if (r.i === filterItem && r.e) targetBuyers.add(r.e); });

    // Build per-customer data from ALL their orders
    const custOrd = {};
    oi.forEach(r => {
      if (!targetBuyers.has(r.e)) return;
      if (!custOrd[r.e]) custOrd[r.e] = [];
      custOrd[r.e].push(r);
    });

    // Purchase count price
    const cntPrice = {};
    Object.entries(custOrd).forEach(([email, ords]) => {
      const uniqueOrders = [...new Set(ords.map(o => o.n).filter(Boolean))].length || 1;
      const bucket = uniqueOrders >= 10 ? '10+' : String(uniqueOrders);
      const totalSpend = ords.reduce((s, o) => s + (o.p || 0), 0);
      if (!cntPrice[bucket]) cntPrice[bucket] = { total: 0, count: 0 };
      cntPrice[bucket].total += totalSpend;
      cntPrice[bucket].count++;
    });
    countPriceData = Object.entries(cntPrice)
      .sort((a, b) => (a[0] === '10+' ? 999 : Number(a[0])) - (b[0] === '10+' ? 999 : Number(b[0])))
      .map(([cnt, v]) => ({ cnt, avgPrice: v.count > 0 ? Math.round(v.total / v.count) : 0, customers: v.count }));

    // First item LTV (for this item's buyers, what was their first item)
    const firstItem = {};
    Object.entries(custOrd).forEach(([email, ords]) => {
      const sorted = [...ords].sort((a, b) => String(a.d).localeCompare(String(b.d)));
      const fi = sorted[0]?.i || '不明';
      const totalSpend = sorted.reduce((s, o) => s + (o.p || 0), 0);
      const uniqueOrders = [...new Set(sorted.map(o => o.n).filter(Boolean))].length || 1;
      if (!firstItem[fi]) firstItem[fi] = { item: fi, totalLTV: 0, count: 0, repeatCount: 0 };
      firstItem[fi].totalLTV += totalSpend;
      firstItem[fi].count++;
      if (uniqueOrders >= 2) firstItem[fi].repeatCount++;
    });
    firstItemData = Object.values(firstItem).map(r => ({
      item: r.item, count: r.count,
      avgLTV: r.count > 0 ? Math.round(r.totalLTV / r.count) : 0,
      f2Rate: r.count > 0 ? Math.round(r.repeatCount / r.count * 1000) / 10 : 0,
    })).sort((a, b) => b.avgLTV - a.avgLTV).slice(0, 30);
  } else {
    countPriceData = D.ltvAnalysis.purchaseCountPrice;
    firstItemData = D.ltvAnalysis.firstItemLTV;
  }

  // Purchase count price table
  buildTable('ltvCountPriceTable', [
    { key: 'cnt', label: '購入回数', fmt: v => v + '回' },
    { key: 'customers', label: '顧客数', fmt: v => comma(v) },
    { key: 'avgPrice', label: '平均累計額', fmt: v => yen(v) },
  ], countPriceData);

  // First item LTV table
  buildTable('ltvFirstItemTable', [
    { key: 'item', label: '初回購入商品', fmt: v => safe(String(v).substring(0, 40)) },
    { key: 'count', label: '顧客数', fmt: v => comma(v) },
    { key: 'avgLTV', label: '平均LTV', fmt: v => yen(v) },
    { key: 'f2Rate', label: 'F2率', fmt: v => pct1(v) },
  ], firstItemData, { limit: 30 });
}

function renderCoProducts() {
  const sel = document.getElementById('basketProductSelect');
  const selected = sel ? sel.value : '';
  if (!selected) {
    document.getElementById('coProductTableWrap').innerHTML = '<div class="no-data">商品を選択してください</div>';
    return;
  }
  const coData = (D.basketAnalysis.coProductData || {})[selected] || [];
  if (coData.length === 0) {
    document.getElementById('coProductTableWrap').innerHTML = '<div class="no-data">同時購入データなし</div>';
    return;
  }
  buildTable('coProductTableWrap', [
    { key: 'item', label: '同時購入商品', fmt: v => safe(String(v).substring(0, 45)) },
    { key: 'count', label: '同時購入回数', fmt: v => comma(v) },
  ], coData, { limit: 20 });
}

// ── RFM分析 ──
function renderRFMTab() {
  if (!D.hasOrders || !D.orderItems || D.orderItems.length === 0) {
    document.getElementById('rfmCards').innerHTML = '<div class="no-data">受注データなし</div>';
    return;
  }

  const now = new Date();
  const oi = D.orderItems;

  // 顧客ごとにR/F/M算出
  const custData = {};
  oi.forEach(r => {
    if (!r.e) return;
    if (!custData[r.e]) custData[r.e] = { orders: new Set(), totalSpend: 0, lastDate: '' };
    if (r.n) custData[r.e].orders.add(r.n);
    custData[r.e].totalSpend += (r.p || 0);
    const d = String(r.d || '').replace(/\\//g, '-').substring(0, 10);
    if (d > custData[r.e].lastDate) custData[r.e].lastDate = d;
  });

  const customers = Object.entries(custData).map(([email, d]) => {
    const lastD = d.lastDate ? new Date(d.lastDate + 'T00:00:00') : now;
    const recencyDays = Math.max(0, Math.floor((now - lastD) / 86400000));
    return { email, recency: recencyDays, frequency: d.orders.size || 1, monetary: d.totalSpend };
  });

  if (customers.length === 0) {
    document.getElementById('rfmCards').innerHTML = '<div class="no-data">顧客データなし</div>';
    return;
  }

  // 5段階スコアリング（均等分割）
  function assignScore(arr, key, reverse) {
    const sorted = [...arr].sort((a, b) => reverse ? b[key] - a[key] : a[key] - b[key]);
    const n = sorted.length;
    sorted.forEach((item, i) => {
      item[key + 'Score'] = Math.min(5, Math.floor(i / n * 5) + 1);
    });
  }
  assignScore(customers, 'recency', true);  // 最近ほど高スコア
  assignScore(customers, 'frequency', false); // 頻度高いほど高スコア
  assignScore(customers, 'monetary', false);  // 金額高いほど高スコア

  // セグメント分類
  function getSegment(r, f, m) {
    const avg = (r + f + m) / 3;
    if (r >= 4 && f >= 4 && m >= 4) return '優良顧客';
    if (r >= 4 && f >= 3) return 'ロイヤル候補';
    if (r >= 4 && f <= 2) return '新規顧客';
    if (r <= 2 && f >= 3) return '離反リスク';
    if (r <= 2 && f <= 2 && m >= 3) return '休眠（高額）';
    if (r <= 2) return '離反・休眠';
    if (avg >= 3.5) return '安定顧客';
    return '育成対象';
  }

  customers.forEach(c => {
    c.segment = getSegment(c.recencyScore, c.frequencyScore, c.monetaryScore);
    c.rfmScore = c.recencyScore + c.frequencyScore + c.monetaryScore;
  });

  // カード
  const segments = {};
  customers.forEach(c => { segments[c.segment] = (segments[c.segment] || 0) + 1; });
  const topSegment = Object.entries(segments).sort((a, b) => b[1] - a[1])[0];
  const avgRFM = customers.reduce((s, c) => s + c.rfmScore, 0) / customers.length;

  document.getElementById('rfmCards').innerHTML = [
    { label: '分析対象顧客数', value: comma(customers.length) },
    { label: '平均RFMスコア', value: avgRFM.toFixed(1) + ' / 15' },
    { label: '優良顧客数', value: comma(segments['優良顧客'] || 0) },
    { label: '離反リスク', value: comma((segments['離反リスク'] || 0) + (segments['離反・休眠'] || 0)) },
    { label: '最多セグメント', value: topSegment ? topSegment[0] : '-' },
  ].map(c => '<div class="metric-card"><div class="metric-label">' + c.label + '</div><div class="metric-value">' + c.value + '</div></div>').join('');

  // セグメント分布テーブル
  const segOrder = ['優良顧客', 'ロイヤル候補', '安定顧客', '新規顧客', '育成対象', '離反リスク', '休眠（高額）', '離反・休眠'];
  const segColors = { '優良顧客': '#0d904f', 'ロイヤル候補': '#1a73e8', '安定顧客': '#5b9bd5', '新規顧客': '#34a853', '育成対象': '#f9ab00', '離反リスク': '#e53935', '休眠（高額）': '#ff6d00', '離反・休眠': '#999' };
  const segRows = segOrder.filter(s => segments[s]).map(s => {
    const custs = customers.filter(c => c.segment === s);
    const avgM = custs.reduce((sum, c) => sum + c.monetary, 0) / custs.length;
    const avgF = custs.reduce((sum, c) => sum + c.frequency, 0) / custs.length;
    const avgR = custs.reduce((sum, c) => sum + c.recency, 0) / custs.length;
    return { segment: s, count: custs.length, pct: (custs.length / customers.length * 100), avgMonetary: Math.round(avgM), avgFrequency: avgF, avgRecency: Math.round(avgR) };
  });

  buildTable('rfmSegmentTable', [
    { key: 'segment', label: 'セグメント', fmt: (v) => '<span style="color:' + (segColors[v] || '#333') + ';font-weight:600">' + v + '</span>' },
    { key: 'count', label: '顧客数', fmt: v => comma(v) },
    { key: 'pct', label: '構成比', fmt: v => v.toFixed(1) + '%' },
    { key: 'avgRecency', label: '平均R(日)', fmt: v => comma(v) },
    { key: 'avgFrequency', label: '平均F(回)', fmt: v => v.toFixed(1) },
    { key: 'avgMonetary', label: '平均M(円)', fmt: v => yen(v) },
  ], segRows);

  // RFスコアヒートマップ（棒チャートで代用）
  destroyChart('chartRfmHeatmap');
  const segChartData = segRows.map(r => ({ label: r.segment, value: r.count }));
  chartInstances['chartRfmHeatmap'] = new Chart(document.getElementById('chartRfmHeatmap'), {
    type: 'bar',
    data: {
      labels: segChartData.map(r => r.label),
      datasets: [{ label: '顧客数', data: segChartData.map(r => r.value), backgroundColor: segChartData.map(r => segColors[r.label] || '#999') }]
    },
    options: {
      indexAxis: 'y',
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false }, tooltip: { callbacks: { label: ctx => comma(ctx.raw) + '人' } } },
      scales: { x: { beginAtZero: true } }
    }
  });

  // セグメントフィルタ
  const rfmSel = document.getElementById('rfmSegmentFilter');
  if (rfmSel.options.length <= 1) {
    segOrder.filter(s => segments[s]).forEach(s => {
      const opt = document.createElement('option');
      opt.value = s; opt.textContent = s + ' (' + segments[s] + ')';
      rfmSel.appendChild(opt);
    });
  }

  // セグメント別 購入商品比率
  const filterSeg = rfmSel.value;
  const segCustomerEmails = new Set();
  customers.forEach(c => {
    if (!filterSeg || c.segment === filterSeg) segCustomerEmails.add(c.email);
  });

  // セグメント内の商品別集計
  const segProductAgg = {};
  let segTotalAmount = 0;
  D.orderItems.forEach(r => {
    if (!segCustomerEmails.has(r.e)) return;
    const item = r.i || '不明';
    if (!segProductAgg[item]) segProductAgg[item] = { item, count: 0, amount: 0 };
    segProductAgg[item].count += 1;
    segProductAgg[item].amount += (r.p || 0);
    segTotalAmount += (r.p || 0);
  });

  const segProductRows = Object.values(segProductAgg).sort((a, b) => b.amount - a.amount).slice(0, 30);
  const segProductLabel = filterSeg || '全セグメント';

  buildTable('rfmProductTable', [
    { key: 'item', label: '商品', fmt: v => { const name = D.masterProducts[v] || v; return '<span title="' + safe(v) + '">' + safe(name.substring(0, 30)) + '</span>'; } },
    { key: 'count', label: '購入回数', fmt: v => comma(v) },
    { key: 'amount', label: '売上', fmt: v => yen(v) },
    { key: 'pct', label: '売上構成比', fmt: v => v.toFixed(1) + '%' },
  ], segProductRows.map(r => ({ ...r, pct: segTotalAmount > 0 ? (r.amount / segTotalAmount * 100) : 0 })));

  // セグメント別商品円グラフ
  destroyChart('chartSegmentProduct');
  const topN = segProductRows.slice(0, 8);
  const othersAmount = segTotalAmount - topN.reduce((s, r) => s + r.amount, 0);
  const pieLabels = topN.map(r => (D.masterProducts[r.item] || r.item).substring(0, 15));
  const pieData = topN.map(r => r.amount);
  if (othersAmount > 0) { pieLabels.push('その他'); pieData.push(othersAmount); }
  const pieColors = ['#4285f4','#ea4335','#fbbc04','#34a853','#ff6d01','#46bdc6','#7b1fa2','#c2185b','#999'];

  chartInstances['chartSegmentProduct'] = new Chart(document.getElementById('chartSegmentProduct'), {
    type: 'doughnut',
    data: { labels: pieLabels, datasets: [{ data: pieData, backgroundColor: pieColors }] },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: { position: 'right', labels: { boxWidth: 12, font: { size: 11 } } },
        title: { display: true, text: segProductLabel + ' - 商品別売上構成' },
        tooltip: { callbacks: { label: ctx => ctx.label + ': ' + yen(ctx.raw) + ' (' + (ctx.raw / segTotalAmount * 100).toFixed(1) + '%)' } }
      }
    }
  });
}

// ── エントランス分析 ──
function renderEntranceTab() {
  if (!D.hasOrders || !D.orderItems) {
    document.getElementById('entranceCards').innerHTML = '<div class="no-data">受注データなし</div>';
    return;
  }
  const oi = D.orderItems;

  // 顧客ごと注文をソート
  const custOrders = {};
  oi.forEach(r => {
    if (!r.e || !r.d) return;
    if (!custOrders[r.e]) custOrders[r.e] = [];
    custOrders[r.e].push(r);
  });
  Object.values(custOrders).forEach(arr => arr.sort((a, b) => String(a.d).localeCompare(String(b.d))));

  // 入口商品別集計
  const entranceMap = {};
  Object.entries(custOrders).forEach(([email, orders]) => {
    const firstItem = orders[0].i || '不明';
    const orderNums = [...new Set(orders.map(o => o.n).filter(Boolean))];
    const totalSpend = orders.reduce((s, o) => s + (o.p || 0), 0);
    const isRepeat = orderNums.length >= 2;
    if (!entranceMap[firstItem]) entranceMap[firstItem] = { item: firstItem, count: 0, repeatCount: 0, totalLTV: 0 };
    entranceMap[firstItem].count++;
    if (isRepeat) entranceMap[firstItem].repeatCount++;
    entranceMap[firstItem].totalLTV += totalSpend;
  });

  const entranceRows = Object.values(entranceMap)
    .map(r => ({ ...r, f2Rate: r.count > 0 ? (r.repeatCount / r.count * 100) : 0, avgLTV: r.count > 0 ? Math.round(r.totalLTV / r.count) : 0 }))
    .sort((a, b) => b.count - a.count);

  const totalEntrance = entranceRows.reduce((s, r) => s + r.count, 0);
  const totalF2 = entranceRows.reduce((s, r) => s + r.repeatCount, 0);
  const overallF2Rate = totalEntrance > 0 ? (totalF2 / totalEntrance * 100) : 0;
  const bestF2Item = [...entranceRows].filter(r => r.count >= 3).sort((a, b) => b.f2Rate - a.f2Rate)[0];

  document.getElementById('entranceCards').innerHTML = [
    { label: '入口商品種類', value: comma(entranceRows.length) },
    { label: '全体F2転換率', value: overallF2Rate.toFixed(1) + '%' },
    { label: 'F2最高商品', value: bestF2Item ? safe(String(bestF2Item.item).substring(0, 20)) : '-' },
    { label: 'F2最高率', value: bestF2Item ? bestF2Item.f2Rate.toFixed(1) + '%' : '-' },
  ].map(c => '<div class="metric-card"><div class="metric-label">' + c.label + '</div><div class="metric-value">' + c.value + '</div></div>').join('');

  buildTable('entranceItemTable', [
    { key: 'item', label: '入口商品', fmt: v => safe(String(v).substring(0, 35)) },
    { key: 'count', label: '初回購入者数', fmt: v => comma(v) },
    { key: 'repeatCount', label: 'F2転換数', fmt: v => comma(v) },
    { key: 'f2Rate', label: 'F2転換率', fmt: v => '<span class="badge ' + (v >= 20 ? 'badge-success' : v >= 10 ? 'badge-neutral' : 'badge-danger') + '">' + v.toFixed(1) + '%</span>' },
    { key: 'avgLTV', label: '平均LTV', fmt: v => yen(v) },
  ], entranceRows, { limit: 30 });

  // チャート
  const top10 = entranceRows.filter(r => r.count >= 2).slice(0, 10);
  destroyChart('chartEntranceF2');
  if (top10.length > 0) {
    chartInstances['chartEntranceF2'] = new Chart(document.getElementById('chartEntranceF2'), {
      type: 'bar', data: {
        labels: top10.map(r => String(r.item).substring(0, 15)),
        datasets: [{ label: 'F2転換率', data: top10.map(r => r.f2Rate), backgroundColor: '#1a73e8' }]
      }, options: { indexAxis: 'y', responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false }, tooltip: { callbacks: { label: ctx => ctx.raw.toFixed(1) + '%' } } }, scales: { x: { beginAtZero: true, max: 100, ticks: { callback: v => v + '%' } } } }
    });
  }
  destroyChart('chartEntranceLTV');
  if (top10.length > 0) {
    chartInstances['chartEntranceLTV'] = new Chart(document.getElementById('chartEntranceLTV'), {
      type: 'bar', data: {
        labels: top10.map(r => String(r.item).substring(0, 15)),
        datasets: [{ label: '平均LTV', data: top10.map(r => r.avgLTV), backgroundColor: '#0d904f' }]
      }, options: { indexAxis: 'y', responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false }, tooltip: { callbacks: { label: ctx => yen(ctx.raw) } } }, scales: { x: { beginAtZero: true, ticks: { callback: v => v >= 10000 ? (v/10000).toFixed(0) + '万' : comma(v) } } } }
    });
  }

  // 入口商品フィルタ
  const eSel = document.getElementById('entranceItemFilter');
  if (eSel.options.length <= 1) {
    entranceRows.filter(r => r.count >= 2).forEach(r => {
      const opt = document.createElement('option');
      opt.value = r.item; opt.textContent = String(r.item).substring(0, 40) + ' (' + r.count + ')';
      eSel.appendChild(opt);
    });
  }

  // 2回目購入商品
  const selItem = eSel.value;
  if (selItem) {
    const buyersOfItem = Object.entries(custOrders).filter(([_, orders]) => orders[0].i === selItem);
    const secondMap = {};
    buyersOfItem.forEach(([_, orders]) => {
      const orderNums = [...new Set(orders.map(o => o.n).filter(Boolean))];
      if (orderNums.length < 2) return;
      const secondOrderNum = orderNums[1];
      const secondItems = orders.filter(o => o.n === secondOrderNum);
      secondItems.forEach(o => {
        const item = o.i || '不明';
        if (!secondMap[item]) secondMap[item] = { item, count: 0 };
        secondMap[item].count++;
      });
    });
    const secondRows = Object.values(secondMap).sort((a, b) => b.count - a.count);
    buildTable('entrance2ndTable', [
      { key: 'item', label: '2回目購入商品', fmt: v => safe(String(v).substring(0, 40)) },
      { key: 'count', label: '件数', fmt: v => comma(v) },
    ], secondRows, { limit: 20 });
  } else {
    document.getElementById('entrance2ndTable').innerHTML = '<div class="no-data">入口商品を選択してください</div>';
  }
}

// ── アプローチタイミング分析 ──
function renderTimingTab() {
  if (!D.hasOrders || !D.orderItems) {
    document.getElementById('timingCards').innerHTML = '<div class="no-data">受注データなし</div>';
    return;
  }
  const oi = D.orderItems;

  // 顧客ごと注文
  const custOrders = {};
  oi.forEach(r => {
    if (!r.e || !r.d) return;
    if (!custOrders[r.e]) custOrders[r.e] = [];
    custOrders[r.e].push(r);
  });

  // F2経過日数
  const f2Days = [];
  Object.values(custOrders).forEach(orders => {
    orders.sort((a, b) => String(a.d).localeCompare(String(b.d)));
    const orderNums = [...new Set(orders.map(o => o.n).filter(Boolean))];
    if (orderNums.length < 2) return;
    const firstDate = orders.find(o => o.n === orderNums[0])?.d;
    const secondDate = orders.find(o => o.n === orderNums[1])?.d;
    if (!firstDate || !secondDate) return;
    const d1 = new Date(String(firstDate).replace(/\\//g, '-').substring(0, 10) + 'T00:00:00');
    const d2 = new Date(String(secondDate).replace(/\\//g, '-').substring(0, 10) + 'T00:00:00');
    const days = Math.floor((d2 - d1) / 86400000);
    if (days >= 0 && days <= 365) f2Days.push(days);
  });

  // 曜日別注文数
  const dowNames = ['日', '月', '火', '水', '木', '金', '土'];
  const dowCounts = [0, 0, 0, 0, 0, 0, 0];
  const monthlyOrders = {};
  oi.forEach(r => {
    const d = String(r.d || '').replace(/\\//g, '-').substring(0, 10);
    if (!d || d.length < 10) return;
    const dt = new Date(d + 'T00:00:00');
    if (!isNaN(dt.getTime())) dowCounts[dt.getDay()]++;
    const ym = d.substring(0, 7);
    monthlyOrders[ym] = (monthlyOrders[ym] || 0) + 1;
  });

  // F2経過日数の統計
  const avgF2 = f2Days.length > 0 ? Math.round(f2Days.reduce((s, v) => s + v, 0) / f2Days.length) : 0;
  const medianF2 = f2Days.length > 0 ? f2Days.sort((a, b) => a - b)[Math.floor(f2Days.length / 2)] : 0;
  const peakDow = dowCounts.indexOf(Math.max(...dowCounts));

  document.getElementById('timingCards').innerHTML = [
    { label: 'F2平均経過日数', value: avgF2 + '日' },
    { label: 'F2中央値', value: medianF2 + '日' },
    { label: 'F2対象者数', value: comma(f2Days.length) },
    { label: '注文最多曜日', value: dowNames[peakDow] + '曜日' },
  ].map(c => '<div class="metric-card"><div class="metric-label">' + c.label + '</div><div class="metric-value">' + c.value + '</div></div>').join('');

  // F2経過日数分布チャート
  destroyChart('chartF2Days');
  if (f2Days.length > 0) {
    const buckets = {};
    f2Days.forEach(d => {
      let label;
      if (d <= 7) label = '1週間以内';
      else if (d <= 14) label = '2週間以内';
      else if (d <= 30) label = '1ヶ月以内';
      else if (d <= 60) label = '2ヶ月以内';
      else if (d <= 90) label = '3ヶ月以内';
      else if (d <= 180) label = '半年以内';
      else label = '半年超';
      buckets[label] = (buckets[label] || 0) + 1;
    });
    const bucketOrder = ['1週間以内', '2週間以内', '1ヶ月以内', '2ヶ月以内', '3ヶ月以内', '半年以内', '半年超'];
    const bucketLabels = bucketOrder.filter(b => buckets[b]);
    chartInstances['chartF2Days'] = new Chart(document.getElementById('chartF2Days'), {
      type: 'bar', data: {
        labels: bucketLabels,
        datasets: [{ label: '人数', data: bucketLabels.map(b => buckets[b] || 0), backgroundColor: '#1a73e8' }]
      }, options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false }, tooltip: { callbacks: { label: ctx => comma(ctx.raw) + '人' } } }, scales: { y: { beginAtZero: true } } }
    });
  }

  // 曜日別チャート
  destroyChart('chartDayOfWeek');
  chartInstances['chartDayOfWeek'] = new Chart(document.getElementById('chartDayOfWeek'), {
    type: 'bar', data: {
      labels: dowNames,
      datasets: [{ label: '注文数', data: dowCounts, backgroundColor: dowCounts.map((_, i) => i === peakDow ? '#e53935' : '#5b9bd5') }]
    }, options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true } } }
  });

  // 月別注文数
  destroyChart('chartMonthlyOrders');
  const moSorted = Object.entries(monthlyOrders).sort((a, b) => a[0].localeCompare(b[0]));
  if (moSorted.length > 0) {
    chartInstances['chartMonthlyOrders'] = new Chart(document.getElementById('chartMonthlyOrders'), {
      type: 'line', data: {
        labels: moSorted.map(([ym]) => D.monthLabels[ym] || ym),
        datasets: [{ label: '注文数', data: moSorted.map(([_, v]) => v), borderColor: '#1a3a5c', backgroundColor: 'rgba(26,58,92,0.08)', fill: true, tension: 0.3, pointRadius: 3 }]
      }, options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true } } }
    });
  }
}

// ── Render all ──
function renderAll() {
  renderSalesTab();
  renderProductTab();
  renderAdsTab();
  renderAcqTab();
  renderRepeatTab();
  renderBasketTab();
  renderLTVTab();
  renderRFMTab();
  renderEntranceTab();
  renderTimingTab();
}

// ── Event handlers ──

// Main tabs
function updateFilterUI(tabId) {
  const monthTabs = ['tab-ads', 'tab-acq', 'tab-product'];
  const noFilterTabs = ['tab-customer'];
  const noCompareTabs = ['tab-product'];
  const mf = document.getElementById('monthFilter');
  const df = document.getElementById('dayFilterFrom');
  const dt = document.getElementById('dayFilterTo');
  const ds = document.getElementById('dayFilterSep');
  const filterBar = document.querySelector('.filter-bar');
  const compareGroup = filterBar.querySelectorAll('.filter-group')[1];
  if (noFilterTabs.includes(tabId)) {
    filterBar.style.display = 'none';
  } else {
    filterBar.style.display = '';
    if (compareGroup) compareGroup.style.display = noCompareTabs.includes(tabId) ? 'none' : '';
    if (monthTabs.includes(tabId)) {
      mf.style.display = ''; df.style.display = 'none'; dt.style.display = 'none'; ds.style.display = 'none';
    } else {
      mf.style.display = 'none'; df.style.display = ''; dt.style.display = ''; ds.style.display = '';
    }
  }
}
document.querySelectorAll('.main-tab').forEach(tab => {
  tab.addEventListener('click', function() {
    document.querySelectorAll('.main-tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    this.classList.add('active');
    const target = document.getElementById(this.dataset.tab);
    if (target) target.classList.add('active');
    updateFilterUI(this.dataset.tab);
    // 商品別分析タブはデフォルト当月
    if (this.dataset.tab === 'tab-product') {
      const mf = document.getElementById('monthFilter');
      const now = new Date();
      const curYm = now.getFullYear() + '-' + String(now.getMonth() + 1).padStart(2, '0');
      if ([...mf.options].some(o => o.value === curYm)) mf.value = curYm;
      renderProductTab();
    }
    // CRM・広告タブ切替時に再描画
    if (this.dataset.tab === 'tab-acq') renderAcqTab();
    if (this.dataset.tab === 'tab-ads') renderAdsTab();
  });
});

// Sub tabs
document.querySelectorAll('.sub-tabs').forEach(tabGroup => {
  tabGroup.querySelectorAll('.sub-tab').forEach(tab => {
    tab.addEventListener('click', function() {
      const parentTabs = this.closest('.sub-tabs');
      // Deactivate sibling tabs
      parentTabs.querySelectorAll('.sub-tab').forEach(t => t.classList.remove('active'));
      this.classList.add('active');
      // Find the container that holds sibling panels (parent of the sub-tabs)
      const container = parentTabs.parentElement;
      // Only toggle panels that are direct children of the same container
      Array.from(container.children).forEach(el => {
        if (el.classList.contains('sub-panel')) el.classList.remove('active');
      });
      const target = document.getElementById(this.dataset.subtab);
      if (target) target.classList.add('active');
    });
  });
});

// Period type toggle (月/日)
const periodTypeSelect = document.getElementById('periodType');
const dayFilterFrom = document.getElementById('dayFilterFrom');
const dayFilterTo = document.getElementById('dayFilterTo');
const dayFilterSep = document.getElementById('dayFilterSep');
// 日付の選択肢を設定（all_rawの全日付を収集、YYYY-MM-DD形式に正規化）
const allDates = [];
D.months.forEach(ym => {
  (D.allByMonth[ym] || []).forEach(r => {
    if (r.date) {
      const normalized = r.date.replace(/\\//g, '-');
      if (!allDates.includes(normalized)) allDates.push(normalized);
    }
  });
});
allDates.sort().reverse();
if (allDates.length > 0) {
  // Default: latest month's full range
  const latestDate = allDates[0]; // most recent date
  const latestYm = latestDate.substring(0, 7);
  const firstOfMonth = latestYm + '-01';
  dayFilterTo.value = latestDate;
  dayFilterFrom.value = firstOfMonth;
  dayFrom = firstOfMonth;
  dayTo = latestDate;
  // Also set currentMonth for compare calculations
  currentMonth = latestYm;
}

dayFilterFrom.addEventListener('change', function() {
  dayFrom = this.value;
  if (dayFrom) currentMonth = dayFrom.substring(0, 7);
  renderAll();
});
dayFilterTo.addEventListener('change', function() {
  dayTo = this.value;
  renderAll();
});

// Month filter
monthSelect.addEventListener('change', function() {
  currentMonth = this.value;
  renderAll();
});

// Compare toggle
document.querySelectorAll('.compare-btn').forEach(btn => {
  btn.addEventListener('click', function() {
    document.querySelectorAll('.compare-btn').forEach(b => b.classList.remove('active'));
    this.classList.add('active');
    compareMode = this.dataset.compare;
    renderAll();
  });
});

// Product metric selector
document.getElementById('productMetricSelect').addEventListener('change', renderProductMonthlyTable);

// Repeat product filter
document.getElementById('repeatProductFilter').addEventListener('change', renderRepeatTab);

// LTV product filter
document.getElementById('ltvProductFilter').addEventListener('change', renderLTVTab);

// Basket product selector
document.getElementById('basketProductSelect').addEventListener('change', renderCoProducts);

// RFM segment filter
document.getElementById('rfmSegmentFilter').addEventListener('change', renderRFMTab);

// Entrance item filter
document.getElementById('entranceItemFilter').addEventListener('change', renderEntranceTab);

// Initial render
renderAll();
<\/script>
</body>
</html>`;
}

// 公開ダッシュボード（認証不要）
functions.http('dashboard', async (req, res) => {
  try {
    // CORS
    res.set('Access-Control-Allow-Origin', '*');
    if (req.method === 'OPTIONS') {
      res.set('Access-Control-Allow-Methods', 'GET');
      res.set('Access-Control-Max-Age', '3600');
      return res.status(204).send('');
    }
    const html = await generateDashboardHtml();
    res.set('Content-Type', 'text/html; charset=utf-8');
    res.set('Cache-Control', 'public, max-age=300');
    return res.status(200).send(html);
  } catch (err) {
    console.error('Dashboard error:', err);
    return res.status(500).send(`Dashboard error: ${err.message}`);
  }
});

