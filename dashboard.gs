// ============================
// 楽天ダッシュボード自動生成 GAS
// ============================
const SS_ID = '1V-CgRs9xpjbbaqb3OasgiCEYxfXP7_bpNsZFso-eiZI';

function buildDashboard() {
  const ss = SpreadsheetApp.openById(SS_ID);

  // 受注データ読み込み
  const orderSheet = ss.getSheetByName('受注_raw');
  if (!orderSheet || orderSheet.getLastRow() < 2) {
    Logger.log('受注_rawが空です');
    return;
  }
  const orderData = orderSheet.getDataRange().getValues();
  const orderHeaders = orderData[0];
  const orders = orderData.slice(1).map(row => {
    const obj = {};
    orderHeaders.forEach((h, i) => obj[h] = row[i]);
    return obj;
  });

  // RPPデータ読み込み
  const rppAll = readSheet(ss, 'rpp_all_raw');
  const rppItems = readSheet(ss, 'rpp_商品_raw');

  // 各ダッシュボード生成
  buildMonthlySales(ss, orders);
  buildProductKPI(ss, orders, rppItems);
  buildForecast(ss, orders);
  buildAdPerformance(ss, rppAll, rppItems);
  buildBasketAnalysis(ss, orders);
  buildF2Analysis(ss, orders);

  SpreadsheetApp.flush();
  Logger.log('ダッシュボード生成完了');
}

function readSheet(ss, name) {
  const sheet = ss.getSheetByName(name);
  if (!sheet || sheet.getLastRow() < 2) return [];
  const data = sheet.getDataRange().getValues();
  const headers = data[0];
  return data.slice(1).map(row => {
    const obj = {};
    headers.forEach((h, i) => obj[h] = row[i]);
    return obj;
  });
}

function getOrCreateSheet(ss, name) {
  let sheet = ss.getSheetByName(name);
  if (!sheet) sheet = ss.insertSheet(name);
  sheet.clear();
  return sheet;
}

// ============================
// 1. 月次売上推移（商品別・カテゴリ別）
// ============================
function buildMonthlySales(ss, orders) {
  const sheet = getOrCreateSheet(ss, '月次売上推移');

  // 月別集計
  const monthlyTotal = {};
  const monthlyByProduct = {};
  const productNames = {}; // manageNumber → itemName

  orders.forEach(o => {
    const dt = parseDate(o.orderDatetime);
    if (!dt) return;
    const ym = Utilities.formatDate(dt, 'Asia/Tokyo', 'yyyy-MM');
    const product = o.manageNumber || '不明';
    const price = num(o.price) * num(o.units || 1);

    // 商品名マスタ
    if (o.itemName && product !== '不明') {
      productNames[product] = (o.itemName || '').substring(0, 40);
    }

    // 月別合計
    if (!monthlyTotal[ym]) monthlyTotal[ym] = { sales: 0, orders: 0, items: 0 };
    monthlyTotal[ym].sales += price;
    monthlyTotal[ym].items += num(o.units || 1);

    // 月別×商品
    const key = `${ym}|||${product}`;
    if (!monthlyByProduct[key]) monthlyByProduct[key] = { sales: 0, units: 0 };
    monthlyByProduct[key].sales += price;
    monthlyByProduct[key].units += num(o.units || 1);
  });

  // 注文数は重複除外（orderNumber単位）
  const ordersByMonth = {};
  orders.forEach(o => {
    const dt = parseDate(o.orderDatetime);
    if (!dt) return;
    const ym = Utilities.formatDate(dt, 'Asia/Tokyo', 'yyyy-MM');
    if (!ordersByMonth[ym]) ordersByMonth[ym] = new Set();
    ordersByMonth[ym].add(o.orderNumber);
  });
  Object.keys(ordersByMonth).forEach(ym => {
    if (monthlyTotal[ym]) monthlyTotal[ym].orders = ordersByMonth[ym].size;
  });

  const months = Object.keys(monthlyTotal).sort();

  // --- 月次サマリ ---
  const summaryRows = [['月', '売上', '注文数', '商品数', '客単価']];
  months.forEach(ym => {
    const m = monthlyTotal[ym];
    summaryRows.push([ym, m.sales, m.orders, m.items, m.orders > 0 ? Math.round(m.sales / m.orders) : 0]);
  });
  sheet.getRange(1, 1, summaryRows.length, 5).setValues(summaryRows);

  // --- 商品別月次売上 ---
  const allProducts = [...new Set(Object.keys(monthlyByProduct).map(k => k.split('|||')[1]))];
  // 売上トップ20商品
  const productTotalSales = {};
  allProducts.forEach(p => {
    productTotalSales[p] = Object.keys(monthlyByProduct)
      .filter(k => k.endsWith('|||' + p))
      .reduce((sum, k) => sum + monthlyByProduct[k].sales, 0);
  });
  const topProducts = allProducts.sort((a, b) => (productTotalSales[b] || 0) - (productTotalSales[a] || 0)).slice(0, 20);

  const productHeader = ['月', ...topProducts.map(p => productNames[p] || p)];
  const productRows = [productHeader];
  months.forEach(ym => {
    const row = [ym];
    topProducts.forEach(p => {
      const key = `${ym}|||${p}`;
      row.push(monthlyByProduct[key] ? monthlyByProduct[key].sales : 0);
    });
    productRows.push(row);
  });

  const startRow = summaryRows.length + 3;
  sheet.getRange(startRow, 1, productRows.length, productRows[0].length).setValues(productRows);

  // 書式設定
  sheet.getRange(1, 1, 1, 5).setFontWeight('bold').setBackground('#4472C4').setFontColor('white');
  sheet.getRange(startRow, 1, 1, productRows[0].length).setFontWeight('bold').setBackground('#4472C4').setFontColor('white');
  sheet.getRange(2, 2, summaryRows.length - 1, 1).setNumberFormat('#,##0');
  sheet.getRange(2, 5, summaryRows.length - 1, 1).setNumberFormat('#,##0');
  sheet.autoResizeColumns(1, Math.max(5, productRows[0].length));
}

// ============================
// 2. 各商品のKPI
// ============================
function buildProductKPI(ss, orders, rppItems) {
  const sheet = getOrCreateSheet(ss, '商品KPI');

  // 受注データから商品別集計
  const productStats = {};
  const productNames = {};
  const orderProducts = {}; // orderNumber → Set of products

  orders.forEach(o => {
    const product = o.manageNumber || '不明';
    const price = num(o.price) * num(o.units || 1);

    if (o.itemName) productNames[product] = (o.itemName || '').substring(0, 50);

    if (!productStats[product]) productStats[product] = { sales: 0, units: 0, orderNumbers: new Set() };
    productStats[product].sales += price;
    productStats[product].units += num(o.units || 1);
    productStats[product].orderNumbers.add(o.orderNumber);
  });

  // RPPデータから商品別アクセス・広告指標
  const rppByProduct = {};
  rppItems.forEach(r => {
    const product = r['商品管理番号'] || '';
    if (!product) return;
    if (!rppByProduct[product]) rppByProduct[product] = { clicks: 0, spend: 0, adSales: 0, adOrders: 0 };
    rppByProduct[product].clicks += num(r['クリック数(合計)']);
    rppByProduct[product].spend += num(r['実績額(合計)']);
    rppByProduct[product].adSales += num(r['売上金額(合計720時間)']);
    rppByProduct[product].adOrders += num(r['売上件数(合計720時間)']);
  });

  // テーブル生成
  const allProducts = Object.keys(productStats).sort((a, b) => productStats[b].sales - productStats[a].sales);

  const rows = [['商品管理番号', '商品名', '売上', '注文数', '販売数', '客単価', 'RPPクリック数', 'RPP広告費', 'RPP経由売上', 'ROAS(%)', '転換率(%)']];

  allProducts.forEach(p => {
    const s = productStats[p];
    const r = rppByProduct[p] || { clicks: 0, spend: 0, adSales: 0, adOrders: 0 };
    const orderCount = s.orderNumbers.size;
    const aov = orderCount > 0 ? Math.round(s.sales / orderCount) : 0;
    const roas = r.spend > 0 ? Math.round(r.adSales / r.spend * 100) : 0;
    const cvr = r.clicks > 0 ? Math.round(r.adOrders / r.clicks * 10000) / 100 : 0;

    rows.push([p, productNames[p] || '', s.sales, orderCount, s.units, aov, r.clicks, r.spend, r.adSales, roas, cvr]);
  });

  sheet.getRange(1, 1, rows.length, rows[0].length).setValues(rows);
  sheet.getRange(1, 1, 1, rows[0].length).setFontWeight('bold').setBackground('#4472C4').setFontColor('white');
  sheet.getRange(2, 3, rows.length - 1, 1).setNumberFormat('#,##0');
  sheet.getRange(2, 6, rows.length - 1, 1).setNumberFormat('#,##0');
  sheet.getRange(2, 8, rows.length - 1, 2).setNumberFormat('#,##0');
  sheet.autoResizeColumns(1, rows[0].length);
}

// ============================
// 3. 着地予測・達成率
// ============================
function buildForecast(ss, orders) {
  const sheet = getOrCreateSheet(ss, '着地予測');

  const now = new Date();
  const thisYear = now.getFullYear();
  const thisMonth = now.getMonth(); // 0-indexed
  const today = now.getDate();
  const daysInMonth = new Date(thisYear, thisMonth + 1, 0).getDate();

  // 当月の受注を集計
  let currentMonthSales = 0;
  let currentMonthOrders = new Set();
  let currentMonthItems = 0;

  // 日別売上
  const dailySales = {};

  orders.forEach(o => {
    const dt = parseDate(o.orderDatetime);
    if (!dt) return;
    if (dt.getFullYear() === thisYear && dt.getMonth() === thisMonth) {
      const price = num(o.price) * num(o.units || 1);
      currentMonthSales += price;
      currentMonthOrders.add(o.orderNumber);
      currentMonthItems += num(o.units || 1);

      const day = dt.getDate();
      if (!dailySales[day]) dailySales[day] = 0;
      dailySales[day] += price;
    }
  });

  const orderCount = currentMonthOrders.size;
  const dailyAvgSales = today > 0 ? currentMonthSales / today : 0;
  const dailyAvgOrders = today > 0 ? orderCount / today : 0;

  // 着地予測
  const forecastSales = Math.round(dailyAvgSales * daysInMonth);
  const forecastOrders = Math.round(dailyAvgOrders * daysInMonth);

  // 前月実績
  const lastMonth = thisMonth === 0 ? 11 : thisMonth - 1;
  const lastMonthYear = thisMonth === 0 ? thisYear - 1 : thisYear;
  let lastMonthSales = 0;
  let lastMonthOrders = new Set();
  orders.forEach(o => {
    const dt = parseDate(o.orderDatetime);
    if (!dt) return;
    if (dt.getFullYear() === lastMonthYear && dt.getMonth() === lastMonth) {
      lastMonthSales += num(o.price) * num(o.units || 1);
      lastMonthOrders.add(o.orderNumber);
    }
  });

  const monthName = `${thisYear}年${thisMonth + 1}月`;
  const lastMonthName = `${lastMonthYear}年${lastMonth + 1}月`;

  // サマリ
  const rows = [
    ['指標', '値'],
    ['対象月', monthName],
    ['集計日', `${thisMonth + 1}/${today}`],
    ['月の日数', daysInMonth],
    ['経過日数', today],
    ['残り日数', daysInMonth - today],
    ['', ''],
    ['■ 当月実績（経過分）', ''],
    ['売上', currentMonthSales],
    ['注文数', orderCount],
    ['日次平均売上', Math.round(dailyAvgSales)],
    ['日次平均注文数', Math.round(dailyAvgOrders * 10) / 10],
    ['', ''],
    ['■ 着地予測（日割り推計）', ''],
    ['売上予測', forecastSales],
    ['注文数予測', forecastOrders],
    ['', ''],
    ['■ 前月実績', ''],
    ['前月', lastMonthName],
    ['前月売上', lastMonthSales],
    ['前月注文数', lastMonthOrders.size],
    ['', ''],
    ['■ 前月比', ''],
    ['売上達成率（予測/前月）', lastMonthSales > 0 ? Math.round(forecastSales / lastMonthSales * 100) + '%' : '-'],
    ['注文数達成率（予測/前月）', lastMonthOrders.size > 0 ? Math.round(forecastOrders / lastMonthOrders.size * 100) + '%' : '-'],
  ];

  sheet.getRange(1, 1, rows.length, 2).setValues(rows);

  // 日別売上テーブル
  const dailyRows = [['日', '売上', '累計売上']];
  let cumulative = 0;
  for (let d = 1; d <= Math.min(today, daysInMonth); d++) {
    const ds = dailySales[d] || 0;
    cumulative += ds;
    dailyRows.push([d, ds, cumulative]);
  }

  sheet.getRange(1, 4, dailyRows.length, 3).setValues(dailyRows);

  // 書式
  sheet.getRange(1, 1, 1, 2).setFontWeight('bold').setBackground('#4472C4').setFontColor('white');
  sheet.getRange(1, 4, 1, 3).setFontWeight('bold').setBackground('#4472C4').setFontColor('white');
  [9, 11, 15, 20].forEach(r => {
    sheet.getRange(r, 2).setNumberFormat('#,##0');
  });
  sheet.getRange(2, 5, dailyRows.length - 1, 2).setNumberFormat('#,##0');
  sheet.autoResizeColumns(1, 6);
}

// ============================
// 4. 広告パフォーマンス
// ============================
function buildAdPerformance(ss, rppAll, rppItems) {
  const sheet = getOrCreateSheet(ss, '広告パフォーマンス');

  // RPP全体サマリ
  let totalClicks = 0, totalSpend = 0, totalSales = 0, totalOrders = 0;
  const dailyAd = {};

  rppAll.forEach(r => {
    const date = r['日付'] || '';
    totalClicks += num(r['クリック数(合計)']);
    totalSpend += num(r['実績額(合計)']);
    totalSales += num(r['売上金額(合計720時間)']);
    totalOrders += num(r['売上件数(合計720時間)']);

    if (date) {
      if (!dailyAd[date]) dailyAd[date] = { clicks: 0, spend: 0, sales: 0, orders: 0, ctr: 0 };
      dailyAd[date].clicks += num(r['クリック数(合計)']);
      dailyAd[date].spend += num(r['実績額(合計)']);
      dailyAd[date].sales += num(r['売上金額(合計720時間)']);
      dailyAd[date].orders += num(r['売上件数(合計720時間)']);
      dailyAd[date].ctr = num(r['CTR(%)']);
    }
  });

  // サマリ
  const overallRoas = totalSpend > 0 ? Math.round(totalSales / totalSpend * 100) : 0;
  const overallCvr = totalClicks > 0 ? Math.round(totalOrders / totalClicks * 10000) / 100 : 0;
  const overallCpc = totalClicks > 0 ? Math.round(totalSpend / totalClicks) : 0;
  const overallCpa = totalOrders > 0 ? Math.round(totalSpend / totalOrders) : 0;

  const summaryRows = [
    ['■ RPP広告 全体サマリ', ''],
    ['総クリック数', totalClicks],
    ['総広告費', totalSpend],
    ['総広告経由売上', totalSales],
    ['総注文数', totalOrders],
    ['ROAS', overallRoas + '%'],
    ['CVR', overallCvr + '%'],
    ['平均CPC', overallCpc],
    ['CPA（注文獲得単価）', overallCpa],
    ['', ''],
  ];

  sheet.getRange(1, 1, summaryRows.length, 2).setValues(summaryRows);

  // 日別RPPパフォーマンス
  const dates = Object.keys(dailyAd).sort();
  const dailyRows = [['日付', 'CTR(%)', 'クリック数', '広告費', '売上', '注文数', 'ROAS(%)', 'CPC']];
  dates.forEach(d => {
    const ad = dailyAd[d];
    const roas = ad.spend > 0 ? Math.round(ad.sales / ad.spend * 100) : 0;
    const cpc = ad.clicks > 0 ? Math.round(ad.spend / ad.clicks) : 0;
    dailyRows.push([d, ad.ctr, ad.clicks, ad.spend, ad.sales, ad.orders, roas, cpc]);
  });

  const startRow = summaryRows.length + 1;
  sheet.getRange(startRow, 1, dailyRows.length, dailyRows[0].length).setValues(dailyRows);

  // 商品別広告パフォーマンス
  const productAd = {};
  const productNames = {};
  rppItems.forEach(r => {
    const p = r['商品管理番号'] || '';
    if (!p) return;
    if (!productAd[p]) productAd[p] = { clicks: 0, spend: 0, sales: 0, orders: 0, bid: 0 };
    productAd[p].clicks += num(r['クリック数(合計)']);
    productAd[p].spend += num(r['実績額(合計)']);
    productAd[p].sales += num(r['売上金額(合計720時間)']);
    productAd[p].orders += num(r['売上件数(合計720時間)']);
    productAd[p].bid = num(r['入札単価']) || productAd[p].bid;
  });

  const productRows = [['商品管理番号', '入札単価', 'クリック数', '広告費', '売上', '注文数', 'ROAS(%)', 'CVR(%)', 'CPC', 'CPA']];
  Object.keys(productAd).sort((a, b) => productAd[b].spend - productAd[a].spend).forEach(p => {
    const ad = productAd[p];
    const roas = ad.spend > 0 ? Math.round(ad.sales / ad.spend * 100) : 0;
    const cvr = ad.clicks > 0 ? Math.round(ad.orders / ad.clicks * 10000) / 100 : 0;
    const cpc = ad.clicks > 0 ? Math.round(ad.spend / ad.clicks) : 0;
    const cpa = ad.orders > 0 ? Math.round(ad.spend / ad.orders) : 0;
    productRows.push([p, ad.bid, ad.clicks, ad.spend, ad.sales, ad.orders, roas, cvr, cpc, cpa]);
  });

  const prodStartRow = startRow + dailyRows.length + 2;
  sheet.getRange(prodStartRow, 1, productRows.length, productRows[0].length).setValues(productRows);

  // 書式
  sheet.getRange(1, 1, 1, 2).setFontWeight('bold').setBackground('#4472C4').setFontColor('white');
  sheet.getRange(startRow, 1, 1, dailyRows[0].length).setFontWeight('bold').setBackground('#4472C4').setFontColor('white');
  sheet.getRange(prodStartRow, 1, 1, productRows[0].length).setFontWeight('bold').setBackground('#ED7D31').setFontColor('white');
  sheet.autoResizeColumns(1, 10);
}

// ============================
// 5. バスケット分析
// ============================
function buildBasketAnalysis(ss, orders) {
  const sheet = getOrCreateSheet(ss, 'バスケット分析');

  // 同一注文内の商品組み合わせを分析
  const orderItems = {}; // orderNumber → [manageNumber, ...]
  const productNames = {};

  orders.forEach(o => {
    const orderNum = o.orderNumber;
    const product = o.manageNumber || '';
    if (!product) return;
    if (!orderItems[orderNum]) orderItems[orderNum] = [];
    if (!orderItems[orderNum].includes(product)) {
      orderItems[orderNum].push(product);
    }
    if (o.itemName) productNames[product] = (o.itemName || '').substring(0, 40);
  });

  // 同時購入ペアのカウント
  const pairCount = {};
  const productOrderCount = {};

  Object.values(orderItems).forEach(items => {
    // 各商品の出現回数
    items.forEach(p => {
      productOrderCount[p] = (productOrderCount[p] || 0) + 1;
    });

    // 2商品以上の注文のみペア分析
    if (items.length < 2) return;
    items.sort();
    for (let i = 0; i < items.length; i++) {
      for (let j = i + 1; j < items.length; j++) {
        const key = `${items[i]}|||${items[j]}`;
        pairCount[key] = (pairCount[key] || 0) + 1;
      }
    }
  });

  // 注文サイズ分布
  const basketSizes = {};
  Object.values(orderItems).forEach(items => {
    const size = items.length;
    basketSizes[size] = (basketSizes[size] || 0) + 1;
  });

  // --- 注文サイズ分布 ---
  const sizeRows = [['■ 注文あたり商品種類数', ''], ['商品種類数', '注文数']];
  Object.keys(basketSizes).sort((a, b) => a - b).forEach(size => {
    sizeRows.push([Number(size), basketSizes[size]]);
  });

  sheet.getRange(1, 1, sizeRows.length, 2).setValues(sizeRows);

  // --- 同時購入ペアランキング ---
  const pairs = Object.entries(pairCount).sort((a, b) => b[1] - a[1]).slice(0, 30);

  const pairRows = [['■ 同時購入ペア TOP30', '', '', '', ''], ['商品A', '商品B', '同時購入数', '商品A注文数', '商品B注文数']];
  pairs.forEach(([key, count]) => {
    const [a, b] = key.split('|||');
    pairRows.push([
      productNames[a] || a,
      productNames[b] || b,
      count,
      productOrderCount[a] || 0,
      productOrderCount[b] || 0,
    ]);
  });

  const pairStartRow = sizeRows.length + 2;
  sheet.getRange(pairStartRow, 1, pairRows.length, 5).setValues(pairRows);

  // --- 商品別 よく一緒に買われる商品 ---
  const topProducts = Object.entries(productOrderCount).sort((a, b) => b[1] - a[1]).slice(0, 15);

  const coRows = [['■ 商品別 よく一緒に買われる商品', '', '', ''], ['基準商品', '一緒に買われる商品', '回数', 'リフト値']];
  const totalOrders = Object.keys(orderItems).length;

  topProducts.forEach(([product]) => {
    // この商品と同時購入される商品を集計
    const coPurchase = {};
    Object.values(orderItems).forEach(items => {
      if (!items.includes(product)) return;
      items.forEach(p => {
        if (p === product) return;
        coPurchase[p] = (coPurchase[p] || 0) + 1;
      });
    });

    // トップ3
    const topCo = Object.entries(coPurchase).sort((a, b) => b[1] - a[1]).slice(0, 3);
    topCo.forEach(([coProduct, count]) => {
      // リフト値 = P(A&B) / (P(A) * P(B))
      const pA = (productOrderCount[product] || 0) / totalOrders;
      const pB = (productOrderCount[coProduct] || 0) / totalOrders;
      const pAB = count / totalOrders;
      const lift = (pA * pB) > 0 ? Math.round(pAB / (pA * pB) * 100) / 100 : 0;

      coRows.push([productNames[product] || product, productNames[coProduct] || coProduct, count, lift]);
    });
  });

  const coStartRow = pairStartRow + pairRows.length + 2;
  sheet.getRange(coStartRow, 1, coRows.length, 4).setValues(coRows);

  // 書式
  sheet.getRange(1, 1, 1, 2).setFontWeight('bold').setBackground('#4472C4').setFontColor('white');
  sheet.getRange(pairStartRow, 1, 1, 5).setFontWeight('bold').setBackground('#4472C4').setFontColor('white');
  sheet.getRange(pairStartRow + 1, 1, 1, 5).setFontWeight('bold').setBackground('#D9E2F3');
  sheet.getRange(coStartRow, 1, 1, 4).setFontWeight('bold').setBackground('#ED7D31').setFontColor('white');
  sheet.getRange(coStartRow + 1, 1, 1, 4).setFontWeight('bold').setBackground('#FCE4D6');
  sheet.autoResizeColumns(1, 5);
}

// ============================
// 6. F2分析（リピート購入分析）
// ============================
function buildF2Analysis(ss, orders) {
  const sheet = getOrCreateSheet(ss, 'F2分析');

  // 顧客別の注文を時系列で集計（メールアドレスで識別）
  const customerOrders = {}; // email → [{orderNumber, date, totalPrice}, ...]
  const processedOrders = new Set();

  orders.forEach(o => {
    const email = o.ordererEmailAddress || '';
    if (!email) return;
    const orderNum = o.orderNumber;
    if (processedOrders.has(orderNum)) return;
    processedOrders.add(orderNum);

    const dt = parseDate(o.orderDatetime);
    if (!dt) return;

    if (!customerOrders[email]) customerOrders[email] = [];
    customerOrders[email].push({
      orderNumber: orderNum,
      date: dt,
      totalPrice: num(o.totalPrice),
    });
  });

  // 各顧客の注文を日付順にソート
  Object.values(customerOrders).forEach(orders => {
    orders.sort((a, b) => a.date - b.date);
  });

  const totalCustomers = Object.keys(customerOrders).length;

  // F分布（購入回数別の顧客数）
  const fDistribution = {};
  const fSales = {}; // F値別の売上合計
  Object.values(customerOrders).forEach(orders => {
    const f = orders.length;
    fDistribution[f] = (fDistribution[f] || 0) + 1;
    const totalSales = orders.reduce((sum, o) => sum + o.totalPrice, 0);
    fSales[f] = (fSales[f] || 0) + totalSales;
  });

  // F1→F2転換率
  const f1Count = fDistribution[1] || 0;
  const f2PlusCount = totalCustomers - f1Count;
  const f2ConversionRate = totalCustomers > 0 ? Math.round(f2PlusCount / totalCustomers * 10000) / 100 : 0;

  // サマリ
  const summaryRows = [
    ['■ F2分析サマリ', ''],
    ['総顧客数（ユニークメール）', totalCustomers],
    ['1回購入（F1）', f1Count],
    ['2回以上購入（F2+）', f2PlusCount],
    ['リピート率（F2転換率）', f2ConversionRate + '%'],
    ['', ''],
  ];

  sheet.getRange(1, 1, summaryRows.length, 2).setValues(summaryRows);

  // F値分布テーブル
  const maxF = Math.min(Math.max(...Object.keys(fDistribution).map(Number)), 20);
  const fRows = [['■ 購入回数別 顧客分布', '', '', '', ''], ['購入回数(F値)', '顧客数', '構成比(%)', '売上合計', '顧客単価']];
  for (let f = 1; f <= maxF; f++) {
    const count = fDistribution[f] || 0;
    const ratio = totalCustomers > 0 ? Math.round(count / totalCustomers * 10000) / 100 : 0;
    const sales = fSales[f] || 0;
    const ltv = count > 0 ? Math.round(sales / count) : 0;
    fRows.push([f, count, ratio, sales, ltv]);
  }
  // F20+
  let overCount = 0, overSales = 0;
  Object.keys(fDistribution).forEach(f => {
    if (Number(f) > maxF) {
      overCount += fDistribution[f];
      overSales += fSales[f] || 0;
    }
  });
  if (overCount > 0) {
    fRows.push([`${maxF + 1}+`, overCount, Math.round(overCount / totalCustomers * 10000) / 100, overSales, Math.round(overSales / overCount)]);
  }

  const fStartRow = summaryRows.length + 1;
  sheet.getRange(fStartRow, 1, fRows.length, 5).setValues(fRows);

  // 月別 新規/リピート比率
  const monthlyNR = {}; // ym → {new: count, repeat: count, newSales: amount, repeatSales: amount}
  const firstPurchaseMonth = {}; // email → first purchase ym

  Object.entries(customerOrders).forEach(([email, orders]) => {
    const firstYm = Utilities.formatDate(orders[0].date, 'Asia/Tokyo', 'yyyy-MM');
    firstPurchaseMonth[email] = firstYm;
  });

  // 全注文を走査
  const processedOrders2 = new Set();
  orders.forEach(o => {
    const email = o.ordererEmailAddress || '';
    const orderNum = o.orderNumber;
    if (!email || processedOrders2.has(orderNum)) return;
    processedOrders2.add(orderNum);

    const dt = parseDate(o.orderDatetime);
    if (!dt) return;
    const ym = Utilities.formatDate(dt, 'Asia/Tokyo', 'yyyy-MM');
    const price = num(o.totalPrice);

    if (!monthlyNR[ym]) monthlyNR[ym] = { newCount: 0, repeatCount: 0, newSales: 0, repeatSales: 0 };

    if (firstPurchaseMonth[email] === ym) {
      monthlyNR[ym].newCount++;
      monthlyNR[ym].newSales += price;
    } else {
      monthlyNR[ym].repeatCount++;
      monthlyNR[ym].repeatSales += price;
    }
  });

  const months = Object.keys(monthlyNR).sort();
  const nrRows = [['■ 月別 新規/リピート分析', '', '', '', '', '', ''],
                   ['月', '新規注文数', 'リピート注文数', 'リピート率(%)', '新規売上', 'リピート売上', 'リピート売上比率(%)']];
  months.forEach(ym => {
    const nr = monthlyNR[ym];
    const total = nr.newCount + nr.repeatCount;
    const repeatRate = total > 0 ? Math.round(nr.repeatCount / total * 10000) / 100 : 0;
    const totalSales = nr.newSales + nr.repeatSales;
    const repeatSalesRate = totalSales > 0 ? Math.round(nr.repeatSales / totalSales * 10000) / 100 : 0;
    nrRows.push([ym, nr.newCount, nr.repeatCount, repeatRate, nr.newSales, nr.repeatSales, repeatSalesRate]);
  });

  const nrStartRow = fStartRow + fRows.length + 2;
  sheet.getRange(nrStartRow, 1, nrRows.length, 7).setValues(nrRows);

  // 書式
  sheet.getRange(1, 1, 1, 2).setFontWeight('bold').setBackground('#4472C4').setFontColor('white');
  sheet.getRange(fStartRow, 1, 1, 5).setFontWeight('bold').setBackground('#4472C4').setFontColor('white');
  sheet.getRange(fStartRow + 1, 1, 1, 5).setFontWeight('bold').setBackground('#D9E2F3');
  sheet.getRange(nrStartRow, 1, 1, 7).setFontWeight('bold').setBackground('#ED7D31').setFontColor('white');
  sheet.getRange(nrStartRow + 1, 1, 1, 7).setFontWeight('bold').setBackground('#FCE4D6');
  sheet.getRange(fStartRow + 2, 4, fRows.length - 2, 1).setNumberFormat('#,##0');
  sheet.getRange(fStartRow + 2, 5, fRows.length - 2, 1).setNumberFormat('#,##0');
  sheet.getRange(nrStartRow + 2, 5, months.length, 2).setNumberFormat('#,##0');
  sheet.autoResizeColumns(1, 7);
}

// ============================
// ユーティリティ
// ============================
function parseDate(val) {
  if (!val) return null;
  if (val instanceof Date) return val;
  const str = String(val);
  // ISO形式: 2026-04-14T21:11:13+0900
  const m = str.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})/);
  if (m) return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]), Number(m[4]), Number(m[5]), Number(m[6]));
  // yyyy年MM月dd日
  const m2 = str.match(/(\d{4})年(\d{1,2})月(\d{1,2})日/);
  if (m2) return new Date(Number(m2[1]), Number(m2[2]) - 1, Number(m2[3]));
  return null;
}

function num(val) {
  if (val === null || val === undefined || val === '') return 0;
  const n = Number(String(val).replace(/[,%]/g, ''));
  return isNaN(n) ? 0 : n;
}
