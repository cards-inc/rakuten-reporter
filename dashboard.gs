// ============================
// 楽天ダッシュボード ウェブアプリ版
// ============================
const SS_ID = '1V-CgRs9xpjbbaqb3OasgiCEYxfXP7_bpNsZFso-eiZI';

function doGet() {
  return HtmlService.createHtmlOutputFromFile('index')
    .setTitle('百福堂_楽天ストアアナリティクス')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

// --- データ取得API ---
function getDashboardData() {
  const ss = SpreadsheetApp.openById(SS_ID);

  const orders = readSheet(ss, 'order_raw');
  const rppAll = readSheet(ss, 'rpp_all_raw');
  const rppItems = readSheet(ss, 'rpp_item_raw');

  return {
    monthlySales: calcMonthlySales(orders),
    productKPI: calcProductKPI(orders, rppItems),
    forecast: calcForecast(orders),
    adPerformance: calcAdPerformance(rppAll, rppItems),
    basket: calcBasketAnalysis(orders),
    f2: calcF2Analysis(orders),
  };
}

function readSheet(ss, name) {
  try {
    const sheet = ss.getSheetByName(name);
    if (!sheet || sheet.getLastRow() < 2) return [];
    const data = sheet.getDataRange().getValues();
    const headers = data[0];
    return data.slice(1).map(row => {
      const obj = {};
      headers.forEach((h, i) => obj[h] = row[i]);
      return obj;
    });
  } catch (e) {
    // DATASOURCEシート（BigQuery接続）はSheets APIで読む
    if (e.message && e.message.includes('DATASOURCE')) {
      try {
        const resp = Sheets.Spreadsheets.Values.get(ss.getId(), name);
        if (!resp.values || resp.values.length < 2) return [];
        const headers = resp.values[0];
        return resp.values.slice(1).map(row => {
          const obj = {};
          headers.forEach((h, i) => obj[h] = row[i] || '');
          return obj;
        });
      } catch (e2) {
        console.log('Sheets API fallback failed for ' + name + ': ' + e2.message);
        return [];
      }
    }
    console.log('readSheet error for ' + name + ': ' + e.message);
    return [];
  }
}

function parseDate(val) {
  if (!val) return null;
  if (val instanceof Date) return val;
  const str = String(val);
  const m = str.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})/);
  if (m) return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]), Number(m[4]), Number(m[5]), Number(m[6]));
  const m2 = str.match(/(\d{4})年(\d{1,2})月(\d{1,2})日/);
  if (m2) return new Date(Number(m2[1]), Number(m2[2]) - 1, Number(m2[3]));
  return null;
}

function num(val) {
  if (val === null || val === undefined || val === '') return 0;
  const n = Number(String(val).replace(/[,%]/g, ''));
  return isNaN(n) ? 0 : n;
}

// ============================
// 1. 月次売上推移
// ============================
function calcMonthlySales(orders) {
  const monthlyTotal = {};
  const monthlyByProduct = {};
  const productNames = {};
  const ordersByMonth = {};

  orders.forEach(o => {
    const dt = parseDate(o.orderDatetime);
    if (!dt) return;
    const ym = Utilities.formatDate(dt, 'Asia/Tokyo', 'yyyy-MM');
    const product = o.manageNumber || '不明';
    const price = num(o.price) * num(o.units || 1);

    if (o.itemName && product !== '不明') productNames[product] = (o.itemName || '').substring(0, 30);

    if (!monthlyTotal[ym]) monthlyTotal[ym] = { sales: 0, orders: 0, items: 0 };
    monthlyTotal[ym].sales += price;
    monthlyTotal[ym].items += num(o.units || 1);

    if (!ordersByMonth[ym]) ordersByMonth[ym] = {};
    ordersByMonth[ym][o.orderNumber] = true;

    const key = ym + '|||' + product;
    if (!monthlyByProduct[key]) monthlyByProduct[key] = 0;
    monthlyByProduct[key] += price;
  });

  Object.keys(ordersByMonth).forEach(ym => {
    if (monthlyTotal[ym]) monthlyTotal[ym].orders = Object.keys(ordersByMonth[ym]).length;
  });

  const months = Object.keys(monthlyTotal).sort();

  // トップ10商品
  const productTotals = {};
  Object.entries(monthlyByProduct).forEach(([k, v]) => {
    const p = k.split('|||')[1];
    productTotals[p] = (productTotals[p] || 0) + v;
  });
  const topProducts = Object.entries(productTotals).sort((a, b) => b[1] - a[1]).slice(0, 10).map(e => e[0]);

  const productSeries = topProducts.map(p => ({
    name: productNames[p] || p,
    data: months.map(ym => monthlyByProduct[ym + '|||' + p] || 0),
  }));

  return {
    months,
    summary: months.map(ym => ({
      month: ym,
      sales: monthlyTotal[ym].sales,
      orders: monthlyTotal[ym].orders,
      items: monthlyTotal[ym].items,
      aov: monthlyTotal[ym].orders > 0 ? Math.round(monthlyTotal[ym].sales / monthlyTotal[ym].orders) : 0,
    })),
    productSeries,
  };
}

// ============================
// 2. 商品KPI
// ============================
function calcProductKPI(orders, rppItems) {
  const productStats = {};
  const productNames = {};

  orders.forEach(o => {
    const product = o.manageNumber || '不明';
    const price = num(o.price) * num(o.units || 1);
    if (o.itemName) productNames[product] = (o.itemName || '').substring(0, 40);
    if (!productStats[product]) productStats[product] = { sales: 0, units: 0, orderNums: {} };
    productStats[product].sales += price;
    productStats[product].units += num(o.units || 1);
    productStats[product].orderNums[o.orderNumber] = true;
  });

  const rppByProduct = {};
  rppItems.forEach(r => {
    const p = r['商品管理番号'] || '';
    if (!p) return;
    if (!rppByProduct[p]) rppByProduct[p] = { clicks: 0, spend: 0, adSales: 0, adOrders: 0 };
    rppByProduct[p].clicks += num(r['クリック数(合計)']);
    rppByProduct[p].spend += num(r['実績額(合計)']);
    rppByProduct[p].adSales += num(r['売上金額(合計720時間)']);
    rppByProduct[p].adOrders += num(r['売上件数(合計720時間)']);
  });

  const rows = Object.keys(productStats)
    .sort((a, b) => productStats[b].sales - productStats[a].sales)
    .slice(0, 50)
    .map(p => {
      const s = productStats[p];
      const r = rppByProduct[p] || { clicks: 0, spend: 0, adSales: 0, adOrders: 0 };
      const orderCount = Object.keys(s.orderNums).length;
      return {
        id: p,
        name: productNames[p] || p,
        sales: s.sales,
        orders: orderCount,
        units: s.units,
        aov: orderCount > 0 ? Math.round(s.sales / orderCount) : 0,
        rppClicks: r.clicks,
        rppSpend: r.spend,
        rppSales: r.adSales,
        roas: r.spend > 0 ? Math.round(r.adSales / r.spend * 100) : 0,
        cvr: r.clicks > 0 ? Math.round(r.adOrders / r.clicks * 10000) / 100 : 0,
      };
    });

  return rows;
}

// ============================
// 3. 着地予測
// ============================
function calcForecast(orders) {
  const now = new Date();
  const thisYear = now.getFullYear();
  const thisMonth = now.getMonth();
  const today = now.getDate();
  const daysInMonth = new Date(thisYear, thisMonth + 1, 0).getDate();

  let currentSales = 0;
  const currentOrderNums = {};
  const dailySales = {};

  orders.forEach(o => {
    const dt = parseDate(o.orderDatetime);
    if (!dt) return;
    if (dt.getFullYear() === thisYear && dt.getMonth() === thisMonth) {
      const price = num(o.price) * num(o.units || 1);
      currentSales += price;
      currentOrderNums[o.orderNumber] = true;
      const day = dt.getDate();
      dailySales[day] = (dailySales[day] || 0) + price;
    }
  });

  const orderCount = Object.keys(currentOrderNums).length;
  const dailyAvg = today > 0 ? currentSales / today : 0;
  const forecastSales = Math.round(dailyAvg * daysInMonth);

  // 前月
  const lm = thisMonth === 0 ? 11 : thisMonth - 1;
  const lmYear = thisMonth === 0 ? thisYear - 1 : thisYear;
  let lastSales = 0;
  const lastOrderNums = {};
  orders.forEach(o => {
    const dt = parseDate(o.orderDatetime);
    if (!dt) return;
    if (dt.getFullYear() === lmYear && dt.getMonth() === lm) {
      lastSales += num(o.price) * num(o.units || 1);
      lastOrderNums[o.orderNumber] = true;
    }
  });

  const daily = [];
  let cum = 0;
  for (let d = 1; d <= Math.min(today, daysInMonth); d++) {
    const s = dailySales[d] || 0;
    cum += s;
    daily.push({ day: d, sales: s, cumulative: cum });
  }

  return {
    monthLabel: (thisMonth + 1) + '月',
    today,
    daysInMonth,
    currentSales,
    orderCount,
    dailyAvg: Math.round(dailyAvg),
    forecastSales,
    lastMonthSales: lastSales,
    lastMonthOrders: Object.keys(lastOrderNums).length,
    achieveRate: lastSales > 0 ? Math.round(forecastSales / lastSales * 100) : 0,
    daily,
  };
}

// ============================
// 4. 広告パフォーマンス
// ============================
function calcAdPerformance(rppAll, rppItems) {
  let totalClicks = 0, totalSpend = 0, totalSales = 0, totalOrders = 0;
  const dailyAd = {};

  rppAll.forEach(r => {
    const date = r['日付'] || '';
    totalClicks += num(r['クリック数(合計)']);
    totalSpend += num(r['実績額(合計)']);
    totalSales += num(r['売上金額(合計720時間)']);
    totalOrders += num(r['売上件数(合計720時間)']);
    if (date) {
      if (!dailyAd[date]) dailyAd[date] = { clicks: 0, spend: 0, sales: 0, orders: 0 };
      dailyAd[date].clicks += num(r['クリック数(合計)']);
      dailyAd[date].spend += num(r['実績額(合計)']);
      dailyAd[date].sales += num(r['売上金額(合計720時間)']);
      dailyAd[date].orders += num(r['売上件数(合計720時間)']);
    }
  });

  const dates = Object.keys(dailyAd).sort();
  const daily = dates.map(d => ({
    date: d,
    clicks: dailyAd[d].clicks,
    spend: dailyAd[d].spend,
    sales: dailyAd[d].sales,
    roas: dailyAd[d].spend > 0 ? Math.round(dailyAd[d].sales / dailyAd[d].spend * 100) : 0,
  }));

  // 商品別
  const productAd = {};
  rppItems.forEach(r => {
    const p = r['商品管理番号'] || '';
    if (!p) return;
    if (!productAd[p]) productAd[p] = { clicks: 0, spend: 0, sales: 0, orders: 0 };
    productAd[p].clicks += num(r['クリック数(合計)']);
    productAd[p].spend += num(r['実績額(合計)']);
    productAd[p].sales += num(r['売上金額(合計720時間)']);
    productAd[p].orders += num(r['売上件数(合計720時間)']);
  });

  const products = Object.entries(productAd)
    .sort((a, b) => b[1].spend - a[1].spend)
    .slice(0, 20)
    .map(([p, ad]) => ({
      id: p,
      clicks: ad.clicks,
      spend: ad.spend,
      sales: ad.sales,
      orders: ad.orders,
      roas: ad.spend > 0 ? Math.round(ad.sales / ad.spend * 100) : 0,
      cvr: ad.clicks > 0 ? Math.round(ad.orders / ad.clicks * 10000) / 100 : 0,
      cpc: ad.clicks > 0 ? Math.round(ad.spend / ad.clicks) : 0,
    }));

  return {
    summary: {
      clicks: totalClicks,
      spend: totalSpend,
      sales: totalSales,
      orders: totalOrders,
      roas: totalSpend > 0 ? Math.round(totalSales / totalSpend * 100) : 0,
      cvr: totalClicks > 0 ? Math.round(totalOrders / totalClicks * 10000) / 100 : 0,
      cpc: totalClicks > 0 ? Math.round(totalSpend / totalClicks) : 0,
      cpa: totalOrders > 0 ? Math.round(totalSpend / totalOrders) : 0,
    },
    daily,
    products,
  };
}

// ============================
// 5. バスケット分析
// ============================
function calcBasketAnalysis(orders) {
  const orderItems = {};
  const productNames = {};
  const productOrderCount = {};

  orders.forEach(o => {
    const orderNum = o.orderNumber;
    const product = o.manageNumber || '';
    if (!product) return;
    if (!orderItems[orderNum]) orderItems[orderNum] = [];
    if (!orderItems[orderNum].includes(product)) orderItems[orderNum].push(product);
    if (o.itemName) productNames[product] = (o.itemName || '').substring(0, 30);
  });

  const pairCount = {};
  Object.values(orderItems).forEach(items => {
    items.forEach(p => { productOrderCount[p] = (productOrderCount[p] || 0) + 1; });
    if (items.length < 2) return;
    items.sort();
    for (let i = 0; i < items.length; i++) {
      for (let j = i + 1; j < items.length; j++) {
        const key = items[i] + '|||' + items[j];
        pairCount[key] = (pairCount[key] || 0) + 1;
      }
    }
  });

  // 注文サイズ分布
  const basketSizes = {};
  Object.values(orderItems).forEach(items => {
    basketSizes[items.length] = (basketSizes[items.length] || 0) + 1;
  });
  const sizeDistribution = Object.entries(basketSizes)
    .sort((a, b) => Number(a[0]) - Number(b[0]))
    .map(([size, count]) => ({ size: Number(size), count }));

  // トップ20ペア
  const totalOrders = Object.keys(orderItems).length;
  const pairs = Object.entries(pairCount)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 20)
    .map(([key, count]) => {
      const [a, b] = key.split('|||');
      const pA = (productOrderCount[a] || 0) / totalOrders;
      const pB = (productOrderCount[b] || 0) / totalOrders;
      const pAB = count / totalOrders;
      const lift = (pA * pB) > 0 ? Math.round(pAB / (pA * pB) * 100) / 100 : 0;
      return {
        productA: productNames[a] || a,
        productB: productNames[b] || b,
        count,
        lift,
      };
    });

  return { sizeDistribution, pairs, totalOrders };
}

// ============================
// 6. F2分析
// ============================
function calcF2Analysis(orders) {
  const customerOrders = {};
  const seen = {};

  orders.forEach(o => {
    const email = o.ordererEmailAddress || '';
    if (!email) return;
    const orderNum = o.orderNumber;
    if (seen[orderNum]) return;
    seen[orderNum] = true;
    const dt = parseDate(o.orderDatetime);
    if (!dt) return;
    if (!customerOrders[email]) customerOrders[email] = [];
    customerOrders[email].push({ date: dt, totalPrice: num(o.totalPrice) });
  });

  Object.values(customerOrders).forEach(arr => arr.sort((a, b) => a.date - b.date));

  const totalCustomers = Object.keys(customerOrders).length;

  // F分布
  const fDist = {};
  const fSales = {};
  Object.values(customerOrders).forEach(arr => {
    const f = arr.length;
    fDist[f] = (fDist[f] || 0) + 1;
    fSales[f] = (fSales[f] || 0) + arr.reduce((s, o) => s + o.totalPrice, 0);
  });

  const f1 = fDist[1] || 0;
  const f2plus = totalCustomers - f1;
  const f2Rate = totalCustomers > 0 ? Math.round(f2plus / totalCustomers * 10000) / 100 : 0;

  const maxF = Math.min(Math.max(...Object.keys(fDist).map(Number), 1), 10);
  const distribution = [];
  for (let f = 1; f <= maxF; f++) {
    const c = fDist[f] || 0;
    distribution.push({
      f,
      count: c,
      ratio: totalCustomers > 0 ? Math.round(c / totalCustomers * 10000) / 100 : 0,
      sales: fSales[f] || 0,
    });
  }
  let overCount = 0, overSales = 0;
  Object.keys(fDist).forEach(f => {
    if (Number(f) > maxF) { overCount += fDist[f]; overSales += fSales[f] || 0; }
  });
  if (overCount > 0) {
    distribution.push({ f: (maxF + 1) + '+', count: overCount, ratio: Math.round(overCount / totalCustomers * 10000) / 100, sales: overSales });
  }

  // 月別 新規/リピート
  const firstMonth = {};
  Object.entries(customerOrders).forEach(([email, arr]) => {
    firstMonth[email] = Utilities.formatDate(arr[0].date, 'Asia/Tokyo', 'yyyy-MM');
  });

  const monthlyNR = {};
  const seen2 = {};
  orders.forEach(o => {
    const email = o.ordererEmailAddress || '';
    const orderNum = o.orderNumber;
    if (!email || seen2[orderNum]) return;
    seen2[orderNum] = true;
    const dt = parseDate(o.orderDatetime);
    if (!dt) return;
    const ym = Utilities.formatDate(dt, 'Asia/Tokyo', 'yyyy-MM');
    if (!monthlyNR[ym]) monthlyNR[ym] = { newCount: 0, repeatCount: 0 };
    if (firstMonth[email] === ym) monthlyNR[ym].newCount++;
    else monthlyNR[ym].repeatCount++;
  });

  const months = Object.keys(monthlyNR).sort();
  const monthlyData = months.map(ym => ({
    month: ym,
    newCount: monthlyNR[ym].newCount,
    repeatCount: monthlyNR[ym].repeatCount,
    repeatRate: (monthlyNR[ym].newCount + monthlyNR[ym].repeatCount) > 0
      ? Math.round(monthlyNR[ym].repeatCount / (monthlyNR[ym].newCount + monthlyNR[ym].repeatCount) * 10000) / 100 : 0,
  }));

  return { totalCustomers, f1, f2plus, f2Rate, distribution, monthlyData };
}
