const HLP_VAULT = '0xdfc24b077bc1425ad1dea75bcb6f8158e10df303';
const API_URL = 'https://api.hyperliquid.xyz/info';

export async function fetchMarketContext() {
  const resp = await fetch(API_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type: 'metaAndAssetCtxs' })
  });
  if (!resp.ok) throw new Error(`Hyperliquid market context API error: ${resp.status}`);
  const [meta, assetCtxs] = await resp.json();

  let totalOI = 0;
  let totalVolume = 0;
  let weightedFunding = 0;
  let totalOIForWeighting = 0;

  for (const ctx of assetCtxs) {
    const oi = parseFloat(ctx.openInterest || 0);
    const vol = parseFloat(ctx.dayNtlVlm || 0);
    const funding = parseFloat(ctx.funding || 0);

    totalOI += oi;
    totalVolume += vol;
    if (oi > 0) {
      weightedFunding += funding * oi;
      totalOIForWeighting += oi;
    }
  }

  const fundingRate = totalOIForWeighting > 0 ? weightedFunding / totalOIForWeighting : 0;

  return { fundingRate, openInterest: totalOI, volume24h: totalVolume };
}

export async function fetchVaultDetails() {
  const resp = await fetch(API_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type: 'vaultDetails', vaultAddress: HLP_VAULT })
  });
  if (!resp.ok) throw new Error(`Hyperliquid API error: ${resp.status}`);
  return resp.json();
}

export function parseVaultData(data) {
  const portfolioMap = Object.fromEntries(data.portfolio);
  const allTime = portfolioMap['allTime'];

  if (!allTime) throw new Error('No allTime portfolio data');

  const navHistory = allTime.accountValueHistory
    .map(([ts, val]) => ({ time: ts, value: parseFloat(val) }))
    .filter(d => d.value > 0);

  const pnlHistory = allTime.pnlHistory
    .map(([ts, val]) => ({ time: ts, value: parseFloat(val) }))
    .filter(d => d.time > (navHistory[0]?.time || 0));

  const currentNav = navHistory[navHistory.length - 1]?.value || 0;
  const currentPnl = pnlHistory[pnlHistory.length - 1]?.value || 0;

  // Compute ATH and drawdown history
  let ath = 0;
  const drawdownHistory = [];
  for (const point of navHistory) {
    if (point.value > ath) ath = point.value;
    const dd = (point.value - ath) / ath;
    drawdownHistory.push({ time: point.time, value: dd });
  }
  const currentDrawdown = ath > 0 ? (currentNav - ath) / ath : 0;

  // Max drawdown
  let maxDD = 0;
  for (const d of drawdownHistory) {
    if (d.value < maxDD) maxDD = d.value;
  }

  return {
    navHistory,
    pnlHistory,
    drawdownHistory,
    currentNav,
    currentPnl,
    ath,
    currentDrawdown,
    maxDD,
    apr: data.apr || 0,
    vlm: data.vlm || 0,
    allowDeposits: data.allowDeposits,
    maxDistributable: data.maxDistributable || 0,
    portfolio: data.portfolio
  };
}

// Compute signal scores from analytics data (same logic as dashboard)
export function computeSignalScores(analytics) {
  const { currentDrawdown, currentNav, ath } = analytics;

  // 1. Drawdown from ATH (25%)
  const ddPct = Math.abs(currentDrawdown) * 100;
  let ddScore;
  if (ddPct < 0.1) ddScore = 5;
  else if (ddPct < 0.5) ddScore = 15;
  else if (ddPct < 1) ddScore = 25;
  else if (ddPct < 2) ddScore = 40;
  else if (ddPct < 3) ddScore = 55;
  else if (ddPct < 5) ddScore = 70;
  else if (ddPct < 7) ddScore = 85;
  else if (ddPct < 9) ddScore = 92;
  else ddScore = 98;

  // 2. TVL Momentum (15%) — requires trailing data
  const tvlScore = analytics.tvlScore ?? 50;

  // 3. Return Momentum (15%) — requires trailing data
  const momentumScore = analytics.momentumScore ?? 50;

  // 4. Volatility Regime (15%) — requires trailing data
  const volScore = analytics.volScore ?? 50;

  // 5. APR Relative Value (5%)
  const aprPct = (analytics.apr || 0) * 100;
  let aprScore;
  if (aprPct > 40) aprScore = 15;
  else if (aprPct > 25) aprScore = 30;
  else if (aprPct > 15) aprScore = 50;
  else if (aprPct > 8) aprScore = 65;
  else if (aprPct > 3) aprScore = 75;
  else aprScore = 90;

  // 6. Funding Rate (15%)
  const fundingBps = (analytics.fundingRate || 0) * 10000; // basis points per 8h
  let fundingScore;
  if (fundingBps > 5) fundingScore = 90;
  else if (fundingBps > 2) fundingScore = 75;
  else if (fundingBps > 0.5) fundingScore = 60;
  else if (fundingBps > -0.5) fundingScore = 45;
  else if (fundingBps > -2) fundingScore = 25;
  else fundingScore = 15;

  // 7. Open Interest Trend (10%)
  const oiScore = analytics.oiScore ?? 50;

  const composite = Math.round(
    ddScore * 0.25 +
    tvlScore * 0.15 +
    momentumScore * 0.15 +
    volScore * 0.15 +
    aprScore * 0.05 +
    fundingScore * 0.15 +
    oiScore * 0.10
  );

  return { composite, ddScore, tvlScore, momentumScore, volScore, aprScore, fundingScore, oiScore };
}

// Fetch DeFiLlama daily TVL for HLP
export async function fetchDeFiLlamaTVL() {
  const resp = await fetch('https://api.llama.fi/protocol/hyperliquid-hlp');
  if (!resp.ok) throw new Error(`DeFiLlama API error: ${resp.status}`);
  const data = await resp.json();
  // tvl array: each entry has { date (unix seconds), totalLiquidityUSD }
  const tvl = data.tvl || data.chainTvls?.Hyperliquid?.tvl || [];
  return tvl.map(d => ({
    time: d.date * 1000, // convert to ms
    value: d.totalLiquidityUSD
  })).filter(d => d.value > 0);
}

// Parse all timeframes from vaultDetails response
export function parseAllTimeframes(data) {
  const portfolioMap = Object.fromEntries(data.portfolio);
  const timeframes = ['allTime', 'perpAllTime', 'month', 'week', 'day'];
  const result = {};

  for (const tf of timeframes) {
    const tfData = portfolioMap[tf];
    if (!tfData) continue;

    const navHistory = tfData.accountValueHistory
      .map(([ts, val]) => ({ time: ts, value: parseFloat(val) }))
      .filter(d => d.value > 0);

    const pnlHistory = tfData.pnlHistory
      .map(([ts, val]) => ({ time: ts, value: parseFloat(val) }));

    result[tf] = { navHistory, pnlHistory };
  }

  return result;
}

// Compute trailing signals from DB snapshot history
export function computeTrailingSignals(snapshots, currentNav) {
  if (!snapshots || snapshots.length < 2) {
    return { tvlScore: 50, momentumScore: 50, volScore: 50 };
  }

  // TVL/NAV momentum — compare current to 7 days ago
  const now = Date.now();
  const snap7d = snapshots.filter(s => new Date(s.collected_at).getTime() >= now - 7 * 24 * 3600 * 1000);
  const snap30d = snapshots;

  const nav7dAgo = snap7d.length > 0 ? parseFloat(snap7d[0].nav) : currentNav;
  const tvl7 = nav7dAgo > 0 ? ((currentNav - nav7dAgo) / nav7dAgo) * 100 : 0;

  let tvlScore;
  if (tvl7 > 3) tvlScore = 10;
  else if (tvl7 > 1) tvlScore = 25;
  else if (tvl7 > 0) tvlScore = 40;
  else if (tvl7 > -1) tvlScore = 55;
  else if (tvl7 > -3) tvlScore = 70;
  else if (tvl7 > -5) tvlScore = 85;
  else tvlScore = 95;

  // Return momentum — compute daily returns from snapshots
  const recent = snapshots.slice(-30);
  const returns = [];
  for (let i = 1; i < recent.length; i++) {
    const prev = parseFloat(recent[i - 1].nav);
    const curr = parseFloat(recent[i].nav);
    if (prev > 0) returns.push((curr - prev) / prev);
  }

  const recent7Returns = returns.slice(-7);
  const recent7Avg = recent7Returns.length > 0
    ? recent7Returns.reduce((s, v) => s + v, 0) / recent7Returns.length : 0;

  let momentumScore;
  if (recent7Avg > 0.003) momentumScore = 10;
  else if (recent7Avg > 0.001) momentumScore = 25;
  else if (recent7Avg > 0) momentumScore = 40;
  else if (recent7Avg > -0.001) momentumScore = 55;
  else if (recent7Avg > -0.003) momentumScore = 70;
  else if (recent7Avg > -0.01) momentumScore = 85;
  else momentumScore = 95;

  // Volatility regime
  const avgReturn = returns.length > 0
    ? returns.reduce((s, v) => s + v, 0) / returns.length : 0;
  const variance = returns.length > 1
    ? returns.reduce((s, v) => s + Math.pow(v - avgReturn, 2), 0) / (returns.length - 1) : 0;
  const currentVol = Math.sqrt(variance);

  const firstHalf = returns.slice(0, Math.floor(returns.length / 2));
  const firstAvg = firstHalf.length > 0 ? firstHalf.reduce((s, v) => s + v, 0) / firstHalf.length : 0;
  const firstVar = firstHalf.length > 1
    ? firstHalf.reduce((s, v) => s + Math.pow(v - firstAvg, 2), 0) / (firstHalf.length - 1) : 0;
  const priorVol = Math.sqrt(firstVar);

  const volTrend = priorVol > 0 ? (currentVol - priorVol) / priorVol : 0;
  const vol7dAnnualized = currentVol * Math.sqrt(365) * 100;

  let volScore;
  if (volTrend < -0.3 && vol7dAnnualized > 10) volScore = 90;
  else if (volTrend < -0.1) volScore = 70;
  else if (Math.abs(volTrend) < 0.1) volScore = 50;
  else if (volTrend < 0.3) volScore = 35;
  else volScore = 15;

  // OI trend — compare latest OI to 7-day-ago snapshot OI
  let oiScore = 50; // default neutral
  const snapsWithOI = snapshots.filter(s => s.open_interest != null && parseFloat(s.open_interest) > 0);
  if (snapsWithOI.length >= 2) {
    const latestOI = parseFloat(snapsWithOI[snapsWithOI.length - 1].open_interest);
    const sevenDaysAgoTime = now - 7 * 24 * 3600 * 1000;
    const snap7dOI = snapsWithOI.filter(s => new Date(s.collected_at).getTime() >= sevenDaysAgoTime);
    const oiAgo = snap7dOI.length > 0 ? parseFloat(snap7dOI[0].open_interest) : latestOI;
    if (oiAgo > 0) {
      const oiChange = (latestOI - oiAgo) / oiAgo;
      if (oiChange > 0.10) oiScore = 90;       // surging flow
      else if (oiChange > 0.03) oiScore = 70;
      else if (oiChange > -0.03) oiScore = 50;  // stable
      else if (oiChange > -0.05) oiScore = 30;
      else oiScore = 15;                         // collapsing flow
    }
  }

  return { tvlScore, momentumScore, volScore, oiScore };
}
