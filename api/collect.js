import { getDb, insertSnapshot, getSnapshots } from '../lib/db.js';
import { fetchVaultDetails, fetchMarketContext, parseVaultData, computeSignalScores, computeTrailingSignals, parseAllTimeframes } from '../lib/hyperliquid.js';

export default async function handler(req, res) {
  // Handle CORS preflight
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  // Auth: verify CRON_SECRET (from GitHub Actions or Vercel cron)
  const authHeader = req.headers['authorization'];
  const cronSecret = process.env.CRON_SECRET;

  if (cronSecret && authHeader !== `Bearer ${cronSecret}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  try {
    const sql = getDb();

    // Fetch live data from Hyperliquid + market context in parallel
    const [raw, marketCtx] = await Promise.all([
      fetchVaultDetails(),
      fetchMarketContext().catch(() => ({ fundingRate: 0, openInterest: 0, volume24h: 0 }))
    ]);
    const parsed = parseVaultData(raw);

    // Get trailing 30-day snapshots from DB for signal computation
    const trailing = await getSnapshots(sql, '30d', 'hourly');

    // Compute trailing signals from DB history (includes oiScore)
    const trailingSignals = computeTrailingSignals(trailing, parsed.currentNav);

    // Compute full signal scores (7 signals)
    const scores = computeSignalScores({
      ...parsed,
      tvlScore: trailingSignals.tvlScore,
      momentumScore: trailingSignals.momentumScore,
      volScore: trailingSignals.volScore,
      fundingRate: marketCtx.fundingRate,
      oiScore: trailingSignals.oiScore
    });

    // Build snapshot row
    const snapshot = {
      collected_at: new Date().toISOString(),
      nav: parsed.currentNav,
      pnl: parsed.currentPnl,
      apr: parsed.apr,
      vlm: parsed.vlm,
      allow_deposits: parsed.allowDeposits !== false,
      nav_ath: parsed.ath,
      drawdown_pct: parsed.currentDrawdown,
      max_drawdown: parsed.maxDD,
      composite_score: scores.composite,
      dd_score: scores.ddScore,
      tvl_score: scores.tvlScore,
      momentum_score: scores.momentumScore,
      vol_score: scores.volScore,
      apr_score: scores.aprScore,
      funding_rate: marketCtx.fundingRate,
      open_interest: marketCtx.openInterest,
      volume_24h: marketCtx.volume24h,
      funding_score: scores.fundingScore,
      oi_score: scores.oiScore
    };

    const inserted = await insertSnapshot(sql, snapshot);

    // ── Daily gap-fill: at hour 0, backfill from month/week timeframes ──
    let gapFilled = 0;
    const currentHour = new Date().getUTCHours();
    if (currentHour === 0) {
      try {
        const timeframes = parseAllTimeframes(raw);
        for (const tf of ['month', 'week']) {
          const tfData = timeframes[tf];
          if (!tfData) continue;

          const pnlMap = new Map();
          for (const p of tfData.pnlHistory) {
            pnlMap.set(p.time, p.value);
          }

          // Compute ATH/drawdown for this timeframe's points
          let ath = 0;
          let maxDD = 0;
          for (const point of tfData.navHistory) {
            if (point.value > ath) ath = point.value;
            const dd = ath > 0 ? (point.value - ath) / ath : 0;
            if (dd < maxDD) maxDD = dd;

            const gapSnapshot = {
              collected_at: new Date(point.time).toISOString(),
              nav: point.value,
              pnl: pnlMap.get(point.time) ?? null,
              apr: null,
              nav_ath: ath,
              drawdown_pct: dd,
              max_drawdown: maxDD,
              allow_deposits: true
            };

            const gapInserted = await insertSnapshot(sql, gapSnapshot);
            if (gapInserted) gapFilled++;
          }
        }
      } catch (gapErr) {
        console.warn('Gap-fill failed:', gapErr.message);
      }
    }

    return res.status(200).json({
      success: true,
      snapshot: inserted || snapshot,
      skipped: !inserted,
      gapFilled
    });
  } catch (err) {
    console.error('Collect error:', err);
    return res.status(500).json({ error: err.message });
  }
}
