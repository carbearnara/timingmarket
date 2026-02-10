import { getDb, insertSnapshot, getSnapshots } from '../lib/db.js';
import { fetchVaultDetails, parseVaultData, computeSignalScores, computeTrailingSignals } from '../lib/hyperliquid.js';

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

    // Fetch live data from Hyperliquid
    const raw = await fetchVaultDetails();
    const parsed = parseVaultData(raw);

    // Get trailing 30-day snapshots from DB for signal computation
    const trailing = await getSnapshots(sql, '30d', 'hourly');

    // Compute trailing signals from DB history
    const trailingSignals = computeTrailingSignals(trailing, parsed.currentNav);

    // Compute full signal scores
    const scores = computeSignalScores({
      ...parsed,
      tvlScore: trailingSignals.tvlScore,
      momentumScore: trailingSignals.momentumScore,
      volScore: trailingSignals.volScore
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
      apr_score: scores.aprScore
    };

    const inserted = await insertSnapshot(sql, snapshot);

    return res.status(200).json({
      success: true,
      snapshot: inserted || snapshot,
      skipped: !inserted
    });
  } catch (err) {
    console.error('Collect error:', err);
    return res.status(500).json({ error: err.message });
  }
}
