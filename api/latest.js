import { getDb, getLatestSnapshot } from '../lib/db.js';
import { fetchVaultDetails, parseVaultData } from '../lib/hyperliquid.js';

export default async function handler(req, res) {
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  try {
    const sql = getDb();

    // Fetch DB snapshot and live data in parallel
    const [snapshot, raw] = await Promise.all([
      getLatestSnapshot(sql),
      fetchVaultDetails()
    ]);

    const live = parseVaultData(raw);

    return res.status(200).json({
      snapshot,
      live: {
        nav: live.currentNav,
        pnl: live.currentPnl,
        apr: live.apr,
        vlm: live.vlm,
        ath: live.ath,
        drawdown: live.currentDrawdown,
        maxDD: live.maxDD,
        allowDeposits: live.allowDeposits,
        maxDistributable: live.maxDistributable
      },
      history_available: !!snapshot
    });
  } catch (err) {
    console.error('Latest error:', err);
    return res.status(500).json({ error: err.message });
  }
}
