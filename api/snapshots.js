import { getDb, getSnapshots } from '../lib/db.js';

export default async function handler(req, res) {
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  try {
    const sql = getDb();

    const range = req.query.range || 'all';
    const resolution = req.query.resolution || 'auto';

    const validRanges = ['24h', '7d', '30d', '90d', '1y', 'all'];
    if (!validRanges.includes(range)) {
      return res.status(400).json({ error: `Invalid range. Use: ${validRanges.join(', ')}` });
    }

    const snapshots = await getSnapshots(sql, range, resolution);

    const effectiveResolution = resolution === 'auto'
      ? ((range === '24h' || range === '7d') ? 'hourly' : 'daily')
      : resolution;

    return res.status(200).json({
      snapshots,
      meta: {
        count: snapshots.length,
        range,
        resolution: effectiveResolution,
        oldest: snapshots[0]?.collected_at || null,
        newest: snapshots[snapshots.length - 1]?.collected_at || null
      }
    });
  } catch (err) {
    console.error('Snapshots error:', err);
    return res.status(500).json({ error: err.message });
  }
}
