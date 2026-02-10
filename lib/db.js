import { neon } from '@neondatabase/serverless';

export function getDb() {
  if (!process.env.DATABASE_URL) {
    throw new Error('DATABASE_URL environment variable is not set');
  }
  return neon(process.env.DATABASE_URL);
}

export async function insertSnapshot(sql, data) {
  const result = await sql`
    INSERT INTO snapshots (
      collected_at, nav, pnl, apr, vlm, allow_deposits,
      nav_ath, drawdown_pct, max_drawdown,
      composite_score, dd_score, tvl_score, momentum_score, vol_score, apr_score
    ) VALUES (
      ${data.collected_at || new Date().toISOString()},
      ${data.nav}, ${data.pnl}, ${data.apr}, ${data.vlm || null}, ${data.allow_deposits},
      ${data.nav_ath}, ${data.drawdown_pct}, ${data.max_drawdown},
      ${data.composite_score || null}, ${data.dd_score || null}, ${data.tvl_score || null},
      ${data.momentum_score || null}, ${data.vol_score || null}, ${data.apr_score || null}
    )
    ON CONFLICT (date_trunc('hour', collected_at AT TIME ZONE 'UTC')) DO NOTHING
    RETURNING id, collected_at
  `;
  return result[0] || null;
}

export async function getLatestSnapshot(sql) {
  const rows = await sql`
    SELECT * FROM snapshots ORDER BY collected_at DESC LIMIT 1
  `;
  return rows[0] || null;
}

export async function getSnapshots(sql, range = 'all', resolution = 'auto') {
  // Determine time cutoff
  const now = new Date();
  let cutoff = null;
  switch (range) {
    case '24h': cutoff = new Date(now - 24 * 60 * 60 * 1000); break;
    case '7d':  cutoff = new Date(now - 7 * 24 * 60 * 60 * 1000); break;
    case '30d': cutoff = new Date(now - 30 * 24 * 60 * 60 * 1000); break;
    case '90d': cutoff = new Date(now - 90 * 24 * 60 * 60 * 1000); break;
    case '1y':  cutoff = new Date(now - 365 * 24 * 60 * 60 * 1000); break;
    case 'all': cutoff = null; break;
    default: cutoff = null;
  }

  // Auto-select resolution: raw hourly for <=7d, daily for longer
  if (resolution === 'auto') {
    resolution = (range === '24h' || range === '7d') ? 'hourly' : 'daily';
  }

  if (resolution === 'daily' && cutoff) {
    // Aggregate to daily resolution
    return await sql`
      SELECT
        date_trunc('day', collected_at) AS collected_at,
        AVG(nav)::numeric AS nav,
        AVG(pnl)::numeric AS pnl,
        AVG(apr)::numeric AS apr,
        AVG(vlm)::numeric AS vlm,
        bool_and(allow_deposits) AS allow_deposits,
        MAX(nav_ath)::numeric AS nav_ath,
        AVG(drawdown_pct)::numeric AS drawdown_pct,
        MIN(max_drawdown)::numeric AS max_drawdown,
        AVG(composite_score)::integer AS composite_score,
        AVG(dd_score)::integer AS dd_score,
        AVG(tvl_score)::integer AS tvl_score,
        AVG(momentum_score)::integer AS momentum_score,
        AVG(vol_score)::integer AS vol_score,
        AVG(apr_score)::integer AS apr_score
      FROM snapshots
      WHERE collected_at >= ${cutoff.toISOString()}
      GROUP BY date_trunc('day', collected_at)
      ORDER BY collected_at ASC
    `;
  } else if (resolution === 'daily' && !cutoff) {
    // All time, daily
    return await sql`
      SELECT
        date_trunc('day', collected_at) AS collected_at,
        AVG(nav)::numeric AS nav,
        AVG(pnl)::numeric AS pnl,
        AVG(apr)::numeric AS apr,
        AVG(vlm)::numeric AS vlm,
        bool_and(allow_deposits) AS allow_deposits,
        MAX(nav_ath)::numeric AS nav_ath,
        AVG(drawdown_pct)::numeric AS drawdown_pct,
        MIN(max_drawdown)::numeric AS max_drawdown,
        AVG(composite_score)::integer AS composite_score,
        AVG(dd_score)::integer AS dd_score,
        AVG(tvl_score)::integer AS tvl_score,
        AVG(momentum_score)::integer AS momentum_score,
        AVG(vol_score)::integer AS vol_score,
        AVG(apr_score)::integer AS apr_score
      FROM snapshots
      GROUP BY date_trunc('day', collected_at)
      ORDER BY collected_at ASC
    `;
  } else if (cutoff) {
    // Raw hourly with time filter
    return await sql`
      SELECT * FROM snapshots
      WHERE collected_at >= ${cutoff.toISOString()}
      ORDER BY collected_at ASC
    `;
  } else {
    // All raw
    return await sql`
      SELECT * FROM snapshots ORDER BY collected_at ASC
    `;
  }
}
