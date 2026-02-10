import { neon } from '@neondatabase/serverless';
import { config } from 'dotenv';

// Load .env.local for local execution
config({ path: '.env.local' });

const HLP_VAULT = '0xdfc24b077bc1425ad1dea75bcb6f8158e10df303';
const API_URL = 'https://api.hyperliquid.xyz/info';

async function main() {
  if (!process.env.DATABASE_URL) {
    console.error('ERROR: DATABASE_URL not set. Add it to .env.local');
    process.exit(1);
  }

  const sql = neon(process.env.DATABASE_URL);

  // 1. Create table
  console.log('Creating snapshots table...');
  await sql`
    CREATE TABLE IF NOT EXISTS snapshots (
      id              SERIAL PRIMARY KEY,
      collected_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      nav             NUMERIC NOT NULL,
      pnl             NUMERIC NOT NULL,
      apr             NUMERIC NOT NULL,
      vlm             NUMERIC,
      allow_deposits  BOOLEAN DEFAULT TRUE,
      nav_ath         NUMERIC NOT NULL,
      drawdown_pct    NUMERIC NOT NULL,
      max_drawdown    NUMERIC NOT NULL,
      composite_score INTEGER,
      dd_score        INTEGER,
      tvl_score       INTEGER,
      momentum_score  INTEGER,
      vol_score       INTEGER,
      apr_score       INTEGER
    )
  `;

  // 2. Create indexes
  console.log('Creating indexes...');
  await sql`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshots_hourly
    ON snapshots (date_trunc('hour', collected_at AT TIME ZONE 'UTC'))
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS idx_snapshots_time
    ON snapshots (collected_at DESC)
  `;

  console.log('Table and indexes created.');

  // 3. Seed with allTime data from Hyperliquid
  console.log('Fetching allTime data from Hyperliquid API...');

  const resp = await fetch(API_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type: 'vaultDetails', vaultAddress: HLP_VAULT })
  });

  if (!resp.ok) {
    console.error(`API error: ${resp.status}`);
    process.exit(1);
  }

  const data = await resp.json();
  const portfolioMap = Object.fromEntries(data.portfolio);
  const allTime = portfolioMap['allTime'];

  if (!allTime) {
    console.error('No allTime portfolio data');
    process.exit(1);
  }

  const navHistory = allTime.accountValueHistory
    .map(([ts, val]) => ({ time: ts, value: parseFloat(val) }))
    .filter(d => d.value > 0);

  const pnlHistory = allTime.pnlHistory
    .map(([ts, val]) => ({ time: ts, value: parseFloat(val) }));

  // Build a PnL lookup by timestamp
  const pnlMap = new Map();
  for (const p of pnlHistory) {
    pnlMap.set(p.time, p.value);
  }

  // Compute ATH and drawdown for each point
  let ath = 0;
  let maxDD = 0;
  const points = navHistory.map(point => {
    if (point.value > ath) ath = point.value;
    const dd = (point.value - ath) / ath;
    if (dd < maxDD) maxDD = dd;
    return {
      time: point.time,
      nav: point.value,
      pnl: pnlMap.get(point.time) || 0,
      ath,
      dd,
      maxDD
    };
  });

  console.log(`Seeding ${points.length} historical snapshots...`);
  let inserted = 0;
  let skipped = 0;

  for (const p of points) {
    try {
      const result = await sql`
        INSERT INTO snapshots (
          collected_at, nav, pnl, apr, vlm, allow_deposits,
          nav_ath, drawdown_pct, max_drawdown
        ) VALUES (
          ${new Date(p.time).toISOString()},
          ${p.nav}, ${p.pnl}, ${data.apr || 0}, ${null}, ${true},
          ${p.ath}, ${p.dd}, ${p.maxDD}
        )
        ON CONFLICT (date_trunc('hour', collected_at AT TIME ZONE 'UTC')) DO NOTHING
        RETURNING id
      `;
      if (result.length > 0) {
        inserted++;
      } else {
        skipped++;
      }
    } catch (err) {
      // Skip duplicate hour conflicts
      skipped++;
    }
  }

  console.log(`Done! Inserted: ${inserted}, Skipped (duplicate hour): ${skipped}`);
  console.log(`Total data points available: ${points.length}`);
}

main().catch(err => {
  console.error('Setup failed:', err);
  process.exit(1);
});
