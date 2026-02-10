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

  // ── 1. Create table (with nullable apr/pnl) ──────────────────
  console.log('Creating snapshots table...');
  await sql`
    CREATE TABLE IF NOT EXISTS snapshots (
      id              SERIAL PRIMARY KEY,
      collected_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      nav             NUMERIC NOT NULL,
      pnl             NUMERIC,
      apr             NUMERIC,
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

  // Make apr/pnl nullable if they were NOT NULL from a previous schema
  await sql`ALTER TABLE snapshots ALTER COLUMN apr DROP NOT NULL`;
  await sql`ALTER TABLE snapshots ALTER COLUMN pnl DROP NOT NULL`;

  // ── 1b. Add market context + new signal columns ─────────────
  console.log('Adding market context columns...');
  await sql`ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS funding_rate NUMERIC`;
  await sql`ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS open_interest NUMERIC`;
  await sql`ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS volume_24h NUMERIC`;
  await sql`ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS funding_score INTEGER`;
  await sql`ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS oi_score INTEGER`;

  // ── 2. Create indexes ─────────────────────────────────────────
  console.log('Creating indexes...');
  await sql`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshots_hourly
    ON snapshots (date_trunc('hour', collected_at AT TIME ZONE 'UTC'))
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS idx_snapshots_time
    ON snapshots (collected_at DESC)
  `;

  console.log('Table and indexes ready.');

  // ── 2b. Truncate existing data for a clean re-seed ────────────
  console.log('Truncating existing snapshots for clean re-seed...');
  await sql`TRUNCATE TABLE snapshots RESTART IDENTITY`;

  // ── 3. Fetch Hyperliquid vaultDetails (all timeframes) ────────
  console.log('\nFetching Hyperliquid vaultDetails...');
  const hlResp = await fetch(API_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type: 'vaultDetails', vaultAddress: HLP_VAULT })
  });

  if (!hlResp.ok) {
    console.error(`Hyperliquid API error: ${hlResp.status}`);
    process.exit(1);
  }

  const hlData = await hlResp.json();
  const portfolioMap = Object.fromEntries(hlData.portfolio);

  const summary = { allTime: 0, month: 0, week: 0, day: 0 };

  // ── Helper: insert a batch of points ──────────────────────────
  async function insertPoints(points, source) {
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
            ${p.nav}, ${p.pnl ?? null}, ${p.apr ?? null}, ${null}, ${true},
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
        skipped++;
      }
    }

    console.log(`  ${source}: ${inserted} inserted, ${skipped} skipped (duplicate hour)`);
    return inserted;
  }

  // ── Helper: compute ATH/drawdown/maxDD for a nav+pnl series ──
  function computeWithATH(navHistory, pnlMap) {
    let ath = 0;
    let maxDD = 0;
    return navHistory.map(point => {
      if (point.value > ath) ath = point.value;
      const dd = ath > 0 ? (point.value - ath) / ath : 0;
      if (dd < maxDD) maxDD = dd;
      return {
        time: point.time,
        nav: point.value,
        pnl: pnlMap ? (pnlMap.get(point.time) ?? null) : null,
        apr: null, // Historical APR unknown
        ath,
        dd,
        maxDD
      };
    });
  }

  // ── 4a. Seed from allTime ─────────────────────────────────────
  // NOTE: Only using HL timeframes that measure total account value.
  // DeFiLlama TVL and perpAllTime measure different things (TVL ≠ account value,
  // perpAllTime = perps-only subset) and cause ~36% zig-zag artifacts when mixed.
  const allTime = portfolioMap['allTime'];
  if (allTime) {
    console.log('\n[Source 1/4] Hyperliquid allTime...');
    const navHistory = allTime.accountValueHistory
      .map(([ts, val]) => ({ time: ts, value: parseFloat(val) }))
      .filter(d => d.value > 0);

    const pnlMap = new Map();
    for (const [ts, val] of allTime.pnlHistory) {
      pnlMap.set(ts, parseFloat(val));
    }

    const points = computeWithATH(navHistory, pnlMap);
    summary.allTime = await insertPoints(points, 'allTime');
  }

  // ── 4b-d. Seed from month, week, day ──────────────────────────
  const hlTimeframes = [
    { key: 'month', label: 'month', sourceNum: 2 },
    { key: 'week', label: 'week', sourceNum: 3 },
    { key: 'day', label: 'day', sourceNum: 4 },
  ];

  for (const { key, label, sourceNum } of hlTimeframes) {
    const tfData = portfolioMap[key];
    if (!tfData) {
      console.log(`\n[Source ${sourceNum}/4] Hyperliquid ${label}: not available`);
      continue;
    }

    console.log(`\n[Source ${sourceNum}/4] Hyperliquid ${label}...`);
    const navHistory = tfData.accountValueHistory
      .map(([ts, val]) => ({ time: ts, value: parseFloat(val) }))
      .filter(d => d.value > 0);

    const pnlMap = new Map();
    for (const [ts, val] of tfData.pnlHistory) {
      pnlMap.set(ts, parseFloat(val));
    }

    const points = computeWithATH(navHistory, pnlMap);
    summary[key] = await insertPoints(points, label);
  }

  // ── 5. Recompute ATH/drawdown/maxDD globally across all rows ──
  console.log('\n[Backfill 1/2] Recomputing ATH/drawdown/maxDD across all rows...');
  const allRows = await sql`
    SELECT id, nav, collected_at FROM snapshots ORDER BY collected_at ASC
  `;

  let globalAth = 0;
  let globalMaxDD = 0;
  let athUpdated = 0;

  for (const row of allRows) {
    const nav = parseFloat(row.nav);
    if (nav > globalAth) globalAth = nav;
    const dd = globalAth > 0 ? (nav - globalAth) / globalAth : 0;
    if (dd < globalMaxDD) globalMaxDD = dd;

    await sql`
      UPDATE snapshots
      SET nav_ath = ${globalAth}, drawdown_pct = ${dd}, max_drawdown = ${globalMaxDD}
      WHERE id = ${row.id}
    `;
    athUpdated++;
  }
  console.log(`  Updated ATH/drawdown on ${athUpdated} rows`);

  // ── 6. Backfill signal scores for rows with NULL composite_score
  console.log('\n[Backfill 2/2] Computing signal scores for rows missing them...');
  const nullScoreRows = await sql`
    SELECT id, nav, pnl, apr, collected_at, nav_ath, drawdown_pct, max_drawdown
    FROM snapshots
    WHERE composite_score IS NULL
    ORDER BY collected_at ASC
  `;

  // Load all rows for trailing window lookups
  const allSnaps = await sql`
    SELECT id, nav, collected_at FROM snapshots ORDER BY collected_at ASC
  `;

  let scoresComputed = 0;

  for (const row of nullScoreRows) {
    const rowTime = new Date(row.collected_at).getTime();
    const thirtyDaysAgo = rowTime - 30 * 24 * 3600 * 1000;

    // Get trailing 30-day window
    const trailing = allSnaps.filter(s => {
      const t = new Date(s.collected_at).getTime();
      return t >= thirtyDaysAgo && t <= rowTime;
    });

    if (trailing.length < 2) continue;

    const currentNav = parseFloat(row.nav);
    const navAth = parseFloat(row.nav_ath);
    const currentDrawdown = parseFloat(row.drawdown_pct);

    // Compute trailing signals
    const now = rowTime;
    const snap7d = trailing.filter(s => new Date(s.collected_at).getTime() >= now - 7 * 24 * 3600 * 1000);
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

    // Return momentum
    const returns = [];
    for (let i = 1; i < trailing.length; i++) {
      const prev = parseFloat(trailing[i - 1].nav);
      const curr = parseFloat(trailing[i].nav);
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

    // Drawdown score
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

    // APR score — NULL for historical rows, use neutral score
    const aprPct = row.apr != null ? parseFloat(row.apr) * 100 : null;
    let aprScore;
    if (aprPct == null) aprScore = 50; // neutral when unknown
    else if (aprPct > 40) aprScore = 15;
    else if (aprPct > 25) aprScore = 30;
    else if (aprPct > 15) aprScore = 50;
    else if (aprPct > 8) aprScore = 65;
    else if (aprPct > 3) aprScore = 75;
    else aprScore = 90;

    // Historical rows get neutral funding/OI scores
    const fundingScore = 50;
    const oiScore = 50;

    const composite = Math.round(
      ddScore * 0.25 +
      tvlScore * 0.15 +
      momentumScore * 0.15 +
      volScore * 0.15 +
      aprScore * 0.05 +
      fundingScore * 0.15 +
      oiScore * 0.10
    );

    await sql`
      UPDATE snapshots
      SET composite_score = ${composite},
          dd_score = ${ddScore},
          tvl_score = ${tvlScore},
          momentum_score = ${momentumScore},
          vol_score = ${volScore},
          apr_score = ${aprScore},
          funding_score = ${fundingScore},
          oi_score = ${oiScore}
      WHERE id = ${row.id}
    `;
    scoresComputed++;
  }
  console.log(`  Computed signal scores for ${scoresComputed} rows`);

  // ── 7. Print summary ──────────────────────────────────────────
  const totalRows = await sql`SELECT COUNT(*) AS count FROM snapshots`;
  const scoredRows = await sql`SELECT COUNT(*) AS count FROM snapshots WHERE composite_score IS NOT NULL`;
  const dateRange = await sql`SELECT MIN(collected_at) AS first, MAX(collected_at) AS last FROM snapshots`;

  console.log('\n════════════════════════════════════════════');
  console.log('  SEED SUMMARY');
  console.log('════════════════════════════════════════════');
  console.log(`  Total rows in DB:    ${totalRows[0].count}`);
  console.log(`  Rows with scores:    ${scoredRows[0].count}`);
  console.log(`  Date range:          ${new Date(dateRange[0].first).toISOString().slice(0, 10)} → ${new Date(dateRange[0].last).toISOString().slice(0, 10)}`);
  console.log('  Inserted by source:');
  console.log(`    allTime:           ${summary.allTime}`);
  console.log(`    month:             ${summary.month}`);
  console.log(`    week:              ${summary.week}`);
  console.log(`    day:               ${summary.day}`);
  console.log('════════════════════════════════════════════');
}

main().catch(err => {
  console.error('Setup failed:', err);
  process.exit(1);
});
