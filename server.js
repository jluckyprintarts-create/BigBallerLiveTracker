// MONOPOLY BIG BALLER LIVE TRACKER
// Node.js 18+

'use strict';

const express = require('express');
const path = require('path');
const { Pool } = require('pg');

const app = express();

app.use(express.json());

const PORT = process.env.PORT || 3000;

const POLL_INTERVAL_MS = Math.max(
  10000,
  Number(process.env.POLL_INTERVAL_MS || 30000)
);

const UPSTREAM_URL =
  process.env.UPSTREAM_URL ||
  'https://api-cs.casino.org/svc-evolution-game-events/api/bigballer?page=0&size=14&sort=data.settledAt,desc&duration=6&isThreeRolls=true,false&isFiveRolls=true,false';

if (!process.env.DATABASE_URL) {
  console.error('DATABASE_URL is required');
  process.exit(1);
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false
  }
});

let pollInProgress = false;
let lastPollAt = null;
let lastSuccessAt = null;
let lastPollError = null;
let lastFetchedCount = 0;
let lastSavedCount = 0;


// ----------------------------------
// DATABASE
// ----------------------------------

async function initializeDatabase() {

  await pool.query(`
    CREATE TABLE IF NOT EXISTS game_history (
      id BIGSERIAL PRIMARY KEY,
      event_id TEXT UNIQUE NOT NULL,
      settled_at TIMESTAMPTZ NOT NULL,
      adjusted_at TIMESTAMPTZ NOT NULL,
      is_three_rolls BOOLEAN DEFAULT FALSE,
      is_five_rolls BOOLEAN DEFAULT FALSE,
      is_big_win BOOLEAN DEFAULT FALSE,
      big_win_multiplier NUMERIC,
      dealer_name TEXT,
      raw_data JSONB,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_game_history_settled
    ON game_history(settled_at DESC)
  `);

}


// ----------------------------------
// HELPERS
// ----------------------------------

function getDate(value) {

  if (!value) return null;

  const date = typeof value === 'number'
    ? new Date(value < 1e12 ? value * 1000 : value)
    : new Date(value);

  return Number.isNaN(date.getTime()) ? null : date;

}


function getNumber(value) {

  if (
    value === undefined ||
    value === null ||
    value === ''
  ) {
    return null;
  }

  const number = Number(value);

  return Number.isFinite(number) ? number : null;

}


function extractRows(payload) {

  if (Array.isArray(payload)) return payload;

  for (const key of [
    'content',
    'events',
    'items',
    'data',
    'results',
    'rounds'
  ]) {

    if (Array.isArray(payload[key])) {
      return payload[key];
    }

  }

  if (
    payload.data &&
    Array.isArray(payload.data.content)
  ) {
    return payload.data.content;
  }

  return [];

}


// ----------------------------------
// GET BONUS ROUND MULTIPLIER
// ----------------------------------

function getBonusMultiplier(result, expectedMode) {

  if (!result || typeof result !== 'object') {
    return null;
  }

  const bonusRounds = Array.isArray(result.bonusRounds)
    ? result.bonusRounds
    : [];

  const matchingRound = bonusRounds.find(round => {

    const mode = String(
      round.gameMode ||
      round.mode ||
      round.gameType ||
      round.type ||
      ''
    );

    return new RegExp(expectedMode, 'i').test(mode);

  });

  let round = matchingRound;

  // Kung walang gameMode pero iisa lang ang bonus round,
  // gamitin ang nag-iisang round.
  if (!round && bonusRounds.length === 1) {
    round = bonusRounds[0];
  }

  if (round) {

    const value = getNumber(round.totalMultiplier);

    if (value !== null) {
      return value;
    }

  }

  // Fallback sa official result fields.
  if (expectedMode === 'ThreeRolls') {

    return getNumber(
      result.threeRollsTotalMultiplier
    );

  }

  if (expectedMode === 'FiveRolls') {

    return getNumber(
      result.fiveRollsTotalMultiplier
    );

  }

  return null;

}


// ----------------------------------
// NORMALIZE EVENT
// ----------------------------------

function normalizeEvent(raw) {

  if (!raw || typeof raw !== 'object') {
    return null;
  }

  const data = raw.data || {};

  const result =
    data.result ||
    raw.result ||
    {};

  const eventId =
    raw.id ||
    data.id ||
    raw.eventId ||
    data.eventId ||
    raw.roundId ||
    data.roundId;

  const settledAt = getDate(
    data.settledAt ||
    raw.settledAt ||
    data.finishedAt ||
    raw.finishedAt ||
    data.startedAt ||
    raw.startedAt
  );

  if (!eventId || !settledAt) {
    return null;
  }


  // ----------------------------------
  // DETECT ROLL TYPES
  // ----------------------------------

  let isThreeRolls = Boolean(
    result.isThreeRolls ||
    data.isThreeRolls ||
    raw.isThreeRolls
  );

  let isFiveRolls = Boolean(
    result.isFiveRolls ||
    data.isFiveRolls ||
    raw.isFiveRolls
  );


  const bonusRounds = Array.isArray(result.bonusRounds)
    ? result.bonusRounds
    : [];


  for (const round of bonusRounds) {

    if (!round || typeof round !== 'object') {
      continue;
    }

    const mode = String(
      round.gameMode ||
      round.mode ||
      round.gameType ||
      round.type ||
      ''
    );

    if (/three.?roll/i.test(mode)) {
      isThreeRolls = true;
    }

    if (/five.?roll/i.test(mode)) {
      isFiveRolls = true;
    }

  }


  // ----------------------------------
  // GET ACTUAL GAME MULTIPLIER
  // ----------------------------------

  let bigWinMultiplier = null;

  if (isThreeRolls) {

    bigWinMultiplier = getBonusMultiplier(
      result,
      'ThreeRolls'
    );

  }

  else if (isFiveRolls) {

    bigWinMultiplier = getBonusMultiplier(
      result,
      'FiveRolls'
    );

  }


  // ----------------------------------
  // BIG WIN DISABLED
  // ----------------------------------

  const isBigWin = false;


  // ----------------------------------
  // TIME ADJUSTMENT
  // ----------------------------------

  let adjustment = 0;

  if (isThreeRolls) {
    adjustment = 3;
  }

  if (isFiveRolls) {
    adjustment = 4;
  }

  const adjustedAt = new Date(
    settledAt.getTime() - adjustment * 60000
  );


  // ----------------------------------
  // HOST / DEALER NAME
  // ----------------------------------

  const dealer =
    data.dealer ||
    raw.dealer ||
    {};

  const host =
    data.host ||
    raw.host ||
    {};

  const dealerName = [

    data.host_name,
    data.hostName,

    raw.host_name,
    raw.hostName,

    data.dealerName,
    raw.dealerName,

    host.name,
    host.displayName,

    dealer.name,
    dealer.displayName

  ].find(value =>
    typeof value === 'string' &&
    value.trim() !== ''
  ) || null;


  // ----------------------------------
  // RETURN EVENT
  // ----------------------------------

  return {

    eventId: String(eventId),

    settledAt,

    adjustedAt,

    isThreeRolls,

    isFiveRolls,

    isBigWin,

    bigWinMultiplier,

    dealerName,

    raw

  };

}


// ----------------------------------
// SAVE EVENT
// ----------------------------------

async function saveEvent(event) {

  const result = await pool.query(

    `INSERT INTO game_history
    (
      event_id,
      settled_at,
      adjusted_at,
      is_three_rolls,
      is_five_rolls,
      is_big_win,
      big_win_multiplier,
      dealer_name,
      raw_data
    )

    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)

    ON CONFLICT (event_id)

    DO UPDATE SET

      settled_at = EXCLUDED.settled_at,

      adjusted_at = EXCLUDED.adjusted_at,

      is_three_rolls = EXCLUDED.is_three_rolls,

      is_five_rolls = EXCLUDED.is_five_rolls,

      is_big_win = EXCLUDED.is_big_win,

      big_win_multiplier = EXCLUDED.big_win_multiplier,

      dealer_name = COALESCE(
        EXCLUDED.dealer_name,
        game_history.dealer_name
      ),

      raw_data = EXCLUDED.raw_data

    RETURNING (xmax = 0) AS inserted`,

    [

      event.eventId,
      event.settledAt,
      event.adjustedAt,
      event.isThreeRolls,
      event.isFiveRolls,
      event.isBigWin,
      event.bigWinMultiplier,
      event.dealerName,
      JSON.stringify(event.raw)

    ]

  );

  return Boolean(result.rows[0].inserted);

}


// ----------------------------------
// LIVE MONITOR
// ----------------------------------

async function pollUpstream() {

  if (pollInProgress) return;

  pollInProgress = true;

  lastPollAt = new Date().toISOString();

  try {

    const response = await fetch(UPSTREAM_URL, {

      headers: {
        Accept: 'application/json',
        'User-Agent': 'Mozilla/5.0'
      },

      signal: AbortSignal.timeout(20000)

    });

    if (!response.ok) {

      throw new Error(
        `Upstream HTTP ${response.status}`
      );

    }

    const payload = await response.json();

    const rows = extractRows(payload);

    lastFetchedCount = rows.length;

    let saved = 0;

    for (const raw of rows) {

      const event = normalizeEvent(raw);

      if (!event) continue;

      if (await saveEvent(event)) {
        saved++;
      }

    }

    lastSavedCount = saved;

    lastSuccessAt = new Date().toISOString();

    lastPollError = null;

    console.log(
      `Fetched: ${rows.length}, New records: ${saved}`
    );

  } catch (error) {

    lastPollError = error.message;

    console.error(
      'MONITOR ERROR:',
      lastPollError
    );

  } finally {

    pollInProgress = false;

  }

}


// ----------------------------------
// HEALTH CHECK
// ----------------------------------

app.get('/health', async (req, res) => {

  let database = 'ok';

  try {

    await pool.query('SELECT 1');

  } catch {

    database = 'error';

  }

  res.status(
    database === 'ok' ? 200 : 503
  ).json({

    status:
      database === 'ok'
        ? 'ok'
        : 'degraded',

    database,

    lastPollAt,

    lastSuccessAt,

    lastPollError,

    lastFetchedCount,

    lastSavedCount,

    pollIntervalMs: POLL_INTERVAL_MS

  });

});


// ----------------------------------
// BIG BALLER API
// ----------------------------------

app.get('/api/bigballer', async (req, res) => {

  try {

    const limit = Math.min(
      500,
      Math.max(1, Number(req.query.limit) || 100)
    );

    const result = await pool.query(

      `SELECT *
       FROM game_history
       ORDER BY settled_at DESC
       LIMIT $1`,

      [limit]

    );

    res.json({

      count: result.rowCount,

      records: result.rows

    });

  } catch (error) {

    res.status(500).json({

      error: error.message

    });

  }

});


// ----------------------------------
// HISTORY API
// ----------------------------------

app.get('/api/history', async (req, res) => {

  try {

    const limit = Math.min(
      1000,
      Math.max(1, Number(req.query.limit) || 200)
    );

    const type = String(
      req.query.type || 'all'
    ).toLowerCase();

    let where = '';

    if (['3', 'three', 'three_rolls'].includes(type)) {

      where = 'WHERE is_three_rolls = TRUE';

    }

    else if (['5', 'five', 'five_rolls'].includes(type)) {

      where = 'WHERE is_five_rolls = TRUE';

    }

    else if (type === 'all') {

      where = `
        WHERE (
          is_three_rolls = TRUE
          OR is_five_rolls = TRUE
        )
      `;

    }

    else {

      return res.status(400).json({

        error: 'Invalid type'

      });

    }

    const result = await pool.query(

      `SELECT *
       FROM game_history
       ${where}
       ORDER BY settled_at DESC
       LIMIT $1`,

      [limit]

    );

    res.json({

      count: result.rowCount,

      records: result.rows

    });

  } catch (error) {

    res.status(500).json({

      error: error.message

    });

  }

});


// ----------------------------------
// STATISTICS
// ----------------------------------

app.get('/api/stats', async (req, res) => {

  try {

    const totals = await pool.query(`

      SELECT

        COUNT(*)::int AS total_records,

        COUNT(*) FILTER (
          WHERE is_three_rolls
        )::int AS three_rolls,

        COUNT(*) FILTER (
          WHERE is_five_rolls
        )::int AS five_rolls,

        COUNT(*) FILTER (
          WHERE is_big_win
        )::int AS big_wins

      FROM game_history

    `);

    const hourly = await pool.query(`

      SELECT

        EXTRACT(
          HOUR FROM settled_at AT TIME ZONE 'Asia/Manila'
        )::int AS hour,

        COUNT(*)::int AS total,

        COUNT(*) FILTER (
          WHERE is_three_rolls
        )::int AS three_rolls,

        COUNT(*) FILTER (
          WHERE is_five_rolls
        )::int AS five_rolls,

        COUNT(*) FILTER (
          WHERE is_big_win
        )::int AS big_wins

      FROM game_history

      GROUP BY hour

      ORDER BY hour

    `);

    res.json({

      totals: totals.rows[0],

      hourlyAsiaManila: hourly.rows,

      note:
        'Historical records do not predict future random outcomes.'

    });

  } catch (error) {

    res.status(500).json({

      error: error.message

    });

  }

});


// ----------------------------------
// FRONTEND
// ----------------------------------

app.use(
  express.static(
    path.join(__dirname, 'public')
  )
);

app.get('*', (req, res, next) => {

  if (
    req.path.startsWith('/api/') ||
    req.path === '/health'
  ) {
    return next();
  }

  res.sendFile(

    path.join(
      __dirname,
      'public',
      'index.html'
    ),

    (error) => {

      if (error) {

        res.status(404).send(
          'Frontend file not found. Add public/index.html.'
        );

      }

    }

  );

});


// ----------------------------------
// START SERVER
// ----------------------------------

async function start() {

  await initializeDatabase();

  app.listen(PORT, () => {

    console.log(
      `MONOPOLY TRACKER running on port ${PORT}`
    );

    console.log(
      `Polling every ${POLL_INTERVAL_MS}ms`
    );

  });

  await pollUpstream();

  setInterval(
    pollUpstream,
    POLL_INTERVAL_MS
  );

}

start().catch((error) => {

  console.error(
    'STARTUP ERROR:',
    error
  );

  process.exit(1);

});


process.on('SIGTERM', async () => {

  await pool.end();

  process.exit(0);

});
