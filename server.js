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
  'https://api-cs.casino.org/svc-evolution-game-events/api/bigballer?page=0&size=100&sort=data.settledAt,desc&duration=6&isThreeRolls=true&isFiveRolls=true,false';

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

  if (value === undefined || value === null || value === '') {
    return null;
  }

  const number = Number(value);

  return Number.isFinite(number) ? number : null;
}

function findMultipliers(obj, output = []) {

  if (Array.isArray(obj)) {
    for (const item of obj) {
      findMultipliers(item, output);
    }
    return output;
  }

  if (!obj || typeof obj !== 'object') return output;

  for (const [key, value] of Object.entries(obj)) {

    if (/multiplier/i.test(key)) {

      const number = getNumber(value);

      if (number !== null) {
        output.push(number);
      }

    } else if (value && typeof value === 'object') {

      findMultipliers(value, output);

    }

  }

  return output;
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
// NORMALIZE EVENT
// ----------------------------------

  function normalizeEvent(raw) {
  if (!raw || typeof raw !== 'object') return null;

  const data = raw.data || {};
  const result = raw.result || data.result || {};

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

  if (!eventId || !settledAt) return null;

  /*
   * Keep the original flag detection.
   */
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

  /*
   * Additional detection for APIs where the roll type
   * is stored inside bonusRounds/gameMode/mode.
   */
  const bonusRounds = [];

  function collectBonusRounds(value) {
    if (!value || typeof value !== 'object') return;

    if (Array.isArray(value)) {
      for (const item of value) {
        collectBonusRounds(item);
      }
      return;
    }

    for (const [key, value] of Object.entries(value)) {
      if (/bonusRounds/i.test(key) && Array.isArray(value)) {
        for (const round of value) {
          bonusRounds.push(round);
        }
      } else if (value && typeof value === 'object') {
        collectBonusRounds(value);
      }
    }
  }

  collectBonusRounds(raw);

  for (const round of bonusRounds) {
    if (!round || typeof round !== 'object') continue;

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

  /*
   * Also check common game-mode fields directly.
   */
  const directModes = [
    raw.gameMode,
    raw.mode,
    raw.gameType,
    data.gameMode,
    data.mode,
    data.gameType,
    result.gameMode,
    result.mode,
    result.gameType
  ];

  for (const value of directModes) {
    const mode = String(value || '');

    if (/three.?roll/i.test(mode)) {
      isThreeRolls = true;
    }

    if (/five.?roll/i.test(mode)) {
      isFiveRolls = true;
    }
  }

  /*
   * Original multiplier detection is preserved.
   */
  const multipliers = findMultipliers(raw);

  const bigWinMultiplier = multipliers.length
    ? Math.max(...multipliers)
    : null;

  const isBigWin =
    bigWinMultiplier !== null &&
    bigWinMultiplier >= 100;

  if (
    !isThreeRolls &&
    !isFiveRolls &&
    !isBigWin
  ) {
    return null;
  }

  /*
   * Time adjustment:
   * 3 Rolls = -2 minutes
   * 5 Rolls = -4 minutes
   *
   * If an event is marked as both, 5 Rolls takes precedence.
   */
  let adjustment = 0;

  if (isThreeRolls) {
    adjustment = 2;
  }

  if (isFiveRolls) {
    adjustment = 4;
  }

  const adjustedAt = new Date(
    settledAt.getTime() - adjustment * 60000
  );

  const dealer =
    data.dealer ||
    raw.dealer ||
    {};

  return {
    eventId: String(eventId),

    settledAt,

    adjustedAt,

    isThreeRolls,

    isFiveRolls,

    isBigWin,

    bigWinMultiplier,

    dealerName:
      dealer.name ||
      dealer.displayName ||
      null,

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

    else if (['bigwin', 'big_win'].includes(type)) {
      where = 'WHERE is_big_win = TRUE';
    }

    else if (type !== 'all') {

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
