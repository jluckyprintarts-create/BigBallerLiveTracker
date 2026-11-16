// MONOPOLY BIG BALLER HISTORICAL TRACKER
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
  console.error('ERROR: DATABASE_URL is required.');
  process.exit(1);
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false
  },
  connectionTimeoutMillis: 10000,
  idleTimeoutMillis: 30000
});


// ----------------------------------
// SERVER STATUS
// ----------------------------------

let pollInProgress = false;

let lastPollAt = null;
let lastSuccessAt = null;
let lastPollError = null;

let lastFetchedCount = 0;
let lastSavedCount = 0;


// ----------------------------------
// DATABASE INITIALIZATION
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

  console.log('DATABASE INITIALIZED');

}


// ----------------------------------
// DATE HELPER
// ----------------------------------

function getDate(value) {

  if (!value) return null;

  const date = typeof value === 'number'
    ? new Date(
        value < 1e12
          ? value * 1000
          : value
      )
    : new Date(value);

  return Number.isNaN(date.getTime())
    ? null
    : date;

}


// ----------------------------------
// EXTRACT UPSTREAM RECORDS
// ----------------------------------

function extractRows(payload) {

  if (!payload || typeof payload !== 'object') {
    return [];
  }

  if (Array.isArray(payload)) {
    return payload;
  }

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
    typeof payload.data === 'object' &&
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

  if (!raw || typeof raw !== 'object') {
    return null;
  }

  const data =
    raw.data &&
    typeof raw.data === 'object'
      ? raw.data
      : {};

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


  // OFFICIAL EVENT FLAGS

  const isThreeRolls = Boolean(
    result.isThreeRolls ||
    data.isThreeRolls ||
    raw.isThreeRolls
  );

  const isFiveRolls = Boolean(
    result.isFiveRolls ||
    data.isFiveRolls ||
    raw.isFiveRolls
  );


  // PRESERVE OFFICIAL TIMESTAMP

  const adjustedAt = new Date(settledAt);


  // DEALER NAME

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


  return {

    eventId: String(eventId),

    settledAt,

    adjustedAt,

    isThreeRolls,

    isFiveRolls,

    dealerName

  };

}


// ----------------------------------
// SAVE EVENT
// ----------------------------------

async function saveEvent(event) {

  const result = await pool.query(

    `
    INSERT INTO game_history
    (
      event_id,
      settled_at,
      adjusted_at,
      is_three_rolls,
      is_five_rolls,
      is_big_win,
      big_win_multiplier,
      dealer_name
    )

    VALUES ($1,$2,$3,$4,$5,FALSE,NULL,$6)

    ON CONFLICT (event_id)

    DO UPDATE SET

      settled_at = EXCLUDED.settled_at,

      adjusted_at = EXCLUDED.adjusted_at,

      is_three_rolls = EXCLUDED.is_three_rolls,

      is_five_rolls = EXCLUDED.is_five_rolls,

      dealer_name = COALESCE(
        EXCLUDED.dealer_name,
        game_history.dealer_name
      )

    RETURNING (xmax = 0) AS inserted

    `,

    [

      event.eventId,

      event.settledAt,

      event.adjustedAt,

      event.isThreeRolls,

      event.isFiveRolls,

      event.dealerName

    ]

  );

  return Boolean(result.rows[0].inserted);

}


// ----------------------------------
// LIVE DATA POLLING
// ----------------------------------

async function pollUpstream() {

  if (pollInProgress) {
    return;
  }

  pollInProgress = true;

  lastPollAt = new Date().toISOString();

  try {

    const response = await fetch(UPSTREAM_URL, {

      method: 'GET',

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

    console.log(
      `UPSTREAM RESPONSE: ${rows.length} records fetched`
    );


    let saved = 0;
    let invalid = 0;


    for (const raw of rows) {

      const event = normalizeEvent(raw);

      if (!event) {

        invalid++;

        continue;

      }

      const inserted = await saveEvent(event);

      if (inserted) {
        saved++;
      }

    }


    lastSavedCount = saved;

    lastSuccessAt = new Date().toISOString();

    lastPollError = null;


    console.log(
      `POLL SUCCESS: Fetched ${rows.length}, New ${saved}, Invalid ${invalid}`
    );


  } catch (error) {

    lastPollError = error.message;

    console.error(
      'MONITOR ERROR:',
      error.message
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

  } catch (error) {

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
// ALL GAME RECORDS
// ----------------------------------

app.get('/api/bigballer', async (req, res) => {

  try {

    const limit = Math.min(
      500,
      Math.max(
        1,
        Number(req.query.limit) || 100
      )
    );


    const result = await pool.query(

      `
      SELECT

        id,

        event_id,

        settled_at,

        adjusted_at,

        is_three_rolls,

        is_five_rolls,

        dealer_name,

        created_at

      FROM game_history

      ORDER BY settled_at DESC

      LIMIT $1
      `,

      [limit]

    );


    res.json({

      success: true,

      count: result.rowCount,

      records: result.rows

    });


  } catch (error) {

    console.error(
      'BIGBALLER API ERROR:',
      error.message
    );

    res.status(500).json({

      success: false,

      error: 'Unable to retrieve game records.'

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
      Math.max(
        1,
        Number(req.query.limit) || 200
      )
    );


    const type = String(
      req.query.type || 'all'
    ).toLowerCase();


    let where = '';


    if (
      ['3', 'three', 'three_rolls'].includes(type)
    ) {

      where = 'WHERE is_three_rolls = TRUE';

    }

    else if (
      ['5', 'five', 'five_rolls'].includes(type)
    ) {

      where = 'WHERE is_five_rolls = TRUE';

    }

    else if (type === 'all') {

      where = '';

    }

    else {

      return res.status(400).json({

        success: false,

        error: 'Invalid history type.'

      });

    }


    const result = await pool.query(

      `
      SELECT

        id,

        event_id,

        settled_at,

        adjusted_at,

        is_three_rolls,

        is_five_rolls,

        dealer_name,

        created_at

      FROM game_history

      ${where}

      ORDER BY settled_at DESC

      LIMIT $1
      `,

      [limit]

    );


    res.json({

      success: true,

      count: result.rowCount,

      type,

      records: result.rows

    });


  } catch (error) {

    console.error(
      'HISTORY API ERROR:',
      error.message
    );

    res.status(500).json({

      success: false,

      error: 'Unable to retrieve history.'

    });

  }

});


// ----------------------------------
// STATISTICS API
// ----------------------------------

app.get('/api/stats', async (req, res) => {

  try {

    const totals = await pool.query(`

      SELECT

        COUNT(*)::int AS total_records,

        COUNT(*) FILTER (
          WHERE is_three_rolls = TRUE
        )::int AS three_rolls,

        COUNT(*) FILTER (
          WHERE is_five_rolls = TRUE
        )::int AS five_rolls

      FROM game_history

    `);


    const hourly = await pool.query(`

      SELECT

        EXTRACT(
          HOUR FROM settled_at AT TIME ZONE 'Asia/Manila'
        )::int AS hour,

        COUNT(*)::int AS total_records,

        COUNT(*) FILTER (
          WHERE is_three_rolls = TRUE
        )::int AS three_rolls,

        COUNT(*) FILTER (
          WHERE is_five_rolls = TRUE
        )::int AS five_rolls

      FROM game_history

      GROUP BY hour

      ORDER BY hour

    `);


    const daily = await pool.query(`

      SELECT

        EXTRACT(
          DOW FROM settled_at AT TIME ZONE 'Asia/Manila'
        )::int AS day_number,

        COUNT(*)::int AS total_records

      FROM game_history

      GROUP BY day_number

      ORDER BY day_number

    `);


    res.json({

      success: true,

      lastUpdated: new Date().toISOString(),

      totals: totals.rows[0],

      hourlyAsiaManila: hourly.rows,

      dailyAsiaManila: daily.rows,

      note:
        'Historical statistics only. These records do not predict future game results.'

    });


  } catch (error) {

    console.error(
      'STATS API ERROR:',
      error.message
    );

    res.status(500).json({

      success: false,

      error: 'Unable to retrieve statistics.'

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
      `MONOPOLY HISTORICAL TRACKER running on port ${PORT}`
    );

    console.log(
      `Polling interval: ${POLL_INTERVAL_MS}ms`
    );

  });


  // INITIAL DATA FETCH

  await pollUpstream();


  // AUTOMATIC DATA FETCH

  setInterval(
    pollUpstream,
    POLL_INTERVAL_MS
  );

}


// ----------------------------------
// STARTUP ERROR HANDLING
// ----------------------------------

start().catch((error) => {

  console.error(
    'STARTUP ERROR:',
    error
  );

  process.exit(1);

});


// ----------------------------------
// SHUTDOWN HANDLING
// ----------------------------------

async function shutdown() {

  console.log('Shutting down server...');

  try {

    await pool.end();

  } catch (error) {

    console.error(
      'DATABASE SHUTDOWN ERROR:',
      error.message
    );

  }

  process.exit(0);

}


process.on('SIGTERM', shutdown);

process.on('SIGINT', shutdown);
