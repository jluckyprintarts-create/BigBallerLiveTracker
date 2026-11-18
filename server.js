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

// SMART ANALYSIS: EVERY 5 MINUTES
const ANALYSIS_INTERVAL_MS = 5 * 60 * 1000;

// ----------------------------------
// HISTORICAL BACKFILL SETTINGS
// ----------------------------------

const BACKFILL_PAGE_SIZE = 14;
const BACKFILL_MAX_PAGES = 10;
// Continue historical backfill from the next page.
let nextBackfillPage = 1;
const BACKFILL_INTERVAL_MS = 15 * 60 * 1000;

let backfillInProgress = false;

let backfillStatus = {
  running: false,
  currentPage: 0,
  pagesFetched: 0,
  recordsFetched: 0,
  recordsInserted: 0,
  lastRunAt: null,
  lastCompletedAt: null,
  error: null,
  note: 'Waiting for first backfill run.'
};

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

// SMART ANALYSIS STATUS
let analysisInProgress = false;

let analysisCache = {
  lastAnalysisAt: null,

  threeRolls: {
    bestHours: [],
    bestDays: []
  },

  fiveRolls: {
    bestHours: [],
    bestDays: []
  },

  totalRecords: 0,
  error: null
};


// ----------------------------------
// BONUS INTELLIGENCE ENGINE STATUS
// ----------------------------------

let intelligenceInProgress = false;

function emptyForecast() {
  return {
    totalBonuses: 0,
    intervalCount: 0,
    lastBonusAt: null,
    averageIntervalMinutes: null,
    medianIntervalMinutes: null,
    recentAverageIntervalMinutes: null,
    estimatedIntervalMinutes: null,
    selectedModel: null,
    backtestMaeMinutes: null,
    backtestBiasMinutes: null,
    backtestSamples: 0,
    coefficientOfVariation: null,
    currentGapMinutes: null,
    anomaly: 'not-enough-data',
    confidence: 'Low',
    confidenceReason: 'Not enough historical intervals.',
    estimatedWindows: []
  };
}

let intelligenceCache = {
  lastAnalysisAt: null,
  threeRolls: emptyForecast(),
  fiveRolls: emptyForecast(),
  error: null,
  note:
    'Historical interval analysis only. Bootstrap resampling estimates uncertainty in historical intervals; it does not simulate or predict the game RNG, winning probability, or guaranteed bonus times.'
};


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

  await pool.query(`
    CREATE TABLE IF NOT EXISTS forecast_evaluations (
      id BIGSERIAL PRIMARY KEY,
      game_type TEXT NOT NULL,
      source TEXT NOT NULL DEFAULT 'live',
      forecast_origin_at TIMESTAMPTZ NOT NULL,
      forecast_at TIMESTAMPTZ NOT NULL,
      window_start TIMESTAMPTZ NOT NULL,
      window_end TIMESTAMPTZ NOT NULL,
      actual_bonus_at TIMESTAMPTZ,
      error_minutes NUMERIC,
      signed_error_minutes NUMERIC,
      central_hit BOOLEAN,
      candidate_hit BOOLEAN,
      status TEXT NOT NULL DEFAULT 'pending',
      created_at TIMESTAMPTZ DEFAULT NOW(),
      evaluated_at TIMESTAMPTZ,
      UNIQUE (
        game_type,
        source,
        forecast_origin_at
      )
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_forecast_pending
    ON forecast_evaluations (
      game_type,
      status,
      forecast_origin_at
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_forecast_created
    ON forecast_evaluations (
      created_at DESC
    )
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

  if (!round && bonusRounds.length === 1) {
    round = bonusRounds[0];
  }

  if (round) {

    const value = getNumber(round.totalMultiplier);

    if (value !== null) {
      return value;
    }

  }

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

console.log('BONUS MULTIPLIER DEBUG:', JSON.stringify({
  eventId,
  isThreeRolls,
  isFiveRolls,
  bigWinMultiplier,
  multiplierType: typeof bigWinMultiplier,
  bonusRounds: result.bonusRounds,
  threeRollsTotalMultiplier: result.threeRollsTotalMultiplier,
  fiveRollsTotalMultiplier: result.fiveRollsTotalMultiplier
}));

// BIG WIN RULE: 100x AND ABOVE
const isBigWin =
  (isThreeRolls || isFiveRolls) &&
  bigWinMultiplier !== null &&
  Number.isFinite(Number(bigWinMultiplier)) &&
  Number(bigWinMultiplier) >= 100;

  // TIME ADJUSTMENT
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

  // HOST / DEALER NAME
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

    isBigWin,

    bigWinMultiplier,

    dealerName,

    raw

  };

}

// ----------------------------------
// SAFE JSON CLEANER
// ----------------------------------

function cleanJsonValue(value) {
  if (typeof value === 'string') {
    return value
      .replace(/[\uD800-\uDBFF](?![\uDC00-\uDFFF])/g, '\uFFFD')
      .replace(/(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g, '\uFFFD');
  }

  if (Array.isArray(value)) {
    return value.map(cleanJsonValue);
  }

  if (value && typeof value === 'object') {
    const cleaned = {};

    for (const [key, item] of Object.entries(value)) {
      cleaned[key] = cleanJsonValue(item);
    }

    return cleaned;
  }

  return value;
}

// ----------------------------------
// SAVE EVENT
// ----------------------------------

async function saveEvent(event) {
  const rawData = event.raw ?? {};

  try {
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
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)

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
        JSON.stringify(cleanJsonValue(rawData))
      ]
    );

    const inserted = Boolean(result.rows[0].inserted);

    return inserted;

  } catch (error) {
    console.error('SAVE EVENT ERROR:', {
      message: error.message,
      code: error.code,
      detail: error.detail,
      eventId: event.eventId,
      rawType: typeof event.raw
    });

    throw error;
  }
}

// ----------------------------------
// AUTOMATIC HISTORICAL BACKFILL
// ----------------------------------

async function runHistoricalBackfill() {
  if (backfillInProgress || pollInProgress) return;

  backfillInProgress = true;

  backfillStatus = {
    ...backfillStatus,
    running: true,
    currentPage: 0,
    pagesFetched: 0,
    recordsFetched: 0,
    recordsInserted: 0,
    lastRunAt: new Date().toISOString(),
    error: null,
    note: 'Historical backfill is running.'
  };

  let insertedTotal = 0;
  let fetchedTotal = 0;
  let pagesCompleted = 0;
  let page = nextBackfillPage;

  try {
    for (
      let count = 0;
      count < BACKFILL_MAX_PAGES;
      count++
    ) {
      backfillStatus.currentPage = page;

      const pageUrl = new URL(UPSTREAM_URL);
      pageUrl.searchParams.set('page', String(page));
      pageUrl.searchParams.set(
        'size',
        String(BACKFILL_PAGE_SIZE)
      );

      const response = await fetch(pageUrl.toString(), {
        headers: {
          Accept: 'application/json',
          'User-Agent': 'Mozilla/5.0'
        },
        signal: AbortSignal.timeout(20000)
      });

      if (!response.ok) {
        throw new Error(
          `Backfill HTTP ${response.status} on page ${page}`
        );
      }

      const payload = await response.json();
      const rows = extractRows(payload);

      pagesCompleted++;
      fetchedTotal += rows.length;

      backfillStatus.pagesFetched = pagesCompleted;
      backfillStatus.recordsFetched = fetchedTotal;

      console.log('BACKFILL PAGE:', {
        page,
        fetched: rows.length,
        firstRawId:
          rows[0]?.id ??
          rows[0]?.data?.id ??
          null
      });

      if (rows.length === 0) {
        backfillStatus.note =
          `No records returned on page ${page}.`;

        console.log(
          'BACKFILL STOPPED: no records on page',
          page
        );

        break;
      }

      let insertedThisPage = 0;
      let skippedThisPage = 0;
      let existingThisPage = 0;
      let saveErrorThisPage = 0;

      for (const raw of rows) {
        try {
          const event = normalizeEvent(raw);

          if (!event) {
            skippedThisPage++;
            continue;
          }

          const wasInserted = await saveEvent(event);

          if (wasInserted) {
            insertedThisPage++;
            insertedTotal++;
          } else {
            existingThisPage++;
          }
        } catch (error) {
          saveErrorThisPage++;

          console.error('BACKFILL EVENT ERROR:', {
            page,
            eventId:
              raw?.id ??
              raw?.data?.id ??
              null,
            error: error.message
          });
        }
      }

      backfillStatus.recordsInserted = insertedTotal;

      console.log('BACKFILL PAGE SUMMARY:', {
        page,
        fetched: rows.length,
        inserted: insertedThisPage,
        existingOrUpdated: existingThisPage,
        skipped: skippedThisPage,
        errors: saveErrorThisPage
      });

      // Stop if the API returns fewer records than expected.
      if (rows.length < BACKFILL_PAGE_SIZE) {
        backfillStatus.note =
          `Partial page ${page} returned.`;

        break;
      }

      // Remember the next page for the next run.
      nextBackfillPage = page + 1;

      await new Promise(resolve =>
        setTimeout(resolve, 1000)
      );

      page++;
    }

    backfillStatus.lastCompletedAt =
      new Date().toISOString();

    if (
      backfillStatus.note ===
      'Historical backfill is running.'
    ) {
      backfillStatus.note =
        `Backfill run completed. Next page: ${nextBackfillPage}.`;
    }

    console.log('BACKFILL COMPLETE:', {
      pages: pagesCompleted,
      fetched: fetchedTotal,
      inserted: insertedTotal,
      nextPage: nextBackfillPage
    });
  } catch (error) {
    backfillStatus.error = error.message;
    backfillStatus.note =
      'Backfill stopped because of an error.';

    console.error(
      'HISTORICAL BACKFILL ERROR:',
      error.message
    );
  } finally {
    backfillStatus.running = false;
    backfillInProgress = false;
  }
}

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
      throw new Error(`Upstream HTTP ${response.status}`);
    }

    const payload = await response.json();
    const rows = extractRows(payload);

    // Search for possible official Big Win classification fields.
    if (rows.length > 0) {
      const sample = rows[0];
      const matches = [];

      function scanBigWinFields(obj, path = '') {
        if (!obj || typeof obj !== 'object') return;

        for (const [key, value] of Object.entries(obj)) {
          const currentPath = path ? `${path}.${key}` : key;

          if (/big.?win|jackpot/i.test(key)) {
            matches.push({
              path: currentPath,
              value
            });
          }

          if (value && typeof value === 'object') {
            scanBigWinFields(value, currentPath);
          }
        }
      }

      scanBigWinFields(sample);

      console.log(
        'BIG WIN CLASSIFICATION SEARCH:',
        JSON.stringify(matches, null, 2)
      );
    }

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

    console.error('MONITOR ERROR DETAILS:', {
      message: error.message,
      code: error.code || null,
      detail: error.detail || null,
      hint: error.hint || null,
      where: error.where || null,
      table: error.table || null,
      column: error.column || null,
      dataType: error.dataType || null,
      constraint: error.constraint || null
    });

  } finally {
    pollInProgress = false;
  }
}

// ----------------------------------
// BIG WIN REPAIR
// ----------------------------------

async function repairBigWinFlags() {
  try {
    const result = await pool.query(`
      UPDATE game_history
      SET is_big_win = TRUE
      WHERE big_win_multiplier >= 100
        AND (
          is_three_rolls = TRUE
          OR is_five_rolls = TRUE
        )
        AND is_big_win IS DISTINCT FROM TRUE
      RETURNING event_id, big_win_multiplier;
    `);

    console.log(
      'BIG WIN FLAGS REPAIRED:',
      JSON.stringify(result.rows)
    );

  } catch (error) {
    console.error('BIG WIN REPAIR ERROR:', error.message);
  }
}

// ----------------------------------
// SMART GAME ANALYSIS
// ----------------------------------

async function runAutomaticAnalysis() {

  if (analysisInProgress) return;

  analysisInProgress = true;

  try {
    const bigWinCheck = await pool.query(`
      SELECT
        COUNT(*)::int AS total_records,

        COUNT(*) FILTER (
          WHERE big_win_multiplier >= 100
        )::int AS records_100x_or_higher,

        COUNT(*) FILTER (
          WHERE is_big_win = TRUE
        )::int AS marked_big_wins,

        COUNT(*) FILTER (
          WHERE big_win_multiplier >= 100
            AND is_big_win = FALSE
        )::int AS unmarked_big_wins

      FROM game_history
    `);

    console.log(
      'BIG WIN DATABASE CHECK:',
      bigWinCheck.rows[0]
    );

    const hourly = await pool.query(`
      SELECT
        EXTRACT(
          HOUR FROM adjusted_at AT TIME ZONE 'Asia/Manila'
        )::int AS hour,

        COUNT(*) FILTER (
          WHERE is_three_rolls = TRUE
        )::int AS three_rolls,

        COUNT(*) FILTER (
          WHERE is_five_rolls = TRUE
        )::int AS five_rolls

      FROM game_history

      WHERE (
        is_three_rolls = TRUE
        OR is_five_rolls = TRUE
      )

      GROUP BY hour
      ORDER BY hour
    `);

    const daily = await pool.query(`
      SELECT
        EXTRACT(
          DOW FROM adjusted_at AT TIME ZONE 'Asia/Manila'
        )::int AS day_number,

        COUNT(*) FILTER (
          WHERE is_three_rolls = TRUE
        )::int AS three_rolls,

        COUNT(*) FILTER (
          WHERE is_five_rolls = TRUE
        )::int AS five_rolls

      FROM game_history

      WHERE (
        is_three_rolls = TRUE
        OR is_five_rolls = TRUE
      )

      GROUP BY day_number
      ORDER BY day_number
    `);

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

      WHERE (
        is_three_rolls = TRUE
        OR is_five_rolls = TRUE
      )
    `);

    const dayNames = [
      'Sunday',
      'Monday',
      'Tuesday',
      'Wednesday',
      'Thursday',
      'Friday',
      'Saturday'
    ];

    function topHours(rows, type) {
      return rows
        .map(row => ({
          hour: Number(row.hour),
          count: Number(row[type])
        }))
        .filter(row => row.count > 0)
        .sort((a, b) => b.count - a.count)
        .slice(0, 5);
    }

    function topDays(rows, type) {
      return rows
        .map(row => ({
          day: dayNames[Number(row.day_number)],
          count: Number(row[type])
        }))
        .filter(row => row.count > 0)
        .sort((a, b) => b.count - a.count)
        .slice(0, 7);
    }

    analysisCache = {
      lastAnalysisAt: new Date().toISOString(),

      threeRolls: {
        bestHours: topHours(
          hourly.rows,
          'three_rolls'
        ),

        bestDays: topDays(
          daily.rows,
          'three_rolls'
        )
      },

      fiveRolls: {
        bestHours: topHours(
          hourly.rows,
          'five_rolls'
        ),

        bestDays: topDays(
          daily.rows,
          'five_rolls'
        )
      },

      totalRecords: totals.rows[0].total_records,

      totals: {
        threeRolls: totals.rows[0].three_rolls,
        fiveRolls: totals.rows[0].five_rolls
      },

      error: null,

      note:
        'Historical frequency only. Not a prediction or guarantee of winning.'
    };

    console.log(
      'SMART ANALYSIS UPDATED:',
      analysisCache.lastAnalysisAt
    );

  } catch (error) {
    analysisCache.error = error.message;

    console.error(
      'SMART ANALYSIS ERROR:',
      error.message
    );

  } finally {
    analysisInProgress = false;
  }
}

// ----------------------------------
// BONUS INTELLIGENCE MATH HELPERS
// ----------------------------------

function average(values) {
  if (!values.length) return null;

  return values.reduce((sum, value) => sum + value, 0) /
    values.length;
}

function median(values) {
  if (!values.length) return null;

  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);

  if (sorted.length % 2) {
    return sorted[middle];
  }

  return (sorted[middle - 1] + sorted[middle]) / 2;
}

function standardDeviation(values) {
  if (values.length < 2) return 0;

  const avg = average(values);

  const variance = average(
    values.map(value => Math.pow(value - avg, 2))
  );

  return Math.sqrt(variance);
}

function percentile(values, percent) {
  if (!values.length) return null;

  const sorted = [...values].sort((a, b) => a - b);
  const position = (sorted.length - 1) * percent;
  const lower = Math.floor(position);
  const upper = Math.ceil(position);

  if (lower === upper) {
    return sorted[lower];
  }

  const fraction = position - lower;

  return sorted[lower] +
    (sorted[upper] - sorted[lower]) * fraction;
}

function roundNumber(value, digits = 1) {
  if (!Number.isFinite(value)) return null;

  const factor = Math.pow(10, digits);

  return Math.round(value * factor) / factor;
}

// ----------------------------------
// MODEL ESTIMATORS
// ----------------------------------

function estimateWithModel(values, model) {
  if (!values.length) return null;

  if (model === 'mean') {
    return average(values);
  }

  if (model === 'median') {
    return median(values);
  }

  if (model === 'recent10') {
    return average(values.slice(-10));
  }

  if (model === 'weighted') {
    let weightedTotal = 0;
    let totalWeight = 0;

    values.forEach((value, index) => {
      const age = values.length - 1 - index;
      const weight = Math.pow(0.85, age);

      weightedTotal += value * weight;
      totalWeight += weight;
    });

    return totalWeight
      ? weightedTotal / totalWeight
      : average(values);
  }

  return median(values);
}

// ----------------------------------
// WALK-FORWARD BACKTEST
// ----------------------------------

function runIntervalBacktest(intervals) {
  const models = [
    'mean',
    'median',
    'recent10',
    'weighted'
  ];

  const results = {};

  for (const model of models) {
    const errors = [];
    const signedErrors = [];

    for (
      let index = 5;
      index < intervals.length;
      index++
    ) {
      const training = intervals.slice(0, index);
      const actual = intervals[index];

      const predicted = estimateWithModel(
        training,
        model
      );

      if (
        !Number.isFinite(predicted) ||
        !Number.isFinite(actual)
      ) {
        continue;
      }

      errors.push(Math.abs(predicted - actual));
      signedErrors.push(predicted - actual);
    }

    results[model] = {
      maeMinutes: errors.length
        ? average(errors)
        : null,

      biasMinutes: signedErrors.length
        ? average(signedErrors)
        : null,

      samples: errors.length
    };
  }

  const eligible = models
    .filter(model => results[model].samples >= 5)
    .sort((a, b) =>
      results[a].maeMinutes - results[b].maeMinutes
    );

  const selectedModel = eligible.length
    ? eligible[0]
    : 'median';

  return {
    selectedModel,
    results,
    baselineMaeMinutes:
      results.median.maeMinutes,
    baselineSamples:
      results.median.samples
  };
}

// ----------------------------------
// BOOTSTRAP INTERVAL UNCERTAINTY
// ----------------------------------

function createBootstrapEstimates(
  intervals,
  model,
  simulations = 1500
) {
  if (!intervals.length) return [];

  const sampleSize = Math.min(10, intervals.length);
  const estimates = [];

  for (
    let simulation = 0;
    simulation < simulations;
    simulation++
  ) {
    const sample = [];

    for (let index = 0; index < sampleSize; index++) {
      const randomIndex = Math.floor(
        Math.random() * intervals.length
      );

      sample.push(intervals[randomIndex]);
    }

    const estimate = estimateWithModel(sample, model);

    if (Number.isFinite(estimate) && estimate > 0) {
      estimates.push(estimate);
    }
  }

  return estimates;
}


// ----------------------------------
// BUILD ONE TYPE'S INTELLIGENCE
// ----------------------------------

function buildBonusForecast(timestampValues) {
  const timestamps = timestampValues
    .map(value => getDate(value))
    .filter(Boolean)
    .sort((a, b) => a.getTime() - b.getTime());

  const result = emptyForecast();

  result.totalBonuses = timestamps.length;

  if (timestamps.length) {
    result.lastBonusAt =
      timestamps[timestamps.length - 1].toISOString();
  }

  if (timestamps.length < 2) {
    return result;
  }

  const intervals = [];

  for (let index = 1; index < timestamps.length; index++) {
    const minutes = (
      timestamps[index].getTime() -
      timestamps[index - 1].getTime()
    ) / 60000;

    if (minutes > 0 && Number.isFinite(minutes)) {
      intervals.push(minutes);
    }
  }

  result.intervalCount = intervals.length;

  if (!intervals.length) {
    return result;
  }

  const avg = average(intervals);
  const med = median(intervals);
  const recent = intervals.slice(-10);
  const recentAvg = average(recent);
  const deviation = standardDeviation(intervals);
  const cv = avg > 0 ? deviation / avg : null;

  result.averageIntervalMinutes = roundNumber(avg);
  result.medianIntervalMinutes = roundNumber(med);
  result.recentAverageIntervalMinutes = roundNumber(recentAvg);
  result.coefficientOfVariation = roundNumber(cv, 3);

  const backtest = runIntervalBacktest(intervals);
  const selectedModel = backtest.selectedModel;

  const estimatedInterval = estimateWithModel(
    intervals,
    selectedModel
  );

  result.selectedModel = selectedModel;
  result.estimatedIntervalMinutes =
    roundNumber(estimatedInterval);

  result.backtestMaeMinutes =
    backtest.selected.maeMinutes === null
      ? null
      : roundNumber(backtest.selected.maeMinutes);

  result.backtestBiasMinutes =
    backtest.selected.biasMinutes === null
      ? null
      : roundNumber(backtest.selected.biasMinutes);

  result.backtestSamples = backtest.selected.samples;

  const lastTimestamp = timestamps[timestamps.length - 1];

  const currentGap = Math.max(
    0,
    (Date.now() - lastTimestamp.getTime()) / 60000
  );

  result.currentGapMinutes = roundNumber(currentGap);

  const absoluteDeviations = intervals.map(
    value => Math.abs(value - med)
  );

  const mad = median(absoluteDeviations);

  if (intervals.length < 5) {
    result.anomaly = 'not-enough-data';
  } else if (
    mad > 0 &&
    currentGap > med + (3 * 1.4826 * mad)
  ) {
    result.anomaly = 'unusually-long-gap';
  } else if (
    mad === 0 &&
    currentGap > med * 2
  ) {
    result.anomaly = 'unusually-long-gap';
  } else {
    result.anomaly = 'within-historical-range';
  }

  const mae = backtest.selected.maeMinutes;

  const relativeMae = (
    mae !== null &&
    med > 0
  ) ? mae / med : null;

  if (
    intervals.length >= 30 &&
    backtest.selected.samples >= 15 &&
    cv !== null &&
    cv <= 0.35 &&
    relativeMae !== null &&
    relativeMae <= 0.25
  ) {
    result.confidence = 'Higher';

    result.confidenceReason =
      'Large interval sample with relatively consistent gaps and measurable backtest error. This is not a chance-of-bonus percentage.';
  } else if (
    intervals.length >= 15 &&
    backtest.selected.samples >= 5 &&
    cv !== null &&
    cv <= 0.65 &&
    relativeMae !== null &&
    relativeMae <= 0.50
  ) {
    result.confidence = 'Moderate';

    result.confidenceReason =
      'Some historical consistency is present, but timing uncertainty remains substantial. This is not a chance-of-bonus percentage.';
  } else {
    result.confidence = 'Low';

    result.confidenceReason =
      'Historical intervals are limited or variable, so estimated timing is uncertain. This is not a chance-of-bonus percentage.';
  }

  const bootstrap = createBootstrapEstimates(
    intervals,
    selectedModel,
    1500
  );

  if (
    !bootstrap.length ||
    !Number.isFinite(estimatedInterval)
  ) {
    return result;
  }

  const quantiles = [
    { label: 'EARLIER RANGE', value: 0.10 },
    { label: 'EARLY-MIDDLE RANGE', value: 0.30 },
    { label: 'CENTRAL ESTIMATE', value: 0.50 },
    { label: 'LATE-MIDDLE RANGE', value: 0.70 },
    { label: 'LATER RANGE', value: 0.90 }
  ];

  const intervalSpread = mad > 0 ? mad : deviation;

  const halfWindow = Math.max(
    3,
    Math.min(20, intervalSpread * 0.25)
  );

  result.estimatedWindows = quantiles.map((item, index) => {
    const minutesFromLast = Math.max(
      1,
      percentile(bootstrap, item.value)
    );

    const centerDate = new Date(
      lastTimestamp.getTime() +
      minutesFromLast * 60000
    );

    const startDate = new Date(
      centerDate.getTime() - halfWindow * 60000
    );

    const endDate = new Date(
      centerDate.getTime() + halfWindow * 60000
    );

    return {
      rank: index + 1,
      label: item.label,
      minutesFromLast: roundNumber(minutesFromLast),
      centerAt: centerDate.toISOString(),
      windowStart: startDate.toISOString(),
      windowEnd: endDate.toISOString()
    };
  });

  return result;
}


// ----------------------------------
// HISTORICAL FORECAST EVALUATION
// ----------------------------------

function evaluateHistoricalForecasts(
  timestamps,
  gameType
) {
  const results = [];

  const dates = timestamps
    .map(value => getDate(value))
    .filter(Boolean)
    .sort((a, b) => a.getTime() - b.getTime());

  if (dates.length < 7) {
    return results;
  }

  for (let index = 6; index < dates.length; index++) {
    const training = dates.slice(0, index);
    const intervals = [];

    for (let i = 1; i < training.length; i++) {
      const minutes = (
        training[i].getTime() -
        training[i - 1].getTime()
      ) / 60000;

      if (minutes > 0 && Number.isFinite(minutes)) {
        intervals.push(minutes);
      }
    }

    if (intervals.length < 5) {
      continue;
    }

    const backtest = runIntervalBacktest(intervals);
    const model = backtest.selectedModel;

    const predictedMinutes = estimateWithModel(
      intervals,
      model
    );

    if (
      !Number.isFinite(predictedMinutes) ||
      predictedMinutes <= 0
    ) {
      continue;
    }

    const lastTimestamp = training[training.length - 1];
    const actualTimestamp = dates[index];

    const forecastTimestamp = new Date(
      lastTimestamp.getTime() +
      predictedMinutes * 60000
    );

    const actualMinutes = (
      actualTimestamp.getTime() -
      forecastTimestamp.getTime()
    ) / 60000;

    const absoluteError = Math.abs(actualMinutes);

    const intervalSpread = Math.max(
      3,
      Math.min(
        20,
        standardDeviation(intervals) * 0.25
      )
    );

    const windowStart = new Date(
      forecastTimestamp.getTime() -
      intervalSpread * 60000
    );

    const windowEnd = new Date(
      forecastTimestamp.getTime() +
      intervalSpread * 60000
    );

    const centralHit = absoluteError <= 5;

    const candidateHit =
      actualTimestamp >= windowStart &&
      actualTimestamp <= windowEnd;

    results.push({
      game_type: gameType,
      forecast_origin_at: lastTimestamp.toISOString(),
      forecast_at: forecastTimestamp.toISOString(),
      window_start: windowStart.toISOString(),
      window_end: windowEnd.toISOString(),
      actual_bonus_at: actualTimestamp.toISOString(),
      error_minutes: roundNumber(absoluteError),

      signed_error_minutes: roundNumber(
        (actualTimestamp.getTime() -
          forecastTimestamp.getTime()) / 60000
      ),

      central_hit: centralHit,
      candidate_hit: candidateHit,

      status: centralHit
        ? 'central-hit'
        : candidateHit
          ? 'window-hit'
          : 'missed'
    });
  }

  return results;
}


// ----------------------------------
// SAVE HISTORICAL FORECAST EVALUATIONS
// ----------------------------------

async function saveHistoricalForecasts(results) {
  if (!Array.isArray(results) || results.length === 0) {
    return 0;
  }

  let saved = 0;

  for (const item of results) {
    await pool.query(
      `
      INSERT INTO forecast_evaluations (
        game_type,
        source,
        forecast_origin_at,
        forecast_at,
        window_start,
        window_end,
        actual_bonus_at,
        error_minutes,
        signed_error_minutes,
        central_hit,
        candidate_hit,
        status,
        evaluated_at
      )
      VALUES (
        $1, 'backtest', $2, $3, $4, $5,
        $6, $7, $8, $9, $10, $11, NOW()
      )
      ON CONFLICT (
        game_type,
        source,
        forecast_origin_at
      )
      DO UPDATE SET
        forecast_at = EXCLUDED.forecast_at,
        window_start = EXCLUDED.window_start,
        window_end = EXCLUDED.window_end,
        actual_bonus_at = EXCLUDED.actual_bonus_at,
        error_minutes = EXCLUDED.error_minutes,
        signed_error_minutes = EXCLUDED.signed_error_minutes,
        central_hit = EXCLUDED.central_hit,
        candidate_hit = EXCLUDED.candidate_hit,
        status = EXCLUDED.status,
        evaluated_at = NOW()
      `,
      [
        item.game_type,
        item.forecast_origin_at,
        item.forecast_at,
        item.window_start,
        item.window_end,
        item.actual_bonus_at,
        item.error_minutes,
        item.signed_error_minutes,
        item.central_hit,
        item.candidate_hit,
        item.status
      ]
    );

    saved++;
  }

  console.log(`Historical forecasts saved: ${saved}`);

  return saved;
}


// ----------------------------------
// RUN HISTORICAL FORECAST EVALUATION
// ----------------------------------

async function runHistoricalForecastEvaluation() {
  try {
    const threeRollsResult = await pool.query(`
      SELECT adjusted_at
      FROM game_history
      WHERE is_three_rolls = TRUE
        AND adjusted_at IS NOT NULL
      ORDER BY adjusted_at ASC
    `);

    const fiveRollsResult = await pool.query(`
      SELECT adjusted_at
      FROM game_history
      WHERE is_five_rolls = TRUE
        AND adjusted_at IS NOT NULL
      ORDER BY adjusted_at ASC
    `);

    const threeRollsEvaluations =
      evaluateHistoricalForecasts(
        threeRollsResult.rows.map(row => row.adjusted_at),
        'threeRolls'
      );

    const fiveRollsEvaluations =
      evaluateHistoricalForecasts(
        fiveRollsResult.rows.map(row => row.adjusted_at),
        'fiveRolls'
      );

    const threeSaved =
      await saveHistoricalForecasts(
        threeRollsEvaluations
      );

    const fiveSaved =
      await saveHistoricalForecasts(
        fiveRollsEvaluations
      );

    console.log(
      'HISTORICAL FORECAST EVALUATION COMPLETE',
      {
        threeRolls: threeSaved,
        fiveRolls: fiveSaved
      }
    );
  } catch (error) {
    console.error(
      'Historical forecast evaluation error:',
      error.message
    );
  }
}

// ----------------------------------
// SAVE LIVE FORECAST
// ----------------------------------

async function saveLiveForecast(
  gameType,
  forecast
) {

  try {

    if (
      !forecast ||
      !forecast.lastBonusAt ||
      !Array.isArray(forecast.estimatedWindows)
    ) {
      return false;
    }

    const central = forecast.estimatedWindows.find(
      item => item.label === 'CENTRAL ESTIMATE'
    );

    if (!central) {
      return false;
    }

    const result = await pool.query(
      `
      INSERT INTO forecast_evaluations (
        game_type,
        source,
        forecast_origin_at,
        forecast_at,
        window_start,
        window_end,
        status
      )
      VALUES (
        $1,
        'live',
        $2,
        $3,
        $4,
        $5,
        'pending'
      )
      ON CONFLICT (
        game_type,
        source,
        forecast_origin_at
      )
      DO NOTHING
      RETURNING id
      `,
      [
        gameType,
        forecast.lastBonusAt,
        central.centerAt,
        central.windowStart,
        central.windowEnd
      ]
    );

    return result.rowCount > 0;

  } catch (error) {

    console.error(
      'LIVE FORECAST SAVE ERROR:',
      error.message
    );

    return false;

  }

}


// ----------------------------------
// AUTOMATIC LIVE FORECAST EVALUATION
// ----------------------------------

async function evaluatePendingLiveForecasts() {

  try {

    const result = await pool.query(`

      WITH pending AS (

        SELECT
          f.id,
          f.game_type,
          f.forecast_at,
          f.window_start,
          f.window_end,
          next_bonus.actual_bonus_at

        FROM forecast_evaluations f

        CROSS JOIN LATERAL (

          SELECT
            gh.adjusted_at AS actual_bonus_at

          FROM game_history gh

          WHERE
            gh.adjusted_at > f.forecast_origin_at

            AND (

              (
                f.game_type = 'threeRolls'
                AND gh.is_three_rolls = TRUE
              )

              OR

              (
                f.game_type = 'fiveRolls'
                AND gh.is_five_rolls = TRUE
              )

            )

          ORDER BY gh.adjusted_at ASC

          LIMIT 1

        ) next_bonus

        WHERE
          f.source = 'live'
          AND f.status = 'pending'

      )

      UPDATE forecast_evaluations f

      SET
        actual_bonus_at = p.actual_bonus_at,

        error_minutes = ROUND(
          ABS(
            EXTRACT(
              EPOCH FROM (
                p.actual_bonus_at - p.forecast_at
              )
            ) / 60
          )::numeric,
          1
        ),

        signed_error_minutes = ROUND(
          (
            EXTRACT(
              EPOCH FROM (
                p.actual_bonus_at - p.forecast_at
              )
            ) / 60
          )::numeric,
          1
        ),

        central_hit = (
          ABS(
            EXTRACT(
              EPOCH FROM (
                p.actual_bonus_at - p.forecast_at
              )
            ) / 60
          ) <= 5
        ),

        candidate_hit = (
          p.actual_bonus_at >= p.window_start
          AND p.actual_bonus_at <= p.window_end
        ),

        status = CASE

          WHEN ABS(
            EXTRACT(
              EPOCH FROM (
                p.actual_bonus_at - p.forecast_at
              )
            ) / 60
          ) <= 5

          THEN 'central-hit'

          WHEN (
            p.actual_bonus_at >= p.window_start
            AND p.actual_bonus_at <= p.window_end
          )

          THEN 'window-hit'

          ELSE 'missed'

        END,

        evaluated_at = NOW()

      FROM pending p

      WHERE f.id = p.id

      RETURNING f.id, f.game_type, f.status

    `);

    if (result.rowCount > 0) {

      console.log(
        'LIVE FORECASTS EVALUATED:',
        result.rowCount
      );

      console.log(
        'LIVE FORECAST RESULTS:',
        result.rows
      );

    }

    return result.rowCount;

  } catch (error) {

    console.error(
      'LIVE FORECAST EVALUATION ERROR:',
      error.message
    );

    return 0;

  }

}


// ----------------------------------
// RUN BONUS INTELLIGENCE ANALYSIS
// ----------------------------------

async function runBonusIntelligenceAnalysis() {

  if (intelligenceInProgress) return;

  intelligenceInProgress = true;

  try {

    const result = await pool.query(`

      SELECT
        adjusted_at,
        is_three_rolls,
        is_five_rolls

      FROM game_history

      WHERE (
        is_three_rolls = TRUE
        OR is_five_rolls = TRUE
      )

      ORDER BY adjusted_at ASC

    `);

    const threeRollTimestamps = [];
    const fiveRollTimestamps = [];

    for (const row of result.rows) {

      if (row.is_three_rolls) {
        threeRollTimestamps.push(row.adjusted_at);
      }

      if (row.is_five_rolls) {
        fiveRollTimestamps.push(row.adjusted_at);
      }

    }

    intelligenceCache = {

      lastAnalysisAt: new Date().toISOString(),

      threeRolls: buildBonusForecast(
        threeRollTimestamps
      ),

      fiveRolls: buildBonusForecast(
        fiveRollTimestamps
      ),

      error: null,

      note:
        'Five candidate windows are calculated by resampling historical bonus intervals. They are not five guaranteed predictions. Past timing patterns do not change the game odds or reveal the game RNG.'

    };

    // Evaluate old live forecasts first.
    await evaluatePendingLiveForecasts();

    // Save live Three Rolls forecast.
    await saveLiveForecast(
      'threeRolls',
      intelligenceCache.threeRolls
    );

    // Save live Five Rolls forecast.
    await saveLiveForecast(
      'fiveRolls',
      intelligenceCache.fiveRolls
    );

    console.log(
      'BONUS INTELLIGENCE UPDATED:',
      intelligenceCache.lastAnalysisAt
    );

  } catch (error) {

    intelligenceCache.error = error.message;

    console.error(
      'BONUS INTELLIGENCE ERROR:',
      error.message
    );

  } finally {

    intelligenceInProgress = false;

  }

}


// ----------------------------------
// AI PERFORMANCE MONITOR API
// ----------------------------------

app.get('/api/performance', async (req, res) => {

  try {

    const summary = await pool.query(`
      SELECT
        game_type,

        COUNT(*) FILTER (
          WHERE status <> 'pending'
        )::int AS total_evaluated,

        COUNT(*) FILTER (
          WHERE status = 'pending'
        )::int AS total_pending,

        ROUND(
          AVG(error_minutes) FILTER (
            WHERE status <> 'pending'
          ), 1
        ) AS average_error_minutes,

        ROUND(
          (
            PERCENTILE_CONT(0.5)
            WITHIN GROUP (ORDER BY error_minutes)
            FILTER (WHERE status <> 'pending')
          )::numeric,
          1
        ) AS median_absolute_error_minutes,

        ROUND(
          AVG(signed_error_minutes) FILTER (
            WHERE status <> 'pending'
          ), 1
        ) AS average_signed_error_minutes,

        CASE
          WHEN COUNT(*) FILTER (
            WHERE status <> 'pending'
          ) = 0 THEN NULL

          ELSE ROUND(
            100.0 * COUNT(*) FILTER (
              WHERE status <> 'pending'
              AND central_hit = TRUE
            ) / COUNT(*) FILTER (
              WHERE status <> 'pending'
            ),
            1
          )
        END AS central_hit_rate,

        CASE
          WHEN COUNT(*) FILTER (
            WHERE status <> 'pending'
          ) = 0 THEN NULL

          ELSE ROUND(
            100.0 * COUNT(*) FILTER (
              WHERE status <> 'pending'
              AND candidate_hit = TRUE
            ) / COUNT(*) FILTER (
              WHERE status <> 'pending'
            ),
            1
          )
        END AS candidate_hit_rate,

        MAX(COALESCE(evaluated_at, created_at))
          AS last_updated

      FROM forecast_evaluations

      WHERE source = 'live'

      GROUP BY game_type
    `);

    const recent = await pool.query(`
      SELECT
        game_type,
        forecast_at,
        actual_bonus_at,
        error_minutes,
        signed_error_minutes,
        central_hit,
        candidate_hit,
        status,
        evaluated_at,
        created_at

      FROM forecast_evaluations

      WHERE source = 'live'

      ORDER BY created_at DESC

      LIMIT 15
    `);

    // Historical backtest metrics provide a reference baseline,
    // not a guarantee.
    const backtestSummary = await pool.query(`
      SELECT
        game_type,

        COUNT(*) FILTER (
          WHERE status <> 'pending'
        )::int AS samples,

        ROUND(
          AVG(error_minutes) FILTER (
            WHERE status <> 'pending'
          ),
          1
        ) AS mae_minutes,

        ROUND(
          (
            PERCENTILE_CONT(0.5)
            WITHIN GROUP (ORDER BY error_minutes)
            FILTER (WHERE status <> 'pending')
          )::numeric,
          1
        ) AS median_absolute_error_minutes,

        ROUND(
          AVG(signed_error_minutes) FILTER (
            WHERE status <> 'pending'
          ),
          1
        ) AS signed_error_minutes,

        ROUND(
          100.0 * COUNT(*) FILTER (
            WHERE status <> 'pending'
            AND candidate_hit = TRUE
          )
          / NULLIF(
            COUNT(*) FILTER (
              WHERE status <> 'pending'
            ),
            0
          ),
          1
        ) AS window_coverage_rate

      FROM forecast_evaluations

      WHERE source = 'backtest'

      GROUP BY game_type
    `);

    const byType = new Map(
      summary.rows.map(row => [
        row.game_type,
        row
      ])
    );

    function performanceFor(gameType) {

      const row = byType.get(gameType) || {};

      const baseline =
        backtestSummary.rows.find(
          item => item.game_type === gameType
        ) || {};

      return {

        totalEvaluated:
          Number(row.total_evaluated || 0),

        totalPending:
          Number(row.total_pending || 0),

        sampleCount:
          Number(row.total_evaluated || 0),

        medianAbsoluteErrorMinutes:
          row.median_absolute_error_minutes == null
            ? null
            : Number(row.median_absolute_error_minutes),

        baseline: {

          sampleCount:
            Number(baseline.samples || 0),

          maeMinutes:
            baseline.mae_minutes == null
              ? null
              : Number(baseline.mae_minutes),

          medianAbsoluteErrorMinutes:
            baseline.median_absolute_error_minutes == null
              ? null
              : Number(
                  baseline.median_absolute_error_minutes
                ),

          signedErrorMinutes:
            baseline.signed_error_minutes == null
              ? null
              : Number(baseline.signed_error_minutes),

          windowCoverageRate:
            baseline.window_coverage_rate == null
              ? null
              : Number(baseline.window_coverage_rate)

        },

        averageErrorMinutes:
          row.average_error_minutes == null
            ? null
            : Number(row.average_error_minutes),

        averageSignedErrorMinutes:
          row.average_signed_error_minutes == null
            ? null
            : Number(row.average_signed_error_minutes),

        centralHitRate:
          row.central_hit_rate == null
            ? null
            : Number(row.central_hit_rate),

        candidateHitRate:
          row.candidate_hit_rate == null
            ? null
            : Number(row.candidate_hit_rate)

      };

    }

    const lastUpdated = summary.rows
      .map(row => row.last_updated)
      .filter(Boolean)
      .sort(
        (a, b) => new Date(b) - new Date(a)
      )[0] || null;

    res.json({

      threeRolls:
        performanceFor('threeRolls'),

      fiveRolls:
        performanceFor('fiveRolls'),

      recent:
        recent.rows,

      lastUpdated

    });

  } catch (error) {

    console.error(
      'PERFORMANCE API ERROR:',
      error.message
    );

    res.status(500).json({

      error: 'Unable to load performance data',

      details: error.message

    });

  }

});

// ----------------------------------
// ROLLING STATISTICS API
// ----------------------------------

app.get('/api/rolling-stats', async (req, res) => {
  try {
    const result = await pool.query(`
      WITH bonus_events AS (
        SELECT 'threeRolls'::text AS game_type, adjusted_at
        FROM game_history
        WHERE is_three_rolls = TRUE
          AND adjusted_at IS NOT NULL

        UNION ALL

        SELECT 'fiveRolls'::text AS game_type, adjusted_at
        FROM game_history
        WHERE is_five_rolls = TRUE
          AND adjusted_at IS NOT NULL
      ),

      numbered AS (
        SELECT
          game_type,
          adjusted_at,

          ROW_NUMBER() OVER (
            PARTITION BY game_type
            ORDER BY adjusted_at DESC
          ) AS rn,

          LAG(adjusted_at) OVER (
            PARTITION BY game_type
            ORDER BY adjusted_at DESC
          ) AS next_newer_at

        FROM bonus_events
      ),

      recent AS (
        SELECT
          game_type,
          rn,
          adjusted_at,

          ABS(
            EXTRACT(
              EPOCH FROM (next_newer_at - adjusted_at)
            ) / 60.0
          ) AS interval_minutes

        FROM numbered
      )

      SELECT
        game_type,

        COUNT(*) FILTER (
          WHERE rn <= 50
        )::int AS recent_50_samples,

        ROUND(
          AVG(interval_minutes) FILTER (
            WHERE rn <= 50
          ),
          1
        ) AS recent_50_average_interval_minutes,

        ROUND(
          (
            PERCENTILE_CONT(0.5)
            WITHIN GROUP (ORDER BY interval_minutes)
            FILTER (WHERE rn <= 50)
          )::numeric,
          1
        ) AS recent_50_median_interval_minutes,

        COUNT(*) FILTER (
          WHERE rn <= 100
        )::int AS recent_100_samples,

        ROUND(
          AVG(interval_minutes) FILTER (
            WHERE rn <= 100
          ),
          1
        ) AS recent_100_average_interval_minutes,

        ROUND(
          (
            PERCENTILE_CONT(0.5)
            WITHIN GROUP (ORDER BY interval_minutes)
            FILTER (WHERE rn <= 100)
          )::numeric,
          1
        ) AS recent_100_median_interval_minutes,

        COUNT(*) FILTER (
          WHERE rn <= 250
        )::int AS recent_250_samples,

        ROUND(
          AVG(interval_minutes) FILTER (
            WHERE rn <= 250
          ),
          1
        ) AS recent_250_average_interval_minutes,

        ROUND(
          (
            PERCENTILE_CONT(0.5)
            WITHIN GROUP (ORDER BY interval_minutes)
            FILTER (WHERE rn <= 250)
          )::numeric,
          1
        ) AS recent_250_median_interval_minutes,

        COUNT(*)::int AS total_bonus_events,
        MIN(adjusted_at) AS oldest_bonus_at,
        MAX(adjusted_at) AS newest_bonus_at

      FROM recent
      GROUP BY game_type
    `);

    const daily = await pool.query(`
      SELECT
        (
          adjusted_at AT TIME ZONE 'Asia/Manila'
        )::date AS day,

        COUNT(*) FILTER (
          WHERE is_three_rolls = TRUE
        )::int AS three_rolls,

        COUNT(*) FILTER (
          WHERE is_five_rolls = TRUE
        )::int AS five_rolls,

        COUNT(*) FILTER (
          WHERE is_big_win = TRUE
        )::int AS big_wins

      FROM game_history

      WHERE adjusted_at >= NOW() - INTERVAL '30 days'

      GROUP BY day
      ORDER BY day DESC
    `);

    const byType = Object.fromEntries(
      result.rows.map(row => [
        row.game_type,
        row
      ])
    );

    res.json({
      threeRolls: byType.threeRolls || null,
      fiveRolls: byType.fiveRolls || null,
      dailyLast30Days: daily.rows,

      note:
        'Rolling values describe historical intervals and frequency only; they do not predict or guarantee future outcomes.'
    });

  } catch (error) {
    console.error(
      'ROLLING STATS API ERROR:',
      error.message
    );

    res.status(500).json({
      error: 'Unable to load rolling statistics.'
    });
  }
});


// ----------------------------------
// DATA QUALITY MONITOR API
// ----------------------------------

app.get('/api/data-quality', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        COUNT(*)::int AS total_records,

        COUNT(DISTINCT event_id)::int
          AS unique_event_ids,

        (
          COUNT(*) - COUNT(DISTINCT event_id)
        )::int AS duplicate_event_ids,

        COUNT(*) FILTER (
          WHERE settled_at IS NULL
        )::int AS missing_timestamps,

        COUNT(*) FILTER (
          WHERE event_id IS NULL
             OR BTRIM(event_id) = ''
        )::int AS missing_event_ids,

        COUNT(*) FILTER (
          WHERE COALESCE(is_three_rolls, FALSE) = FALSE
            AND COALESCE(is_five_rolls, FALSE) = FALSE
        )::int AS unclassified_events,

        COUNT(*) FILTER (
          WHERE big_win_multiplier IS NOT NULL
            AND big_win_multiplier < 0
        )::int AS invalid_negative_multipliers,

        COUNT(*) FILTER (
          WHERE is_big_win = TRUE
            AND COALESCE(big_win_multiplier, 0) < 100
        )::int AS inconsistent_big_win_flags,

        MIN(settled_at) AS oldest_record,
        MAX(settled_at) AS newest_record

      FROM game_history
    `);

    const db = result.rows[0];

    res.json({
      checkedAt: new Date().toISOString(),

      database: db,

      polling: {
        inProgress: pollInProgress,
        lastPollAt,
        lastSuccessAt,
        lastPollError,
        lastFetchedCount,
        lastSavedCount,
        intervalMs: POLL_INTERVAL_MS
      },

      backfill: {
        ...backfillStatus,
        nextPage: nextBackfillPage,
        maxPagesPerRun: BACKFILL_MAX_PAGES,
        pageSize: BACKFILL_PAGE_SIZE,
        intervalMs: BACKFILL_INTERVAL_MS
      },

      dataQuality: {
        duplicateEventIds:
          Number(db.duplicate_event_ids || 0),

        missingTimestamps:
          Number(db.missing_timestamps || 0),

        missingEventIds:
          Number(db.missing_event_ids || 0),

        unclassifiedEvents:
          Number(db.unclassified_events || 0),

        invalidNegativeMultipliers:
          Number(db.invalid_negative_multipliers || 0),

        inconsistentBigWinFlags:
          Number(db.inconsistent_big_win_flags || 0)
      },

      note:
        'Quality checks summarize stored records and recent collection status. Historical frequencies do not guarantee future outcomes.'
    });

  } catch (error) {
    console.error(
      'DATA QUALITY API ERROR:',
      error.message
    );

    res.status(500).json({
      error: 'Unable to load data quality status.'
    });
  }
});


// ----------------------------------
// HISTORICAL BACKFILL STATUS API
// ----------------------------------

app.get('/api/backfill/status', (req, res) => {
  res.json({
    ...backfillStatus,
    maxPagesPerRun: BACKFILL_MAX_PAGES,
    pageSize: BACKFILL_PAGE_SIZE,
    intervalMs: BACKFILL_INTERVAL_MS
  });
});


// ----------------------------------
// DATABASE STATUS API
// ----------------------------------

app.get('/api/database/status', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        COUNT(*)::int AS total_records,
        COUNT(DISTINCT event_id)::int AS unique_event_ids,
        MIN(settled_at) AS oldest_record,
        MAX(settled_at) AS newest_record
      FROM game_history
    `);

    res.json({
      success: true,
      database: result.rows[0]
    });

  } catch (error) {
    console.error(
      'DATABASE STATUS ERROR:',
      error.message
    );

    res.status(500).json({
      success: false,
      error: 'Hindi makuha ang database statistics.'
    });
  }
});


// ----------------------------------
// DATABASE EVENT CHECK API
// ----------------------------------

app.get('/api/database/check-event', async (req, res) => {
  try {
    const eventId = req.query.id;

    if (!eventId) {
      return res.status(400).json({
        success: false,
        error: 'Missing event ID. Use ?id=EVENT_ID'
      });
    }

    const result = await pool.query(
      `
      SELECT event_id, settled_at, created_at
      FROM game_history
      WHERE event_id = $1
      LIMIT 1
      `,
      [eventId]
    );

    res.json({
      success: true,
      exists: result.rows.length > 0,
      record: result.rows[0] ?? null
    });

  } catch (error) {
    console.error(
      'EVENT CHECK ERROR:',
      error.message
    );

    res.status(500).json({
      success: false,
      error: 'Database check failed.'
    });
  }
});


// ----------------------------------
// ANALYSIS API
// ----------------------------------

app.get('/api/analysis', (req, res) => {
  res.json(analysisCache);
});


// ----------------------------------
// BONUS INTELLIGENCE API
// ----------------------------------

app.get('/api/intelligence', (req, res) => {
  res.json(intelligenceCache);
});


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

    pollIntervalMs: POLL_INTERVAL_MS,
    analysisIntervalMs: ANALYSIS_INTERVAL_MS,

    lastAnalysisAt: analysisCache.lastAnalysisAt,

    lastIntelligenceAnalysisAt:
      intelligenceCache.lastAnalysisAt
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
      `
      SELECT *
      FROM game_history
      ORDER BY settled_at DESC
      LIMIT $1
      `,
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

    if (
      ['3', 'three', 'three_rolls'].includes(type)
    ) {
      where = 'WHERE is_three_rolls = TRUE';

    } else if (
      ['5', 'five', 'five_rolls'].includes(type)
    ) {
      where = 'WHERE is_five_rolls = TRUE';

    } else if (type === 'all') {
      where = `
        WHERE (
          is_three_rolls = TRUE
          OR is_five_rolls = TRUE
        )
      `;

    } else {
      return res.status(400).json({
        error: 'Invalid type'
      });
    }

    const result = await pool.query(
      `
      SELECT *
      FROM game_history
      ${where}
      ORDER BY settled_at DESC
      LIMIT $1
      `,
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
// STATISTICS API
// ----------------------------------

app.get('/api/stats', async (req, res) => {
  try {

    // Check records with 100x or higher multiplier.
    const bigWinCheck = await pool.query(`
      SELECT
        event_id,
        settled_at,
        is_three_rolls,
        is_five_rolls,
        big_win_multiplier,
        is_big_win
      FROM game_history
      WHERE big_win_multiplier >= 100
      ORDER BY settled_at DESC
    `);

    console.log(
      'BIG WIN RECORD DETAILS:',
      JSON.stringify(bigWinCheck.rows)
    );

    // Overall statistics.
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

    // Hourly statistics in Philippine time.
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
      bigWinRecords: bigWinCheck.rows,
      note:
        'Historical records do not predict future random outcomes.'
    });

  } catch (error) {
    console.error('STATS API ERROR:', error);

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

  // Repair old Big Win records.
  await repairBigWinFlags();

  app.listen(PORT, () => {
    console.log(
      `MONOPOLY TRACKER running on port ${PORT}`
    );

    console.log(
      `Polling every ${POLL_INTERVAL_MS}ms`
    );

    console.log(
      `Smart analysis every ${ANALYSIS_INTERVAL_MS}ms`
    );

    console.log(
      `Bonus intelligence every ${ANALYSIS_INTERVAL_MS}ms`
    );
  });

  await pollUpstream();

  // First analysis on startup.
  await runAutomaticAnalysis();

  // First bonus intelligence analysis.
  await runBonusIntelligenceAnalysis();

  // First historical forecast evaluation.
  runHistoricalForecastEvaluation();

  // Data collection.
  setInterval(
    pollUpstream,
    POLL_INTERVAL_MS
  );

  // Automatic historical backfill scheduler.
  setTimeout(() => {
    runHistoricalBackfill();
  }, 5000);

  setInterval(() => {
    runHistoricalBackfill();
  }, BACKFILL_INTERVAL_MS);

  // Smart analysis every five minutes.
  setInterval(
    runAutomaticAnalysis,
    ANALYSIS_INTERVAL_MS
  );

  // Bonus intelligence every five minutes.
  setInterval(
    runBonusIntelligenceAnalysis,
    ANALYSIS_INTERVAL_MS
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

// ----------------------------------
// LIVE MONITOR
// ----------------------------------

async function pollUpstream() {

