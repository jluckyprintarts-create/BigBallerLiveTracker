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


  // ----------------------------------
  // AI FORECAST PERFORMANCE TABLE
  // ----------------------------------

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ai_forecast_performance (

      id BIGSERIAL PRIMARY KEY,

      game_type TEXT NOT NULL,

      generated_at TIMESTAMPTZ NOT NULL,

      reference_last_bonus_at TIMESTAMPTZ NOT NULL,

      central_center_at TIMESTAMPTZ NOT NULL,

      central_window_start TIMESTAMPTZ NOT NULL,

      central_window_end TIMESTAMPTZ NOT NULL,

      candidate_windows JSONB NOT NULL,

      status TEXT DEFAULT 'pending',

      actual_bonus_at TIMESTAMPTZ,

      error_minutes NUMERIC,

      central_hit BOOLEAN,

      candidate_hit BOOLEAN,

      evaluated_at TIMESTAMPTZ,

      UNIQUE (game_type, generated_at)

    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_ai_performance_pending
    ON ai_forecast_performance(game_type, status)
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


  // DETECT ROLL TYPES

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


  // GET ACTUAL GAME MULTIPLIER

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


  // BIG WIN DISABLED

  const isBigWin = false;


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
// AI FORECAST PERFORMANCE
// ----------------------------------

// SAVE FORECAST SNAPSHOT

async function saveForecastSnapshot(
  gameType,
  forecast,
  generatedAt
) {

  if (
    !forecast ||
    !forecast.lastBonusAt ||
    !Array.isArray(forecast.estimatedWindows) ||
    forecast.estimatedWindows.length !== 5
  ) {
    return;
  }

  const central = forecast.estimatedWindows.find(
    window => window.rank === 3
  );

  if (!central) return;

  await pool.query(`

    INSERT INTO ai_forecast_performance
    (
      game_type,
      generated_at,
      reference_last_bonus_at,
      central_center_at,
      central_window_start,
      central_window_end,
      candidate_windows
    )

    VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb)

    ON CONFLICT (game_type, generated_at)
    DO NOTHING

  `, [

    gameType,
    generatedAt,
    forecast.lastBonusAt,
    central.centerAt,
    central.windowStart,
    central.windowEnd,
    JSON.stringify(forecast.estimatedWindows)

  ]);

}


// EVALUATE ACTUAL BONUS

async function evaluateActualBonus(
  gameType,
  actualBonusAt
) {

  await pool.query(`

    UPDATE ai_forecast_performance

    SET

      status = 'evaluated',

      actual_bonus_at = $2,

      error_minutes = EXTRACT(
        EPOCH FROM (
          $2::timestamptz - central_center_at
        )
      ) / 60,

      central_hit = (
        $2::timestamptz
        BETWEEN central_window_start
        AND central_window_end
      ),

      candidate_hit = EXISTS (

        SELECT 1

        FROM jsonb_array_elements(candidate_windows) AS w

        WHERE

          $2::timestamptz
          BETWEEN
            (w->>'windowStart')::timestamptz

          AND
            (w->>'windowEnd')::timestamptz

      ),

      evaluated_at = NOW()

    WHERE

      game_type = $1

      AND status = 'pending'

      AND generated_at < $2::timestamptz

      AND reference_last_bonus_at < $2::timestamptz

  `, [

    gameType,
    actualBonusAt

  ]);

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

console.log(
  `UPSTREAM RESPONSE: ${rows.length} records fetched`
);
    
    const rows = extractRows(payload);

    lastFetchedCount = rows.length;

    let saved = 0;

    for (const raw of rows) {

      const event = normalizeEvent(raw);

      if (!event) continue;

      const inserted = await saveEvent(event);

      if (inserted) {

        saved++;

        // THREE ROLLS PERFORMANCE EVALUATION

        if (event.isThreeRolls) {

          await evaluateActualBonus(
            'three_rolls',
            event.adjustedAt
          );

        }

        // FIVE ROLLS PERFORMANCE EVALUATION

        if (event.isFiveRolls) {

          await evaluateActualBonus(
            'five_rolls',
            event.adjustedAt
          );

        }

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
// SMART GAME ANALYSIS
// ----------------------------------

async function runAutomaticAnalysis() {

  if (analysisInProgress) return;

  analysisInProgress = true;

  try {

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

          day: dayNames[
            Number(row.day_number)
          ],

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

    for (let index = 5; index < intervals.length; index++) {

      const training = intervals.slice(0, index);
      const actual = intervals[index];
      const predicted = estimateWithModel(training, model);

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
    selected: results[selectedModel]
  };

}


// ----------------------------------
// BOOTSTRAP INTERVAL UNCERTAINTY
// ----------------------------------

function createBootstrapEstimates(intervals, model, simulations = 1500) {

  if (!intervals.length) return [];

  const sampleSize = Math.min(10, intervals.length);
  const estimates = [];

  for (let simulation = 0; simulation < simulations; simulation++) {

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

  } else if (mad > 0 && currentGap > med + (3 * 1.4826 * mad)) {

    result.anomaly = 'unusually-long-gap';

  } else if (mad === 0 && currentGap > med * 2) {

    result.anomaly = 'unusually-long-gap';

  } else {

    result.anomaly = 'within-historical-range';

  }

  const mae = backtest.selected.maeMinutes;

  const relativeMae = (
    mae !== null && med > 0
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

  if (!bootstrap.length || !Number.isFinite(estimatedInterval)) {
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
      lastTimestamp.getTime() + minutesFromLast * 60000
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


    // ----------------------------------
    // SAVE FORECAST PERFORMANCE SNAPSHOTS
    // ----------------------------------

    await saveForecastSnapshot(
      'three_rolls',
      intelligenceCache.threeRolls,
      intelligenceCache.lastAnalysisAt
    );

    await saveForecastSnapshot(
      'five_rolls',
      intelligenceCache.fiveRolls,
      intelligenceCache.lastAnalysisAt
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
// AI PERFORMANCE API
// ----------------------------------

app.get('/api/performance', async (req, res) => {

  try {

    const summary = await pool.query(`

      SELECT

        game_type,

        COUNT(*) FILTER (
          WHERE status = 'evaluated'
        )::int AS total_evaluated,

        COUNT(*) FILTER (
          WHERE status = 'pending'
        )::int AS total_pending,

        ROUND(
          AVG(ABS(error_minutes)) FILTER (
            WHERE status = 'evaluated'
          ),
          1
        ) AS average_error_minutes,

        ROUND(
          AVG(error_minutes) FILTER (
            WHERE status = 'evaluated'
          ),
          1
        ) AS average_signed_error_minutes,

        COUNT(*) FILTER (
          WHERE central_hit = TRUE
        )::int AS central_hits,

        COUNT(*) FILTER (
          WHERE candidate_hit = TRUE
        )::int AS candidate_hits,

        ROUND(
          100.0 * COUNT(*) FILTER (
            WHERE central_hit = TRUE
          ) /
          NULLIF(
            COUNT(*) FILTER (
              WHERE status = 'evaluated'
            ),
            0
          ),
          1
        ) AS central_hit_rate,

        ROUND(
          100.0 * COUNT(*) FILTER (
            WHERE candidate_hit = TRUE
          ) /
          NULLIF(
            COUNT(*) FILTER (
              WHERE status = 'evaluated'
            ),
            0
          ),
          1
        ) AS candidate_hit_rate

      FROM ai_forecast_performance

      GROUP BY game_type

    `);


    const recent = await pool.query(`

      SELECT

        id,

        game_type,

        generated_at,

        reference_last_bonus_at,

        central_center_at,

        actual_bonus_at,

        error_minutes,

        central_hit,

        candidate_hit,

        status,

        evaluated_at

      FROM ai_forecast_performance

      ORDER BY generated_at DESC

      LIMIT 50

    `);


    function getTypeSummary(type) {

      const row = summary.rows.find(
        item => item.game_type === type
      );

      if (!row) {

        return {

          totalEvaluated: 0,

          totalPending: 0,

          averageErrorMinutes: null,

          averageSignedErrorMinutes: null,

          centralHits: 0,

          candidateHits: 0,

          centralHitRate: null,

          candidateHitRate: null

        };

      }

      return {

        totalEvaluated: Number(row.total_evaluated),

        totalPending: Number(row.total_pending),

        averageErrorMinutes:
          row.average_error_minutes === null
            ? null
            : Number(row.average_error_minutes),

        averageSignedErrorMinutes:
          row.average_signed_error_minutes === null
            ? null
            : Number(row.average_signed_error_minutes),

        centralHits: Number(row.central_hits),

        candidateHits: Number(row.candidate_hits),

        centralHitRate:
          row.central_hit_rate === null
            ? null
            : Number(row.central_hit_rate),

        candidateHitRate:
          row.candidate_hit_rate === null
            ? null
            : Number(row.candidate_hit_rate)

      };

    }


    res.json({

      lastUpdated: new Date().toISOString(),

      threeRolls: getTypeSummary('three_rolls'),

      fiveRolls: getTypeSummary('five_rolls'),

      recent: recent.rows,

      note:
        'Performance is measured against saved historical forecast snapshots. Multiple snapshots may be evaluated against the same later bonus. These statistics do not represent winning probability or guarantee future outcomes.'

    });

  } catch (error) {

    console.error(
      'PERFORMANCE API ERROR:',
      error.message
    );

    res.status(500).json({

      error: error.message

    });

  }

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

    console.log(
      `Smart analysis every ${ANALYSIS_INTERVAL_MS}ms`
    );

    console.log(
      `Bonus intelligence every ${ANALYSIS_INTERVAL_MS}ms`
    );

    console.log(
      'AI Performance Monitor enabled'
    );

  });

  await pollUpstream();

  await runAutomaticAnalysis();

  await runBonusIntelligenceAnalysis();

  setInterval(
    pollUpstream,
    POLL_INTERVAL_MS
  );

  setInterval(
    runAutomaticAnalysis,
    ANALYSIS_INTERVAL_MS
  );

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
// BIG WIN MONITOR API (100x+)
// ADD-ON ONLY - EXISTING CODE UNCHANGED
// ----------------------------------

app.get('/api/bigwins', async (req, res) => {

  try {

    const limit = Math.min(
      500,
      Math.max(1, Number(req.query.limit) || 100)
    );

    const summary = await pool.query(`

      SELECT

        COUNT(*)::int AS total_big_wins,

        COUNT(*) FILTER (
          WHERE is_three_rolls = TRUE
        )::int AS three_rolls_big_wins,

        COUNT(*) FILTER (
          WHERE is_five_rolls = TRUE
        )::int AS five_rolls_big_wins,

        MAX(big_win_multiplier) AS highest_multiplier,

        ROUND(
          AVG(big_win_multiplier),
          2
        ) AS average_multiplier

      FROM game_history

      WHERE big_win_multiplier >= 100

    `);


    const recent = await pool.query(`

      SELECT

        event_id,

        settled_at,

        adjusted_at,

        is_three_rolls,

        is_five_rolls,

        big_win_multiplier,

        dealer_name

      FROM game_history

      WHERE big_win_multiplier >= 100

      ORDER BY settled_at DESC

      LIMIT $1

    `, [limit]);


    const hourly = await pool.query(`

      SELECT

        EXTRACT(
          HOUR FROM adjusted_at AT TIME ZONE 'Asia/Manila'
        )::int AS hour,

        COUNT(*)::int AS total_big_wins,

        ROUND(
          MAX(big_win_multiplier),
          2
        ) AS highest_multiplier

      FROM game_history

      WHERE big_win_multiplier >= 100

      GROUP BY hour

      ORDER BY hour

    `);


    res.json({

      success: true,

      threshold: 100,

      lastUpdated: new Date().toISOString(),

      summary: {

        totalBigWins: Number(
          summary.rows[0].total_big_wins
        ),

        threeRollsBigWins: Number(
          summary.rows[0].three_rolls_big_wins
        ),

        fiveRollsBigWins: Number(
          summary.rows[0].five_rolls_big_wins
        ),

        highestMultiplier:
          summary.rows[0].highest_multiplier === null
            ? null
            : Number(summary.rows[0].highest_multiplier),

        averageMultiplier:
          summary.rows[0].average_multiplier === null
            ? null
            : Number(summary.rows[0].average_multiplier)

      },

      recent: recent.rows,

      hourly: hourly.rows,

      note:
        'Big Wins are based on recorded official multipliers of 100x or higher. Historical results do not predict future outcomes.'

    });

  } catch (error) {

    console.error(
      'BIG WIN MONITOR ERROR:',
      error.message
    );

    res.status(500).json({

      success: false,

      error: error.message

    });

  }

});
