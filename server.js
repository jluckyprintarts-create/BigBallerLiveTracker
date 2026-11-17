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

  console.log(
    `Historical forecasts saved: ${saved}`
  );

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

    // EVALUATE OLD LIVE FORECASTS FIRST
    await evaluatePendingLiveForecasts();

    // SAVE LIVE THREE ROLLS FORECAST
    await saveLiveForecast(
      'threeRolls',
      intelligenceCache.threeRolls
    );

    // SAVE LIVE FIVE ROLLS FORECAST
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

  });

  await pollUpstream();

  // FIRST ANALYSIS ON STARTUP
  await runAutomaticAnalysis();

  // FIRST BONUS INTELLIGENCE ANALYSIS
  await runBonusIntelligenceAnalysis();

  // FIRST HISTORICAL FORECAST EVALUATION
  runHistoricalForecastEvaluation();

  // DATA COLLECTION
  setInterval(
    pollUpstream,
    POLL_INTERVAL_MS
  );

  // SMART ANALYSIS EVERY 5 MINUTES
  setInterval(
    runAutomaticAnalysis,
    ANALYSIS_INTERVAL_MS
  );

  // BONUS INTELLIGENCE EVERY 5 MINUTES
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
