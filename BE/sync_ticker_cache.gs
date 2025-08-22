/** ===================== sync_ticker_cache_fmp_dropin.gs ===================== **
 * Live-safe daily appender:
 *   - Existing tickers: fetch FMP batch quotes → INSERT one new daily row (OHLCV) after market close.
 *   - New tickers: call your existing AlphaVantage history importer (unchanged).
 *   - No overwrites. No schema/header changes. No fabricated values.
 *
 * Sheet headers (ticker_cache): ticker | last_refreshed | date | open | high | low | close | volume
 * Sheet headers (fe_fixed): must contain a symbol column (prefer header 'ticker' or 'symbol'; falls back to col 1)
 *
 * Requirements:
 *   - Project Settings → Script properties → FMP_API_KEY
 *   - Config.SHEETS.FRONTEND_FIXED & Config.SHEETS.TICKER_CACHE already defined in your project.
 */

/* ===================== Options ===================== */
const SYNC_OPTIONS = {
  DRY_RUN: true,  // true: log what would happen; false: actually write
  DO_SORT: true,   // resort ticker_cache at the end (ticker ASC, date DESC)
};

const INSERT_GUARDS = {
  // Only insert after US market close (16:00 ET). If false, we still enforce “one row per (ticker, date)”.
  REQUIRE_MARKET_CLOSED: true,
  MARKET_TZ: 'America/New_York',
  MARKET_CLOSE_HOUR: 16,
  MARKET_CLOSE_MIN: 0,
  // We require ALL of: open, high, low, price, volume to be finite numbers. If any missing → skip insert.
  REQUIRE_FULL_OHLCV: true,
};

/* ===================== Column bindings (exact headers) ===================== */
const COLS = {
  ticker:        'ticker',
  last_refreshed:'last_refreshed',
  date:          'date',
  open:          'open',
  high:          'high',
  low:           'low',
  close:         'close',
  volume:        'volume'
};

/* ===================== Public entrypoints ===================== */

/** Full sync:
 *  - Existing tickers → INSERT a new daily row from FMP.
 *  - New tickers → AV history importer.
 *  - No duplicates, only after close.
 *  - Sort table at the end.
 */
function sync_ticker_cache() {
  _run_sync_core_();
}

/* ===================== Core ===================== */

function _run_sync_core_() {
  const feSymbols = getFeFixedSymbols_();
  if (!feSymbols.length) { console.log('[SYNC] fe_fixed empty'); return; }

  const cacheSyms = getCacheSymbols_();
  const cacheSet  = new Set(cacheSyms.map(upr));

  const newSymbols      = feSymbols.filter(s => !cacheSet.has(upr(s)));
  const existingSymbols = feSymbols.filter(s =>  cacheSet.has(upr(s)));

  // 1) Existing → FMP INSERT daily rows (no overwrite)
  if (existingSymbols.length) {
    const inserted = insertDailyRowsFromFmp_(existingSymbols);
    console.log(`[SYNC] FMP inserted rows: ${inserted}${SYNC_OPTIONS.DRY_RUN ? ' (dry-run)' : ''}`);
  } else {
    console.log('[SYNC] No existing tickers to insert via FMP.');
  }

  // 2) New → AlphaVantage history importer (unchanged)
  if (newSymbols.length && !SYNC_OPTIONS.DRY_RUN) {
    importNewTickersHistoryWithAV_(newSymbols);
  } else if (newSymbols.length) {
    console.log(`[SYNC] DRY_RUN: would call AV history for: ${newSymbols.join(', ')}`);
  }

  // 3) Sort canonical order to match your pipeline
  if (SYNC_OPTIONS.DO_SORT && !SYNC_OPTIONS.DRY_RUN) {
    sortTickerCache_(); // ticker ASC, date DESC
  }
}

/* ===================== FMP (fetch + parse) ===================== */

function getFmpApiKey_() {
  const key = PropertiesService.getScriptProperties().getProperty('FMP_KEY');
  if (!key) throw new Error('FMP_KEY not set in Script Properties.');
  return key;
}

function fmpFetchBatchRaw_(symbols) {
  if (!symbols || !symbols.length) return [];
  const key = getFmpApiKey_();
  const chunks = chunk_(symbols, 100);
  const out = [];

  for (const group of chunks) {
    const url = 'https://financialmodelingprep.com/api/v3/quote/' +
                encodeURIComponent(group.join(',')) +
                `?apikey=${encodeURIComponent(key)}`;
    let code = 0, payload = [];
    try {
      const res = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
      code = res.getResponseCode();
      const text = res.getContentText() || '[]';
      payload = JSON.parse(text);
    } catch (e) {
      console.log(`[FMP] Request error for [${group.join(', ')}]: ${e && e.message ? e.message : e}`);
      payload = [];
    }
    out.push({ group, code, payload });
  }
  return out;
}

/** Parse /quote → strict OHLCV + timestamp (no fabrication; skip if incomplete) */
function fmpFetchBatchDailyOhlcv_(symbols) {
  const raw = fmpFetchBatchRaw_(symbols);
  const out = [];

  for (const { group, code, payload } of raw) {
    if (code !== 200 || !Array.isArray(payload)) continue;
    payload.forEach(item => {
      if (!item || !item.symbol) return;

      // Extract fields we care about
      const open   = toNum_(item.open);
      const high   = toNum_(item.dayHigh);
      const low    = toNum_(item.dayLow);
      const close  = toNum_(item.price);
      const volume = toNum_(item.volume);

      // If strict required and any invalid → skip
      if (INSERT_GUARDS.REQUIRE_FULL_OHLCV &&
          !(isFinite(open) && isFinite(high) && isFinite(low) && isFinite(close) && isFinite(volume))) {
        console.log(`[FMP] Skip ${item.symbol}: incomplete OHLCV`);
        return;
      }

      const isoTs = item.timestamp ? new Date(item.timestamp * 1000).toISOString()
                                   : new Date().toISOString();

      out.push({
        symbol: String(item.symbol).trim(),
        open, high, low, close, volume,
        ts: isoTs
      });
    });
  }
  return out;
}

/* ===================== Writes (INSERT-only) ===================== */

function insertDailyRowsFromFmp_(symbols) {
  const quotes = fmpFetchBatchDailyOhlcv_(symbols);
  if (!quotes.length) { console.log('[FMP] No quotes parsed for insert.'); return 0; }

  if (INSERT_GUARDS.REQUIRE_MARKET_CLOSED) {
    const now = new Date();
    const h = parseInt(Utilities.formatDate(now, INSERT_GUARDS.MARKET_TZ, 'HH'), 10);
    const m = parseInt(Utilities.formatDate(now, INSERT_GUARDS.MARKET_TZ, 'mm'), 10);

    const closed = (h > INSERT_GUARDS.MARKET_CLOSE_HOUR) ||
                  (h === INSERT_GUARDS.MARKET_CLOSE_HOUR && m >= INSERT_GUARDS.MARKET_CLOSE_MIN);
    if (!closed) {
      console.log(`[INSERT] Market not closed yet in ${INSERT_GUARDS.MARKET_TZ}; skip inserting daily rows. Current ET=${h}:${m}`);
      return 0;
    }
  }

  const sh   = getSheet_(Config.SHEETS.TICKER_CACHE);
  const hdrs = readHeaderMap_(sh);

  const colTicker = mustCol_(hdrs, COLS.ticker);
  const colLR     = mustCol_(hdrs, COLS.last_refreshed);
  const colDate   = mustCol_(hdrs, COLS.date);
  const colOpen   = mustCol_(hdrs, COLS.open);
  const colHigh   = mustCol_(hdrs, COLS.high);
  const colLow    = mustCol_(hdrs, COLS.low);
  const colClose  = mustCol_(hdrs, COLS.close);
  const colVol    = mustCol_(hdrs, COLS.volume);

  const lastCol  = sh.getLastColumn();
  const lastRow0 = sh.getLastRow();

  // Build (TICKER -> Set(yyyy-MM-dd)) to avoid duplicates
  const existingDatesBySym = buildExistingDatesIndex_(sh, colTicker, colDate);

  // Construct rows
  const rows = [];
  let skippedDup = 0;

  for (const q of quotes) {
    const ts = new Date(q.ts);
    const dateStr = Utilities.formatDate(ts, INSERT_GUARDS.MARKET_TZ, 'yyyy-MM-dd');

    const key = upr(q.symbol);
    const seen = existingDatesBySym.get(key);
    if (seen && seen.has(dateStr)) {
      skippedDup++;
      continue;
    }

    const seen2 = existingDatesBySym.get(key);
    if (seen2 && seen2.has(dateStr)) {
      skippedDup++;
      continue;
    }

    // Row buffer initialised to width
    const row = new Array(lastCol).fill('');
    row[colTicker - 1] = q.symbol;
    row[colLR - 1]     = q.ts;       // ISO UTC
    row[colDate - 1]   = dateStr;    // Market date (ET)
    row[colOpen - 1]   = q.open;
    row[colHigh - 1]   = q.high;
    row[colLow - 1]    = q.low;
    row[colClose - 1]  = q.close;
    row[colVol - 1]    = q.volume;

    rows.push(row);
  }

  if (skippedDup) console.log(`[INSERT] Skipped ${skippedDup} duplicate (ticker,date) rows.`);

  if (!rows.length) {
    console.log('[INSERT] Nothing to insert (after duplicate checks).');
    return 0;
  }

  if (SYNC_OPTIONS.DRY_RUN) {
    console.log(`[INSERT] DRY_RUN: would insert ${rows.length} rows after R=${lastRow0}`);
    return rows.length;
  }

  // Append in one shot
  sh.insertRowsAfter(lastRow0, rows.length);
  sh.getRange(lastRow0 + 1, 1, rows.length, lastCol).setValues(rows);
  console.log(`[INSERT] Inserted ${rows.length} rows.`);

  return rows.length;
}

/* ===================== New ticker flow (AV history) ===================== */

function importNewTickersHistoryWithAV_(symbols) {
  const loader = resolveAvHistoryLoader_();
  if (!loader) {
    console.log('[AV] No AV history loader function found. Skipping new symbols.');
    return;
  }
  console.log(`[AV] Importing AV history for ${symbols.length} ticker(s) via ${loader.name}`);
  for (const s of symbols) {
    try {
      loader(s);
    } catch (e) {
      console.log(`[AV] Failed ${s}: ${e && e.message ? e.message : e}`);
    }
  }
}

function resolveAvHistoryLoader_() {
  const candidates = [
    'avFetchHistoryForNewTicker',
    'fetchAlphaVantageHistoryForNewTicker',
    'avFetchHistory',
    'fetchAVHistory'
  ];
  for (const name of candidates) {
    const fn = (typeof globalThis !== 'undefined' ? globalThis[name] : this[name]);
    if (typeof fn === 'function') return fn;
  }
  return null;
}

/* ===================== Sorting (same as your pipeline) ===================== */

function sortTickerCache_() {
  const sh = getSheet_(Config.SHEETS.TICKER_CACHE);
  const hdrs = readHeaderMap_(sh);
  const tickerCol = hdrs[COLS.ticker];
  const dateCol   = hdrs[COLS.date];
  if (!tickerCol || !dateCol) {
    console.log('[SORT] Missing headers; skip.');
    return;
  }
  const lastRow = sh.getLastRow();
  const lastCol = sh.getLastColumn();
  if (lastRow <= 1) return;

  sh.getRange(2, 1, lastRow - 1, lastCol).sort([
    { column: tickerCol, ascending: true  },
    { column: dateCol,   ascending: false }
  ]);
  console.log('[SORT] ticker_cache sorted: ticker ASC, date DESC');
}

/* ===================== Sheet helpers ===================== */

function getSheet_(name) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(name);
  if (!sh) throw new Error(`Sheet not found: ${name}`);
  return sh;
}

function readHeaderMap_(sheet) {
  const row = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0] || [];
  const map = {};
  row.forEach((h, i) => {
    const key = String(h || '').trim();
    if (key) map[key] = i + 1; // 1-based
  });
  return map;
}

function mustCol_(hdrs, name) {
  const c = hdrs[name];
  if (!c) throw new Error(`Header missing in ticker_cache: "${name}"`);
  return c;
}

function getCacheSymbols_() {
  const cache = getSheet_(Config.SHEETS.TICKER_CACHE);
  const hdrs = readHeaderMap_(cache);
  const symCol = mustCol_(hdrs, COLS.ticker);
  const count = Math.max(0, cache.getLastRow() - 1);
  if (!count) return [];
  const vals = cache.getRange(2, symCol, count, 1).getValues();
  return vals.map(r => String(r[0] || '').trim()).filter(Boolean);
}

function getFeFixedSymbols_() {
  const fe = getSheet_(Config.SHEETS.FRONTEND_FIXED);
  const hdrs = readHeaderMap_(fe);
  // prefer 'ticker' or 'symbol'
  const symCol = hdrs['ticker'] || hdrs['symbol'] || 1;
  const count = Math.max(0, fe.getLastRow() - 1);
  if (!count) return [];
  const vals = fe.getRange(2, symCol, count, 1).getValues();
  return vals.map(r => String(r[0] || '').trim()).filter(Boolean);
}

/* ===================== Index builders & utils ===================== */

function buildExistingDatesIndex_(sheet, colTicker, colDate) {
  const lastRow = sheet.getLastRow();
  const rowCount = Math.max(0, lastRow - 1);
  const width = Math.max(colTicker, colDate);
  const map = new Map();
  if (!rowCount) return map;

  // Use display values so dates are always strings as rendered in the sheet
  const vals = sheet.getRange(2, 1, rowCount, width).getDisplayValues();

  for (let i = 0; i < rowCount; i++) {
    const sym  = String(vals[i][colTicker - 1] || '').trim().toUpperCase();
    const dStr = String(vals[i][colDate   - 1] || '').trim();
    if (!sym || !dStr) continue;

    // Normalize to yyyy-MM-dd (handles e.g. 2025/08/21, 8/21/2025, 2025-08-21 00:00:00, etc.)
    const norm = normalizeDateStr_(dStr);
    if (!norm) continue;

    if (!map.has(sym)) map.set(sym, new Set());
    map.get(sym).add(norm);
  }
  return map;
}

function normalizeDateStr_(s) {
  // try common forms, fall back to first 10 chars if already yyyy-MM-dd
  // 1) yyyy-MM-dd or yyyy/MM/dd
  let m = s.match(/^(\d{4})[\/-](\d{1,2})[\/-](\d{1,2})/);
  if (m) {
    const y = m[1], mo = m[2].padStart(2,'0'), d = m[3].padStart(2,'0');
    return `${y}-${mo}-${d}`;
  }
  // 2) mm/dd/yyyy
  m = s.match(/^(\d{1,2})[\/-](\d{1,2})[\/-](\d{4})/);
  if (m) {
    const mo = m[1].padStart(2,'0'), d = m[2].padStart(2,'0'), y = m[3];
    return `${y}-${mo}-${d}`;
  }
  // 3) ISO-like: 2025-08-21T...
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0,10);
  return null;
}


function toEt_(d) {
  // Convert Date 'd' to America/New_York wall time by formatting & re-parsing
  const s = Utilities.formatDate(d, INSERT_GUARDS.MARKET_TZ, "yyyy/MM/dd HH:mm:ss");
  return new Date(s + " GMT-0500"); // value never used as UTC; only hours/min extraction via formatDate above anyway
}

function upr(s) { return String(s || '').trim().toUpperCase(); }

function toNum_(v) {
  if (v === null || v === undefined || v === '') return NaN;
  const n = Number(v);
  return isFinite(n) ? n : NaN;
}

function chunk_(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}



/** ===================== DEDUPERS (safe) ===================== **/

const DEDUPE_OPTIONS = {
  DRY_RUN: false,                  // set to false to actually delete
  MARKET_TZ: 'America/New_York',  // must match your daily-date semantics
};

// Entrypoints you can run from the menu:

function dedupe_today_only() {
  const todayEt = Utilities.formatDate(new Date(), DEDUPE_OPTIONS.MARKET_TZ, 'yyyy-MM-dd');
  _dedupe_by_date_(todayEt);
}

function dedupe_specific_date_yyyy_mm_dd() {
  // Edit this value then run:
  const target = '2025-08-21'; // <-- change as needed
  _dedupe_by_date_(target);
}

function dedupe_all_dates() {
  _dedupe_by_date_(null);  // null => scan all dates
}

// Core:

function _dedupe_by_date_(yyyy_mm_dd /* string | null */) {
  const sh = getSheet_(Config.SHEETS.TICKER_CACHE);
  const hdrs = readHeaderMap_(sh);

  const colTicker = mustCol_(hdrs, 'ticker');
  const colLR     = mustCol_(hdrs, 'last_refreshed');
  const colDate   = mustCol_(hdrs, 'date');

  const lastRow = sh.getLastRow();
  const lastCol = sh.getLastColumn();
  if (lastRow <= 1) { console.log('[DEDUP] No rows.'); return; }

  // Always use display values for date; raw values for last_refreshed to parse ISO safely
  const tickers = sh.getRange(2, colTicker, lastRow - 1, 1).getDisplayValues();
  const datesD  = sh.getRange(2, colDate,   lastRow - 1, 1).getDisplayValues();
  const lrefRaw = sh.getRange(2, colLR,     lastRow - 1, 1).getValues();

  // Build groups: key = TICKER|YYYY-MM-DD
  const groups = new Map(); // key -> [{row, lrMs}]
  for (let i = 0; i < tickers.length; i++) {
    const row = i + 2;
    const t = String(tickers[i][0] || '').trim().toUpperCase();
    const dDisp = String(datesD[i][0] || '').trim();
    if (!t || !dDisp) continue;

    const dNorm = normalizeDateStr_(dDisp); // from the earlier patch
    if (!dNorm) continue;

    if (yyyy_mm_dd && dNorm !== yyyy_mm_dd) continue; // filter by specific date if provided

    const lrVal = lrefRaw[i][0];
    const lrMs = typeof lrVal === 'string' ? Date.parse(lrVal)
               : (lrVal instanceof Date ? lrVal.getTime() : NaN);

    const key = `${t}|${dNorm}`;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push({ row, lrMs: isFinite(lrMs) ? lrMs : -Infinity });
  }

  // Decide deletions: keep the one with max last_refreshed (lrMs), delete the rest
  const toDelete = [];
  groups.forEach((arr, key) => {
    if (arr.length <= 1) return;
    arr.sort((a,b)=> b.lrMs - a.lrMs);
    const keep = arr[0];
    const dels = arr.slice(1).map(x => x.row);
    toDelete.push(...dels);
    console.log(`[DEDUP] ${key} keep R=${keep.row}, delete R=${dels.join(',')}`);
  });

  if (!toDelete.length) {
    console.log('[DEDUP] Nothing to delete (no duplicates found).');
    return;
  }

  // Delete from bottom up so row indices remain valid
  toDelete.sort((a,b)=> b - a);

  if (DEDUPE_OPTIONS.DRY_RUN) {
    console.log(`[DEDUP] DRY RUN: would delete ${toDelete.length} rows: ${toDelete.join(',')}`);
    return;
  }

  toDelete.forEach(r => sh.deleteRow(r));
  console.log(`[DEDUP] Deleted ${toDelete.length} duplicate rows.`);

  // Optional: re-apply your canonical sort
  if (SYNC_OPTIONS?.DO_SORT) {
    sortTickerCache_();
  }
}
