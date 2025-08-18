/** ===================== sync_ticker_cache.gs (DROP-IN, incremental + formats) ===================== **/

const TickerCache = (() => {
  const SHEET   = Config.SHEETS.TICKER_CACHE;
  const HEADERS = Config.HEADERS.TICKER_CACHE; // ['ticker','last_refreshed','date','open','high','low','close','volume']
  const AV_BASE = Config.API.ALPHA_VANTAGE_BASE;  // 'https://www.alphavantage.co/query'
  const AV_KEY  = Config.getApiKey(Config.API.ALPHA_VANTAGE_KEY);
  const THROTTLE_MS = 12000; // Alpha Vantage ≈5/min

  /** Ensure sheet exists with header. */
  function ensure_() {
    const ss = SpreadsheetApp.getActive();
    return Utils.Sheets.ensureSheet(ss, SHEET, HEADERS);
  }

  /** Map 'TICKER|YYYY-MM-DD' -> row# (2-based). */
  function buildKeyIndex_(sh) {
    const map = new Map();
    const hdr = Utils.Sheets.header(sh);
    const H   = Utils.Sheets.index(hdr);
    const lastR = sh.getLastRow(), lastC = sh.getLastColumn();
    if (lastR < 2 || lastC === 0) return map;
    const vals = sh.getRange(2, 1, lastR - 1, lastC).getValues();
    for (let i = 0; i < vals.length; i++) {
      const t = String(vals[i][H['ticker']] || '').trim().toUpperCase();
      const d = vals[i][H['date']];
      const day = (d instanceof Date) ? isoDay_(d) : String(d || '').slice(0, 10);
      if (t && day) map.set(t + '|' + day, i + 2);
    }
    return map;
  }

  /** Map 'TICKER' -> latest Date we already have (UTC date, not datetime). */
  function buildLatestByTicker_(sh) {
    const map = new Map();
    const hdr = Utils.Sheets.header(sh);
    const H   = Utils.Sheets.index(hdr);
    const lastR = sh.getLastRow(), lastC = sh.getLastColumn();
    if (lastR < 2 || lastC === 0) return map;
    const vals = sh.getRange(2, 1, lastR - 1, lastC).getValues();
    for (let i = 0; i < vals.length; i++) {
      const t = String(vals[i][H['ticker']] || '').trim().toUpperCase();
      const dCell = vals[i][H['date']];
      const d = (dCell instanceof Date) ? new Date(Date.UTC(dCell.getUTCFullYear(), dCell.getUTCMonth(), dCell.getUTCDate()))
                                        : new Date((String(dCell || '').slice(0, 10)) + 'T00:00:00Z');
      if (!t || isNaN(d)) continue;
      const prev = map.get(t);
      if (!prev || d > prev) map.set(t, d);
    }
    return map;
  }

  /** UTC 'YYYY-MM-DD' from Date. */
  function isoDay_(d) {
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, '0');
    const day = String(d.getUTCDate()).padStart(2, '0');
    return y + '-' + m + '-' + day;
  }

  /** Fetch TIME_SERIES_DAILY JSON for a symbol. */
  function fetchDaily_(symbol, outputsize) {
    const url = AV_BASE + [
      '?function=TIME_SERIES_DAILY',
      '&symbol=', encodeURIComponent(symbol),
      '&apikey=', encodeURIComponent(AV_KEY),
      '&outputsize=', encodeURIComponent(outputsize || 'compact')
    ].join('');
    if (typeof httpFetch_ === 'function') {
      return httpFetch_(url, {
        parseAs: 'json',
        maxRetries: 2,
        backoffMs: 800,
        fetchOpts: { muteHttpExceptions: true, followRedirects: true }
      });
    }
    const res = UrlFetchApp.fetch(url, { muteHttpExceptions: true, followRedirects: true });
    if (res.getResponseCode() !== 200) {
      throw new Error('HTTP ' + res.getResponseCode() + ' ' + res.getContentText().slice(0, 300));
    }
    return JSON.parse(res.getContentText());
  }

  /** Sort by ticker ASC, date DESC. */
  function sortTickerThenDate_(sh) {
    const hdr = Utils.Sheets.header(sh);
    const H   = Utils.Sheets.index(hdr);
    const last = sh.getLastRow();
    if (last <= 2) return;
    sh.getRange(2, 1, last - 1, sh.getLastColumn())
      .sort([
        { column: (H['ticker'] + 1), ascending: true  },
        { column: (H['date']   + 1), ascending: false }
      ]);
  }

  /**
   * Incremental upsert of Alpha Vantage TIME_SERIES_DAILY into ticker_cache.
   * - Only appends days NEWER than the latest date already stored per ticker.
   * - last_refreshed = the write timestamp (now) for each newly appended row.
   * @param {{symbols?: string[], outputsize?: 'compact'|'full', limit?: number, sleepMs?: number}} options
   */
  function fillFromAlpha(options) {
    options = options || {};
    const sh = ensure_();
    const ss = SpreadsheetApp.getActive();

    // ---- Build symbol list (from options or fe_fixed)
    let symbols = [];
    if (Array.isArray(options.symbols) && options.symbols.length) {
      symbols = options.symbols.slice();
    } else {
      const fe = ss.getSheetByName(Config.SHEETS.FRONTEND_FIXED);
      if (fe) {
        const feHdr = Utils.Sheets.header(fe);
        const Hfe   = Utils.Sheets.index(feHdr);
        const col   = Hfe['ticker'];
        if (col != null) {
          const lastR = fe.getLastRow();
          if (lastR > 1) {
            const vals = fe.getRange(2, col + 1, lastR - 1, 1).getValues();
            const set = new Set();
            for (let i = 0; i < vals.length; i++) {
              const v = (vals[i][0] || '').toString().trim();
              if (v) set.add(v);
            }
            symbols = Array.from(set);
          }
        }
      }
    }
    symbols = symbols
      .map(s => String(s || '').toUpperCase().trim())
      .filter(s => s && /^[A-Z.]{1,6}$/.test(s));
    if (options.limit && options.limit > 0) symbols = symbols.slice(0, options.limit);

    SpreadsheetApp.getActive().toast('TickerCache: fetching ' + symbols.length + ' symbol(s)', 'WSB', 3);
    if (!symbols.length) return;

    const keyIndex = buildKeyIndex_(sh);
    const latestBy = buildLatestByTicker_(sh); // NEW: per-symbol newest date we have
    const hdr = Utils.Sheets.header(sh);
    const H   = Utils.Sheets.index(hdr);
    const sleepMs = (typeof options.sleepMs === 'number' && options.sleepMs >= 0) ? options.sleepMs : THROTTLE_MS;

    let fetched = 0;
    for (let i = 0; i < symbols.length; i++) {
      const sym = symbols[i];

      if (fetched > 0 && sleepMs) Utilities.sleep(sleepMs);

      let json;
      try {
        json = fetchDaily_(sym, options.outputsize || 'compact');
      } catch (e) {
        Utils.Log.append('error', 'AlphaVantage fetch failed', sym + ' :: ' + e.message);
        SpreadsheetApp.getActive().toast('AV error: ' + sym, 'WSB', 3);
        continue;
      }

      if (json && (json.Note || json['Error Message'])) {
        Utils.Log.append('warn', 'AlphaVantage note/error', sym + ' :: ' + (json.Note || json['Error Message']));
        SpreadsheetApp.getActive().toast('AV limit hit on ' + sym + ' — stopping', 'WSB', 4);
        break;
      }

      // Find the time series object
      let series = null;
      for (const k in json) { if (/Time Series/i.test(k)) { series = json[k]; break; } }
      if (!series) {
        Utils.Log.append('warn', 'AlphaVantage: no series', sym);
        SpreadsheetApp.getActive().toast('No series: ' + sym, 'WSB', 3);
        continue;
      }

      const cutoffDate = latestBy.get(sym) || new Date(0); // only add days newer than this
      const nowStamp = new Date(); // write-time we store in last_refreshed
      const dates = Object.keys(series).sort(); // oldest -> newest
      const rowsForSymbol = [];

      for (let d = 0; d < dates.length; d++) {
        const day = dates[d]; // 'YYYY-MM-DD'
        const dayObj = new Date(day + 'T00:00:00Z');
        if (!(dayObj > cutoffDate)) continue; // only strictly newer

        // Avoid duplicates via keyIndex (should not happen after cutoff, but safe)
        const key = sym + '|' + day;
        if (keyIndex.has(key)) continue;

        const rowJson = series[day] || {};
        const open   = parseFloat(rowJson['1. open']  || rowJson['1. Open']  || '');
        const high   = parseFloat(rowJson['2. high']  || rowJson['2. High']  || '');
        const low    = parseFloat(rowJson['3. low']   || rowJson['3. Low']   || '');
        const close  = parseFloat(rowJson['4. close'] || rowJson['4. Close'] || '');
        const volume = parseFloat(rowJson['6. volume'] || rowJson['5. volume'] || '');

        const obj = {
          ticker: sym,
          last_refreshed: nowStamp,              // <-- per-row write timestamp (fix #1)
          date: dayObj,
          open:   isFinite(open)   ? open   : '',
          high:   isFinite(high)   ? high   : '',
          low:    isFinite(low)    ? low    : '',
          close:  isFinite(close)  ? close  : '',
          volume: isFinite(volume) ? volume : ''
        };

        const arr = Utils.Sheets.toRowArray(HEADERS, obj);
        rowsForSymbol.push(arr);
        keyIndex.set(key, -1); // reserve
      }

      // Append only the new rows for this symbol
      if (rowsForSymbol.length) {
        const start = sh.getLastRow() + 1;
        sh.getRange(start, 1, rowsForSymbol.length, HEADERS.length).setValues(rowsForSymbol);
      }

      fetched++;
      SpreadsheetApp.getActive().toast('Cached ' + sym + ' (' + rowsForSymbol.length + ' new)', 'WSB', 2);
    }

    // Formats (fix #2: 2 decimals for OHLC)
    const lastR = sh.getLastRow();
    if (lastR > 1) {
      if (H['last_refreshed'] != null) sh.getRange(2, H['last_refreshed']+1, lastR-1, 1).setNumberFormat('yyyy-mm-dd hh:mm');
      if (H['date'] != null)           sh.getRange(2, H['date']+1,          lastR-1, 1).setNumberFormat('yyyy-mm-dd');
      const twoDec = '0.00';
      if (H['open']   != null) sh.getRange(2, H['open']  +1, lastR-1, 1).setNumberFormat(twoDec);
      if (H['high']   != null) sh.getRange(2, H['high']  +1, lastR-1, 1).setNumberFormat(twoDec);
      if (H['low']    != null) sh.getRange(2, H['low']   +1, lastR-1, 1).setNumberFormat(twoDec);
      if (H['close']  != null) sh.getRange(2, H['close'] +1, lastR-1, 1).setNumberFormat(twoDec);
      if (H['volume'] != null) sh.getRange(2, H['volume']+1, lastR-1, 1).setNumberFormat('0');
    }

    // Final sort + tidy
    sortTickerThenDate_(sh);
    fixRowFormat(sh, 21);
    SpreadsheetApp.getActive().toast('TickerCache: done', 'WSB', 3);
  }

  /** Debug helper: preview one symbol. */
  function debugAlpha(symbol) {
    const sym = (symbol || 'AAPL').toUpperCase();
    let json;
    try {
      json = fetchDaily_(sym, 'compact');
    } catch (e) {
      SpreadsheetApp.getActive().toast('AlphaVantage error: ' + e.message, 'WSB', 5);
      throw e;
    }

    const ss   = SpreadsheetApp.getActive();
    const name = 'debug_alpha_' + sym;
    const sh   = ss.getSheetByName(name) || ss.insertSheet(name);
    sh.clear();

    let series = null;
    for (const k in json) { if (/Time Series/i.test(k)) { series = json[k]; break; } }
    if (!series) {
      const msg =
        json.Note ||
        json.Information ||            
        json['Error Message'] ||
        'No "Time Series" in response';
      sh.getRange(1,1,1,2).setValues([['message', msg]]);
      fixRowFormat(sh, 21);
      return;
    }

    const dates = Object.keys(series).sort().reverse(); // newest first
    const table = [['date','open','high','low','close','volume']];
    const n = Math.min(10, dates.length);
    for (let i = 0; i < n; i++) {
      const day = dates[i];
      const row = series[day] || {};
      table.push([
        day,
        row['1. open']  || row['1. Open']  || '',
        row['2. high']  || row['2. High']  || '',
        row['3. low']   || row['3. Low']   || '',
        row['4. close'] || row['4. Close'] || '',
        row['6. volume'] || row['5. volume'] || ''
      ]);
    }

    sh.getRange(1, 1, table.length, table[0].length).setValues(table);
    fixRowFormat(sh, 21);
    SpreadsheetApp.getActive().toast('Debug sheet: ' + name, 'WSB', 3);
  }

  return { fillFromAlpha, debugAlpha };
})();

/* Wrappers */
function TickerCache_fillFromAlpha() { return TickerCache.fillFromAlpha({}); }
function TickerCache_debugAlpha()    { return TickerCache.debugAlpha('AAPL'); }
