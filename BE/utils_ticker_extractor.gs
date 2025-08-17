/** ===================== utils_ticker_extractor.gs ===================== **
 * Bulletproof ticker extraction with ranked evidence & title-first selection.
 * API:
 *   - SymbolsIndex.build(sheetOrTable)
 *   - Ticker.extract(title, idx)                 // title-only, fast
 *   - Ticker.extractFallback(title, body, idx)   // title+body, robust
 *   - Ticker.testActiveRow()                     // helper
 */

/* ---------------- Symbols index ---------------- */

const SymbolsIndex = (() => {
  function build(input) {
    const table = (typeof input.getRange === 'function')
      ? Utils.Sheets.readTable(input)
      : input;

    const H = Utils.Sheets.index(table.header);
    ['symbol','name','exchange'].forEach(k => {
      if (!(k in H)) throw new Error('Symbols needs columns: symbol, name, exchange (missing: ' + k + ')');
    });

    const tickerSet            = new Set();   // all valid tickers (UPPER)
    const rootBuckets          = {};          // root -> [full tickers]
    const nameFullOrCollapsed  = {};          // exact name / collapsed name -> full
    const singleLetterKeywords = {};          // 'U' -> Set{'UNITY','UNITYSOFTWARE',...}

    const NAME_STOP = new Set([
      'INC','INCORPORATED','CORP','CORPORATION','HOLDING','HOLDINGS','LTD','PLC','LLC',
      'CO','COMPANY','TRUST','FUND','ETF','CAPITAL','ACQUISITION','ACQUISITIONS','SPAC',
      'ENTERPRISES','INDUSTRIES','INDUSTRY','GROUP','SYSTEMS','SOFTWARE','TECH','TECHNOLOGY',
      'BIO','BIOSCIENCES','PHARMA','PHARMACEUTICALS','RESOURCES','LABS','LABORATORIES',
      'CLASS','UNIT','UNITS','RIGHT','RIGHTS','WARRANT','WARRANTS'
    ]);

    const up = s => (s || '').toString().trim().toUpperCase();

    for (const r of table.rows) {
      const full = up(r[H.symbol]);
      if (!full) continue;

      tickerSet.add(full);
      const root = full.split('.')[0];
      (rootBuckets[root] || (rootBuckets[root] = [])).push(full);

      const comp = up(r[H.name]);
      if (!comp) continue;

      // strong brand cue: exact & collapsed
      if (nameFullOrCollapsed[comp] === undefined) nameFullOrCollapsed[comp] = full;
      if (comp.indexOf(' ') >= 0) {
        const collapsed = comp.replace(/\s+/g, '');
        if (!nameFullOrCollapsed[collapsed]) nameFullOrCollapsed[collapsed] = full;
      }

      // single-letter support: collect brand keywords (>=3 chars, not generic)
      if (full.length === 1) {
        const kw = new Set();
        for (const w of comp.split(/\W+/).filter(Boolean)) {
          const W = up(w);
          if (W.length >= 3 && !NAME_STOP.has(W)) kw.add(W);
        }
        if (comp.indexOf(' ') >= 0) {
          const collapsedKW = comp.replace(/\s+/g, '').toUpperCase();
          if (collapsedKW.length >= 3 && !NAME_STOP.has(collapsedKW)) kw.add(collapsedKW);
        }
        if (kw.size) singleLetterKeywords[full] = kw;
      }
    }

    // roots that map uniquely to a full variant
    const rootToFull = {};
    Object.keys(rootBuckets).forEach(root => {
      const arr = rootBuckets[root];
      if (arr.length === 1) rootToFull[root] = arr[0];
    });

    return { tickerSet, rootToFull, nameFullOrCollapsed, singleLetterKeywords };
  }

  return { build };
})();

/* ---------------- Ranked extraction ---------------- */

const Ticker = (() => {
  // Stopwords for bare tokens (expanded to kill English words that collide with tickers)
  const STOP = new Set([
    'A','I','M','AND','OR','THE','FOR','WITH','FROM','THIS','THAT','YOLO',
    'DD','OP','TA','ETF','EV','AI','USA','USD','IMO','CEO','CFO','IPO','OTC',
    'MOON','HODL','PUMP','ATH','FUD','SEC','CPI','LOSS','GAIN','GAINS','GAINZ',
    'OPEN','CLOSE','LONG','SHORT','CALL','CALLS','PUT','PUTS','IV','COD','PTSD',
    'WHY','IS','ARE','ON','TO','IN','UP','OF','BY','AT','Q1','Q2','Q3','Q4',
    'YEAR','WANT','WILL','SAVE','OUR','PORTFOLIOS','BACK','TRUCK','WAY','PLAY','MY','YOUR',
    'BLOW','LID','OFF','JUST','MANY','MORE','LESS','BEST','YALL'
  ]);

  // Unambiguous brand synonyms (extend if needed)
  const BRAND_SYNONYM = {
    'REDDIT': 'RDDT',
    'BATTLEFIELD': 'EA',
    'ELECTRONICARTS': 'EA',
    'UNITY': 'U',
    'VISA': 'V'
  };

  // Finance domains whitelist for URL evidence (reduces random /symbol/ links)
  const FIN_HOST_HINTS = ['yahoo', 'nasdaq', 'marketwatch', 'seekingalpha', 'bloomberg',
                          'tradingview', 'barrons', 'investopedia', 'investing', 'fool', 'finviz'];

  function norm(s){return (s||'').toString().replace(/[“”‘’]/g,'"').replace(/[–—]/g,'-').replace(/\s+/g,' ').trim();}
  function isFull(idx,t){return idx && idx.tickerSet && idx.tickerSet.has(t);}
  function resolve(idx,t){
    const up=(t||'').toUpperCase();
    if(isFull(idx,up))return up;
    const root=up.split('.')[0];
    return (idx&&idx.rootToFull&&idx.rootToFull[root])||'';
  }

  // push candidate with flags so we can prefer title/explicit
  function push(map, sym, score, pos, {src='', kind=''}={}) {
    if (!sym) return;
    const up = sym.toUpperCase();
    const cur = map.get(up) || { score:0, pos:pos, len:up.length, srcs:new Set(), kinds:new Set(), titleScore:0, explicitScore:0 };
    cur.score = Math.max(cur.score, score);
    cur.pos   = Math.min(cur.pos, pos);
    cur.len   = up.length;
    if (src)  cur.srcs.add(src);
    if (kind) cur.kinds.add(kind);
    if (src==='title') cur.titleScore = Math.max(cur.titleScore, score);
    if (kind==='cashtag' || kind==='paren' || kind==='url') cur.explicitScore = Math.max(cur.explicitScore, score);
    map.set(up, cur);
  }

  function hostLooksFinancial(s, i){
    const start = Math.max(0, i-60), end = Math.min(s.length, i+60);
    const ctx = s.slice(start, end).toLowerCase();
    return FIN_HOST_HINTS.some(h => ctx.indexOf(h) !== -1);
  }

  // scan a block of text
  function scan(out, text, weight, idx, src) {
    if (!text) return;
    const S = text;
    const U = (' ' + text.toUpperCase() + ' ');

    // 1) cashtags
    let m, re1 = /\$([A-Za-z]{1,6}(?:\.[A-Za-z]{1,4})?)(?![A-Za-z])/g;
    while ((m = re1.exec(S)) !== null) {
      const full = resolve(idx, m[1]);
      if (full) push(out, full, 100*weight, m.index, {src, kind:'cashtag'});
    }

    // 2) parentheses
    let re2 = /\(([A-Z]{1,6}(?:\.[A-Z]{1,4})?)\)/g;
    while ((m = re2.exec(S)) !== null) {
      const full = resolve(idx, m[1]);
      if (full) push(out, full, 95*weight, m.index, {src, kind:'paren'});
    }

    // 3) finance URLs only
    let re3 = /(\/(quote|symbol)\/|[?&]symbol=)([A-Z]{1,6}(?:\.[A-Z]{1,4})?)(?![A-Za-z])/ig;
    while ((m = re3.exec(S)) !== null) {
      if (!hostLooksFinancial(S, m.index)) continue;
      const full = resolve(idx, m[3]);
      if (full) push(out, full, 92*weight, m.index, {src, kind:'url'});
    }

    // 4) brand cues: exact/ collapsed company name
    if (idx && idx.nameFullOrCollapsed) {
      for (const key in idx.nameFullOrCollapsed) {
        const sym = idx.nameFullOrCollapsed[key];
        if (!sym) continue;
        const p = U.indexOf(' '+key+' ');
        const q = U.indexOf(key);
        if (p !== -1 || q !== -1) {
          const pos = (p !== -1 ? p : q);
          push(out, sym, 90*weight, pos, {src, kind:'brand'});
        }
      }
    }
    // 4b) manual unambiguous brand synonyms
    for (const alias in BRAND_SYNONYM) {
      const sym = BRAND_SYNONYM[alias];
      const p = U.indexOf(' '+alias+' ');
      const q = U.indexOf(alias);
      if (p !== -1 || q !== -1) {
        const full = resolve(idx, sym);
        if (full) push(out, full, 90*weight, (p!==-1?p:q), {src, kind:'brand'});
      }
    }

    // 5) token scan (bare)
    let tokenRe = /[A-Za-z.]+/g; let t;
    while ((t = tokenRe.exec(S)) !== null) {
      const raw = t[0], upTok = raw.toUpperCase();
      if (STOP.has(upTok)) continue;
      if (/\.W([SA]?)$/.test(upTok) || /\.R$/.test(upTok)) continue; // ignore bare warrants/rights

      // single-letter: require brand keyword presence
      if (/^[A-Z]$/.test(upTok)) {
        const kw = idx && idx.singleLetterKeywords && idx.singleLetterKeywords[upTok];
        if (!kw) continue;
        let hit = false;
        for (const k of kw) { if (U.indexOf(' '+k+' ')!==-1 || U.indexOf(k)!==-1) { hit = true; break; } }
        if (!hit) continue;
        const full = resolve(idx, upTok);
        if (full) push(out, full, 88*weight, t.index, {src, kind:'single'});
        continue;
      }

      if (/^[A-Z]{2,6}(?:\.[A-Z]{1,4})?$/.test(upTok)) {
        const full = resolve(idx, upTok);
        if (full) push(out, full, 70*weight, t.index, {src, kind:'bare'});
      }
    }
  }

  // choose best with title-first + explicit-first tiers
  function choose(map){
    if (!map || map.size===0) return '';

    const items = [];
    for (const [sym, v] of map) items.push({sym, ...v});

    // Tier 1: explicit IN TITLE (cashtag/paren/url)
    let tier = items.filter(x => x.srcs.has('title') && x.explicitScore>0);
    if (tier.length) return tier.sort((a,b)=> (b.explicitScore-a.explicitScore)||(b.len-a.len)||(a.pos-b.pos))[0].sym;

    // Tier 2: ANY explicit (title or body)
    tier = items.filter(x => x.explicitScore>0);
    if (tier.length) return tier.sort((a,b)=> (b.explicitScore-a.explicitScore)||(b.len-a.len)||(a.pos-b.pos))[0].sym;

    // Tier 3: bare IN TITLE (strong title intent)
    tier = items.filter(x => x.srcs.has('title') && x.kinds.has('bare'));
    if (tier.length) return tier.sort((a,b)=> (b.titleScore-a.titleScore)||(b.len-a.len)||(a.pos-b.pos))[0].sym;

    // Tier 4: brand/single evidence (title or body)
    tier = items.filter(x => x.kinds.has('brand') || x.kinds.has('single'));
    if (tier.length) return tier.sort((a,b)=> (b.score-a.score)||(b.len-a.len)||(a.pos-b.pos))[0].sym;

    // Tier 5: anything else, highest score wins
    return items.sort((a,b)=> (b.score-a.score)||(b.len-a.len)||(a.pos-b.pos))[0].sym;
  }

  /* ---------- API ---------- */

  // Title-only (fast)
  function extract(title, idx) {
    const out = new Map();
    const s = norm(title);
    scan(out, s, 2, idx, 'title');
    return choose(out);
  }

  // Title + body (robust)
  function extractFallback(title, body, idx) {
    const out = new Map();
    const t = norm(title), b = norm(body);
    scan(out, t, 2, idx, 'title');
    if (!choose(out)) { // only read body if title gave nothing decisive
      scan(out, b, 1, idx, 'body');
    } else {
      // still scan body, but won't override Tier 1-3 title wins
      scan(out, b, 1, idx, 'body');
    }
    return choose(out);
  }

  // quick tester
  function testActiveRow() {
    const ss = SpreadsheetApp.getActive();
    const front = ss.getSheetByName(Config.SHEETS.FRONTEND_FIXED || Config.SHEETS.FRONTEND);
    const symbols = Utils.Sheets.readTable(ss.getSheetByName(Config.SHEETS.SYMBOLS));
    const idx = SymbolsIndex.build(symbols);

    const H = Utils.Sheets.index(Utils.Sheets.header(front));
    const row = front.getActiveRange().getRow();
    const title = front.getRange(row, (H['title'] || 3) + 1).getValue();
    const body = front.getRange(row, (H['post_content'] || 4) + 1).getValue();
    const tkr = extract(title, idx) || extractFallback(title, body, idx) || '(no match)';
    SpreadsheetApp.getUi().alert('Title:\n'+title+'\n\nBody:\n'+body+'\n\nTicker: '+tkr);
  }

  return { extract, extractFallback, testActiveRow };
})();
