const Posts = (() => {
  const SHEET_NAME = Config.SHEETS.POSTS;
  const HEADERS    = Config.HEADERS.POSTS;
  const P          = Config.POSTS;

  function init_() {
    const ss = SpreadsheetApp.getActive();
    return Utils.Sheets.ensureSheet(ss, SHEET_NAME, HEADERS);
  }

  function buildIdIndex_(sh) {
    const last = sh.getLastRow();
    const map = new Map();
    if (last < 2) return map;
    const ids = sh.getRange(2,1,last-1,1).getValues();
    ids.forEach((row,i) => {
      const id = String(row[0] || '').trim();
      if (id) map.set(id, i+2);
    });
    return map;
  }

  function formatCols_(sh) {
    const H = Utils.Sheets.index(Utils.Sheets.header(sh));
    const last = sh.getLastRow();
    if (last < 2) return;
    if (H['created_utc'] != null) sh.getRange(2, H['created_utc']+1, last-1, 1).setNumberFormat('0');
    if (H['created']    != null) sh.getRange(2, H['created']+1,    last-1, 1).setNumberFormat('yyyy-mm-dd hh:mm');
  }

  function fetchAndStore() {
    const sh = init_();
    const idIndex = buildIdIndex_(sh);

    const cutoffEpoch = (function(){
      const d = new Date();
      d.setDate(d.getDate() - P.CUTOFF_DAYS);
      return Math.floor(d.getTime()/1000);
    })();

    let after = null, pages = 0;
    const updates = [], appends = [];

    while (pages < P.MAX_PAGES) {
      pages++;
      const json = fetchRedditSearch_({
        q: P.QUERY,
        subreddit: P.SUBREDDIT,
        sort: 'new',
        limit: P.PAGE_LIMIT,
        after: after,
        useProxy: P.USE_PROXY,
        useProxyFallback: true,
        raw_json: 1
      });

      const data = json && json.data;
      const children = (data && data.children) ? data.children : [];
      if (!children.length) break;

      for (const c of children) {
        const d = c && c.data;
        if (!d || !d.id) continue;
        const createdUtc = Number(d.created_utc || d.created || 0);
        if (cutoffEpoch && createdUtc && createdUtc < cutoffEpoch) continue;

        const obj = Utils.Mappers.mapRedditPostToRowObj(d);
        const arr = Utils.Sheets.toRowArray(HEADERS, obj);
        const row = idIndex.get(d.id);
        if (row) updates.push({row: row, values: arr});
        else     appends.push(arr);
      }

      after = data.after;
      if (!after) break;
    }

    if (updates.length) {
      updates.sort((a,b)=>a.row-b.row);
      for (const u of updates) sh.getRange(u.row, 1, 1, HEADERS.length).setValues([u.values]);
    }
    if (appends.length) {
      const start = sh.getLastRow() + 1;
      sh.getRange(start, 1, appends.length, HEADERS.length).setValues(appends);
    }

    formatCols_(sh);
    Utils.Sheets.sortByHeader(sh, 'created_utc', { ascending: false });
    fixRowFormat(sh, 21);

    SpreadsheetApp.getActive().toast(
      `Posts: ${updates.length} updated, ${appends.length} added`, 'WSB', 4
    );
  }

  return { fetchAndStore };
})();

function Posts_fetchAndStore() { return Posts.fetchAndStore(); }
