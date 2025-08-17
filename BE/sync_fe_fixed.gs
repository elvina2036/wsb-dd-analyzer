/** ===================== sync_fe_fixed.gs (APPEND-ONLY, DROP-IN) ===================== **/

const FEFixed = (() => {
  const SRC_SHEET = Config.SHEETS.POSTS;
  const DST_SHEET = Config.SHEETS.FRONTEND_FIXED;
  const DST_HDR   = Config.HEADERS.FRONTEND_FIXED; // ['id','author','title','post_content','direction','ticker','created_at','price_at_post']

  function syncFromPosts() {
    const ss  = SpreadsheetApp.getActive();
    const src = ss.getSheetByName(SRC_SHEET);
    if (!src) throw new Error('Missing sheet: ' + SRC_SHEET);

    const dst = Utils.Sheets.ensureSheet(ss, DST_SHEET, DST_HDR);

    // Build source header index + read rows
    const srcHdr   = Utils.Sheets.header(src);
    const S        = Utils.Sheets.index(srcHdr);
    const srcLastR = src.getLastRow(), srcLastC = src.getLastColumn();
    const srcRows  = (srcLastR > 1 && srcLastC > 0) ? src.getRange(2,1,srcLastR-1,srcLastC).getValues() : [];
    const get = (row, key) => {
      const i = S[(key || '').toLowerCase()];
      return (i == null) ? '' : row[i];
    };

    // Build existing id set in destination
    const dstHdr   = Utils.Sheets.header(dst);
    const D        = Utils.Sheets.index(dstHdr);
    const idColDst = D['id'];
    const existing = new Set();
    const dstLastR = dst.getLastRow(), dstLastC = dst.getLastColumn();
    if (dstLastR > 1 && idColDst != null) {
      const vals = dst.getRange(2,1,dstLastR-1,dstLastC).getValues();
      for (var i=0;i<vals.length;i++) {
        var v = (vals[i][idColDst] || '').toString().trim();
        if (v) existing.add(v);
      }
    }

    // Map and collect only new ids
    const idxDst = (function hdrIdx(h){ var m={}; for (var i=0;i<h.length;i++){ var k=(h[i]||'').toString().trim().toLowerCase(); if(k)m[k]=i; } return m; })(DST_HDR);
    const out = [];
    for (const r of srcRows) {
      const id = (get(r,'id') || '').toString().trim();
      if (!id || existing.has(id)) continue;

      const createdUtc = Number(get(r,'created_utc')) || 0;
      const createdObj = get(r,'created');
      const created_at = createdObj instanceof Date ? createdObj : (createdUtc>0 ? new Date(createdUtc*1000) : '');

      const row = new Array(DST_HDR.length).fill('');
      if (idxDst.id           != null) row[idxDst.id]           = id;
      if (idxDst.author       != null) row[idxDst.author]       = get(r,'author') || '';
      if (idxDst.title        != null) row[idxDst.title]        = get(r,'title') || '';
      if (idxDst.post_content != null) row[idxDst.post_content] = Utils.Val.oneline(get(r,'selftext') || '');
      if (idxDst.created_at   != null) row[idxDst.created_at]   = created_at;

      out.push(row);
      existing.add(id);
    }

    if (out.length) {
      const start = dst.getLastRow() + 1;
      dst.getRange(start, 1, out.length, DST_HDR.length).setValues(out);
    }

    // format/sort/fix
    if (idxDst.created_at != null) {
      const col = idxDst.created_at + 1;
      const rows = Math.max(0, dst.getLastRow() - 1);
      if (rows > 0) dst.getRange(2, col, rows, 1).setNumberFormat('yyyy-mm-dd hh:mm');
    }
    Utils.Sheets.sortByHeader(dst, 'created_at', { ascending: false });
    fixRowFormat(dst, 21);

    SpreadsheetApp.getActive().toast(`FE Fixed: appended ${out.length} row(s)`, 'WSB', 4);
  }

  return { syncFromPosts };
})();

function FEFixed_syncFromPosts() { return FEFixed.syncFromPosts(); }
