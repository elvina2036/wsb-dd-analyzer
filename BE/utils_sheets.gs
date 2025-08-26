var Utils = (function() {
  var Val = {
    str:     function(v){ return (v == null) ? '' : ('' + v).trim(); },
    isBlank: function(v){ return v === '' || v === null || v === undefined; },
    oneline: function(s){ return s == null ? '' : String(s).replace(/\r?\n/g, ' '); }
  };
  var Log = {
    append: function(type, message, detail) {
      var ss = SpreadsheetApp.getActive();
      var sh = Sheets.ensureSheet(ss, Config.SHEETS.LOG, Config.HEADERS.LOG);
      var row = [type || 'info', new Date(), message || '', detail || ''];
      sh.appendRow(row);
    }
  };
  var Mappers = {
    mapRedditPostToRowObj: function(d) {
      var createdUtc  = Number(d.created_utc || d.created || 0);
      var createdDate = createdUtc ? new Date(createdUtc * 1000) : '';
      var permalink   = d.permalink ? ('https://www.reddit.com' + d.permalink) : '';
      var flair       = (d.link_flair_text || d.author_flair_text || '') + '';
      return {
        id: d.id || '',
        title: Val.oneline(d.title),
        author: d.author || '',
        created_utc: createdUtc || 0,
        created: createdDate,
        score: d.score || 0,
        ups: d.ups || '',
        upvote_ratio: d.upvote_ratio || '',
        selftext: Val.oneline(d.selftext),
        num_comments: d.num_comments || 0,
        permalink: permalink,
        url: d.url || '',
        flair: flair,
        over_18: !!d.over_18,
        stickied: !!d.stickied,
        is_video: !!d.is_video,
        is_self: !!d.is_self,
        subreddit: d.subreddit || '',
        author_fullname: d.author_fullname || '',
        total_awards_received: d.total_awards_received || 0,
        num_crossposts: d.num_crossposts || 0,
        thumbnail: d.thumbnail || ''
      };
    }
  };
  var Sheets = {
    ensureSheet: function(ss, name, header) {
      var sh = ss.getSheetByName(name) || ss.insertSheet(name);
      if (sh.getLastRow() === 0 || sh.getLastColumn() === 0) {
        if (header && header.length) {
          if (sh.getMaxColumns() < header.length) {
            sh.insertColumnsAfter(sh.getMaxColumns(), header.length - sh.getMaxColumns());
          }
          sh.getRange(1,1,1,header.length).setValues([header]);
          sh.setFrozenRows(1);
        }
      }
      return sh;
    },
    header: function(sh) {
      if (!sh || sh.getLastRow() === 0 || sh.getLastColumn() === 0) return [];
      var hdr = sh.getRange(1,1,1,sh.getLastColumn()).getValues()[0];
      return hdr.map(function(h){ return (h || '').toString().trim(); });
    },
    index: function(header) {
      var m = {};
      for (var i=0;i<header.length;i++) {
        var k = (header[i] || '').toString().trim().toLowerCase();
        if (k) m[k] = i;
      }
      return m;
    },
    toRowArray: function(headers, obj) {
      var out = new Array(headers.length);
      for (var i=0;i<headers.length;i++) {
        var k = headers[i];
        out[i] = Object.prototype.hasOwnProperty.call(obj, k) ? obj[k] : '';
      }
      return out;
    },
    sortByHeader: function(sh, headerName, opts) {
      opts = opts || {};
      var hdr = Sheets.header(sh);
      var H   = Sheets.index(hdr);
      var key = (headerName || '').toLowerCase();
      if (!(key in H)) return;
      var col = H[key] + 1;
      var rows = sh.getLastRow() - 1;
      if (rows <= 0) return;
      sh.getRange(2,1,rows,sh.getLastColumn()).sort([{column: col, ascending: !!opts.ascending}]);
    }
  };
  return { Sheets: Sheets, Val: Val, Mappers: Mappers, Log: Log };
})();

function fixRowFormat(sheet, height) {
  height = height || 21;
  SpreadsheetApp.flush();
  try { var filter = sheet.getFilter(); if (filter) filter.remove(); } catch(_) {}
  var lastRow = sheet.getLastRow();
  var lastCol = sheet.getLastColumn();
  if (lastRow <= 1 || lastCol === 0) return;
  var rng = sheet.getRange(1,1,lastRow,lastCol);
  try { rng.setWrap(false); } catch(_) {}
  try { rng.setWrapStrategy(SpreadsheetApp.WrapStrategy.CLIP); } catch(_) {}
  try { rng.setVerticalAlignment('middle'); } catch(_) {}

  SpreadsheetApp.flush();
  for (var r=2; r<=lastRow; r++) sheet.setRowHeight(r, height);
  SpreadsheetApp.flush();
}

function estimateVotesFromScoreRatio(score, ratio) {
  var S = Number(score);
  var R = Number(ratio);
  if (!isFinite(S) || !isFinite(R)) return [null, null];
  if (R <= 0) return [0, Math.max(0, Math.round(-S))];
  if (R >= 1) return [Math.max(0, Math.round(S)), 0];
  var denom = 2 * R - 1;               
  if (Math.abs(denom) < 1e-6) return [null, null]; 
  var T = S / denom;                    
  if (!isFinite(T) || T < 0) return [null, null];
  var U = Math.round(R * T);            
  var D = U - S;                        
  if (D < 0) {                          
    D = 0;
    U = Math.max(0, Math.round(S));
  }
  return [U, Math.round(D)];
}
