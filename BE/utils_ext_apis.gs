function httpFetch_(url, opts) {
  opts = opts || {};
  var maxRetries = opts.maxRetries != null ? opts.maxRetries : 3;
  var backoffMs  = opts.backoffMs  != null ? opts.backoffMs  : 800;
  var parseAs    = opts.parseAs    || 'text';   
  var fetchOpts  = opts.fetchOpts  || {};       
  var tryProxy   = !!opts.tryProxy;             

  var attempt = 0;
  while (++attempt <= maxRetries) {
    try {
      var res  = UrlFetchApp.fetch(url, fetchOpts);
      var code = res.getResponseCode();
      if (code >= 200 && code < 300) {
        if (parseAs === 'json')  return JSON.parse(res.getContentText());
        if (parseAs === 'bytes') return res.getContent();
        return res.getContentText();
      }

      var transient = (code === 408 || code === 429 || code === 500 || code === 502 || code === 503 || code === 504);
      if (transient && attempt < maxRetries) {
        Utilities.sleep(backoffMs * Math.pow(2, attempt - 1));
        continue;
      }
      throw new Error('HTTP ' + code + ' ' + url + ' body=' + res.getContentText());
    } catch (e) {
      if (attempt >= maxRetries) throw e;
      Utilities.sleep(backoffMs * Math.pow(2, attempt - 1));
    }
  }
  throw new Error('httpFetch_ failed: ' + url);
}

function withProxy_(rawUrl) { return 'https://corsproxy.io/?' + encodeURIComponent(rawUrl); }

function fetchNasdaqListed_() {
  return httpFetch_(Config.SYMBOLS.NASDAQ_LISTED_URL, {
    parseAs: 'text',
    fetchOpts: { muteHttpExceptions: true, followRedirects: true }
  });
}
function fetchOtherListed_() {
  return httpFetch_(Config.SYMBOLS.OTHER_LISTED_URL, {
    parseAs: 'text',
    fetchOpts: { muteHttpExceptions: true, followRedirects: true }
  });
}

function fetchRedditSearch_(p) {
  p = p || {};
  var sub  = p.subreddit || 'wallstreetbets';
  var base = 'https://www.reddit.com/r/' + sub + '/search.json';
  var qs   = [];
  qs.push('q=' + encodeURIComponent(p.q || ''));
  qs.push('restrict_sr=1');
  if (p.sort)   qs.push('sort='   + encodeURIComponent(p.sort));
  if (p.limit)  qs.push('limit='  + encodeURIComponent(p.limit));
  if (p.after)  qs.push('after='  + encodeURIComponent(p.after));
  if (p.before) qs.push('before=' + encodeURIComponent(p.before));
  qs.push('raw_json=' + encodeURIComponent(p.raw_json != null ? p.raw_json : 1));

  var url = base + '?' + qs.join('&');
  var direct  = url;
  var proxied = withProxy_(url);
  var first   = p.useProxy ? proxied : direct;
  var second  = p.useProxy ? direct  : proxied;

  try {
    return httpFetch_(first, {
      parseAs: 'json',
      maxRetries: p.maxRetries || 3,
      backoffMs:  p.backoffMs  || 800,
      fetchOpts: {
        muteHttpExceptions: true, followRedirects: true,
        headers: { 'User-Agent': 'Mozilla/5.0 (compatible; GoogleAppsScriptBot/1.0)', 'Accept': 'application/json' }
      }
    });
  } catch (e) {
    if (p.useProxyFallback) {
      return httpFetch_(second, {
        parseAs: 'json',
        maxRetries: 2,
        backoffMs:  1000,
        fetchOpts: {
          muteHttpExceptions: true, followRedirects: true,
          headers: { 'User-Agent': 'Mozilla/5.0 (compatible; GoogleAppsScriptBot/1.0)', 'Accept': 'application/json' }
        }
      });
    }
    throw e;
  }
}
