let companyList = [];

function normalize(str) {
  return str.toLowerCase().replace(/[^a-z0-9]/g, '');
}

function extractNamePhrasesFromTitle(title) {
  const stopWords = new Set([
    'the','this','that','why','what','when','how','who',
    'if','all','in','on','of','for','and','but','a','i','we','you',
    'my','your','it','its','to','with','at','by','be','or','as','is',
    'are','was','were','from','up','down','over','under','more','less','bear','bull'
  ]);

  const words = title.replace(/[^\w\s]/g, '').split(/\s+/);
  const phrases = [];
  let buffer = [];

  for (const word of words) {
    const isCapitalized = /^[A-Z][a-z]/.test(word);
    const isNotStopWord = !stopWords.has(word.toLowerCase());

    if (isCapitalized && isNotStopWord) {
      buffer.push(word);
      phrases.push(word); // individual word
    } else {
      if (buffer.length) {
        phrases.push(buffer.join(' ')); // full phrase
        buffer = [];
      }
    }
  }

  if (buffer.length) {
    phrases.push(buffer.join(' '));
  }

  return phrases;
}

function searchTickerByCompanyName(title) {
  const phrases = extractNamePhrasesFromTitle(title)
    .map(normalize)
    .sort((a, b) => b.length - a.length);

  for (const phrase of phrases) {
    for (const company of companyList) {
      const normName = normalize(company.name);
      const nameWords = normName.split(/\s|-/);
      if (nameWords.some(word => word === phrase)) return company.symbol;
      if (normName.includes(phrase)) return company.symbol;
    }
  }
  return null;
}

function extractTickerSymbols(title) {
  const matches = title.match(/\b[A-Z]{2,5}\b/g);
  return matches || [];
}

async function loadCompanyListFromCSV() {
  const res = await fetch('./nasdaq_screener.csv');
  const text = await res.text();
  const lines = text.split('\n').slice(1);
  companyList = lines.map(line => {
    const [symbol, name] = line.split(',').map(s => s?.trim());
    return { symbol, name };
  }).filter(entry => entry.symbol && entry.name);
}

function formatTimestamp(unix) {
  const d = new Date(unix * 1000);
  const date = `${d.getFullYear()}/${String(d.getMonth() + 1).padStart(2, '0')}/${String(d.getDate()).padStart(2, '0')}`;
  const time = `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
  return `${date}-${time}`;
}

// Replace the old function with this:
async function fetchDDPostsFromSheet(daysBack = 1) {
  const sheetId = '1X8aBiGCBL5rHvToZiZqMiLdEfMWTuvZT5NwuITWdqKo';
  const url = `https://docs.google.com/spreadsheets/d/${sheetId}/gviz/tq?tqx=out:json`;

  const res = await fetch(url);
  const text = await res.text();
  const json = JSON.parse(text.substr(47).slice(0, -2));

  const headers = json.table.cols.map(col => col.label);

  // cutoff for time-range
  const nowSec = Math.floor(Date.now() / 1000);
  const cutoff = nowSec - daysBack * 86400;

  // rows -> objects
  const rows = json.table.rows.map(row => {
    const obj = {};
    headers.forEach((h, i) => { obj[h] = row.c[i] ? row.c[i].v : null; });

    // created_utc from sheet might be string → coerce safely
    const createdUtc = obj.created_utc == null
      ? null
      : Number(String(obj.created_utc).replace(/[^0-9.]/g, ''));

    return {
      id: obj.id ?? null,
      title: obj.title ?? '',
      url: obj.permalink ? obj.permalink : obj.url,
      created_utc: createdUtc,
      raw: obj, // keep the whole row if you need more later
    };
  });

  // filter & sort (newest first)
  return rows
    .filter(p => typeof p.created_utc === 'number' && p.created_utc >= cutoff)
    .sort((a, b) => b.created_utc - a.created_utc);
}


// === UI handler ===
async function handleFetchClick() {
  const statusEl = document.getElementById('status');
  const days = parseInt(document.getElementById('timeRange').value, 10);
  const tbody = document.querySelector('#results tbody');
  tbody.innerHTML = '';
  statusEl.textContent = '⏳ Fetching...';
  statusEl.classList.remove('hidden', 'done');

  const posts = await fetchDDPostsFromSheet(days); // <-- pass days

  for (const post of posts) {
    const tr = document.createElement('tr');

    let tickers = extractTickerSymbols(post.title);
    const inferred = searchTickerByCompanyName(post.title);
    if (inferred && !tickers.includes(inferred)) tickers.push(inferred);

    const tickerHTML = tickers.length
      ? tickers.map(t => `<span class="ticker">${t}</span>`).join('')
      : '<span class="no-ticker">❌ No ticker found</span>';

    tr.innerHTML = `
      <td>${tickerHTML}<a href="${post.url}" target="_blank">${post.title}</a></td>
      <td>${formatTimestamp(post.created_utc)}</td>
    `;
    tbody.appendChild(tr);
  }

  statusEl.textContent = '✅ All Posts Fetched!';
  statusEl.classList.add('done');
}

document.addEventListener('DOMContentLoaded', async () => {
  await loadCompanyListFromCSV();
  document.getElementById('fetchBtn').addEventListener('click', handleFetchClick);
});

