/** ===================== utils_direction_classifier.gs ===================== **
 * Rule-based "Bull"/"Bear" classifier for WSB posts.
 * Direction.classify(title, body, { upvote_ratio }) -> 'Bull' | 'Bear'
 */

const Direction = (() => {
  // --- helpers ---------------------------------------------------------
  function clean(text) {
    if (!text) return '';
    let s = String(text);
    // strip basic HTML tags/entities
    s = s.replace(/<[^>]+>/g, ' ')
         .replace(/&nbsp;/gi, ' ')
         .replace(/&amp;/gi, '&')
         .replace(/&lt;/gi, '<')
         .replace(/&gt;/gi, '>')
         .replace(/\s+/g, ' ')
         .trim()
         .toLowerCase();
    return s;
  }

  // Quick “is this negated?” helper for a found keyword
  function hasNonDilutiveCue(s) {
    return /non[-\s]?dilut/i.test(s) ||
           /no (risk of )?dilution/i.test(s) ||
           /little to no[^.]{0,30}dilution/i.test(s);
  }

  // --- dictionaries ----------------------------------------------------
  // Generic bullish/bearish finance slang
  const POS_GENERIC = [
    /(^|\W)long(\W|$)/, /(^|\W)bull(ish)?(\W|$)/, /(^|\W)buy(ing)?(\W|$)/,
    /(^|\W)call(s| option|s option)?(\W|$)/, /price\s*target|(^|\W)pt(\W|$)/,
    /(^|\W)beat(s|en)?(\W|$)/, /upgrade(d|s)?\b/, /raise(d)?\s+guidance/,
    /undervalued/, /break(out|ing)\b/, /up(side|trend)\b/,
    /short\s*squeeze/, /\bbtfd\b|\bbuy\s+the\s+dip\b/,
    /\bmoon(ing)?\b/, /\byolo\b/, /multi[-\s]?bagger|multibagger/,
    /room to grow|growth runway|multi[-\s]?year growth/
  ];

  // Domain cues we see in DDs like yours (defense/industrial catalysts)
  const POS_DOMAIN = [
    /partnership|exclusive partnership|collab(oratio)?n/,
    /contract(s)?\b|task order(s)?\b|award(s)?\b|backlog\b|order(s)?\b/,
    /\bprogram of record\b|\bpor\b/, /\bdod\b|\busaf\b|\bafwerx\b|\bprime\b/,
    /acquisitio(n|ns)\b|facility\b|hiring\b|expanding|build[-\s]?up/,
    /raise(d)?(\s+\$|\s+capital| funding)/, /\bfunding\b/,
    /\bcertification\b|\btype certification\b|\bfaa\b|\bapproval\b/,
    /\bota\b(?!\w)/, // Other Transaction Authority (defense)
    /delivered|delivery|first (vehicle|aircraft|unit)\b/
  ];

  const NEG_GENERIC = [
    /(^|\W)short(?!\s*squeeze)(\W|$)/, /(^|\W)bear(ish)?(\W|$)/,
    /(^|\W)sell(ing)?(\W|$)/, /(^|\W)put(s| option|s option)?(\W|$)/,
    /\bmiss(ed)?\b/, /\bdowngrade(d|s)?\b/, /cut\s+guidance/,
    /overvalued/, /\bfraud\b|\bscam\b/, /(chapter\s*11|bankrupt|bk)\b/,
    /dump(ing)?\b/, /(downtrend|sell[-\s]*off|going\s+down)\b/,
    /\bsec\b[^.]{0,15}\b(investigation|probe)\b/,
    /\bdilution\b|\boffering\b|\breverse split\b|\br\/s\b/
  ];

  // Option-context exceptions
  const EXC_BULL = [
    /(sell(ing)?|writing)\s+puts?/, /\bselling\s+csp(s)?\b/, // selling puts ~ bullish
    /non[-\s]?dilut(ive|ion)/, /no (risk of )?dilution/, /little to no[^.]{0,30}dilution/
  ];
  const EXC_BEAR = [
    /(sell(ing)?|writing)\s+calls?/, /\bcovered calls?\b/
  ];

  // Strong title booster phrases seen in WSB headlines
  const TITLE_BOOST = [
    /blow the lid off|game[-\s]?changer|about to (rip|explode|run)/,
    /new contract|big (deal|award)|major (catalyst|program)/,
    /exclusive|official|confirmed|approved/
  ];

  // Position disclosure often implies bullish intent
  const POS_POSITION = [ /position:\s*\d+(\,\d{3})*\s*share/i ];

  // --- scoring ---------------------------------------------------------
  function scoreSection(text, weight = 1, isTitle = false) {
    if (!text) return 0;
    const s = clean(text);
    let score = 0;

    // exceptions first (soft)
    EXC_BULL.forEach(r => { if (r.test(s)) score += 1 * weight; });
    EXC_BEAR.forEach(r => { if (r.test(s)) score -= 1 * weight; });

    // strong domain cues
    POS_DOMAIN.forEach(r => { if (r.test(s)) score += 3 * weight; });

    // generic cues
    POS_GENERIC.forEach(r => { if (r.test(s)) score += 2 * weight; });

    // handle dilution/offering with negation as positive if present
    if (/\bdilution\b/.test(s) || /\boffering\b/.test(s)) {
      if (hasNonDilutiveCue(s)) {
        score += 2 * weight; // reward “non-dilutive / no risk of dilution”
      } else {
        score -= 2 * weight; // otherwise bearish
      }
    }

    NEG_GENERIC.forEach(r => {
      // skip ones we explicitly handled above
      if (/(dilution|offering)/.test(String(r))) return;
      if (r.test(s)) score -= 2 * weight;
    });

    // position disclosure (bullish)
    POS_POSITION.forEach(r => { if (r.test(s)) score += 2 * weight; });

    // title-only boost
    if (isTitle) {
      TITLE_BOOST.forEach(r => { if (r.test(s)) score += 2 * weight; });
    }

    // exclamation intensity bias (very small)
    const bangs = (s.match(/!/g) || []).length;
    if (bangs >= 3) score += 0.5 * Math.sign(score || 1) * weight;

    return score;
  }

  function classify(title, body, meta) {
    const tScore = scoreSection(title, 3, true);  // title weighs 3×
    const bScore = scoreSection(body, 1.5, false);
    let total = tScore + bScore;

    // small bias from upvote_ratio (don’t override strong text)
    const upr = meta && typeof meta.upvote_ratio === 'number' ? meta.upvote_ratio : parseFloat(meta?.upvote_ratio);
    if (!isNaN(upr)) {
      if (upr >= 0.60) total += 0.5;
      else if (upr <= 0.45) total -= 0.5;
    }

    // default to Bull on near-neutral; require meaningful negative to be Bear
    return total >= -0.5 ? 'Bull' : 'Bear';
  }

  return { classify };
})();
