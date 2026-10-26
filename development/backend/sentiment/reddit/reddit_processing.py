"""
Filtering, ticker matching, item building and aggregation for the Reddit
sentiment pipeline. Pure Python (no torch / bs4), so it can be tested alone.

Pipeline (driven by process_data() in reddit.py)
------------------------------------------------
  posts + comments
    -> clean_text / reject_reason      drop deleted, bots, link-only, too short/long
    -> TickerMatcher.find              which tickers does this text talk about?
    -> build_scoring_items             one item per (post|comment, ticker), scored
                                       on the *relevant text only*
    -> score_items()                   (sentiment_scorer.py)
    -> aggregate_rows                  per ticker per day, one vote per author
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone

__all__ = [
    "TickerMatcher",
    "load_tickers",
    "load_json_file",
    "clean_text",
    "is_discussion_title",
    "build_scoring_items",
    "aggregate_rows",
    "write_csv_atomic",
    "ROW_FIELDS",
    "DAILY_FIELDS",
    "SUMMARY_FIELDS",
    "parse_dt",
    "parse_int",
    "split_sentences",
    "norm_key",
    "CLAUSE_SPLIT",
    "MAX_TITLE_TICKERS",
]

# --------------------------------------------------------------------------- #
# Tunables
# --------------------------------------------------------------------------- #
MIN_WORDS = 3  # drop items shorter than this (after cleaning)
SHORT_WORDS = 6  # snippets shorter than this get parent/title context
INHERIT_MIN_WORDS = 4  # a comment must be at least this long to inherit a title ticker
MAX_TITLE_TICKERS = (
    2  # titles naming more tickers than this are round-ups: no inheriting
)
MAX_CHARS = 8000  # longer items are copy-pasted articles / outliers
LONG_CHARS = 1000  # longer texts are scored sentence-level around the ticker
MAX_SNIPPET = 1500
CONTEXT_CHARS = 400
CONTEXT_WINDOW = (
    60  # chars either side of an ambiguous match searched for finance words
)
MIN_NAME_CHARS = 4  # company names shorter than this are never matched
BULL_THRESHOLD, BEAR_THRESHOLD = 6.5, 4.5  # on the 1..10 score scale (5.5 = neutral)

# Symbols that are English words / jargon: a finance word nearby does NOT prove
# the author means the company ("AI stocks", "IT is going up"), so they are only
# accepted as a cashtag: $AI, $IT.
CASHTAG_ONLY_SYMBOLS = {
    "A",
    "AI",
    "AM",
    "AN",
    "ANY",
    "ARE",
    "AS",
    "AT",
    "ATH",
    "BE",
    "BY",
    "CEO",
    "CFO",
    "CPI",
    "DCA",
    "DD",
    "DO",
    "EPS",
    "ETF",
    "EV",
    "FDA",
    "FED",
    "FOMO",
    "FYI",
    "GDP",
    "GO",
    "HE",
    "IF",
    "IMO",
    "IN",
    "IPO",
    "IRA",
    "IT",
    "ITM",
    "IV",
    "LOL",
    "ME",
    "MY",
    "OF",
    "OK",
    "OR",
    "OTM",
    "PT",
    "ROI",
    "SEC",
    "SO",
    "TO",
    "UK",
    "UP",
    "US",
    "USA",
    "USD",
    "WTF",
    "YOLO",
}
# Real tickers that are also common words: accepted in CAPS only when a finance
# word is nearby (ON earnings, NOW calls), and never inside shouting text.
NEEDS_CONTEXT_SYMBOLS = {
    "ALL",
    "BIG",
    "CAN",
    "CAR",
    "CASH",
    "COST",
    "DAY",
    "FAST",
    "FOR",
    "FREE",
    "FUN",
    "GOOD",
    "HAS",
    "HOME",
    "HOT",
    "KEY",
    "LIFE",
    "LIKE",
    "LIVE",
    "LOVE",
    "LOW",
    "MAN",
    "MAY",
    "MET",
    "NEW",
    "NEXT",
    "NICE",
    "NOW",
    "ON",
    "ONE",
    "OPEN",
    "OUT",
    "PAY",
    "PEAK",
    "PLAY",
    "POST",
    "REAL",
    "RIDE",
    "ROCK",
    "RUN",
    "SAVE",
    "SEE",
    "SHOP",
    "TECH",
    "TOP",
    "TRUE",
    "TWO",
    "WAY",
    "WELL",
    "WIN",
    "WORK",
    "YOU",
}
# Company names that are also ordinary words: need the exact capitalisation
# ("Apple", not "apple") AND a finance word nearby.
NEEDS_CONTEXT_NAMES = {
    "apple",
    "target",
    "block",
    "visa",
    "ford",
    "meta",
    "oracle",
    "square",
    "snap",
    "shell",
    "match",
    "delta",
    "united",
    "carnival",
    "discovery",
    "general",
    "american",
    "first",
    "national",
    "pacific",
    "atlas",
    "summit",
    "mosaic",
    "ally",
    "ross",
    "dollar",
    "southwest",
    "paramount",
}
# Hand-made aliases (only used if the ticker is in your tickers.json).
DEFAULT_ALIASES = {
    "google": ["GOOGL", "GOOG"],
    "alphabet": ["GOOGL", "GOOG"],
    "facebook": ["META"],
    "mag 7": ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA"],
    "mag7": ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA"],
    "magnificent 7": ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA"],
    "magnificent seven": ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA"],
}

BOT_AUTHORS = {
    "automoderator",
    "visualmod",
    "wsbapp",
    "sneakpeekbot",
    "remindmebot",
    "repostsleuthbot",
    "wallstreetbets-modteam",
    "b0trank",
    "friendlybot",
}
# "RemindMeBot", "foo_bot", "bot_x" -- but not the surname "Talbot"
_BOT_NAME_RE = re.compile(r"(?:[_-][Bb]ot$|(?<=[a-z0-9])Bot$|^[Bb]ot[_-])")
_BOT_BODY_RE = re.compile(
    r"\bi am a bot\b|\bi'm a bot\b|this action was performed automatically"
    r"|\bbeep boop\b",
    re.I,
)
REMOVED_MARKERS = {"[deleted]", "[removed]", "[deleted by user]"}

_FIN_RE = re.compile(
    r"\b(?:stocks?|shares?|calls?|puts?|options?|earnings|eps|revenue|guidance|"
    r"rall(?:y|ies|ied)|bull(?:s|ish)?|bear(?:s|ish)?|buy(?:ing)?|bought|sell(?:ing)?|"
    r"sold|short(?:s|ing|ed)?|dividends?|ipo|market ?cap|valuation|price targets?|"
    r"pt|ath|position|holding|holders?|invest(?:ed|ing|ment|or|ors)?|ticker|etf|"
    r"portfolio|dip|moon|squeeze|premarket|pre-market|after-?hours|breakout|"
    r"resistance|yoy|ytd|shareholders?|quarter|q[1-4]|10-?[kq])\b",
    re.I,
)

# Stricter context used for ambiguous company names when capitalisation can't be
# trusted (spoken transcripts): "Target stock" yes, "buy calls, my target is" no.
_CORP_RE = re.compile(
    r"\b(?:stocks?|shares?|earnings|eps|revenue|guidance|ceo|cfo|company|quarter|"
    r"q[1-4]|dividends?|ipo|valuation|market ?cap|investors?|investing|shareholders?|"
    r"analysts?|ticker|portfolio|bull(?:ish)?|bear(?:ish)?)\b",
    re.I,
)
_NAME_PREV_BLOCK = {
    "price",
    "analyst",
    "analysts",
    "street",
    "average",
    "median",
    "consensus",
    "revised",
    "raised",
    "lowered",
    "cut",
}

_DISCUSSION_RE = re.compile(
    r"\b(?:daily|weekly|weekend)\b.*\b(?:thread|discussion|chat|moves|talk)\b"
    r"|discussion thread|mega ?thread|what are your moves|moves tomorrow"
    r"|live thread|free talk|rate my portfolio|general discussion"
    r"|questions? thread|earnings thread|^\s*(?:daily|weekly)\b",
    re.I,
)

_TOKEN_RE = re.compile(r"\$?[A-Za-z0-9]+")
_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.I)
_MD_LINK_RE = re.compile(r"\[([^\]]*)\]\([^)]*\)")
_SENT_SPLIT = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT = re.compile(
    r"\s*[;,]\s*|\s+[-\u2013\u2014]\s+|\s+\b(?:but|however|while|whereas|although|though|yet)\b\s+",
    re.I,
)
_SUFFIX_RE = re.compile(
    r"[,\s]+(inc|incorporated|corp|corporation|co|company|ltd|limited|plc|llc|"
    r"holdings?|group|class [a-c])\.?$",
    re.I,
)


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
def load_json_file(path, default=None):
    """Read a JSON file; return `default` if no path was given. Raises on bad files."""
    if not path:
        return default
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_tickers(path):
    """Read {company name: ticker} from a JSON file."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict) or not data:
        raise ValueError(
            f"{path} must hold a non-empty JSON object of {{company name: ticker}}"
        )
    return data


def parse_dt(value):
    try:
        d = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def parse_int(value, default=0):
    m = re.search(r"-?\d[\d,]*", str(value) if value is not None else "")
    if not m:
        return default
    try:
        return int(m.group().replace(",", ""))
    except ValueError:
        return default


def _short_name(name):
    """'Apple Inc.' -> 'Apple' (people rarely write the legal suffix)."""
    prev = None
    while prev != name:
        prev, name = name, _SUFFIX_RE.sub("", name).strip()
    return re.sub(r"\.com$", "", name, flags=re.I)  # "Amazon.com" -> "Amazon"


def is_discussion_title(title):
    """Daily / weekly / mega threads: comments there are about many tickers."""
    return bool(_DISCUSSION_RE.search(title or ""))


def clean_text(text):
    """Strip URLs and markdown links, collapse whitespace."""
    if not text:
        return ""
    text = _MD_LINK_RE.sub(r"\1", text)
    text = _URL_RE.sub(" ", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n", text)
    return text.strip()


def split_sentences(text):
    return [s.strip() for s in _SENT_SPLIT.split(text) if s and s.strip()]


def _norm_key(text):
    """Case/punctuation/number-insensitive fingerprint for duplicate detection."""
    norm = re.sub(r"[^a-z]+", " ", text.lower()).strip()
    return hashlib.sha1(norm.encode("utf-8")).hexdigest()[:16]


def is_bot(author):
    a = (author or "").strip()
    return a.lower() in BOT_AUTHORS or bool(_BOT_NAME_RE.search(a))


def reject_reason(text, author, min_words=MIN_WORDS, max_chars=MAX_CHARS):
    """Return why an item is noise, or None if it is worth keeping."""
    if is_bot(author):
        return "bot_author"
    if not text or text.strip().lower() in REMOVED_MARKERS:
        return "removed_or_empty"
    if _BOT_BODY_RE.search(text):
        return "bot_text"
    if not re.search(r"[A-Za-z]{2,}", text):
        return "no_words"  # link-only / emoji-only
    if len(text.split()) < min_words:
        return "too_short"
    if len(text) > max_chars:
        return "too_long"
    return None


def _is_shouting(text):
    letters = [c for c in text if c.isalpha()]
    return len(letters) >= 8 and sum(c.isupper() for c in letters) / len(letters) > 0.7


# --------------------------------------------------------------------------- #
# Ticker matching
# --------------------------------------------------------------------------- #
class TickerMatcher:
    """
    Find which tickers a text is about. Token / dict lookups instead of one huge
    regex alternation, so it stays fast with thousands of tickers.

    Rules
      * $CASHTAG (any case)              -> always a match
      * AAPL (3+ letters, CAPS)          -> match
      * nvda / Nvda (4+ letters)         -> match, once the corpus shows that
                                            symbol is really used as a ticker
                                            (fit())
      * ON, NOW, ALL ... (word-tickers)  -> CAPS + finance word nearby, not in
                                            shouting text
      * AI, IT, A, F ... (jargon, 1 letter) -> cashtag only
      * Company names                    -> case-insensitive, with and without the
                                            legal suffix; ambiguous names (Apple,
                                            Target, Visa...) need exact capitals
                                            AND a finance word nearby
      * Aliases                          -> google -> GOOGL, mag 7 -> 7 tickers ...
    """

    def __init__(self, tickers, aliases=None, extra_ambiguous=(), strict_case=True):
        # strict_case=False is for spoken transcripts / auto-captions, where
        # "apple" is never capitalised: ambiguous names then rely on corporate
        # context words instead of exact capitalisation.
        self.strict_case = strict_case
        if not isinstance(tickers, dict):
            raise ValueError("tickers must be a {company name: ticker} dict")
        extra = {str(x).strip() for x in (extra_ambiguous or []) if str(x).strip()}
        amb_syms = {x.upper() for x in NEEDS_CONTEXT_SYMBOLS} | {
            x.upper() for x in extra
        }
        amb_names = {x.lower() for x in NEEDS_CONTEXT_NAMES} | {
            x.lower() for x in extra
        }

        self.known = set()
        self._cash = {}  # any symbol form -> symbol (cashtag use)
        self._strong = {}  # 3-letter symbols, CAPS only
        self._weak = {}  # word-tickers: CAPS + finance context
        self._cand = {}  # 4+ letter symbols: CAPS always, lowercase once fitted
        self._lower_ok = set()
        self._punct = {}  # symbols containing . or - (BRK.B)
        self._names = defaultdict(
            dict
        )  # first word -> {(words, needs_ctx): [orig, {syms}]}
        self._queries = {}
        self._terms = defaultdict(list)  # symbol -> search terms (symbol, short name)

        for name, raw in tickers.items():
            sym = str(raw).strip().upper().lstrip("$")
            name = str(name).strip()
            if not sym:
                continue
            self.known.add(sym)
            letters = len(re.sub(r"[^A-Z0-9]", "", sym))
            for form in {sym, sym.replace("-", "."), sym.replace(".", "-")}:
                self._cash[form] = sym
                if re.search(r"[.\-]", form):
                    self._punct[form] = sym
                elif letters == 1 or sym in CASHTAG_ONLY_SYMBOLS:
                    pass
                elif sym in amb_syms or letters == 2:
                    self._weak[form] = sym
                elif letters == 3:
                    self._strong[form] = sym
                else:
                    self._cand[form] = sym

            short = _short_name(name)
            for nm in {name, short}:
                self._add_name(nm, sym, amb_names)

            # search-engine queries (one per term; ambiguous terms get " stock")
            terms = []
            if letters >= 2:
                terms.append(
                    (
                        sym,
                        sym in amb_syms or sym in CASHTAG_ONLY_SYMBOLS or letters == 2,
                    )
                )
            if len(short) >= MIN_NAME_CHARS:
                terms.append((short, short.lower() in amb_names))
            for term, ambiguous in terms:
                q = f'"{term}"' + (" stock" if ambiguous else "")
                self._queries.setdefault(q.lower(), q)
                if term.lower() not in [t.lower() for t in self._terms[sym]]:
                    self._terms[sym].append(term)

        merged = {k.lower(): v for k, v in DEFAULT_ALIASES.items()}
        merged.update({str(k).lower(): v for k, v in (aliases or {}).items()})
        for alias, target in merged.items():
            targets = [target] if isinstance(target, str) else list(target)
            syms = {str(t).strip().upper().lstrip("$") for t in targets} & self.known
            words = tuple(w.lower() for w in _TOKEN_RE.findall(alias))
            if syms and words:
                slot = self._names[words[0]].setdefault((words, False), [words, set()])
                slot[1].update(syms)

        self._punct_re = None
        if self._punct:
            alt = "|".join(
                re.escape(f) for f in sorted(self._punct, key=len, reverse=True)
            )
            self._punct_re = re.compile(
                rf"(?<![A-Za-z0-9])(\$)?({alt})(?![A-Za-z0-9])", re.I
            )

    def _add_name(self, nm, sym, amb_names):
        if len(nm) < MIN_NAME_CHARS:
            return
        orig = tuple(_TOKEN_RE.findall(nm))
        if not orig:
            return
        low = tuple(w.lower() for w in orig)
        slot = self._names[low[0]].setdefault(
            (low, nm.lower() in amb_names), [orig, set()]
        )
        slot[1].add(sym)

    @property
    def n_tickers(self):
        return len(self.known)

    def search_queries(self):
        """Quoted Reddit search queries covering every ticker (deduplicated)."""
        return sorted(self._queries.values())

    def terms_for(self, symbol):
        """Search terms (symbol and short company name) for one ticker symbol."""
        return list(self._terms.get(str(symbol).strip().upper().lstrip("$"), []))

    def fit(self, texts, min_evidence=3):
        """
        Enable lowercase matching (nvda, tsla) only for symbols that the corpus
        itself uses as tickers (>= min_evidence CAPS or $cashtag uses). This keeps
        'race', 'team', 'snow' from matching RACE, TEAM, SNOW.
        """
        if not self._cand:
            return
        counts = Counter()
        for text in texts:
            for m in _TOKEN_RE.finditer(text):
                tok = m.group()
                bare = tok.lstrip("$")
                key = bare.upper()
                if key in self._cand and (tok[0] == "$" or bare == key):
                    counts[key] += 1
        self._lower_ok = {k for k, c in counts.items() if c >= min_evidence}

    @staticmethod
    def _has_context(text, start, end, regex=_FIN_RE):
        before = text[max(0, start - CONTEXT_WINDOW) : start]  # match itself excluded
        after = text[end : end + CONTEXT_WINDOW]
        return bool(regex.search(before) or regex.search(after))

    def find(self, text):
        """Return the set of ticker symbols this text is about."""
        return set(self.scan(text))

    def scan(self, text):
        """Every ticker mention in `text`, in order, repeats included (a list of symbols)."""
        return [sym for sym, _pos in self.scan_pos(text)]

    def scan_pos(self, text):
        """Like scan(), but a list of (symbol, character offset of the mention)."""
        found = []
        if not text:
            return found
        shouting = None
        toks = [(m.group(), m.start(), m.end()) for m in _TOKEN_RE.finditer(text)]
        for i, (tok, s, e) in enumerate(toks):
            if tok[0] == "$":  # cashtag
                sym = self._cash.get(tok[1:].upper())
                if sym:
                    found.append((sym, s))
                continue
            key = tok.upper()
            upper = tok == key
            sym = self._cand.get(key)
            if sym and (upper or key in self._lower_ok):
                found.append((sym, s))
            elif upper:
                sym = self._strong.get(key)
                if sym:
                    found.append((sym, s))
                else:
                    sym = self._weak.get(key)
                    if (
                        sym
                        and text[e : e + 1] not in ("'", "\u2019")  # IT'S, ON'T
                        and self._has_context(text, s, e)
                    ):
                        if shouting is None:
                            shouting = _is_shouting(text)
                        if not shouting:
                            found.append((sym, s))

            cands = self._names.get(tok.lower())
            if cands:
                for (words_low, needs_ctx), (words_orig, syms) in cands.items():
                    n = len(words_low)
                    seg = toks[i : i + n]
                    if len(seg) < n or any(
                        seg[k][0].lower() != words_low[k] for k in range(n)
                    ):
                        continue
                    if needs_ctx:
                        if self.strict_case:
                            if any(seg[k][0] != words_orig[k] for k in range(n)):
                                continue
                            if not self._has_context(text, s, seg[-1][2]):
                                continue
                        else:
                            prev = toks[i - 1][0].lower() if i else ""
                            if prev in _NAME_PREV_BLOCK:
                                continue
                            if not self._has_context(text, s, seg[-1][2], _CORP_RE):
                                continue
                    found.extend((x, s) for x in syms)

        if self._punct_re:
            for m in self._punct_re.finditer(text):
                if m.group(1) or m.group(2) == m.group(2).upper():
                    found.append((self._punct[m.group(2).upper()], m.start()))
        return found


# --------------------------------------------------------------------------- #
# Building the scoring items
# --------------------------------------------------------------------------- #
ROW_FIELDS = [
    "id",
    "kind",
    "source",
    "ticker",
    "subreddit",
    "post_id",
    "comment_id",
    "author",
    "created_utc",
    "upvotes",
    "n_tickers",
    "score",
    "bullish",
    "bearish",
    "neutral",
    "text",
    "context",
    "permalink",
]


def _snippet(text, ticker, matcher, n_own):
    """The part of `text` that is actually about `ticker`."""
    if n_own <= 1 and len(text) <= LONG_CHARS:
        return text
    picks = []
    for sent in split_sentences(text):
        tickers_here = matcher.find(sent)
        if ticker not in tickers_here:
            continue
        if len(tickers_here) > 1:
            # "NVDA is great but AMD is garbage": keep only the clause(s) about this ticker
            clauses = [
                c
                for c in _CLAUSE_SPLIT.split(sent)
                if c and c.strip() and ticker in matcher.find(c)
            ]
            if clauses:
                sent = " ".join(clauses)
        picks.append(sent)
    return " ".join(picks)[:MAX_SNIPPET].strip()


def build_scoring_items(posts, comments, matcher, cutoff=None, inherit_title=True):
    """
    Turn scraped posts/comments into scoring items: one per (post|comment, ticker).

    * noise is dropped first (deleted/removed, bots, link-only, too short/long)
    * a comment is attributed to the tickers *it* mentions; only if it mentions
      none does it inherit the tickers in its post's TITLE (never the selftext),
      and never in daily/discussion threads or round-up titles
    * multi-ticker or very long texts are cut down to the sentences about that ticker
    * short replies get their parent comment (or the post title) as context
    * near-duplicate texts are counted once per ticker
    Returns (items, stats Counter).
    """
    stats = Counter()
    cmt_by_id = {c["comment_id"]: c for c in comments if c.get("comment_id")}

    # ---- 1. clean posts and comments once
    post_docs = []
    for p in posts:
        title = clean_text(p.get("title", ""))
        body = clean_text(p.get("selftext", ""))
        if body.strip().lower() in REMOVED_MARKERS:
            body = ""
        text = f"{title}\n{body}".strip() if body else title
        post_docs.append((p, title, text, parse_dt(p.get("created_utc"))))

    cmt_docs = []
    for c in comments:
        stats["comments_in"] += 1
        text = clean_text(c.get("body", ""))
        reason = reject_reason(text, c.get("author", ""))
        if reason:
            stats[f"dropped_{reason}"] += 1
            continue
        cmt_docs.append((c, text))

    # ---- 2. let the matcher learn which symbols this corpus uses as tickers
    matcher.fit([d[2] for d in post_docs] + [d[1] for d in cmt_docs])

    # ---- 3. per-post title info
    title_info = {}
    for p, title, _text, _created in post_docs:
        tk = matcher.find(title) if title else set()
        title_info[p["post_id"]] = (title, is_discussion_title(title), tk)

    items, seen = [], set()

    def add_items(kind, base_id, tickers, text, meta, context_for):
        n_own = sum(1 for s in tickers.values() if s == "text")
        for ticker, source in sorted(tickers.items()):
            snippet = (
                text if source == "title" else _snippet(text, ticker, matcher, n_own)
            )
            if not snippet:
                stats["dropped_no_sentence_match"] += 1
                continue
            context = context_for(snippet)
            fingerprint = snippet + "\x1f" + context
            key = (ticker, _norm_key(fingerprint))
            if key in seen:
                stats["dropped_duplicate"] += 1
                continue
            seen.add(key)
            digest = hashlib.sha1(fingerprint.encode("utf-8")).hexdigest()[:8]
            items.append(
                {
                    "id": f"{kind[0]}:{base_id}:{ticker}:{digest}",
                    "kind": kind,
                    "source": source,
                    "ticker": ticker,
                    "n_tickers": len(tickers),
                    "text": snippet,
                    "context": context,
                    **meta,
                }
            )
            stats[f"items_{kind}"] += 1

    # ---- 4a. posts
    for p, title, text, created in post_docs:
        stats["posts_in"] += 1
        if cutoff and created and created < cutoff:
            stats["dropped_post_outside_window"] += 1
            continue
        reason = reject_reason(text, p.get("author", ""))
        if reason:
            stats[f"dropped_post_{reason}"] += 1
            continue
        own = matcher.find(text)
        if not own:
            stats["dropped_no_ticker"] += 1
            continue
        add_items(
            "post",
            p["post_id"],
            {t: "text" for t in own},
            text,
            {
                "subreddit": p.get("subreddit", ""),
                "post_id": p["post_id"],
                "comment_id": "",
                "author": p.get("author", ""),
                "created_utc": created.isoformat() if created else "",
                "upvotes": parse_int(p.get("score")),
                "permalink": p.get("permalink", ""),
            },
            lambda snippet: "",
        )

    # ---- 4b. comments
    for c, text in cmt_docs:
        title, disc, title_tk = title_info.get(c["post_id"], ("", False, set()))
        own = matcher.find(text)
        if own:
            tickers = {t: "text" for t in own}
        elif (
            inherit_title
            and not disc
            and 1 <= len(title_tk) <= MAX_TITLE_TICKERS
            and len(text.split()) >= INHERIT_MIN_WORDS
        ):
            tickers = {t: "title" for t in title_tk}
        else:
            stats["dropped_no_ticker"] += 1
            continue

        def context_for(snippet, c=c, title=title, disc=disc):
            if len(snippet.split()) >= SHORT_WORDS:
                return ""
            parent_id = c.get("parent_id")
            parent = (
                cmt_by_id.get(parent_id)
                if parent_id and parent_id != c["post_id"]
                else None
            )
            if parent:
                ptxt = clean_text(parent.get("body", ""))
                if ptxt and ptxt.strip().lower() not in REMOVED_MARKERS:
                    return ptxt[:CONTEXT_CHARS]
            return "" if disc else title[:CONTEXT_CHARS]

        created = parse_dt(c.get("created_utc"))
        add_items(
            "comment",
            c["comment_id"],
            tickers,
            text,
            {
                "subreddit": c.get("subreddit", ""),
                "post_id": c["post_id"],
                "comment_id": c["comment_id"],
                "author": c.get("author", ""),
                "created_utc": created.isoformat() if created else "",
                "upvotes": parse_int(c.get("score")),
                "permalink": c.get("permalink", ""),
            },
            context_for,
        )

    # comments with no timestamp: borrow the post's, so they still land on a day
    post_time = {
        p["post_id"]: (created.isoformat() if created else "")
        for p, _t, _x, created in post_docs
    }
    for it in items:
        if not it["created_utc"]:
            it["created_utc"] = post_time.get(it["post_id"], "")
    return items, stats


# --------------------------------------------------------------------------- #
# Aggregation
# --------------------------------------------------------------------------- #
DAILY_FIELDS = [
    "ticker",
    "date",
    "n_items",
    "n_voters",
    "total_upvotes",
    "mean_score_weighted",
    "mean_score",
    "pct_bullish",
    "pct_bearish",
    "avg_neutral",
    "mention_z",
    "reliable",
    "reported_score",
]
SUMMARY_FIELDS = [
    "ticker",
    "n_items",
    "n_voters",
    "active_days",
    "peak_day",
    "peak_items",
    "mean_score_weighted",
    "mean_score",
    "pct_bullish",
    "pct_bearish",
    "avg_neutral",
    "reliable",
    "reported_score",
]


def _voter_key(r):
    a = (r.get("author") or "").strip().lower()
    if not a or a in ("[deleted]", "deleted"):
        return "anon:" + str(r.get("comment_id") or r.get("post_id"))
    return a


def _weight(r, field="upvotes"):
    """Popularity weight x confidence (a mostly-neutral item counts for less)."""
    return (1.0 + math.log1p(max(0, r[field]))) * max(0.1, 1.0 - r["neutral"])


def _collapse(rows, field="upvotes"):
    """One author's items -> one vote: (weighted score, weight = their max item weight)."""
    ws = [_weight(r, field) for r in rows]
    total = sum(ws)
    return sum(w * r["score"] for w, r in zip(ws, rows)) / total, max(ws)


def _stats(votes, rows, min_items):
    """votes: [(score, weight)], rows: the item rows behind them."""
    wsum = sum(w for _, w in votes)
    wmean = sum(s * w for s, w in votes) / wsum
    mean = sum(s for s, _ in votes) / len(votes)
    return {
        "mean_score_weighted": round(wmean, 3),
        "mean_score": round(mean, 3),
        "pct_bullish": round(
            sum(s >= BULL_THRESHOLD for s, _ in votes) / len(votes), 3
        ),
        "pct_bearish": round(
            sum(s <= BEAR_THRESHOLD for s, _ in votes) / len(votes), 3
        ),
        "avg_neutral": round(sum(r["neutral"] for r in rows) / len(rows), 3),
    }


def aggregate_rows(rows, min_items=20, weight_field="upvotes"):
    """
    rows -> (daily_rows, summary_rows)

    * one vote per author per ticker per day (their items are averaged; their
      weight is their single largest item weight, so spamming doesn't help)
    * votes are weighted by log(1 + weight_field) x (1 - p_neutral); weight_field is
      "upvotes" for Reddit and "views" for YouTube
    * mention_z: that day's item count vs the ticker's own daily history in the
      window (zeros included); blank if the window is under 3 days
    * `reliable` = at least `min_items` distinct voters; `reported_score` is only
      filled for reliable rows
    """
    by_td = defaultdict(lambda: defaultdict(list))
    for r in rows:
        day = str(r.get("created_utc", ""))[:10]
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day):
            continue
        by_td[(r["ticker"], day)][_voter_key(r)].append(r)
    if not by_td:
        return [], []

    all_days = sorted({d for _, d in by_td})
    d0, d1 = date.fromisoformat(all_days[0]), date.fromisoformat(all_days[-1])
    span = [(d0 + timedelta(days=i)).isoformat() for i in range((d1 - d0).days + 1)]

    counts = defaultdict(dict)  # ticker -> day -> n_items
    votes_by_ticker = defaultdict(list)  # ticker -> [(score, weight)]
    rows_by_ticker = defaultdict(list)
    voters_by_ticker = defaultdict(set)
    daily = []

    for (ticker, day), voters in by_td.items():
        votes = [_collapse(rs, weight_field) for rs in voters.values()]
        item_rows = [r for rs in voters.values() for r in rs]
        counts[ticker][day] = len(item_rows)
        votes_by_ticker[ticker].extend(votes)
        rows_by_ticker[ticker].extend(item_rows)
        voters_by_ticker[ticker].update(voters.keys())
        rec = {
            "ticker": ticker,
            "date": day,
            "n_items": len(item_rows),
            "n_voters": len(voters),
            "total_upvotes": sum(max(0, r[weight_field]) for r in item_rows),
            **_stats(votes, item_rows, min_items),
        }
        rec["reliable"] = len(voters) >= min_items
        rec["reported_score"] = rec["mean_score_weighted"] if rec["reliable"] else ""
        daily.append(rec)

    for rec in daily:
        series = [counts[rec["ticker"]].get(d, 0) for d in span]
        if len(series) >= 3:
            mean = sum(series) / len(series)
            std = math.sqrt(sum((x - mean) ** 2 for x in series) / len(series))
            rec["mention_z"] = (
                round((rec["n_items"] - mean) / std, 3) if std > 0 else 0.0
            )
        else:
            rec["mention_z"] = ""
    daily.sort(key=lambda r: (r["ticker"], r["date"]))

    summary = []
    for ticker, votes in votes_by_ticker.items():
        peak_day, peak_items = max(
            counts[ticker].items(), key=lambda kv: (kv[1], kv[0])
        )
        rec = {
            "ticker": ticker,
            "n_items": len(rows_by_ticker[ticker]),
            "n_voters": len(voters_by_ticker[ticker]),
            "active_days": len(counts[ticker]),
            "peak_day": peak_day,
            "peak_items": peak_items,
            **_stats(votes, rows_by_ticker[ticker], min_items),
        }
        rec["reliable"] = rec["n_voters"] >= min_items
        rec["reported_score"] = rec["mean_score_weighted"] if rec["reliable"] else ""
        summary.append(rec)
    summary.sort(key=lambda r: (-r["n_voters"], r["ticker"]))
    return daily, summary


def write_csv_atomic(path, fields, rows):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    os.replace(tmp, path)


# public aliases for other sources (e.g. the YouTube pipeline)
norm_key = _norm_key
CLAUSE_SPLIT = _CLAUSE_SPLIT
