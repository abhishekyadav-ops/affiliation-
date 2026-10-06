#!/usr/bin/env python3
"""
YouTube Product Tag Scraper  (local Python, writes into Google Sheets)

FLOW
  Creator Input sheet (name, channel URL / ID)
    -> channel ID (resolved via YouTube Data API if missing)
    -> latest 20 videos (YouTube Data API)
    -> fetch each public watch page HTML
    -> parse ytInitialData -> find native product-tag cards
    -> append rows to the "Creator Products" sheet
       (Source = YOUTUBE_TAGGED_PRODUCT)
    -> rebuild the marketplace count table in the SAME sheet
       (columns U onward — no separate "Marketplace Summary" tab)

USAGE
  python product_tag_scraper.py --test VIDEO_ID       # quick check, no Sheets/API keys needed
  python product_tag_scraper.py --diagnose VIDEO_ID   # what does the page HTML contain?
  python product_tag_scraper.py                       # scrape every pending creator
  python product_tag_scraper.py --limit 5             # only the next 5 creators
  python product_tag_scraper.py --fill-ids            # only resolve missing channel IDs
  python product_tag_scraper.py --summary-only        # just rebuild the summary grid

CONFIG (environment variables, or edit the defaults below)
  SPREADSHEET_ID        the long ID in the sheet URL (/d/<ID>/edit)
  SERVICE_ACCOUNT_FILE  path to the service-account JSON key (default service_account.json)
  YOUTUBE_API_KEYS      key1,key2,key3

NETWORK RESILIENCE
  * Every Sheets call and every YouTube call waits out network / DNS drops
    and retries instead of crashing.
  * Appends are never blindly repeated: after a drop the script checks whether
    the timed-out append actually landed, so rows are not duplicated.
  * A creator hit by a network drop is marked RETRY and picked up again
    automatically (up to MAX_PASSES passes in one run).
  * If the network stays down for over an hour the run stops cleanly; the
    creator stays PROCESSING/RETRY and the next run resumes it.

NOTES
  * There is no official API for YouTube product tags. This reads the public
    watch page like a browser does. That is scraping and is against YouTube's
    Terms of Service for automated access.
  * YouTube can change its page structure at any time; the scraper then
    returns empty results instead of crashing.
  * The product panel may be lazy-loaded (not in the raw HTML). Use
    --diagnose on a video you KNOW has tags to check.
  * A home/office IP is much less likely to get HTTP 429 than Apps Script,
    but heavy volume can still be throttled. The run stops early and marks the
    creator RETRY if that happens; rerun later to resume.
"""

import argparse
import json
import logging
import os
import random
import re
import socket
import sys
import time
from datetime import datetime, timezone

import requests

try:
    from google.auth.exceptions import TransportError as _GoogleTransportError
except ImportError:  # pragma: no cover
    _GoogleTransportError = None

# ----------------------------------------------------------------------
# LOGGING
# ----------------------------------------------------------------------
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("product_tag_scraper")

# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------
SPREADSHEET_ID = os.environ.get(
    "SPREADSHEET_ID",
    "1CxNbSKUeWAB_ljShyv52nDM4gK-B8jPzuqkDe-t4HN4",
)

SERVICE_ACCOUNT_FILE = os.environ.get(
    "SERVICE_ACCOUNT_FILE",
    "service_account.json",
)

# Paste your keys inside the quotes. Add more lines for more keys.
# If the YOUTUBE_API_KEYS env var is set, it still takes priority.
_HARDCODED_YT_KEYS = [
    "AIzaSyDic8XgtwkqAEVSf38HyMkprV3rY4MXRiQ",
    "AIzaSyCSoFKgChSCQX0RUJtx1hjkFqcEWbEVZUY",
    "AIzaSyCZ3nUhGIZMDIteBS12U658QsxSYB_HcJU"
]

YOUTUBE_API_KEYS = (
    os.environ.get("YOUTUBE_API_KEYS", "").strip()
    or ",".join(k for k in _HARDCODED_YT_KEYS if k and not k.startswith("PASTE_"))
)

INPUT_SHEET = "Creator Input"
OUTPUT_SHEET = "Creator Products"

MAX_VIDEOS = 20
SOURCE = "YOUTUBE_TAGGED_PRODUCT"

# Delay between watch-page fetches (seconds, random in this range).
FETCH_DELAY = (1.5, 3.0)

# On HTTP 429: wait, then retry the same video this many times.
RETRIES_ON_429 = 2
BACKOFF_SECONDS = 20  # 20s, then 40s

# Stop scanning a creator after this many rate-limited videos in a row.
MAX_CONSECUTIVE_429 = 3

# Network resilience.
SHEETS_TIMEOUT = 60          # seconds per Sheets request
SHEETS_RETRIES = 8           # per Sheets call
NETWORK_MAX_WAIT = 3600      # give up waiting for the network after 1 hour
MAX_PASSES = 3               # extra passes over creators that hit a network drop

INPUT_HEADERS = [
    "Creator", "Channel URL", "Channel ID", "Status",
    "Products Found", "Videos Checked", "Error", "Processed At",
]

OUTPUT_HEADERS = [
    "Creator", "Channel URL", "Channel ID", "Discovered For Product",
    "Product", "Brand", "Entity Type", "Category", "Subcategory",
    "Marketplace", "Marketplace Type", "Evidence", "Scraped From",
    "Reference Link", "Video URL", "Video Description", "Confidence",
    "Source", "Processed At",
]

# Marketplace summary GRID lives in this same sheet (column U onward),
# with column T left blank as a spacer from the 19 data columns (A-S).
# Layout: one row per creator, one column per marketplace, e.g.
#   Creator     Amazon  Flipkart  Myntra  Total
#   Ria Verma   12      4         1       17
#   ...
#   TOTAL       142     88        51      311
SUMMARY_START_COL_INDEX = 21   # column U
SUMMARY_CLEAR_COLS = 40        # generous width to survive new marketplaces/creators
SUMMARY_CLEAR_ROWS = 2000


def _col_letter(idx):
    """1-indexed column number -> spreadsheet column letters (1 -> A, 27 -> AA)."""
    letters = ""
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


KNOWN_STORES = [
    ("purplle", "Purplle.com"),
    ("shopsy", "Shopsy By Flipkart"),
    ("flipkart", "Flipkart"),
    ("nykaa", "Nykaa"),
    ("tira", "Tira"),
    ("myntra", "Myntra"),
    ("amazon", "Amazon"),
]

EXCLUDE_DOMAINS = [
    "gstatic.com", "ytimg.com", "ggpht.com", "youtube.com/youtubei",
    "googleapis.com", "google.com/url", "googleusercontent.com",
]

PREFERRED_DOMAINS = [
    "myntra.com", "amazon.", "amzn.", "nykaa.com", "nykaa.onelink",
    "flipkart.com", "tira.com", "purplle.com", "ajio.com", "meesho.com",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    # Skips the EU cookie-consent interstitial page.
    "Cookie": "CONSENT=YES+cb.20210328-17-p0.en+FX+299",
}

# Errors that mean "the network dropped", as opposed to a real API/logic error.
_net = [
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
]
if _GoogleTransportError is not None:
    _net.append(_GoogleTransportError)
NETWORK_ERRORS = tuple(_net)


class RateLimited(Exception):
    pass


class ConfigError(Exception):
    pass


class NetworkDown(Exception):
    """The network did not come back within NETWORK_MAX_WAIT."""


# ----------------------------------------------------------------------
# HELPERS
# ----------------------------------------------------------------------

def normalize(value):
    return re.sub(r"\s+", " ", str(value or "")).strip()


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def api_keys():
    # Accept either the configured comma-separated environment string or a
    # list, so callers cannot trigger "'list' object has no attribute split".
    if isinstance(YOUTUBE_API_KEYS, str):
        keys = [k.strip() for k in YOUTUBE_API_KEYS.split(",") if k.strip()]
    elif isinstance(YOUTUBE_API_KEYS, (list, tuple)):
        keys = [str(k).strip() for k in YOUTUBE_API_KEYS if str(k).strip()]
    else:
        keys = []

    if not keys:
        raise ConfigError(
            "Set YOUTUBE_API_KEYS in your environment as comma-separated keys."
        )
    return keys


def wait_for_network(host="sheets.googleapis.com", max_wait=NETWORK_MAX_WAIT):
    """Block until a TCP connection to `host` works again."""
    waited, delay = 0, 5
    while True:
        try:
            socket.create_connection((host, 443), timeout=10).close()
            return
        except OSError:
            log.warning("Network down, waiting %ss (%s)...", delay, host)
            time.sleep(delay)
            waited += delay
            delay = min(delay * 2, 60)
            if waited > max_wait:
                raise NetworkDown(
                    "Network did not come back within %d minutes" % (max_wait // 60)
                )


# ----------------------------------------------------------------------
# YOUTUBE DATA API
# ----------------------------------------------------------------------

def yt_api(endpoint, params):
    """Call the YouTube Data API, rotating keys on failure and waiting out
    network drops."""
    last = ""
    for _round in range(4):
        network_failed = False
        for key in api_keys():
            try:
                r = requests.get(
                    "https://www.googleapis.com/youtube/v3/" + endpoint,
                    params={**params, "key": key},
                    timeout=30,
                )
            except requests.RequestException as e:
                last = str(e)
                network_failed = True
                continue

            if r.status_code == 200:
                return r.json()
            last = "HTTP %s: %s" % (r.status_code, r.text[:300])

        if not network_failed:
            break  # real API error (quota, bad ID): retrying won't help
        wait_for_network("www.googleapis.com")

    raise RuntimeError(last)


def parse_channel_input(raw):
    value = normalize(raw)
    if not value:
        return None

    m = re.search(r"UC[a-zA-Z0-9_-]{22}", value)
    if m:
        return ("id", m.group(0))

    m = re.search(r"@([a-zA-Z0-9_.-]+)", value)
    if m:
        return ("handle", "@" + m.group(1))

    m = re.search(r"/(?:c|user)/([a-zA-Z0-9_-]+)", value)
    if m:
        return ("username", m.group(1))

    if not re.search(r"https?://", value) and " " not in value:
        return ("username", value.lstrip("@"))

    return None


def resolve_channel_id(channel_url, creator):
    """id -> forHandle -> forUsername. No search fallback (it can pick the
    wrong channel silently), so unresolved rows are reported instead."""
    parsed = parse_channel_input(channel_url) or parse_channel_input(creator)
    if not parsed:
        raise RuntimeError("Nothing to resolve a channel ID from.")

    kind, value = parsed

    if kind == "id":
        return value

    param = "forHandle" if kind == "handle" else "forUsername"
    data = yt_api("channels", {"part": "id", param: value})
    items = data.get("items") or []
    if not items:
        raise RuntimeError("No channel found for %s" % value)
    return items[0]["id"]


def get_latest_videos(channel_id, max_videos):
    # A channel's uploads playlist ID is its channel ID with "UC" -> "UU".
    playlist_id = "UU" + channel_id[2:]

    data = yt_api("playlistItems", {
        "part": "snippet,contentDetails",
        "playlistId": playlist_id,
        "maxResults": max_videos,
    })

    videos = []
    for item in data.get("items", []):
        snippet = item.get("snippet", {})
        content = item.get("contentDetails", {})
        video_id = content.get("videoId") or snippet.get("resourceId", {}).get("videoId")
        if not video_id:
            continue
        videos.append({
            "id": video_id,
            "url": "https://www.youtube.com/watch?v=" + video_id,
            "description": normalize(snippet.get("description")),
        })
    return videos


# ----------------------------------------------------------------------
# WATCH PAGE FETCH + PARSE
# ----------------------------------------------------------------------

def fetch_watch_html(session, video_id):
    url = "https://www.youtube.com/watch"
    params = {"v": video_id, "hl": "en", "gl": "US", "persist_gl": "1"}

    for attempt in range(RETRIES_ON_429 + 1):
        # Network drops: wait for the connection, then retry (does not use
        # up the 429 retry budget).
        r = None
        for _ in range(5):
            try:
                r = session.get(url, params=params, headers=HEADERS, timeout=30)
                break
            except NETWORK_ERRORS:
                wait_for_network("www.youtube.com")
        if r is None:
            raise RuntimeError("Network kept failing while fetching the watch page")

        if r.status_code == 429:
            if attempt < RETRIES_ON_429:
                wait = BACKOFF_SECONDS * (attempt + 1)
                log.warning("    429 from YouTube, waiting %ss...", wait)
                time.sleep(wait)
                continue
            raise RateLimited("HTTP 429")

        if r.status_code != 200:
            raise RuntimeError("Watch page fetch failed: HTTP %s" % r.status_code)

        return r.text

    raise RateLimited("HTTP 429")


def extract_yt_initial_data(html):
    idx = -1
    marker = ""
    for marker in ("var ytInitialData = ", 'ytInitialData"] = ', "ytInitialData = "):
        idx = html.find(marker)
        if idx != -1:
            break

    if idx == -1:
        raise ValueError(
            "ytInitialData not found (consent page, or YouTube changed its structure)."
        )

    try:
        obj, _ = json.JSONDecoder().raw_decode(html, idx + len(marker))
    except json.JSONDecodeError as e:
        raise ValueError("ytInitialData parse failed: %s" % e)

    return obj


def find_by_key(node, target, out):
    """Walk the whole tree for any key named `target`, so we don't depend on
    YouTube's exact nesting path."""
    if isinstance(node, dict):
        for k, v in node.items():
            if k == target:
                out.append(v)
            find_by_key(v, target, out)
    elif isinstance(node, list):
        for v in node:
            find_by_key(v, target, out)


def extract_text(obj):
    if not obj:
        return ""
    if isinstance(obj, str):
        return obj
    if obj.get("simpleText"):
        return obj["simpleText"]
    if obj.get("runs"):
        return "".join(run.get("text", "") for run in obj["runs"])
    return ""


def extract_url(item):
    """The exact key path of the product link isn't confirmed, so scan every
    string in the card for a real URL, skip known noise, prefer store domains."""
    found = []

    def walk(node):
        if isinstance(node, dict):
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)
        elif isinstance(node, str) and re.match(r"^https?://", node, re.I):
            if not any(d in node for d in EXCLUDE_DOMAINS):
                found.append(node)

    walk(item)

    if not found:
        return ""

    for domain in PREFERRED_DOMAINS:
        for url in found:
            if domain in url:
                return url

    return found[0]


def parse_product_item(item):
    return {
        "title": normalize(extract_text(item.get("title"))),
        "price": normalize(item.get("price") or ""),
        "merchant": normalize(
            item.get("merchantName") or extract_text(item.get("fromVendorText")) or ""
        ),
        "url": extract_url(item),
    }


def get_tagged_products(session, video_id):
    html = fetch_watch_html(session, video_id)
    data = extract_yt_initial_data(html)

    raw_items = []
    find_by_key(data, "productListItemRenderer", raw_items)

    products = []
    for item in raw_items:
        parsed = parse_product_item(item)
        if not parsed["title"]:
            continue
        # Preserve separate appearances; same product in another card/video counts again.
        parsed["card_index"] = len(products) + 1
        products.append(parsed)

    return products


def normalize_marketplace(merchant, url):
    text = (normalize(merchant) + " " + normalize(url)).lower()
    for match, label in KNOWN_STORES:
        if match in text:
            return label
    return merchant or ""


# ----------------------------------------------------------------------
# TEST / DIAGNOSE (no Google Sheets or API keys needed)
# ----------------------------------------------------------------------

def cmd_test(video_id):
    session = requests.Session()
    try:
        products = get_tagged_products(session, video_id)
    except RateLimited:
        sys.exit("HTTP 429: YouTube is rate-limiting this IP too.")
    except Exception as e:
        sys.exit("Error: %s" % e)

    if products:
        print("Found %d tagged product(s):" % len(products))
        for i, p in enumerate(products, 1):
            print("%d. %s | Merchant: %s | Price: %s | URL: %s" % (
                i, p["title"], p["merchant"] or "-", p["price"] or "-", p["url"] or "(none)"
            ))
        return

    print("No product cards found via productListItemRenderer.")
    print("Keys containing 'product' in the page data:")
    html = fetch_watch_html(session, video_id)
    data = extract_yt_initial_data(html)
    paths = []

    def dump(node, path):
        if isinstance(node, dict):
            for k, v in node.items():
                p = path + "." + k
                if re.search("product", k, re.I):
                    paths.append(p)
                dump(v, p)
        elif isinstance(node, list):
            for i, v in enumerate(node):
                dump(v, "%s[%d]" % (path, i))

    dump(data, "root")
    print("\n".join(paths) if paths else "(none)")


def cmd_diagnose(video_id):
    session = requests.Session()
    try:
        html = fetch_watch_html(session, video_id)
    except RateLimited:
        sys.exit("HTTP 429: YouTube is rate-limiting this IP too.")

    print("HTML length:", len(html))
    m = re.search(r"<title>([^<]*)</title>", html)
    print("Page title:", m.group(1) if m else "(none)")

    for needle in [
        "ytInitialData", "productListItemRenderer", "merchandiseItemRenderer",
        "merchandiseShelfRenderer", "shoppingPanel", "merchantName",
        "earns commission", "myntra.com", "nykaa", "flipkart.com",
    ]:
        idx = html.find(needle)
        if idx == -1:
            print("%s: NOT FOUND" % needle)
        else:
            snippet = re.sub(r"\s+", " ", html[max(0, idx - 80): idx + 200])
            print("%s: found at %d -> %s" % (needle, idx, snippet))


# ----------------------------------------------------------------------
# GOOGLE SHEETS
# ----------------------------------------------------------------------

def install_sheets_retry(gc):
    """Make every gspread request wait out network drops / quota errors and
    retry. Appends are excluded: a timed-out append may have landed, so
    write_products verifies that itself instead of blindly repeating it."""
    import gspread

    original = gc.http_client.request

    def wrapped(method, endpoint, *args, **kwargs):
        is_append = ":append" in str(endpoint)
        for attempt in range(SHEETS_RETRIES):
            last_try = attempt == SHEETS_RETRIES - 1
            try:
                return original(method, endpoint, *args, **kwargs)
            except NETWORK_ERRORS as e:
                if is_append or last_try:
                    raise
                log.warning("Sheets network error (%s), retry %d/%d",
                            type(e).__name__, attempt + 1, SHEETS_RETRIES - 1)
                wait_for_network()
                time.sleep(3)
            except gspread.exceptions.APIError as e:
                code = getattr(getattr(e, "response", None), "status_code", 0)
                if code not in (429, 500, 502, 503, 504) or is_append or last_try:
                    raise
                wait = 30 if code == 429 else 10
                log.warning("Sheets API error %s, waiting %ss (retry %d/%d)",
                            code, wait, attempt + 1, SHEETS_RETRIES - 1)
                time.sleep(wait)

    gc.http_client.request = wrapped


def open_sheets():
    import gspread

    if SPREADSHEET_ID in ("", "PASTE_SPREADSHEET_ID_HERE"):
        raise ConfigError("Set SPREADSHEET_ID (env var or at the top of this file).")
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise ConfigError("Service-account key not found: %s" % SERVICE_ACCOUNT_FILE)

    gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
    gc.set_timeout(SHEETS_TIMEOUT)
    install_sheets_retry(gc)
    sh = gc.open_by_key(SPREADSHEET_ID)

    def get_or_create(title, headers):
        try:
            ws = sh.worksheet(title)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=title, rows=1000, cols=max(len(headers), 26))
        if not ws.row_values(1):
            ws.update(range_name="A1", values=[headers])
            ws.freeze(rows=1)
        return ws

    input_ws = get_or_create(INPUT_SHEET, INPUT_HEADERS)
    output_ws = get_or_create(OUTPUT_SHEET, OUTPUT_HEADERS)
    return input_ws, output_ws


def make_key(channel_id, video_url, title):
    """One key format shared by existing_keys() and write_products()."""
    return (normalize(channel_id).lower() + "||" + normalize(video_url) + "||"
            + normalize(title).lower())


def existing_keys(output_ws):
    """Rows already written by this scraper, so reruns don't duplicate."""
    keys = set()
    for row in output_ws.get_all_values()[1:]:
        row = row + [""] * (len(OUTPUT_HEADERS) - len(row))
        if normalize(row[17]) != SOURCE:
            continue
        channel_id, product, video_url = row[2], row[4], row[14]
        if normalize(channel_id) and normalize(product) and normalize(video_url):
            keys.add(make_key(channel_id, video_url, product))
    return keys


def write_products(output_ws, creator, channel_url, channel_id, products, keys):
    rows = []
    new_keys = []
    batch_keys = set()
    now = now_iso()

    for p in products:
        key = make_key(channel_id, p.get("video_url", ""), p["title"])
        if key in keys or key in batch_keys:
            continue
        batch_keys.add(key)
        new_keys.append(key)

        marketplace = normalize_marketplace(p["merchant"], p["url"])
        rows.append([
            creator,                                    # Creator
            channel_url,                                # Channel URL
            channel_id,                                 # Channel ID
            "LAST_20_VIDEOS",                           # Discovered For Product
            p["title"],                                 # Product (card title)
            p["merchant"],                              # Brand (vendor name from card)
            "physical_product",                         # Entity Type
            "",                                         # Category
            "",                                         # Subcategory
            marketplace,                                # Marketplace
            "e-commerce" if marketplace else "",        # Marketplace Type
            "YouTube native product tag",               # Evidence
            "YouTube native product tag (page scrape)", # Scraped From
            p["url"] or p.get("video_url", ""),         # Reference Link fallback to source video
            p.get("video_url", ""),                     # Video URL
            p.get("video_description", ""),             # Video Description
            1,                                          # Confidence
            SOURCE,                                     # Source
            now,                                        # Processed At
        ])

    if rows:
        for attempt in range(5):
            try:
                # RAW so a title starting with "=" is never treated as a formula.
                output_ws.append_rows(rows, value_input_option="RAW")
                break
            except NETWORK_ERRORS:
                wait_for_network()
                # Did the timed-out append actually land? Same batch = same timestamp.
                landed = any(
                    len(r) > 18 and r[18] == now and r[2] == channel_id
                    for r in output_ws.get_all_values()[1:]
                )
                if landed:
                    log.info("  Append had landed before the drop, not repeating")
                    break
                if attempt == 4:
                    raise
                log.warning("  Append did not land, retrying (%d/4)", attempt + 1)

        # Only remember keys once the rows are really in the sheet.
        keys.update(new_keys)

    return len(rows)


def set_status(input_ws, sheet_row, status, found=0, checked=0, error=""):
    input_ws.update(
        range_name="D%d:H%d" % (sheet_row, sheet_row),
        values=[[status, found, checked, error, now_iso()]],
        value_input_option="RAW",
    )


def safe_set_status(input_ws, sheet_row, status, found=0, checked=0, error=""):
    """Status write that can never kill the run."""
    try:
        set_status(input_ws, sheet_row, status, found, checked, error)
    except Exception as e:
        log.error("  Could not write status %s for row %d: %s", status, sheet_row, e)


def update_marketplace_summary(output_ws):
    """Rebuild a per-creator x marketplace count grid from rows written by
    this scraper, directly inside Creator Products starting at column U
    (no separate tab). One row per creator, one column per marketplace."""
    values = output_ws.get_all_values()

    creator_order = []
    creator_seen = set()
    per_creator = {}          # creator -> {marketplace: count}
    marketplace_totals = {}   # marketplace -> total count across all creators

    for row in values[1:]:
        row = row + [""] * (len(OUTPUT_HEADERS) - len(row))
        if normalize(row[17]) != SOURCE:
            continue

        creator = normalize(row[0]) or "(unknown creator)"
        marketplace = normalize(row[9]) or "Unknown / Not detected"

        if creator not in creator_seen:
            creator_seen.add(creator)
            creator_order.append(creator)

        per_creator.setdefault(creator, {})
        per_creator[creator][marketplace] = per_creator[creator].get(marketplace, 0) + 1
        marketplace_totals[marketplace] = marketplace_totals.get(marketplace, 0) + 1

    # Busiest marketplace first, so Amazon/Flipkart land near the front.
    marketplaces = sorted(
        marketplace_totals, key=lambda m: (-marketplace_totals[m], m.lower())
    )

    header = ["Creator"] + marketplaces + ["Total"]
    block = [header]

    for creator in creator_order:
        row_counts = per_creator[creator]
        row = [creator] + [row_counts.get(m, 0) for m in marketplaces]
        row.append(sum(row_counts.values()))
        block.append(row)

    if creator_order:
        totals_row = ["TOTAL"] + [marketplace_totals[m] for m in marketplaces]
        totals_row.append(sum(marketplace_totals.values()))
        block.append(totals_row)

    # Clear a generous block first so a shrinking grid doesn't leave stale cells.
    start_col = _col_letter(SUMMARY_START_COL_INDEX)
    end_col = _col_letter(SUMMARY_START_COL_INDEX + SUMMARY_CLEAR_COLS)
    output_ws.batch_clear(["%s1:%s%d" % (start_col, end_col, SUMMARY_CLEAR_ROWS)])

    output_ws.update(
        range_name="%s1" % start_col,
        values=block,
        value_input_option="RAW",
    )

    log.info("Per-creator marketplace grid (%d creators, %d marketplaces):",
             len(creator_order), len(marketplaces))
    if not creator_order:
        log.info("  No product rows found yet.")


# ----------------------------------------------------------------------
# PROCESS
# ----------------------------------------------------------------------

def process_creator(session, output_ws, creator, channel_url, channel_id, keys):
    videos = get_latest_videos(channel_id, MAX_VIDEOS)
    log.info("  %d videos to scan", len(videos))

    all_products = []
    seen = set()
    checked = with_tags = failed = consecutive_429 = 0
    rate_limited = False

    for i, video in enumerate(videos, 1):
        try:
            products = get_tagged_products(session, video["id"])
            consecutive_429 = 0
            checked += 1
            if products:
                with_tags += 1

            for p in products:
                key = (p["title"].lower(), p["url"])
                if key in seen:
                    continue
                seen.add(key)
                p["video_url"] = video["url"]
                p["video_description"] = video["description"]
                all_products.append(p)

        except RateLimited:
            failed += 1
            consecutive_429 += 1
            log.warning("    video %d/%d: rate limited", i, len(videos))
            if consecutive_429 >= MAX_CONSECUTIVE_429:
                rate_limited = True
                break

        except NetworkDown:
            raise

        except Exception as e:
            failed += 1
            consecutive_429 = 0
            log.warning("    video %s failed: %s", video["id"], e)

        time.sleep(random.uniform(*FETCH_DELAY))

    log.info(
        "  %d/%d videos had tags, %d unique products, %d failures%s",
        with_tags, checked, len(all_products), failed,
        " (RATE LIMITED, stopped early)" if rate_limited else "",
    )

    # Anything found before a rate limit is still saved.
    written = write_products(output_ws, creator, channel_url, channel_id, all_products, keys)

    return {"videos": checked, "products": written, "rate_limited": rate_limited}


def load_input(input_ws):
    rows = input_ws.get_all_values()[1:]
    result = []
    for i, row in enumerate(rows):
        row = row + [""] * (len(INPUT_HEADERS) - len(row))
        result.append({
            "row": i + 2,
            "creator": normalize(row[0]),
            "channel_url": normalize(row[1]),
            "channel_id": normalize(row[2]),
            "status": normalize(row[3]),
        })
    return result


def cmd_fill_ids(input_ws):
    resolved = failed = 0
    for r in load_input(input_ws):
        if r["channel_id"] or not (r["channel_url"] or r["creator"]):
            continue
        try:
            cid = resolve_channel_id(r["channel_url"], r["creator"])
            input_ws.update_cell(r["row"], 3, cid)
            log.info("%s -> %s", r["creator"] or r["channel_url"], cid)
            resolved += 1
            time.sleep(0.2)
        except NetworkDown:
            log.error("Network is down, stopping.")
            break
        except Exception as e:
            log.warning("%s FAILED: %s", r["creator"] or r["channel_url"], e)
            failed += 1
    log.info("Resolved: %d, failed: %d", resolved, failed)


def run_pass(input_ws, output_ws, session, keys, limit):
    """One pass over pending creators.
    Returns (processed, network_retries, stop)."""
    processed = 0
    network_retries = 0

    for r in load_input(input_ws):
        if limit and processed >= limit:
            break

        # blank / PENDING / RETRY / PROCESSING (crashed earlier) get processed.
        if r["status"] and r["status"] not in ("PENDING", "RETRY", "PROCESSING"):
            continue
        if not (r["creator"] or r["channel_id"] or r["channel_url"]):
            continue

        log.info("[%s]", r["creator"] or r["channel_url"])

        try:
            set_status(input_ws, r["row"], "PROCESSING")

            channel_id = r["channel_id"]
            if not channel_id:
                channel_id = resolve_channel_id(r["channel_url"], r["creator"])
                input_ws.update_cell(r["row"], 3, channel_id)

            result = process_creator(
                session, output_ws, r["creator"], r["channel_url"], channel_id, keys
            )

            if result["rate_limited"]:
                safe_set_status(input_ws, r["row"], "RETRY", result["products"],
                                result["videos"], "Rate limited by YouTube (HTTP 429)")
                log.warning(
                    "Stopped: YouTube is rate-limiting this IP. "
                    "Wait a while and rerun to continue."
                )
                return processed, network_retries, True

            set_status(input_ws, r["row"], "COMPLETED", result["products"], result["videos"])
            processed += 1
            log.info("  -> %d tagged products written", result["products"])

        except NetworkDown as e:
            log.error("  %s. Stopping; rerun later to resume.", e)
            return processed, network_retries, True

        except NETWORK_ERRORS as e:
            log.error("  Network failure: %s", type(e).__name__)
            try:
                wait_for_network()
            except NetworkDown as nd:
                log.error("  %s. Stopping; rerun later to resume.", nd)
                return processed, network_retries, True
            safe_set_status(input_ws, r["row"], "RETRY", 0, 0, "Network drop")
            network_retries += 1

        except Exception as e:
            log.error("  ERROR: %s", e)
            safe_set_status(input_ws, r["row"], "ERROR", 0, 0, str(e)[:300])

    return processed, network_retries, False


def cmd_run(input_ws, output_ws, limit):
    keys = existing_keys(output_ws)
    session = requests.Session()
    total = 0

    for pass_no in range(1, MAX_PASSES + 1):
        remaining = (limit - total) if limit else 0
        if limit and remaining <= 0:
            break

        done, network_retries, stop = run_pass(
            input_ws, output_ws, session, keys, remaining
        )
        total += done

        if stop or not network_retries:
            break
        if pass_no < MAX_PASSES:
            log.info("Pass %d: %d creator(s) hit a network drop, starting another pass",
                     pass_no, network_retries)
        else:
            log.warning("%d creator(s) still marked RETRY after %d passes; rerun to finish",
                        network_retries, MAX_PASSES)

    try:
        update_marketplace_summary(output_ws)
    except Exception as e:
        log.error("Could not rebuild summary (%s). Run with --summary-only later.", e)

    log.info("Done. Creators processed: %d", total)


# ----------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="YouTube product tag scraper")
    ap.add_argument("--test", metavar="VIDEO_ID", help="scrape one video and print the cards")
    ap.add_argument("--diagnose", metavar="VIDEO_ID", help="show what the watch page HTML contains")
    ap.add_argument("--fill-ids", action="store_true", help="only resolve missing channel IDs")
    ap.add_argument("--summary-only", action="store_true",
                    help="just rebuild the marketplace summary grid and exit")
    ap.add_argument("--limit", type=int, default=0, help="max creators this run")
    args = ap.parse_args()

    try:
        if args.test:
            return cmd_test(args.test)
        if args.diagnose:
            return cmd_diagnose(args.diagnose)

        input_ws, output_ws = open_sheets()

        if args.fill_ids:
            return cmd_fill_ids(input_ws)

        if args.summary_only:
            return update_marketplace_summary(output_ws)

        cmd_run(input_ws, output_ws, args.limit)

    except ConfigError as e:
        sys.exit(str(e))
    except NetworkDown as e:
        sys.exit("%s. Rerun once your connection is back." % e)
    except KeyboardInterrupt:
        sys.exit("\nInterrupted.")


if __name__ == "__main__":
    main()