import re
import time
import json

# ---------------------------------------------------------------------------
# ID mapping helpers
# ---------------------------------------------------------------------------

def build_sorted_keys(id_map):
    """
    Pre-compute the ID_MAP keys sorted by length descending.
    Used by parse_content_id_to_dvd for efficient prefix matching.
    """
    return sorted(id_map.keys(), key=len, reverse=True)


def build_reverse_map(id_map):
    """
    Build a reverse lookup from DVD prefix (e.g. 'WANZ-') to a list of
    (content_id_prefix, padding) tuples, sorted by key length descending
    (most specific / longest key first).

    Handles cases where multiple content ID prefixes map to the same DVD prefix,
    e.g. both 'wanz00' and '3wanz00' map to 'WANZ-'.
    """
    reverse = {}
    for key, dvd_prefix in id_map.items():
        trailing = len(key) - len(key.rstrip('0123456789'))
        entry = (key, trailing)
        dvd_upper = dvd_prefix.upper()
        if dvd_upper not in reverse:
            reverse[dvd_upper] = []
        reverse[dvd_upper].append(entry)
    # Sort each candidate list by key length descending (longest = most specific first)
    for dvd_prefix in reverse:
        reverse[dvd_prefix].sort(key=lambda x: len(x[0]), reverse=True)
    return reverse


def parse_content_id_to_dvd(content_id, id_map, sorted_keys, re_extensions):
    """
    Convert a DMM content ID (e.g. 'ssis00100') to a DVD ID (e.g. 'SSIS-100').
    Uses pre-sorted ID_MAP keys for efficient prefix matching.
    """
    re_dvd = re.compile(r'^([a-zA-Z]+)(\d+)')
    cid = re_extensions.sub('', content_id.strip())
    cid_lower = cid.lower()
    if id_map:
        for key in sorted_keys:
            if cid_lower.startswith(key.lower()):
                return f"{id_map[key]}{cid[len(key):]}"
    match = re_dvd.match(re.sub(r'[vV]$', '', cid))
    if match:
        prefix, num = match.groups()
        return f"{prefix.upper()}-{num[2:] if num.startswith('000') else num.lstrip('0') or '0'}"
    return cid.upper()


def dvd_to_content_id_candidates(dvd_id, reverse_map):
    """
    Convert a DVD ID (e.g. 'WANZ-066') to a list of candidate DMM content IDs,
    ordered by specificity (longest mapping key first).

    e.g. 'WANZ-066' -> ['3wanz00066', 'wanz00066']

    Falls back to [dvd_id.lower()] if no mapping is found.
    """
    dvd_id = dvd_id.strip()
    if '-' not in dvd_id:
        return [dvd_id.lower()]

    dash_pos   = dvd_id.index('-')
    dvd_prefix = dvd_id[:dash_pos + 1].upper()
    number     = dvd_id[dash_pos + 1:]

    if dvd_prefix not in reverse_map:
        return [dvd_id.lower()]

    candidates = []
    for content_prefix, padding in reverse_map[dvd_prefix]:
        padded_number = number.zfill(padding) if padding > 0 else number
        candidates.append(f"{content_prefix}{padded_number}")
    return candidates


# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------

def scrape_r18(browser, identifier, re_extensions, parse_fn,
               scrape_retries, retry_delay, rate_limit_delay):
    """
    Scrape metadata for a single content ID from r18.dev.
    Returns a metadata dict on success.
    Returns False specifically on 404 (content not found) — fast-fail,
    no retry, so callers can immediately try the next candidate.
    Returns None after all retries are exhausted.
    """
    clean_tid = re_extensions.sub('', identifier).strip()
    api_url = f"https://r18.dev/videos/vod/movies/detail/-/combined={clean_tid}/json"

    for attempt in range(1, scrape_retries + 1):
        page = browser.new_page()
        try:
            res = page.goto(api_url, wait_until="domcontentloaded", timeout=40000)

            # 404 = content doesn't exist — fast-fail, no retry
            if res.status == 404:
                page.close()
                return False

            if res.status != 200:
                page.close()
                backoff = retry_delay * (2 ** (attempt - 1))
                print(f"\n[!] HTTP {res.status} on attempt {attempt} for {clean_tid} — retrying in {backoff}s")
                time.sleep(backoff)
                continue

            data = json.loads(page.evaluate("() => document.body.innerText"))
            page.close()

            api_dvd = data.get("dvd_id")
            final_dvd_id = str(api_dvd).upper() if api_dvd else parse_fn(clean_tid)

            time.sleep(rate_limit_delay)

            directors = [str(d.get("name_romaji")) for d in data.get("directors", []) if d and d.get("name_romaji")]

            return {
                "display_id":      final_dvd_id,
                "web_url":         f"https://r18.dev/videos/vod/movies/detail/-/id={clean_tid}/",
                "title_en":        data.get("title_en"),
                "date":            data.get("release_date"),
                "studio_name":     data.get("maker_name_en"),
                "series_name":     data.get("series_name_en"),
                "performer_names": [str(a.get("name_romaji") or a.get("actress_name_en")) for a in data.get("actresses", []) if a],
                "category_names":  [str(c.get("name_en")) for c in data.get("categories", []) if c and c.get("name_en")],
                "cover":           data.get("jacket_full_url"),
                "director":        ", ".join(directors) if directors else None,
            }

        except Exception as e:
            page.close()
            backoff = retry_delay * (2 ** (attempt - 1))
            print(f"\n[!] Attempt {attempt}/{scrape_retries} failed for {clean_tid}: {type(e).__name__}: {e} — retrying in {backoff}s")
            if attempt < scrape_retries:
                time.sleep(backoff)

    return None
