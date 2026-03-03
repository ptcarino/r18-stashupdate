import os
from pathlib import Path
import re
import time
import json
import sys
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from stashapi.stashapp import StashInterface
from camoufox.sync_api import Camoufox

import r18_gallery
import r18_image_scrape as r18_imagedownload

# Load env files
load_dotenv()

STASH_CONFIG = {
    "scheme": os.getenv("STASH_SCHEME", "http"),
    "host": os.getenv("STASH_HOST"),
    "port": os.getenv("STASH_PORT"),
    "apikey": os.getenv("STASH_APIKEY")
}

USE_PROXY          = os.getenv("USE_PROXY", "false").lower() == "true"
PROXY_CONFIG       = {"server": os.getenv("PROXY_SERVER"), "username": os.getenv("PROXY_USER"), "password": os.getenv("PROXY_PASS")}
SCRAPE_RETRIES     = int(os.getenv("SCRAPE_RETRIES", 5))
RETRY_DELAY        = int(os.getenv("RETRY_DELAY", 5))
RATE_LIMIT_DELAY   = float(os.getenv("RATE_LIMIT_DELAY", 1.5))
MAX_WORKERS        = int(os.getenv("MAX_WORKERS", 3))
INPUT_FILE         = os.getenv("INPUT_FILE")
SHOW_METRICS       = os.getenv("SHOW_METRICS", "true").lower() == "true"
MAPPING_FILE_PATH  = os.getenv("MAPPING_FILE")
DRY_RUN            = os.getenv("DRY_RUN", "false").lower() == "true"
GALLERY_PATH       = os.getenv("GALLERY_PATH")
# SCAN_TIMEOUT=0 means wait indefinitely; any positive value is a second ceiling
SCAN_TIMEOUT       = int(os.getenv("SCAN_TIMEOUT", 0))
SCAN_POLL_INTERVAL = float(os.getenv("SCAN_POLL_INTERVAL", 10.0))
# Auto-detect mode settings
AUTO_CREATED_WITHIN  = int(os.getenv("AUTO_CREATED_WITHIN", 5))   # minutes
AUTO_BLOCKED_PREFIXES = [p.strip() for p in os.getenv("AUTO_BLOCKED_PREFIXES", "1Pondo,Caribbeancom,FC2").split(",") if p.strip()]
AUTO_EXCLUDED_NAMES   = [n.strip() for n in os.getenv("AUTO_EXCLUDED_NAMES", "").split(",") if n.strip()]

if not GALLERY_PATH:
    raise ValueError("[!] GALLERY_PATH is not set in .env. Please add it before running.")

stash = StashInterface(STASH_CONFIG)

stats_lock     = threading.Lock()
failed_lock    = threading.Lock()
phase1_metrics = {"processed": 0, "success": 0, "total": 0, "start_time": 0}
phase2_metrics = {"processed": 0, "success": 0, "total": 0, "start_time": 0}
failed_ids     = []

# Thread-local browser storage — one browser instance reused per worker thread
thread_local = threading.local()

RE_DVD = re.compile(r'^([a-zA-Z]+)(\d+)')

_raw_exts = os.getenv("VIDEO_EXTENSIONS", ".mp4,.mkv,.avi,.wmv,.mov,.flv,.mpg,.mpeg,.ts,.m4v")
_ext_list = [e.strip().lstrip(".") for e in _raw_exts.split(",") if e.strip()]
RE_EXTENSIONS = re.compile(r"\.("+"|".join(re.escape(e) for e in _ext_list)+r")$", re.IGNORECASE)

ID_MAP = {}
if MAPPING_FILE_PATH and os.path.exists(MAPPING_FILE_PATH):
    try:
        with open(MAPPING_FILE_PATH, "r") as f:
            ID_MAP = json.load(f)
    except Exception as e:
        print(f"[!] Failed to load mapping file: {e}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def update_status_bar(metrics, phase):
    if not SHOW_METRICS:
        return
    with stats_lock:
        elapsed = time.perf_counter() - metrics["start_time"]
        avg = elapsed / metrics["processed"] if metrics["processed"] > 0 else 0
        sys.stdout.write(
            f"\r[Phase {phase}] {metrics['processed']}/{metrics['total']} | "
            f"Success: {metrics['success']} | Avg: {avg:.2f}s/ID"
        )
        sys.stdout.flush()


def parse_content_id_to_dvd(content_id):
    cid = RE_EXTENSIONS.sub('', content_id.strip())
    cid_lower = cid.lower()
    if ID_MAP:
        for key in sorted(ID_MAP.keys(), key=len, reverse=True):
            if cid_lower.startswith(key.lower()):
                return f"{ID_MAP[key]}{cid[len(key):]}"
    match = RE_DVD.match(re.sub(r'[vV]$', '', cid))
    if match:
        prefix, num = match.groups()
        return f"{prefix.upper()}-{num[2:] if num.startswith('000') else num.lstrip('0') or '0'}"
    return cid.upper()


# ---------------------------------------------------------------------------
# Reverse mapping — DVD ID back to content ID for --auto mode
# ---------------------------------------------------------------------------

def build_reverse_map():
    """
    Build a reverse lookup from DVD prefix (e.g. 'WANZ-') to a list of
    (content_id_prefix, padding) tuples, sorted by key length descending
    (most specific / longest key first).

    Handles cases where multiple content ID prefixes map to the same DVD prefix,
    e.g. both 'wanz00' and '3wanz00' map to 'WANZ-'.
    """
    reverse = {}
    for key, dvd_prefix in ID_MAP.items():
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

_REVERSE_MAP = build_reverse_map() if ID_MAP else {}


def dvd_to_content_id_candidates(dvd_id):
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

    if dvd_prefix not in _REVERSE_MAP:
        return [dvd_id.lower()]

    candidates = []
    for content_prefix, padding in _REVERSE_MAP[dvd_prefix]:
        padded_number = number.zfill(padding) if padding > 0 else number
        candidates.append(f"{content_prefix}{padded_number}")
    return candidates


# ---------------------------------------------------------------------------
# Auto-detect — scenes with no code, title, or studio, created recently
# ---------------------------------------------------------------------------

def get_unprocessed_scene_ids():
    """
    Query Stash for scenes created within AUTO_CREATED_WITHIN minutes that have
    no code, title, or studio set. Extracts content ID candidates from filenames.
    Skips scenes whose filenames match AUTO_BLOCKED_PREFIXES or cannot be parsed.
    Returns a deduplicated list of (filename, [candidate_content_ids]) tuples.
    """
    from datetime import datetime, timezone, timedelta

    query = """
    query UnprocessedScenes($filter: SceneFilterType!) {
      findScenes(
        scene_filter: $filter
        filter: { per_page: -1 }
      ) {
        scenes {
          id
          files {
            path
          }
        }
      }
    }
    """
    scene_filter = {
        "code":    {"value": "", "modifier": "IS_NULL"},
        "title":   {"value": "", "modifier": "IS_NULL"},
        "studios": {"modifier": "IS_NULL"},
    }
    if AUTO_CREATED_WITHIN > 0:
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=AUTO_CREATED_WITHIN)).strftime("%Y-%m-%dT%H:%M:%SZ")
        scene_filter["created_at"] = {"value": cutoff, "modifier": "GREATER_THAN"}

    variables = {"filter": scene_filter}

    try:
        result = stash.call_GQL(query, variables)
        scenes = result.get("findScenes", {}).get("scenes", [])
    except Exception as e:
        print(f"[!] Failed to query unprocessed scenes: {type(e).__name__}: {e}")
        return []

    entries = []
    seen    = set()
    skipped = 0
    for scene in scenes:
        files = scene.get("files", [])
        if not files:
            skipped += 1
            continue

        filename = Path(files[0]["path"]).stem

        # Skip blocked prefixes
        if any(filename.lower().startswith(p.lower()) for p in AUTO_BLOCKED_PREFIXES):
            print(f"[~] Skipping blocked prefix: {filename}")
            skipped += 1
            continue

        # Skip excluded names (exact match)
        if any(filename.lower() == n.lower() for n in AUTO_EXCLUDED_NAMES):
            print(f"[~] Skipping excluded name: {filename}")
            skipped += 1
            continue

        # A valid content ID must have at least one letter and one digit
        if not re.search(r'[a-zA-Z]', filename) or not re.search(r'\d', filename):
            print(f"[~] Skipping unrecognised filename: {filename}")
            skipped += 1
            continue

        candidates = dvd_to_content_id_candidates(filename)
        if candidates != [filename.lower()]:
            print(f"[~] Mapped {filename} -> {candidates}")
        entries.append((filename, candidates))

    print(f"[*] Found {len(entries)} unprocessed scene(s) in Stash ({skipped} skipped).")
    return entries


# ---------------------------------------------------------------------------
# Browser — one instance per worker thread, reused across all IDs it handles
# ---------------------------------------------------------------------------

def get_browser(proxy):
    """Return a thread-local Camoufox browser, creating it on first access."""
    if not hasattr(thread_local, 'browser') or thread_local.browser is None:
        thread_local.browser = Camoufox(headless=True, proxy=proxy, geoip=True).__enter__()
    return thread_local.browser


def close_thread_browser():
    """Cleanly exit the thread-local browser if it exists."""
    browser = getattr(thread_local, 'browser', None)
    if browser is not None:
        try:
            browser.__exit__(None, None, None)
        except Exception:
            pass
        thread_local.browser = None


# ---------------------------------------------------------------------------
# Stash scan helpers
# ---------------------------------------------------------------------------

def trigger_library_scan():
    """
    Trigger a full MetadataScan across all configured Stash libraries (no paths filter).
    Scan task options mirror the toggles configured in Stash settings.
    Returns the job ID string, or None on failure.
    """
    mutation = """
    mutation MetadataScan($input: ScanMetadataInput!) {
      metadataScan(input: $input)
    }
    """
    try:
        result = stash.call_GQL(mutation, {
            "input": {
                "scanGenerateCovers":      True,
                "scanGeneratePreviews":    True,
                "scanGenerateSprites":     True,
                "scanGeneratePhashes":     True,
                "scanGenerateThumbnails":  False,
                "scanGenerateClipPreviews": False,
            }
        })
        job_id = result.get("metadataScan")
        if job_id:
            print(f"\n[*] Library scan triggered (job {job_id})")
        else:
            print(f"\n[!] Scan mutation returned no job ID — response: {result}")
        return job_id
    except Exception as e:
        print(f"\n[!] Failed to trigger library scan: {type(e).__name__}: {e}")
        return None


def wait_for_job(job_id):
    """
    Poll Stash job queue until the given job_id reaches a terminal state.
    Terminal states: FINISHED, CANCELLED, FAILED.
    If SCAN_TIMEOUT=0, waits indefinitely.
    Prints elapsed time periodically so the console doesn't appear frozen.
    Returns the final status string, or None on timeout/error.
    """
    query = """
    query FindJob($input: FindJobInput!) {
      findJob(input: $input) {
        status
      }
    }
    """
    terminal   = {"FINISHED", "CANCELLED", "FAILED"}
    start      = time.perf_counter()
    deadline   = (start + SCAN_TIMEOUT) if SCAN_TIMEOUT > 0 else None
    last_print = start

    while True:
        now = time.perf_counter()
        if deadline and now >= deadline:
            print(f"\n[!] Scan job {job_id} timed out after {SCAN_TIMEOUT}s — proceeding anyway.")
            return None

        try:
            result = stash.call_GQL(query, {"input": {"id": job_id}})
            status = result.get("findJob", {}).get("status")
            if status in terminal:
                elapsed = now - start
                print(f"\n[*] Scan job {job_id} {status} in {elapsed:.0f}s")
                return status
        except Exception as e:
            print(f"\n[!] Job poll error (job {job_id}): {type(e).__name__}: {e}")

        # Print a progress heartbeat every 30s so the console doesn't look frozen
        if now - last_print >= 30:
            elapsed = now - start
            mins, secs = divmod(int(elapsed), 60)
            print(f"\n[*] Scan in progress... (elapsed: {mins:02d}:{secs:02d})")
            last_print = now

        time.sleep(SCAN_POLL_INTERVAL)


def run_library_scan():
    """
    Trigger a full library scan and block until it completes.
    Logs outcome but never raises — a failed scan should not abort processing.
    """
    if DRY_RUN:
        print("[DRY] Would trigger full library scan.")
        return

    job_id = trigger_library_scan()
    if not job_id:
        return

    final_status = wait_for_job(job_id)
    if final_status and final_status != "FINISHED":
        print(f"[!] Library scan ended with status '{final_status}'")


# ---------------------------------------------------------------------------
# Stash metadata helpers
# ---------------------------------------------------------------------------

def get_metadata_ids(meta):
    """Find or create Stash IDs for Studio, Performers, Tags, and Group (alias-aware)."""

    # Studio
    studio_id = None
    s_name = meta.get('studio_name')
    if s_name:
        studios = stash.find_studios(f={"name": {"value": s_name, "modifier": "EQUALS"}})
        if studios:
            studio_id = studios[0]['id']
        else:
            studios_alias = stash.find_studios(f={"aliases": {"value": s_name, "modifier": "EQUALS"}})
            studio_id = studios_alias[0]['id'] if studios_alias else stash.create_studio({"name": s_name})['id']

    # Performers
    p_ids = []
    for name in meta.get('performer_names', []):
        if not name:
            continue
        found_p_id = None
        ps = stash.find_performers(f={"name": {"value": name, "modifier": "EQUALS"}}, filter={"per_page": 1})
        if ps:
            found_p_id = ps[0]['id']

        if not found_p_id:
            ps_alias = stash.find_performers(f={"aliases": {"value": name, "modifier": "EQUALS"}}, filter={"per_page": 1})
            if ps_alias:
                found_p_id = ps_alias[0]['id']

        if not found_p_id:
            for p in (stash.find_performers(f={"name": {"value": name, "modifier": "INCLUDES"}}) or []):
                if re.match(rf"^{re.escape(name)}\s\(.*\)$", p['name']):
                    found_p_id = p['id']
                    break

        p_ids.append(found_p_id if found_p_id else stash.create_performer({"name": name})['id'])

    # Tags
    t_ids = []
    for tn in meta.get('category_names', []):
        ts = stash.find_tags(f={"name": {"value": tn, "modifier": "EQUALS"}})
        t_ids.append(ts[0]['id'] if ts else stash.create_tag({"name": tn})['id'])

    # Group (Series) — find or create
    group_id = None
    series_name = meta.get('series_name')
    if series_name:
        groups = stash.find_groups(f={"name": {"value": series_name, "modifier": "EQUALS"}})
        if groups:
            group_id = groups[0]['id']
        else:
            group_id = stash.create_group({"name": series_name})['id']

    return studio_id, p_ids, t_ids, group_id


# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------

def scrape_r18(browser, identifier):
    clean_tid = RE_EXTENSIONS.sub('', identifier).strip()
    api_url = f"https://r18.dev/videos/vod/movies/detail/-/combined={clean_tid}/json"

    for attempt in range(1, SCRAPE_RETRIES + 1):
        page = browser.new_page()
        try:
            res = page.goto(api_url, wait_until="domcontentloaded", timeout=40000)
            if res.status != 200:
                page.close()
                backoff = RETRY_DELAY * (2 ** (attempt - 1))
                print(f"\n[!] HTTP {res.status} on attempt {attempt} for {clean_tid} — retrying in {backoff}s")
                time.sleep(backoff)
                continue

            data = json.loads(page.evaluate("() => document.body.innerText"))
            page.close()

            api_dvd = data.get("dvd_id")
            final_dvd_id = str(api_dvd).upper() if api_dvd else parse_content_id_to_dvd(clean_tid)

            time.sleep(RATE_LIMIT_DELAY)

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
            backoff = RETRY_DELAY * (2 ** (attempt - 1))
            print(f"\n[!] Attempt {attempt}/{SCRAPE_RETRIES} failed for {clean_tid}: {type(e).__name__}: {e} — retrying in {backoff}s")
            if attempt < SCRAPE_RETRIES:
                time.sleep(backoff)

    return None


# ---------------------------------------------------------------------------
# Phase 1 — scrape + download
# ---------------------------------------------------------------------------

def phase1_scrape_and_download(tid, proxy, results, candidates=None):
    """
    Scrape r18.dev and download gallery images for `tid`.
    If `candidates` is provided (list of content IDs to try in order),
    each is attempted until one succeeds. Used by --auto mode for ambiguous
    reverse mappings (e.g. multiple content prefixes mapping to the same DVD ID).
    Stores result metadata into the shared `results` dict keyed by tid.
    """
    result_msg = f"[!] Unknown error: {tid}"

    try:
        clean_tid = RE_EXTENSIONS.sub('', tid).strip()
        browser   = get_browser(proxy)
        meta      = None
        tried     = candidates if candidates else [clean_tid]

        for candidate in tried:
            meta = scrape_r18(browser, candidate)
            if meta:
                clean_tid = candidate  # use the successful candidate going forward
                break

        if not meta:
            result_msg = f"[!] Failed scrape: {tid}"
        else:
            dvd_id = meta['display_id']

            if not DRY_RUN:
                r18_imagedownload.download_gallery_images(clean_tid, dvd_id, GALLERY_PATH)

            results[tid] = meta
            result_msg = f"[+] Scraped & downloaded: {dvd_id}"

    except Exception as e:
        result_msg = f"[!] Phase 1 error for {tid}: {type(e).__name__}: {e}"

    finally:
        success = "[+]" in result_msg

        with stats_lock:
            phase1_metrics["processed"] += 1
            if success:
                phase1_metrics["success"] += 1

        if not success:
            with failed_lock:
                failed_ids.append(RE_EXTENSIONS.sub('', tid).strip())

        sys.stdout.write(f"\r\033[K{result_msg}\n")
        update_status_bar(phase1_metrics, 1)


# ---------------------------------------------------------------------------
# Phase 2 — update scenes and galleries
# ---------------------------------------------------------------------------

def phase2_update(tid, meta, cached_galleries):
    """
    Update the matching Stash scene (and gallery) using scraped metadata.
    """
    result_msg = f"[!] Unknown error: {tid}"

    try:
        clean_tid = RE_EXTENSIONS.sub('', tid).strip()
        dvd_id    = meta['display_id']

        scenes = stash.find_scenes(f={"code": {"value": clean_tid, "modifier": "EQUALS"}})
        if not scenes:
            scenes = stash.find_scenes(f={"code": {"value": dvd_id, "modifier": "EQUALS"}})
        if not scenes:
            scenes = stash.find_scenes(f={"path": {"value": clean_tid, "modifier": "INCLUDES"}})
        if not scenes:
            scenes = stash.find_scenes(f={"path": {"value": dvd_id, "modifier": "INCLUDES"}})

        if not scenes:
            result_msg = f"[-] Not in Stash: {dvd_id}"
        else:
            scene = scenes[0]
            studio_id, p_ids, t_ids, group_id = get_metadata_ids(meta)
            gal_id = (
                r18_gallery.find_gallery_match(cached_galleries, dvd_id) or
                r18_gallery.find_gallery_match(cached_galleries, clean_tid)
            )

            existing_tag_ids = [t['id'] for t in scene.get('tags', [])]
            merged_tag_ids   = list(set(existing_tag_ids) | set(t_ids))

            scene_payload = {
                "id":            scene['id'],
                "title":         dvd_id,
                "code":          dvd_id,
                "date":          meta['date'],
                "url":           meta['web_url'],
                "performer_ids": p_ids,
                "tag_ids":       merged_tag_ids,
                "details":       meta['title_en'],
                "cover_image":   meta['cover'],
            }
            if studio_id:            scene_payload["studio_id"]   = studio_id
            if meta.get("director"): scene_payload["director"]    = meta["director"]
            if group_id:             scene_payload["groups"]      = [{"group_id": group_id}]
            if gal_id:               scene_payload["gallery_ids"] = [gal_id]

            if DRY_RUN:
                result_msg = f"[DRY] Would update: {dvd_id}"
            else:
                stash.update_scene(scene_payload)
                gal_status = ""
                if gal_id:
                    gallery_payload = {
                        "id":            gal_id,
                        "title":         dvd_id,
                        "date":          meta['date'],
                        "details":       meta['title_en'],
                        "url":           meta['web_url'],
                        "performer_ids": p_ids,
                        "tag_ids":       t_ids,
                    }
                    if studio_id: gallery_payload["studio_id"] = studio_id
                    stash.update_gallery(gallery_payload)
                    gal_status = " (+Gallery Synced)"
                result_msg = f"[+++] Updated: {dvd_id}{gal_status}"

    except Exception as e:
        result_msg = f"[!] Phase 2 error for {tid}: {type(e).__name__}: {e}"

    finally:
        success = "[+++]" in result_msg
        failed  = not success and "[-]" not in result_msg

        with stats_lock:
            phase2_metrics["processed"] += 1
            if success:
                phase2_metrics["success"] += 1

        if failed:
            with failed_lock:
                failed_ids.append(RE_EXTENSIONS.sub('', tid).strip())

        sys.stdout.write(f"\r\033[K{result_msg}\n")
        update_status_bar(phase2_metrics, 2)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Scrape r18.dev metadata and update Stash.")
    parser.add_argument(
        "--auto",
        action="store_true",
        help="Detect unprocessed scenes from Stash (no code set) instead of using input file."
    )
    args = parser.parse_args()

    if DRY_RUN:
        print("[*] DRY RUN mode — no Stash updates will be made.")

    # ------------------------------------------------------------------
    # Resolve ID list from input file or auto-detect
    # ------------------------------------------------------------------
    if args.auto:
        # Scan first so newly added files are indexed before we query
        print("[*] Auto mode: triggering library scan to index new files...")
        run_library_scan()

        print("[*] Querying Stash for unprocessed scenes...")
        entries = get_unprocessed_scene_ids()
        if not entries:
            print("[!] No unprocessed scenes found — nothing to do.")
            return
        # entries is a list of (filename, [candidates]) tuples
        ids            = [e[0] for e in entries]
        candidates_map = {e[0]: e[1] for e in entries}
    else:
        if not INPUT_FILE or not os.path.exists(INPUT_FILE):
            return print(f"[!] INPUT_FILE '{INPUT_FILE}' not found.")
        with open(INPUT_FILE, "r") as f:
            ids = list(dict.fromkeys(line.strip() for line in f if line.strip()))
        candidates_map = {}

    proxy = PROXY_CONFIG if USE_PROXY else None

    # ------------------------------------------------------------------
    # Phase 1 — scrape r18.dev + download gallery images for all IDs
    # ------------------------------------------------------------------
    print(f"[*] Phase 1: scraping and downloading {len(ids)} ID(s)...")
    phase1_results = {}  # tid -> meta dict for successful scrapes
    phase1_metrics["total"]      = len(ids)
    phase1_metrics["start_time"] = time.perf_counter()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for tid in ids:
            executor.submit(
                phase1_scrape_and_download,
                tid, proxy, phase1_results,
                candidates_map.get(tid)  # None for default mode, list for --auto
            )
        for _ in range(MAX_WORKERS):
            executor.submit(close_thread_browser)

    print(f"\n[*] Phase 1 complete. {phase1_metrics['success']}/{phase1_metrics['total']} scraped successfully.")

    if not phase1_results:
        print("[!] No successful scrapes — skipping scan and update.")
        return

    # ------------------------------------------------------------------
    # Scan — single full library scan across all Stash libraries
    # ------------------------------------------------------------------
    print("[*] Triggering full library scan...")
    run_library_scan()

    # ------------------------------------------------------------------
    # Re-cache galleries after scan so newly indexed folders are visible
    # ------------------------------------------------------------------
    print("[*] Re-caching galleries post-scan...")
    try:
        cached_galleries = r18_gallery.get_all_galleries(stash)
        print(f"[*] {len(cached_galleries)} galleries cached.")
    except Exception as e:
        print(f"[!] Gallery cache failed: {e}")
        cached_galleries = []

    # ------------------------------------------------------------------
    # Phase 2 — update scenes and galleries for successfully scraped IDs
    # ------------------------------------------------------------------
    successful_ids = list(phase1_results.keys())
    print(f"[*] Phase 2: updating {len(successful_ids)} scene(s) in Stash...")
    phase2_metrics["total"]      = len(successful_ids)
    phase2_metrics["start_time"] = time.perf_counter()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for tid in successful_ids:
            executor.submit(phase2_update, tid, phase1_results[tid], cached_galleries)

    print(f"\n[*] Phase 2 complete. {phase2_metrics['success']}/{phase2_metrics['total']} updated successfully.")

    # ------------------------------------------------------------------
    # Write failed IDs (Phase 1 scrape failures + Phase 2 update errors)
    # ------------------------------------------------------------------
    if failed_ids:
        failed_path = "failed_ids.txt"
        with open(failed_path, "w") as f:
            f.writelines(f"{i}\n" for i in sorted(set(failed_ids)))
        print(f"[!] {len(set(failed_ids))} failed ID(s) written to {failed_path} — re-run with INPUT_FILE={failed_path}")


if __name__ == "__main__":
    main()