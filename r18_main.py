import os
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

from lib import r18_gallery
from lib import r18_image_scrape as r18_imagedownload
from lib import r18_scraper
from lib import r18_stash
from lib import r18_auto

load_dotenv()

STASH_CONFIG = {
    "scheme": os.getenv("STASH_SCHEME", "http"),
    "host":   os.getenv("STASH_HOST"),
    "port":   os.getenv("STASH_PORT"),
    "apikey": os.getenv("STASH_APIKEY")
}

USE_PROXY             = os.getenv("USE_PROXY", "false").lower() == "true"
PROXY_CONFIG          = {"server": os.getenv("PROXY_SERVER"), "username": os.getenv("PROXY_USER"), "password": os.getenv("PROXY_PASS")}
SCRAPE_RETRIES        = int(os.getenv("SCRAPE_RETRIES", 5))
RETRY_DELAY           = int(os.getenv("RETRY_DELAY", 5))
RATE_LIMIT_DELAY      = float(os.getenv("RATE_LIMIT_DELAY", 1.5))
MAX_WORKERS           = int(os.getenv("MAX_WORKERS", 3))
INPUT_FILE            = os.getenv("INPUT_FILE")
SHOW_METRICS          = os.getenv("SHOW_METRICS", "true").lower() == "true"
MAPPING_FILE_PATH     = os.getenv("MAPPING_FILE")
DRY_RUN               = os.getenv("DRY_RUN", "false").lower() == "true"
GALLERY_PATH          = os.getenv("GALLERY_PATH")
SCAN_TIMEOUT          = int(os.getenv("SCAN_TIMEOUT", 0))
SCAN_POLL_INTERVAL    = float(os.getenv("SCAN_POLL_INTERVAL", 10.0))
AUTO_CREATED_WITHIN   = int(os.getenv("AUTO_CREATED_WITHIN", 5))
AUTO_BLOCKED_PREFIXES = [p.strip() for p in os.getenv("AUTO_BLOCKED_PREFIXES", "1Pondo,Caribbeancom,FC2").split(",") if p.strip()]
AUTO_EXCLUDED_NAMES   = [n.strip() for n in os.getenv("AUTO_EXCLUDED_NAMES", "").split(",") if n.strip()]

if not GALLERY_PATH:
    raise ValueError("[!] GALLERY_PATH is not set in .env. Please add it before running.")

stash = StashInterface(STASH_CONFIG)

stats_lock     = threading.Lock()
failed_lock    = threading.Lock()
not_found_lock = threading.Lock()
phase1_metrics = {"processed": 0, "success": 0, "total": 0, "start_time": 0}
phase2_metrics = {"processed": 0, "success": 0, "total": 0, "start_time": 0}
failed_ids     = []
not_found_ids  = []

# Thread-local browser storage - one browser instance reused per worker thread
thread_local = threading.local()

_raw_exts = os.getenv("VIDEO_EXTENSIONS", ".mp4,.mkv,.avi,.wmv,.mov,.flv,.mpg,.mpeg,.ts,.m4v")
_ext_list  = [e.strip().lstrip(".") for e in _raw_exts.split(",") if e.strip()]
RE_EXTENSIONS = re.compile(r"\.(" + "|".join(re.escape(e) for e in _ext_list) + r")$", re.IGNORECASE)

ID_MAP = {}
if MAPPING_FILE_PATH and os.path.exists(MAPPING_FILE_PATH):
    try:
        with open(MAPPING_FILE_PATH, "r") as f:
            ID_MAP = json.load(f)
    except Exception as e:
        print(f"[!] Failed to load mapping file: {e}")

# Pre-compute sorted keys and reverse map once at startup
_SORTED_KEYS = r18_scraper.build_sorted_keys(ID_MAP)
_REVERSE_MAP = r18_scraper.build_reverse_map(ID_MAP)


def _parse_fn(content_id):
    """Wrapper binding module globals for parse_content_id_to_dvd."""
    return r18_scraper.parse_content_id_to_dvd(content_id, ID_MAP, _SORTED_KEYS, RE_EXTENSIONS)


def _candidates_fn(dvd_id):
    """Wrapper binding module globals for dvd_to_content_id_candidates."""
    return r18_scraper.dvd_to_content_id_candidates(dvd_id, _REVERSE_MAP)


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


# ---------------------------------------------------------------------------
# Browser - one instance per worker thread, reused across all IDs it handles
# ---------------------------------------------------------------------------

def get_browser(proxy):
    """Return the thread-local Camoufox browser, creating it on first access."""
    if not hasattr(thread_local, "browser") or thread_local.browser is None:
        thread_local.browser = Camoufox(headless=True, proxy=proxy, geoip=True).__enter__()
    return thread_local.browser


def close_thread_browser():
    """Cleanly exit the thread-local browser if it exists."""
    browser = getattr(thread_local, "browser", None)
    if browser is not None:
        try:
            browser.__exit__(None, None, None)
        except Exception:
            pass
        thread_local.browser = None


# ---------------------------------------------------------------------------
# Phase 1 - scrape + download
# ---------------------------------------------------------------------------

def phase1_scrape_and_download(tid, proxy, results, candidates=None):
    """
    Scrape r18.dev and download gallery images for `tid`.

    If `candidates` is provided (list of content IDs to try in order), each is
    attempted until one succeeds. 404s fast-fail to the next candidate.
    Stores result metadata into the shared `results` dict keyed by tid.
    """
    result_msg = f"[!] Unknown error: {tid}"

    try:
        clean_tid     = RE_EXTENSIONS.sub("", tid).strip()
        browser       = get_browser(proxy)
        meta          = None
        tried         = candidates if candidates else [clean_tid]
        all_not_found = True

        for candidate in tried:
            result = r18_scraper.scrape_r18(
                browser, candidate, RE_EXTENSIONS, _parse_fn,
                SCRAPE_RETRIES, RETRY_DELAY, RATE_LIMIT_DELAY
            )
            if result is False:
                # 404 - title doesn't exist on r18.dev, try next candidate
                continue
            if result is None:
                # Exhausted retries on a non-404 error
                all_not_found = False
                continue
            # Success
            meta          = result
            clean_tid     = candidate
            all_not_found = False
            break

        if not meta:
            if all_not_found:
                result_msg = f"[-] Not found on r18.dev: {tid}"
                with not_found_lock:
                    not_found_ids.append(RE_EXTENSIONS.sub("", tid).strip())
            else:
                result_msg = f"[!] Failed scrape: {tid}"
        else:
            dvd_id = meta["display_id"]
            if not DRY_RUN:
                r18_imagedownload.download_gallery_images(clean_tid, dvd_id, GALLERY_PATH)
            results[tid] = meta
            result_msg   = f"[+] Scraped & downloaded: {dvd_id}"

    except Exception as e:
        result_msg = f"[!] Phase 1 error for {tid}: {type(e).__name__}: {e}"

    finally:
        success   = "[+]" in result_msg
        hard_fail = "[!]" in result_msg

        with stats_lock:
            phase1_metrics["processed"] += 1
            if success:
                phase1_metrics["success"] += 1

        if hard_fail:
            with failed_lock:
                failed_ids.append(RE_EXTENSIONS.sub("", tid).strip())

        sys.stdout.write(f"\r\033[K{result_msg}\n")
        update_status_bar(phase1_metrics, 1)


# ---------------------------------------------------------------------------
# Phase 2 - update scenes and galleries
# ---------------------------------------------------------------------------

def phase2_update(tid, meta, cached_galleries):
    """Update the matching Stash scene (and gallery) using scraped metadata."""
    result_msg = f"[!] Unknown error: {tid}"

    try:
        clean_tid = RE_EXTENSIONS.sub("", tid).strip()
        dvd_id    = meta["display_id"]

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
            studio_id, p_ids, t_ids, group_id = r18_stash.get_metadata_ids(stash, meta)
            gal_id = (
                r18_gallery.find_gallery_match(cached_galleries, dvd_id) or
                r18_gallery.find_gallery_match(cached_galleries, clean_tid)
            )

            existing_tag_ids = [t["id"] for t in scene.get("tags", [])]
            merged_tag_ids   = list(set(existing_tag_ids) | set(t_ids))

            scene_payload = {
                "id":            scene["id"],
                "title":         dvd_id,
                "code":          dvd_id,
                "date":          meta["date"],
                "url":           meta["web_url"],
                "performer_ids": p_ids,
                "tag_ids":       merged_tag_ids,
                "details":       meta["title_en"],
                "cover_image":   meta["cover"],
            }
            if studio_id:             scene_payload["studio_id"]   = studio_id
            if meta.get("director"):  scene_payload["director"]    = meta["director"]
            if group_id:              scene_payload["groups"]      = [{"group_id": group_id}]
            if gal_id:                scene_payload["gallery_ids"] = [gal_id]

            if DRY_RUN:
                result_msg = f"[DRY] Would update: {dvd_id}"
            else:
                stash.update_scene(scene_payload)
                gal_status = ""
                if gal_id:
                    gallery_payload = {
                        "id":            gal_id,
                        "title":         dvd_id,
                        "date":          meta["date"],
                        "details":       meta["title_en"],
                        "url":           meta["web_url"],
                        "performer_ids": p_ids,
                        "tag_ids":       t_ids,
                    }
                    if studio_id:
                        gallery_payload["studio_id"] = studio_id
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
                failed_ids.append(RE_EXTENSIONS.sub("", tid).strip())

        sys.stdout.write(f"\r\033[K{result_msg}\n")
        update_status_bar(phase2_metrics, 2)


# ---------------------------------------------------------------------------
# Write output files
# ---------------------------------------------------------------------------

def _write_failed_files():
    """Write failed and not-found IDs to separate files for easy re-runs."""
    if failed_ids:
        path = "failed_ids.txt"
        with open(path, "w") as f:
            f.writelines(f"{i}\n" for i in sorted(set(failed_ids)))
        print(f"[!] {len(set(failed_ids))} failed ID(s) written to {path} — re-run with INPUT_FILE={path}")

    if not_found_ids:
        path = "not_found_ids.txt"
        with open(path, "w") as f:
            f.writelines(f"{i}\n" for i in sorted(set(not_found_ids)))
        print(f"[~] {len(set(not_found_ids))} not-found ID(s) written to {path} (not on r18.dev — re-running will not help)")


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
        print("[*] DRY RUN mode - no Stash updates will be made.")

    # ------------------------------------------------------------------
    # Resolve ID list from input file or auto-detect
    # ------------------------------------------------------------------
    if args.auto:
        print("[*] Auto mode: triggering library scan to index new files...")
        r18_stash.run_library_scan(stash, SCAN_TIMEOUT, SCAN_POLL_INTERVAL, DRY_RUN)

        print("[*] Querying Stash for unprocessed scenes...")
        entries = r18_auto.get_unprocessed_scene_ids(
            stash, AUTO_CREATED_WITHIN, AUTO_BLOCKED_PREFIXES,
            AUTO_EXCLUDED_NAMES, _candidates_fn
        )
        if not entries:
            print("[!] No unprocessed scenes found - nothing to do.")
            return
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
    # Phase 1 - scrape r18.dev + download gallery images for all IDs
    # ------------------------------------------------------------------
    print(f"[*] Phase 1: scraping and downloading {len(ids)} ID(s)...")
    phase1_results               = {}
    phase1_metrics["total"]      = len(ids)
    phase1_metrics["start_time"] = time.perf_counter()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for tid in ids:
            executor.submit(
                phase1_scrape_and_download,
                tid, proxy, phase1_results,
                candidates_map.get(tid)
            )
        for _ in range(MAX_WORKERS):
            executor.submit(close_thread_browser)

    print(f"\n[*] Phase 1 complete. {phase1_metrics['success']}/{phase1_metrics['total']} scraped successfully.")

    if not phase1_results:
        print("[!] No successful scrapes - skipping scan and update.")
        _write_failed_files()
        return

    # ------------------------------------------------------------------
    # Scan - full library scan to index downloaded gallery images
    # ------------------------------------------------------------------
    print("[*] Triggering full library scan...")
    r18_stash.run_library_scan(stash, SCAN_TIMEOUT, SCAN_POLL_INTERVAL, DRY_RUN)

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
    # Phase 2 - update scenes and galleries for successfully scraped IDs
    # ------------------------------------------------------------------
    successful_ids               = list(phase1_results.keys())
    print(f"[*] Phase 2: updating {len(successful_ids)} scene(s) in Stash...")
    phase2_metrics["total"]      = len(successful_ids)
    phase2_metrics["start_time"] = time.perf_counter()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for tid in successful_ids:
            executor.submit(phase2_update, tid, phase1_results[tid], cached_galleries)

    print(f"\n[*] Phase 2 complete. {phase2_metrics['success']}/{phase2_metrics['total']} updated successfully.")

    _write_failed_files()


if __name__ == "__main__":
    main()