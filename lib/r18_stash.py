import re
import time


# ---------------------------------------------------------------------------
# Library scan helpers
# ---------------------------------------------------------------------------

def trigger_library_scan(stash):
    """
    Trigger a full MetadataScan across all configured Stash libraries.
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
                "scanGenerateCovers":       True,
                "scanGeneratePreviews":     True,
                "scanGenerateSprites":      True,
                "scanGeneratePhashes":      True,
                "scanGenerateThumbnails":   False,
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


def wait_for_job(stash, job_id, scan_timeout, scan_poll_interval):
    """
    Poll Stash job queue until the given job_id reaches a terminal state.
    Terminal states: FINISHED, CANCELLED, FAILED.
    If scan_timeout=0, waits indefinitely.
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
    deadline   = (start + scan_timeout) if scan_timeout > 0 else None
    last_print = start

    while True:
        now = time.perf_counter()
        if deadline and now >= deadline:
            print(f"\n[!] Scan job {job_id} timed out after {scan_timeout}s — proceeding anyway.")
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

        # Print a heartbeat every 30s so the console doesn't look frozen
        if now - last_print >= 30:
            elapsed = now - start
            mins, secs = divmod(int(elapsed), 60)
            print(f"\n[*] Scan in progress... (elapsed: {mins:02d}:{secs:02d})")
            last_print = now

        time.sleep(scan_poll_interval)


def run_library_scan(stash, scan_timeout, scan_poll_interval, dry_run):
    """
    Trigger a full library scan and block until it completes.
    Logs outcome but never raises — a failed scan should not abort processing.
    """
    if dry_run:
        print("[DRY] Would trigger full library scan.")
        return

    job_id = trigger_library_scan(stash)
    if not job_id:
        return

    final_status = wait_for_job(stash, job_id, scan_timeout, scan_poll_interval)
    if final_status and final_status != "FINISHED":
        print(f"[!] Library scan ended with status '{final_status}'")


# ---------------------------------------------------------------------------
# Metadata helpers
# ---------------------------------------------------------------------------

def get_metadata_ids(stash, meta):
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
