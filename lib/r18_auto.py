import re
from pathlib import Path
from datetime import datetime, timezone, timedelta


def get_unprocessed_scene_ids(stash, auto_created_within, auto_blocked_prefixes,
                               auto_excluded_names, dvd_to_candidates_fn):
    """
    Query Stash for scenes that have no code, title, or studio set.
    Optionally filters by how recently the scene was created (auto_created_within minutes).
    Extracts content ID candidates from filenames using dvd_to_candidates_fn.
    Skips scenes whose filenames match blocked prefixes, excluded names, or are unrecognisable.
    Returns a deduplicated list of (filename, [candidate_content_ids]) tuples.
    """
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
    if auto_created_within > 0:
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=auto_created_within)).strftime("%Y-%m-%dT%H:%M:%SZ")
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
        if any(filename.lower().startswith(p.lower()) for p in auto_blocked_prefixes):
            print(f"[~] Skipping blocked prefix: {filename}")
            skipped += 1
            continue

        # Skip excluded names (exact match)
        if any(filename.lower() == n.lower() for n in auto_excluded_names):
            print(f"[~] Skipping excluded name: {filename}")
            skipped += 1
            continue

        # A valid content ID must have at least one letter and one digit
        if not re.search(r'[a-zA-Z]', filename) or not re.search(r'\d', filename):
            print(f"[~] Skipping unrecognised filename: {filename}")
            skipped += 1
            continue

        if filename.lower() in seen:
            continue
        seen.add(filename.lower())

        candidates = dvd_to_candidates_fn(filename)
        if candidates != [filename.lower()]:
            print(f"[~] Mapped {filename} -> {candidates}")
        entries.append((filename, candidates))

    print(f"[*] Found {len(entries)} unprocessed scene(s) in Stash ({skipped} skipped).")
    return entries
