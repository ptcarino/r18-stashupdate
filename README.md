# r18-stash-metadata

> ⚠️ **Disclaimer: This project was vibe coded with the assistance of Claude (Anthropic).** It works for my setup but comes with no guarantees. Use at your own risk, and expect rough edges.

A Python automation tool that scrapes JAV metadata from [r18.dev](https://r18.dev) and updates your [Stash](https://stashapp.cc/) library — including scenes, galleries, performers, studios, tags, and series groups.

---

## Features

- Scrapes metadata from the r18.dev JSON API (title, date, studio, performers, categories, series, director, cover)
- Downloads cover and sample images from DMM and saves them to a local gallery folder
- Triggers a full Stash library scan after downloads complete, then waits for it to finish before updating
- Matches scenes in Stash by code, DVD ID, or filename path
- Matches galleries by code, title, or folder name (fuzzy)
- Finds or creates studios, performers, tags, and groups in Stash automatically
- Merges new tags with existing scene tags (non-destructive)
- Two-phase pipeline: all scraping/downloading happens first, then a single library scan, then all Stash updates
- Auto-detect mode (`--auto`) that scans the library, detects newly added unprocessed scenes, and processes them automatically
- Reverse mapping from DVD IDs back to DMM content IDs for accurate API lookups in auto mode
- Multi-threaded with a configurable worker count
- Proxy support via Camoufox
- Dry run mode for safe testing
- Retries with exponential backoff on scrape failures
- Writes failed IDs to `failed_ids.txt` for easy re-runs

---

## Requirements

- Python 3.10+
- A running [Stash](https://stashapp.cc/) instance (Docker or native)
- The following Python packages:

```
camoufox
stashapp-tools
python-dotenv
requests
```

Install with:

```bash
pip install camoufox stashapp-tools python-dotenv requests
```

---

## File Structure

```
r18_main.py          # Main script — orchestrates the full pipeline
r18_gallery.py       # Gallery querying and matching logic
r18_image_scrape.py  # Image downloader (cover + sample images from DMM)
mapping.json         # Content ID prefix → DVD ID prefix mappings
.env                 # Your local config (not committed)
```

---

## Configuration

Create a `.env` file in the same directory as the scripts:

```env
# Stash connection
STASH_HOST=localhost
STASH_PORT=9999
STASH_APIKEY=your_api_key_here
STASH_SCHEME=http

# Proxy settings (optional)
USE_PROXY=false
PROXY_SERVER=
PROXY_USER=
PROXY_PASS=

# Input file — one content ID per line (used in default mode)
INPUT_FILE=codeupdate.txt

# Mapping file
MAPPING_FILE=mapping.json

# Local path where gallery images will be saved (host path), e.g. GALLERY_PATH=C:\Gallery
GALLERY_PATH=

# Video file extensions to strip when normalising IDs
VIDEO_EXTENSIONS=.mp4,.mkv,.avi,.wmv,.m4v

# Concurrency
MAX_WORKERS=3

# Scrape retry settings
SCRAPE_RETRIES=5
RETRY_DELAY=3

# Rate limiting between requests
RATE_LIMIT_DELAY=1.5

# Library scan settings
# SCAN_TIMEOUT=0 waits indefinitely (recommended for large libraries)
SCAN_TIMEOUT=0
SCAN_POLL_INTERVAL=10.0

# Auto-detect mode settings (--auto flag)
# AUTO_CREATED_WITHIN=0 means no time limit — all unprocessed scenes will be detected
AUTO_CREATED_WITHIN=60
AUTO_BLOCKED_PREFIXES=1Pondo,Caribbeancom,FC2
AUTO_EXCLUDED_NAMES=

# Set to true to preview without writing anything to Stash
DRY_RUN=false

# Show progress bar
SHOW_METRICS=true
```

> **Note for Docker users:** `GALLERY_PATH` should be the path on your **host machine** where images are saved. The Stash scan runs against all configured Stash libraries automatically, so no separate container path setting is needed.

---

## Usage

### Default mode — input file

1. Add content IDs (one per line) to your input file (e.g. `codeupdate.txt`):

```
ssis00100
1stars00678
midv00234
```

2. Run the script:

```bash
python r18_main.py
```

### Pipeline order (default mode)

```
Phase 1 (threaded)
  └─ For each ID: scrape r18.dev → download gallery images

Library scan
  └─ Trigger full Stash metadata scan → wait for completion

Gallery re-cache
  └─ Re-query all galleries from Stash post-scan

Phase 2 (threaded)
  └─ For each successful ID: find scene → update scene + gallery metadata
```

---

### Auto mode — detect unprocessed scenes

Run with the `--auto` flag to have the script detect newly added scenes automatically instead of using an input file:

```bash
python r18_main.py --auto
```

Auto mode detects scenes in Stash that have no code, title, or studio set. It filters by how recently the scene was added (`AUTO_CREATED_WITHIN`) and skips filenames that match blocked prefixes or excluded names.

### Pipeline order (auto mode)

```
Library scan
  └─ Trigger full Stash metadata scan → wait for completion
     (indexes any newly added files before querying)

Query unprocessed scenes
  └─ Find scenes with no code, title, or studio
  └─ Convert DVD IDs back to DMM content IDs via reverse mapping
  └─ Skip blocked prefixes and excluded names

Phase 1 (threaded)
  └─ For each ID: scrape r18.dev → download gallery images

Library scan
  └─ Trigger full Stash metadata scan → wait for completion
     (indexes newly downloaded gallery images)

Gallery re-cache
  └─ Re-query all galleries from Stash post-scan

Phase 2 (threaded)
  └─ For each successful ID: find scene → update scene + gallery metadata
```

### Auto mode filtering

| Setting | Description |
|---|---|
| `AUTO_CREATED_WITHIN` | Only detect scenes added within this many minutes. Set to `0` for no time limit. |
| `AUTO_BLOCKED_PREFIXES` | Skip filenames that start with any of these prefixes (comma-separated). |
| `AUTO_EXCLUDED_NAMES` | Skip filenames that exactly match any of these names (comma-separated). |

---

### Re-running failed IDs

If any IDs fail, they are written to `failed_ids.txt`. Re-run with:

```bash
# In .env, set:
INPUT_FILE=failed_ids.txt
```

---

## ID Mapping

`mapping.json` maps DMM content ID prefixes to their DVD ID equivalents, e.g.:

```json
{
  "ssis00": "SSIS-",
  "1stars00": "STARS-"
}
```

This handles the inconsistency between DMM's internal content IDs and the standard DVD IDs used in Stash. Longer keys take priority over shorter ones to avoid false prefix matches.

In `--auto` mode, the mapping is also used in reverse — DVD IDs found in scene filenames (e.g. `SSIS-100`) are converted back to DMM content IDs (e.g. `ssis00100`) before hitting the r18.dev API.

---

## Caveats

- Scene matching relies on your Stash scenes having their filenames or codes set correctly. If a scene can't be found by code or DVD ID, it falls back to a filename path search.
- Gallery matching requires the gallery folder name to match the DVD ID (fuzzy normalized). Stash must have the gallery library configured and the folder indexed before a match is possible — this is why the library scan runs before Phase 2.
- The library scan waits indefinitely by default (`SCAN_TIMEOUT=0`). For very large libraries with many new files this could take a while — the console prints a heartbeat every 30 seconds so you know it hasn't hung.
- In `--auto` mode, scenes are only detected if they have no code, title, and studio set. Partially updated scenes will not be re-processed.
- This tool was built and tested against Stash v0.30.1+.

---

## License

MIT
