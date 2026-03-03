import re
from pathlib import Path


# ---------------------------------------------------------------------------
# GraphQL
# ---------------------------------------------------------------------------

def get_all_galleries(stash):
    """
    Queries all galleries from Stash using a single GraphQL call.
    Uses 'folder { path }' for Stash v0.30.1+ compatibility.
    Returns a list of gallery dicts, or an empty list on failure.
    """
    query = """
    query AllGalleries {
      findGalleries(gallery_filter: {}, filter: { per_page: -1 }) {
        galleries {
          id
          title
          code
          folder {
            path
          }
        }
      }
    }
    """
    try:
        result = stash.call_GQL(query)
        return result.get('findGalleries', {}).get('galleries', [])
    except Exception as e:
        print(f"[!] get_all_galleries failed: {type(e).__name__}: {e}")
        return []


# ---------------------------------------------------------------------------
# Matching helpers
# ---------------------------------------------------------------------------

def _normalize(s):
    """Strip separators and lowercase for fuzzy comparison."""
    return re.sub(r'[-_\s]', '', s).lower()


def _folder_name(gallery):
    """Return the bare folder name for a gallery, or None."""
    folder_data = gallery.get('folder')
    if folder_data and folder_data.get('path'):
        return Path(folder_data['path']).name
    return None


def _folder_matches(folder_name, id_fuzzy):
    """
    Fuzzy folder match: the normalised folder name must equal the identifier
    OR start with it (to catch suffixes like ' [720p]' or ' (uncensored)').
    Avoids false positives from plain substring matching.
    """
    folder_fuzzy = _normalize(folder_name)
    return folder_fuzzy == id_fuzzy or folder_fuzzy.startswith(id_fuzzy)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def find_gallery_match(galleries, identifier):
    """
    Match an identifier against a cached gallery list.

    Priority:
      1. Code  — exact, case-insensitive
      2. Title — exact, case-insensitive
      3. Folder name — normalised fuzzy (equal or starts-with)

    Returns the gallery ID string, or None if no match is found.
    """
    if not identifier or not galleries:
        return None

    id_clean = identifier.strip().upper()
    id_fuzzy = _normalize(id_clean)

    # Priority 1: Gallery code (exact)
    for gal in galleries:
        code = gal.get('code')
        if code and code.strip().upper() == id_clean:
            return gal['id']

    # Priority 2: Gallery title (exact)
    for gal in galleries:
        title = gal.get('title')
        if title and title.strip().upper() == id_clean:
            return gal['id']

    # Priority 3: Folder name (normalised fuzzy — equal or starts-with)
    for gal in galleries:
        fname = _folder_name(gal)
        if fname and _folder_matches(fname, id_fuzzy):
            return gal['id']

    return None