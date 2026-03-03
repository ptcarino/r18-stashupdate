import requests
from pathlib import Path

DMM_IMG_BASE = "https://awsimgsrc.dmm.com/dig/digital/video"


def download_gallery_images(content_id, dvd_id, gallery_path):
    """
    Downloads jp- sample images and the pl cover image from awsimgsrc.dmm.com.
    Saves into {gallery_path}/{dvd_id}/
    - Cover: {content_id}pl.jpg  -> cover_{content_id}.jpg
    - Samples: {content_id}jp-1.jpg, jp-2.jpg, ... until 404
    Skips files that already exist. Logs warnings on failure but never blocks.
    Returns the gallery folder path.
    """
    gallery_folder = Path(gallery_path) / dvd_id
    gallery_folder.mkdir(parents=True, exist_ok=True)

    base_url = f"{DMM_IMG_BASE}/{content_id}"

    # --- Cover image ---
    cover_dest = gallery_folder / "cover.jpg"
    if not cover_dest.exists():
        try:
            r = requests.get(f"{base_url}/{content_id}pl.jpg", timeout=15)
            if r.status_code == 200:
                cover_dest.write_bytes(r.content)
            else:
                print(f"\n[!] Cover download failed for {dvd_id}: HTTP {r.status_code}")
        except Exception as e:
            print(f"\n[!] Cover download error for {dvd_id}: {type(e).__name__}: {e}")

    # --- Sample jp- images — probe until 404 ---
    index = 1
    while True:
        filename = f"{content_id}jp-{index}.jpg"
        dest     = gallery_folder / filename
        url      = f"{base_url}/{filename}"

        if dest.exists():
            index += 1
            continue

        try:
            r = requests.get(url, timeout=15)
            if r.status_code == 404:
                break  # no more images
            elif r.status_code == 200:
                dest.write_bytes(r.content)
                index += 1
            else:
                print(f"\n[!] Image {filename} failed for {dvd_id}: HTTP {r.status_code}")
                index += 1
        except Exception as e:
            print(f"\n[!] Image download error for {dvd_id} ({filename}): {type(e).__name__}: {e}")
            break

    return gallery_folder