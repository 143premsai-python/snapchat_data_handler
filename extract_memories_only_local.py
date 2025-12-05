# Script to extract all memory rows from memories_history.html and write to memories.json
# Usage: python extract_memories.py
import re
import json
import html
import os
import io
import zipfile
import urllib.request
import urllib.error
from pathlib import Path

INPUT = Path(__file__).with_name('memories_history.html')
OUTPUT = Path(__file__).with_name('memories.json')
SNAPCHAT_DIR = Path(__file__).with_name('snapchat_memories')
SNAPCHAT_DIR.mkdir(parents=True, exist_ok=True)

html_text = INPUT.read_text(encoding='utf-8')

# Find the table body content between <table> and </table>
table_match = re.search(r"<table.*?>.*?<tbody>(.*?)</tbody>.*?</table>", html_text, re.S | re.I)
if not table_match:
    # fallback: search entire file for <tr> rows
    tbody = html_text
else:
    tbody = table_match.group(1)

# Find all <tr>...</tr>
rows = re.findall(r"<tr>(.*?)</tr>", tbody, re.S | re.I)

records = []

for r in rows:
    # extract all td contents
    tds = re.findall(r"<td>(.*?)</td>", r, re.S | re.I)
    if not tds or len(tds) < 3:
        continue
    # clean function: remove tags and unescape
    def clean(s):
        s = re.sub(r'<[^>]+>', '', s)
        s = html.unescape(s).strip()
        return s

    date = clean(tds[0])
    media_type = clean(tds[1])
    location = clean(tds[2])

    # extract download URL from onclick if present
    download_url = None
    onclick_match = re.search(r"onclick=\"[^\"]*downloadMemories\('([^']+)'\s*,\s*this\s*,\s*(true|false)\)\"", r)
    if not onclick_match:
        onclick_match = re.search(r"downloadMemories\('([^']+)'\s*,\s*this\s*,\s*(true|false)\)", r)
    if onclick_match:
        download_url = onclick_match.group(1)

    # Also look for hrefs inside the td as fallback
    if not download_url:
        href_match = re.search(r"href=[\'\"]([^\'\"]+)[\'\"]", r)
        if href_match:
            download_url = href_match.group(1)

    record = {
        'Date': date,
        'Media Type': media_type,
        'Location': location,
        'Media Download Url': download_url
    }
    # also add a Download Link field; if the URL contains proxy=true, keep as-is; else leave Media Download Url
    record['Download Link'] = download_url

    records.append(record)

# Write to JSON with pretty formatting
OUTPUT.write_text(json.dumps(records, indent=2, ensure_ascii=False), encoding='utf-8')
print(f'Wrote {len(records)} records to {OUTPUT}')

# --- New: assign numeric ordering (oldest -> 1) and create numbered prefixes for filenames ---
from datetime import datetime


def parse_date_value(date_str):
    if not date_str:
        return None
    s = date_str.strip()
    # remove trailing timezone like ' UTC' if present
    if s.endswith(' UTC'):
        s = s[:-4].strip()
    # try common formats
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M'):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    for fmt in ('%Y-%m-%d', '%Y/%m/%d'):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    # try to parse simple ISO-like strings
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None

# build list of (index, parsed_dt) and sort by parsed_dt ascending (oldest first). Unparseable dates go last.
indexed = []
for i, rec in enumerate(records):
    dt = parse_date_value(rec.get('Date'))
    # use a sentinel for None so they sort after real dates
    sort_dt = dt if dt is not None else datetime.max
    indexed.append((i, sort_dt))

indexed.sort(key=lambda x: (x[1], x[0]))

total = len(indexed)
pad = max(4, len(str(total)))

# assign ranks
rank_map = {}
for rank, (idx, _) in enumerate(indexed, start=1):
    rank_map[idx] = rank


def sanitize_part(s):
    if s is None:
        return ''
    t = re.sub(r"\s+", "_", s.strip())
    t = re.sub(r'[^A-Za-z0-9._-]', '_', t)
    return t

for idx, rec in enumerate(records):
    num = rank_map.get(idx, 0)
    num_pfx = str(num).zfill(pad)
    date_part = sanitize_part(rec.get('Date', ''))
    loc_part = sanitize_part(rec.get('Location', ''))
    rec['_prefix'] = f"{num_pfx}_{date_part}_{loc_part}"

# --- New: download each media URL, handle zips, and rename files ---

def sanitize_prefix(date_str, location_str):
    # join date and location, replace whitespace with underscore, and remove chars invalid for filenames
    combined = f"{date_str}_{location_str}"
    # replace whitespace with underscore
    combined = re.sub(r"\s+", "_", combined)
    # remove or replace characters not allowed in filenames (keep alnum, dot, underscore, hyphen)
    sanitized = re.sub(r'[^A-Za-z0-9._-]', '_', combined)
    return sanitized


def get_extension_from_content_type(ct):
    if not ct:
        return None
    ct = ct.split(';', 1)[0].strip().lower()
    if ct in ('image/jpeg', 'image/jpg'):
        return '.jpg'
    if ct == 'image/png':
        return '.png'
    if ct == 'video/mp4':
        return '.mp4'
    if ct == 'application/zip':
        return '.zip'
    # fallback
    return None


def save_bytes_to_file(bts, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'wb') as f:
        f.write(bts)


def parse_location(location_str):
    """Parse latitude and longitude from a Location string. Returns (lat, lon) or (None, None)."""
    if not location_str:
        return (None, None)
    s = location_str
    # common pattern: "Latitude, Longitude: 43.639523, -79.63285" or "43.639523, -79.63285"
    m = re.search(r'([-+]?\d{1,3}\.\d+)\s*,\s*([-+]?\d{1,3}\.\d+)', s)
    if m:
        try:
            return (float(m.group(1)), float(m.group(2)))
        except Exception:
            return (None, None)
    # fallback patterns
    m2 = re.search(r'Latitude[:\s]*([-+]?\d{1,3}\.\d+).*?Longitude[:\s]*([-+]?\d{1,3}\.\d+)', s, re.I)
    if m2:
        try:
            return (float(m2.group(1)), float(m2.group(2)))
        except Exception:
            return (None, None)
    return (None, None)

def write_file_metadata(path: Path, meta: dict):
    """Try to embed metadata into the file. For JPEGs attempt EXIF GPS (piexif), for MP4 try mutagen MP4 atom; fallback to NTFS ADS 'snapchat_metadata'."""
    # try JPEG EXIF via piexif
    try:
        if path.suffix.lower() in ('.jpg', '.jpeg'):
            try:
                import piexif
                from PIL import Image
                lat, lon = meta.get('Latitude'), meta.get('Longitude')
                if lat is None or lon is None:
                    return False

                def _to_deg(value):
                    # return tuple of rationals ((deg,1),(min,1),(sec,100)) and sign
                    sign = 1
                    if value < 0:
                        sign = -1
                        value = -value
                    deg = int(value)
                    minf = (value - deg) * 60
                    minutes = int(minf)
                    seconds = round((minf - minutes) * 60 * 100)
                    return ((deg, 1), (minutes, 1), (seconds, 100)), sign

                lat_tuple, lat_sign = _to_deg(lat)
                lon_tuple, lon_sign = _to_deg(lon)
                gps_ifd = {
                    piexif.GPSIFD.GPSLatitudeRef: b'N' if lat_sign == 1 else b'S',
                    piexif.GPSIFD.GPSLatitude: lat_tuple,
                    piexif.GPSIFD.GPSLongitudeRef: b'E' if lon_sign == 1 else b'W',
                    piexif.GPSIFD.GPSLongitude: lon_tuple,
                }
                exif_dict = {}
                try:
                    exif_dict = piexif.load(str(path))
                except Exception:
                    exif_dict = {"0th":{}, "Exif":{}, "GPS":{}, "1st":{}, "thumbnail": None}
                exif_dict['GPS'] = gps_ifd
                exif_bytes = piexif.dump(exif_dict)
                piexif.insert(exif_bytes, str(path))
                return True
            except Exception:
                pass
    except Exception:
        pass

    # try MP4 tagging via mutagen
    try:
        if path.suffix.lower() == '.mp4':
            try:
                from mutagen.mp4 import MP4, MP4Tags
                mp4 = MP4(str(path))
                # store JSON metadata in a freeform atom
                key = '----:com.snapchat:metadata'
                mp4.tags = mp4.tags or MP4Tags()
                mp4.tags[key] = [json.dumps(meta).encode('utf-8')]
                mp4.save()
                return True
            except Exception:
                pass
    except Exception:
        pass

    # fallback: write to NTFS Alternate Data Stream (Windows). This keeps metadata attached to file.
    try:
        ads_path = str(path) + ':snapchat_metadata'
        with open(ads_path, 'w', encoding='utf-8') as f:
            json.dump(meta, f, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"Failed writing metadata into file or ADS for {path}: {e}")
        return False

def download_and_process(record):
    url = record.get('Media Download Url') or record.get('Download Link')
    if not url:
        return None

    # use precomputed numbered prefix if available
    prefix = record.get('_prefix') or sanitize_prefix(record.get('Date', ''), record.get('Location', ''))

    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = resp.read()
            content_type = resp.headers.get('Content-Type')
            # try to infer extension from headers
            ext = get_extension_from_content_type(content_type)
    except urllib.error.HTTPError as e:
        print(f"HTTP error downloading {url}: {e}")
        return None
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return None

    # detect zip
    is_zip = False
    if ext == '.zip' or url.lower().endswith('.zip'):
        is_zip = True
    else:
        # robust check using zipfile
        try:
            if zipfile.is_zipfile(io.BytesIO(data)):
                is_zip = True
        except Exception:
            is_zip = False

    saved_files = []

    if is_zip:
        try:
            z = zipfile.ZipFile(io.BytesIO(data))
        except Exception as e:
            print(f"Failed to open zip from {url}: {e}")
            return None

        # extract only jpg/jpeg and mp4 files; ignore png
        count = 0
        for member in z.namelist():
            name_lower = member.lower()
            if name_lower.endswith('.jpg') or name_lower.endswith('.jpeg') or name_lower.endswith('.mp4'):
                ext_inner = Path(member).suffix.lower()
                # build filename: prefix[_index].ext
                filename = f"{prefix}"
                if count > 0:
                    filename += f"_{count}"
                filename += ext_inner
                out_path = SNAPCHAT_DIR / filename
                try:
                    with z.open(member) as fh:
                        content = fh.read()
                        save_bytes_to_file(content, out_path)
                    saved_files.append(str(out_path))
                    # write metadata sidecar file with lat/long
                    lat, lon = parse_location(record.get('Location', ''))
                    meta = {
                        'Date': record.get('Date'),
                        'Media Type': record.get('Media Type'),
                        'Location': record.get('Location'),
                        'Latitude': lat,
                        'Longitude': lon,
                        'Source': url,
                        'ArchiveMember': member
                    }
                    # embed metadata into the file itself (EXIF/MP4 tag or ADS fallback)
                    try:
                        write_file_metadata(out_path, meta)
                    except Exception as e:
                        print(f"Failed embedding metadata for {out_path}: {e}")
                    count += 1
                except Exception as e:
                    print(f"Failed extracting {member} from zip {url}: {e}")
                    continue
        if not saved_files:
            print(f"No JPG/MP4 files found in zip: {url}")
    else:
        # not a zip: determine extension and save directly
        # try to get extension from URL
        url_path = urllib.request.urlparse(url).path
        ext_from_url = Path(url_path).suffix
        final_ext = ext_from_url if ext_from_url else ext
        if final_ext:
            final_ext = final_ext.lower()
        else:
            # fallback to .bin
            final_ext = '.bin'

        # build filename
        out_name = f"{prefix}{final_ext}"
        out_path = SNAPCHAT_DIR / out_name
        try:
            save_bytes_to_file(data, out_path)
            saved_files.append(str(out_path))
            # write metadata sidecar file with lat/long
            lat, lon = parse_location(record.get('Location', ''))
            meta = {
                'Date': record.get('Date'),
                'Media Type': record.get('Media Type'),
                'Location': record.get('Location'),
                'Latitude': lat,
                'Longitude': lon,
                'Source': url
            }
            # embed metadata into the file itself (EXIF/MP4 tag or ADS fallback)
            try:
                write_file_metadata(out_path, meta)
            except Exception as e:
                print(f"Failed embedding metadata for {out_path}: {e}")
        except Exception as e:
            print(f"Failed saving download {url} to {out_path}: {e}")

    return saved_files


# iterate records and download
all_saved = {}
for rec in records:
    url = rec.get('Media Download Url')
    if not url:
        continue
    print(f"Processing: {rec.get('Date')} | {rec.get('Location')} -> {url}")
    saved = download_and_process(rec)
    all_saved[url] = saved

# write a small report
report_path = Path(__file__).with_name('download_report.json')
report_path.write_text(json.dumps(all_saved, indent=2, ensure_ascii=False), encoding='utf-8')
print(f'Downloaded/processed {sum(1 for v in all_saved.values() if v)} items. Report: {report_path}')
