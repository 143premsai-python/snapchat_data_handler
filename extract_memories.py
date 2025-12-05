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
import sys
from typing import Dict, List
import shutil

INPUT = Path(__file__).with_name('memories_history.html')
OUTPUT = Path(__file__).with_name('memories.json')
SNAPCHAT_DIR = Path(__file__).with_name('snapchat_memories')
SNAPCHAT_DIR.mkdir(parents=True, exist_ok=True)

# Destination configuration: local or gdrive
DESTINATION = 'local'
GDRIVE_SERVICE = None
GDRIVE_FOLDER_ID = None
# conflict handling: 'skip' or 'overwrite'
CONFLICT_MODE = 'skip'
# map of existing files in gdrive folder: name -> list of file ids
GDRIVE_EXISTING: Dict[str, List[str]] = {}


def prompt_destination():
    """Prompt user to choose destination and collect GDrive info if requested."""
    global DESTINATION, GDRIVE_SERVICE, GDRIVE_FOLDER_ID, CONFLICT_MODE
    try:
        choice = input('Save to local folder or upload to Google Drive? [local/gdrive] (default: local): ').strip().lower()
    except Exception:
        choice = ''
    if choice == 'gdrive':
        DESTINATION = 'gdrive'
        print('Google Drive selected. You will need:')
        print(' - A service account JSON key file with Drive API access')
        print(' - The target Drive folder ID (from the folder URL)')
        sa = input('Path to service account JSON file: ').strip()
        folder = input('Target Drive folder ID (leave empty to create folder): ').strip()
        # ask conflict behavior
        cm = input('If files already exist in destination, choose action: overwrite, skip, or new (only upload/write new items) [skip]: ').strip().lower()
        if cm in ('overwrite', 'skip'):
            CONFLICT_MODE = cm
        elif cm == 'new':
            CONFLICT_MODE = 'new'
        if not sa:
            print('Service account JSON file is required for gdrive. Exiting.')
            sys.exit(1)
        try:
            GDRIVE_SERVICE = init_gdrive_service(sa)
        except Exception as e:
            print(f'Failed to init Google Drive client: {e}. Falling back to local.')
            return
        if folder:
            # verify access to provided folder id
            GDRIVE_FOLDER_ID = folder
            try:
                # attempt to get folder metadata
                GDRIVE_SERVICE.files().get(fileId=GDRIVE_FOLDER_ID, fields='id,name').execute()
            except Exception as e:
                print(f'Cannot access provided Drive folder ID: {e}')
                sys.exit(1)
        else:
            # create a folder named like the local snapchat directory
            try:
                GDRIVE_FOLDER_ID = create_gdrive_folder(GDRIVE_SERVICE, SNAPCHAT_DIR.name)
                print(f'Created Drive folder "{SNAPCHAT_DIR.name}" with id: {GDRIVE_FOLDER_ID}')
            except Exception as e:
                print(f'Failed to create Drive folder: {e}')
                sys.exit(1)
        # build existing files map for conflict handling
        try:
            GDRIVE_EXISTING = build_gdrive_existing_map(GDRIVE_SERVICE, GDRIVE_FOLDER_ID)
        except Exception as e:
            print(f'Failed listing existing files in Drive folder: {e}')
            sys.exit(1)


def init_gdrive_service(service_account_path: str):
    """Initialize Google Drive v3 service using a service account key file."""
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except Exception as e:
        raise RuntimeError('Missing google libraries: pip install google-api-python-client google-auth')
    scopes = ['https://www.googleapis.com/auth/drive.file']
    creds = service_account.Credentials.from_service_account_file(service_account_path, scopes=scopes)
    service = build('drive', 'v3', credentials=creds, cache_discovery=False)
    return service


def create_gdrive_folder(service, folder_name: str):
    """Create a folder under the service account's drive root and return its id."""
    body = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder'}
    created = service.files().create(body=body, fields='id').execute()
    return created.get('id')


def build_gdrive_existing_map(service, folder_id: str) -> Dict[str, List[str]]:
    """Return a mapping of filename -> list of file IDs in the given Drive folder."""
    existing = {}
    page_token = None
    try:
        while True:
            resp = service.files().list(q=f"'{folder_id}' in parents and trashed=false", fields='nextPageToken, files(id, name)', pageToken=page_token).execute()
            for f in resp.get('files', []):
                name = f.get('name')
                fid = f.get('id')
                existing.setdefault(name, []).append(fid)
            page_token = resp.get('nextPageToken')
            if not page_token:
                break
    except Exception as e:
        raise
    return existing


def get_local_existing_set() -> set:
    """Return set of filenames in SNAPCHAT_DIR (non-recursive)."""
    names = set()
    try:
        if SNAPCHAT_DIR.exists():
            for p in SNAPCHAT_DIR.iterdir():
                if p.is_file():
                    names.add(p.name)
    except Exception as e:
        print(f'Failed reading local folder {SNAPCHAT_DIR}: {e}. Exiting.')
        sys.exit(1)
    return names


def check_local_space(required_bytes: int):
    """Check available disk space where SNAPCHAT_DIR resides. Exit if insufficient or undeterminable."""
    try:
        total, used, free = shutil.disk_usage(str(SNAPCHAT_DIR))
    except Exception as e:
        print(f'Failed to determine local disk space: {e}. Exiting.')
        sys.exit(1)
    if free < required_bytes:
        print(f'Insufficient local disk space: need {required_bytes} bytes but only {free} available. Exiting.')
        sys.exit(1)


def check_gdrive_space(service, required_bytes: int):
    """Check Google Drive storage quota for the authenticated service account. Exit if insufficient or undeterminable."""
    try:
        about = service.about().get(fields='storageQuota').execute()
    except Exception as e:
        print(f'Failed to query Drive storage quota: {e}. Exiting.')
        sys.exit(1)
    quota = about.get('storageQuota', {})
    limit = quota.get('limit')
    usage = quota.get('usage') or '0'
    if not limit:
        # cannot determine limit reliably; per user instruction, exit
        print('Could not determine Drive storage limit for this account. Exiting.')
        sys.exit(1)
    try:
        remaining = int(limit) - int(usage)
    except Exception as e:
        print(f'Error parsing Drive quota values: {e}. Exiting.')
        sys.exit(1)
    if remaining < required_bytes:
        print(f'Insufficient Google Drive space: need {required_bytes} bytes but only {remaining} available. Exiting.')
        sys.exit(1)
    return True


def delete_gdrive_file(service, file_id: str):
    try:
        service.files().delete(fileId=file_id).execute()
    except Exception as e:
        raise


def upload_bytes_to_gdrive(service, folder_id: str, filename: str, bts: bytes, mime_type: str, app_properties: dict = None):
    try:
        from googleapiclient.http import MediaIoBaseUpload
    except Exception:
        raise RuntimeError('Missing googleapiclient.http (install google-api-python-client)')
    fh = io.BytesIO(bts)
    media = MediaIoBaseUpload(fh, mimetype=mime_type or 'application/octet-stream')
    meta = {'name': filename, 'parents': [folder_id]}
    if app_properties:
        meta['appProperties'] = {k: str(v) for k, v in app_properties.items() if v is not None}
    try:
        created = service.files().create(body=meta, media_body=media, fields='id, webViewLink').execute()
        return created
    except Exception as e:
        # try to detect storage quota errors
        from googleapiclient.errors import HttpError
        if isinstance(e, HttpError):
            try:
                msg = e.content.decode() if hasattr(e, 'content') else str(e)
            except Exception:
                msg = str(e)
            if 'storageQuotaExceeded' in msg or 'dailyLimitExceeded' in msg or 'insufficientStorage' in msg or e.status_code == 403:
                print('Google Drive storage quota exceeded or insufficient permissions. Exiting.')
                sys.exit(1)
        # other errors: re-raise
        raise


def save_or_upload_bytes(bts: bytes, filename: str, rec: dict, ext: str):
    """Save locally or upload to GDrive based on DESTINATION. Returns info dict."""
    # check space before attempting to write/upload
    required = len(bts)
    if DESTINATION == 'gdrive':
        # verify drive service available
        if not (GDRIVE_SERVICE and GDRIVE_FOLDER_ID):
            print('GDrive destination selected but Drive client or folder ID not configured. Exiting.')
            sys.exit(1)
        # check drive quota
        check_gdrive_space(GDRIVE_SERVICE, required)
        # proceed with conflict handling and upload
        if not (GDRIVE_SERVICE and GDRIVE_FOLDER_ID):
            print('GDrive destination selected but Drive client or folder ID not configured. Exiting.')
            sys.exit(1)
        # conflict handling: check if filename exists in GDRIVE_EXISTING
        existing_ids = GDRIVE_EXISTING.get(filename, []) if GDRIVE_EXISTING else []
        if existing_ids:
            if CONFLICT_MODE == 'skip':
                return {'skipped': True, 'reason': 'exists', 'gdrive_ids': existing_ids}
            elif CONFLICT_MODE == 'overwrite':
                # delete all existing files with this name before uploading
                for fid in existing_ids:
                    try:
                        delete_gdrive_file(GDRIVE_SERVICE, fid)
                    except Exception as e:
                        print(f'Failed to delete existing Drive file {fid} for {filename}: {e}. Exiting.')
                        sys.exit(1)
                # remove entry so subsequent checks won't consider it
                GDRIVE_EXISTING.pop(filename, None)
        lat, lon = parse_location(rec.get('Location', ''))
        app_props = {
            'Date': rec.get('Date'),
            'MediaType': rec.get('Media Type'),
            'Location': rec.get('Location'),
            'Latitude': lat,
            'Longitude': lon,
            'Source': rec.get('Media Download Url') or rec.get('Download Link')
        }
        mime = 'image/jpeg' if ext in ('.jpg', '.jpeg') else ('video/mp4' if ext == '.mp4' else 'application/octet-stream')
        # try upload, on failure exit (no fallback)
        try:
            created = upload_bytes_to_gdrive(GDRIVE_SERVICE, GDRIVE_FOLDER_ID, filename, bts, mime, app_props)
            # record created in existing map to prevent duplicate uploads within run
            GDRIVE_EXISTING.setdefault(filename, []).append(created.get('id'))
            return {'gdrive_id': created.get('id'), 'webViewLink': created.get('webViewLink')}
        except Exception as e:
            print(f'GDrive upload failed for {filename}: {e}. Exiting.')
            sys.exit(1)
    else:
        # local save: check disk space first
        check_local_space(required)
        out_path = SNAPCHAT_DIR / filename
        save_bytes_to_file(bts, out_path)
        # embed metadata locally
        lat, lon = parse_location(rec.get('Location', ''))
        meta = {
            'Date': rec.get('Date'),
            'Media Type': rec.get('Media Type'),
            'Location': rec.get('Location'),
            'Latitude': lat,
            'Longitude': lon,
            'Source': rec.get('Media Download Url') or rec.get('Download Link')
        }
        try:
            write_file_metadata(out_path, meta)
        except Exception:
            pass
        return {'path': str(out_path)}

# prompt user for destination before processing
prompt_destination()

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
    # rec['_prefix'] = f"{num_pfx}_{date_part}_{loc_part}"
    rec['_prefix'] = f"{date_part}_{loc_part}"

# If user selected 'new' conflict mode, compute which records are already present and skip them
if CONFLICT_MODE == 'new':
    # build existing name sets depending on destination
    existing_names = set()
    if DESTINATION == 'gdrive':
        if not (GDRIVE_SERVICE and GDRIVE_FOLDER_ID):
            print('GDrive destination selected but Drive client or folder ID not configured. Exiting.')
            sys.exit(1)
        existing_names = set(GDRIVE_EXISTING.keys())
    else:
        existing_names = get_local_existing_set()

    new_records = []
    skipped = 0
    for rec in records:
        prefix = rec.get('_prefix')
        # consider record existing if any existing filename starts with the prefix
        exists = any(name.startswith(prefix) for name in existing_names)
        if exists:
            skipped += 1
        else:
            new_records.append(rec)
    print(f'CONFLICT_MODE=new: skipping {skipped} existing records, will process {len(new_records)} new records.')
    records = new_records

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

        # Collect members into mains and overlays by base name
        mains = {}          # base -> member name for -main (jpg/mp4)
        overlays = {}       # base -> list of overlay member names (png)
        for member in z.namelist():
            nm = Path(member).name
            ext = Path(nm).suffix.lower()
            name_no_ext = nm[: -len(ext)] if ext else nm
            m = re.match(r'(.+?)-(?:main|overlay)', name_no_ext, re.I)
            if not m:
                continue
            base = m.group(1)
            if '-main' in name_no_ext.lower() and ext in ('.jpg', '.jpeg', '.mp4'):
                mains[base] = member
            elif '-overlay' in name_no_ext.lower() and ext == '.png':
                overlays.setdefault(base, []).append(member)

        if not mains:
            print(f'No -main JPG/MP4 files found in zip: {url}')
        else:
            count = 0
            import tempfile
            try:
                from PIL import Image
            except ModuleNotFoundError:
                print('Pillow (PIL) is required for compositing overlays onto images. Install with: pip install Pillow')
                sys.exit(1)
            import subprocess

            ffmpeg_path = shutil.which('ffmpeg')
            for base, main_member in mains.items():
                try:
                    with z.open(main_member) as fh:
                        main_bytes = fh.read()
                except Exception as e:
                    print(f"Failed reading main member {main_member} in zip {url}: {e}")
                    continue

                # prepare filename for output
                ext_inner = Path(main_member).suffix.lower()
                out_filename = f"{prefix}"
                if count > 0:
                    out_filename += f"_{count}"
                out_filename += ext_inner

                # get list of overlay members (may be empty)
                overlay_members = overlays.get(base, [])

                if ext_inner in ('.jpg', '.jpeg'):
                    # composite overlays onto main image
                    try:
                        img = Image.open(io.BytesIO(main_bytes)).convert('RGBA')
                        for om in overlay_members:
                            try:
                                with z.open(om) as ofh:
                                    overlay_bytes = ofh.read()
                                overlay_img = Image.open(io.BytesIO(overlay_bytes)).convert('RGBA')
                                img.alpha_composite(overlay_img)
                            except Exception as e:
                                print(f"Failed reading/applying overlay {om} for {main_member}: {e}")
                                # per spec, exit on failure
                                sys.exit(1)
                        # save final image to bytes
                        out_buf = io.BytesIO()
                        rgb = img.convert('RGB')
                        rgb.save(out_buf, format='JPEG')
                        out_bytes = out_buf.getvalue()
                        info = save_or_upload_bytes(out_bytes, out_filename, record, ext_inner)
                        saved_files.append(info)
                        count += 1
                    except Exception as e:
                        print(f"Failed compositing overlays for image {main_member}: {e}")
                        sys.exit(1)

                elif ext_inner == '.mp4':
                    # require ffmpeg to composite overlays onto video
                    if not ffmpeg_path:
                        print('ffmpeg is required to composite overlays onto MP4 files but was not found in PATH. Exiting.')
                        sys.exit(1)
                    # create temp files
                    tmpdir = tempfile.mkdtemp(prefix='snapchat_')
                    try:
                        tmp_main = Path(tmpdir) / ('main' + ext_inner)
                        tmp_main.write_bytes(main_bytes)
                        current_input = str(tmp_main)
                        step = 0
                        for om in overlay_members:
                            try:
                                with z.open(om) as ofh:
                                    overlay_bytes = ofh.read()
                                tmp_overlay = Path(tmpdir) / f'overlay_{step}.png'

                                # produce next output file
                                try:
                                    img = Image.open(io.BytesIO(overlay_bytes)).convert('RGBA')
                                    img.save(tmp_overlay, format='PNG')
                                except Exception as e:
                                    print(f"Failed to decode overlay {om} as image, skipping it: {e}")
                                    # Skip just this overlay, continue with others
                                    continue

                                tmp_out = Path(tmpdir) / f'out_{step}.mp4'
                                cmd = [
                                    ffmpeg_path,
                                    '-y',
                                    '-i', current_input,
                                    '-i', str(tmp_overlay),
                                    '-filter_complex',
                                    '[1:v][0:v]scale2ref[ov][base];[base][ov]overlay=0:0',
                                    '-c:v', 'libx264',
                                    '-c:a', 'copy',
                                    str(tmp_out),
                                ]
                                proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                                if proc.returncode != 0:
                                    print(f"ffmpeg failed overlaying {om} onto {main_member}: {proc.stderr.decode(errors='ignore')}")
                                    sys.exit(1)
                                current_input = str(tmp_out)
                                step += 1
                            except Exception as e:
                                print(f"Failed processing overlay {om} for video {main_member}: {e}")
                                sys.exit(1)
                        # read final file bytes
                        final_bytes = Path(current_input).read_bytes()
                        info = save_or_upload_bytes(final_bytes, out_filename, record, ext_inner)
                        saved_files.append(info)
                        count += 1
                    finally:
                        # cleanup tempdir
                        try:
                            shutil.rmtree(tmpdir)
                        except Exception:
                            pass
                else:
                    # unsupported main extension
                    print(f"Unsupported main file type in zip: {main_member}")
                    continue
        # end zip handling
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
        try:
            save_or_upload_bytes(data, out_name, record, final_ext)
            saved_files.append(out_name)
        except Exception as e:
            print(f"Failed saving download {url} to {out_name}: {e}")

    return saved_files


# iterate records and download
all_saved = {}
# print(json.dumps(records,indent=4))
# images = list(filter(lambda x: x.get("Media Type") == "Image", records))
videos = list(filter(lambda x: x.get("Media Type") == "Video", records))

# print(json.dumps(videos[136],indent=4))
# sys.exit(1)
for rec in videos[137:]:
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
