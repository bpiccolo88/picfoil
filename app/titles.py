import os
import sys
import re
import json

import titledb
from constants import *
from utils import *
from settings import *
from pathlib import Path
from binascii import hexlify as hx, unhexlify as uhx
import logging

from nsz.Fs import Pfs0, Xci, Nsp, Nca, Type, factory
from nsz.nut import Keys

# Retrieve main logger
logger = logging.getLogger('main')

Pfs0.Print.silent = True

app_id_regex = r"\[([0-9A-Fa-f]{16})\]"
version_regex = r"\[v(\d+)\]"

# Global variables for TitleDB data
identification_in_progress_count = 0
_titles_db_loaded = False
_cnmts_db = None
_titles_db = None
_versions_db = None
_versions_txt_db = None

# System version to firmware version mapping
# Source: switchbrew.org/wiki/System_Version_Title
_SYSVER_TO_FIRMWARE = {
    0x0:        '0.0.0',
    0x10000:    '1.0.0',
    0xC000000:  '2.0.0',  0xC010000:  '2.1.0',  0xC020000:  '2.2.0',  0xC030000:  '2.3.0',
    0x10000000: '3.0.0',  0x10010000: '3.0.1',  0x10020000: '3.0.2',  0x10100000: '4.0.0',
    0x14000000: '4.0.1',  0x14010000: '4.1.0',  0x14020000: '5.0.0',  0x14100000: '5.0.1',
    0x18000000: '5.0.2',  0x18010000: '5.1.0',  0x18100000: '6.0.0',  0x18110000: '6.0.1',
    0x18200000: '6.1.0',  0x18300000: '6.2.0',
    0x1C000000: '7.0.0',  0x1C010000: '7.0.1',
    0x20000000: '8.0.0',  0x20010000: '8.0.1',  0x20100000: '8.1.0',  0x20110000: '8.1.1',
    0x24000000: '9.0.0',  0x24010000: '9.0.1',  0x24100000: '9.1.0',  0x24200000: '9.2.0',
    0x28000000: '10.0.0', 0x28010000: '10.0.1', 0x28020000: '10.0.2', 0x28030000: '10.0.3',
    0x28040000: '10.0.4', 0x28100000: '10.1.0', 0x28110000: '10.1.1', 0x28200000: '10.2.0',
    0x2C000000: '11.0.0', 0x2C010000: '11.0.1',
    0x30000000: '12.0.0', 0x30010000: '12.0.1', 0x30020000: '12.0.2', 0x30030000: '12.0.3',
    0x30100000: '12.1.0',
    0x34000000: '13.0.0', 0x34010000: '13.1.0', 0x34020000: '13.2.0', 0x34030000: '13.2.1',
    0x38000000: '14.0.0', 0x38010000: '14.1.0', 0x38020000: '14.1.1', 0x38030000: '14.1.2',
    0x3C000000: '15.0.0', 0x3C010000: '15.0.1',
    0x40000000: '16.0.0', 0x40010000: '16.0.1', 0x40020000: '16.0.2', 0x40030000: '16.0.3',
    0x40100000: '16.1.0',
    0x44000000: '17.0.0', 0x44010000: '17.0.1',
    0x48000000: '18.0.0', 0x48010000: '18.0.1', 0x48100000: '18.1.0',
    0x4C000000: '19.0.0', 0x4C010000: '19.0.1',
}


def decode_required_firmware(required_system_version):
    """Convert a requiredSystemVersion integer to a firmware version string."""
    if not required_system_version:
        return None
    # Exact match
    if required_system_version in _SYSVER_TO_FIRMWARE:
        return _SYSVER_TO_FIRMWARE[required_system_version]
    # Find the highest known version that doesn't exceed this value
    lower = [v for v in _SYSVER_TO_FIRMWARE if v <= required_system_version]
    if lower:
        return _SYSVER_TO_FIRMWARE[max(lower)] + '+'
    return None


def get_title_required_firmware(title_id):
    """Get the highest required firmware version across all content for a title.
    Returns firmware version string or None."""
    global _cnmts_db
    if _cnmts_db is None:
        return None

    title_id_lower = title_id.lower()

    # Collect all app_ids related to this title (base, update, DLC)
    related_app_ids = set()
    # Base
    related_app_ids.add(title_id_lower)
    # Update (title_id with last 3 chars replaced by 800)
    update_id = title_id_lower[:-3] + '800'
    related_app_ids.add(update_id)
    # DLC (scan cnmts_db for entries with otherApplicationId matching this title)
    for app_id, versions in _cnmts_db.items():
        for ver_data in versions.values():
            if ver_data.get('otherApplicationId') == title_id_lower:
                related_app_ids.add(app_id)

    max_rsv = 0
    for app_id in related_app_ids:
        if app_id in _cnmts_db:
            for ver, data in _cnmts_db[app_id].items():
                rsv = data.get('requiredSystemVersion') or 0
                if rsv > max_rsv:
                    max_rsv = rsv

    if max_rsv > 0:
        return decode_required_firmware(max_rsv)
    return None


def getDirsAndFiles(path):
    entries = os.listdir(path)
    allFiles = []
    allDirs = []

    for entry in entries:
        fullPath = os.path.join(path, entry)
        if os.path.isdir(fullPath):
            allDirs.append(fullPath)
            dirs, files = getDirsAndFiles(fullPath)
            allDirs += dirs
            allFiles += files
        elif fullPath.split('.')[-1] in ALLOWED_EXTENSIONS:
            allFiles.append(fullPath)
    return allDirs, allFiles

def get_app_id_from_filename(filename):
    app_id_match = re.search(app_id_regex, filename)
    return app_id_match[1] if app_id_match is not None else None

def get_version_from_filename(filename):
    version_match = re.search(version_regex, filename)
    return version_match[1] if version_match is not None else None

def get_title_id_from_app_id(app_id, app_type):
    base_id = app_id[:-3]
    if app_type == APP_TYPE_UPD:
        title_id = base_id + '000'
    elif app_type == APP_TYPE_DLC:
        title_id = hex(int(base_id, base=16) - 1)[2:].rjust(len(base_id), '0') + '000'
    return title_id.upper()

def get_file_size(filepath):
    return os.path.getsize(filepath)

def get_file_info(filepath):
    filedir, filename = os.path.split(filepath)
    extension = filename.split('.')[-1]
    
    compressed = False
    if extension in ['nsz', 'xcz']:
        compressed = True

    return {
        'filepath': filepath,
        'filedir': filedir,
        'filename': filename,
        'extension': extension,
        'compressed': compressed,
        'size': get_file_size(filepath),
    }

def identify_appId(app_id):
    app_id = app_id.lower()
    
    global _cnmts_db
    if _cnmts_db is None:
        logger.error("cnmts_db is not loaded. Call load_titledb first.")
        return None, None

    if app_id in _cnmts_db:
        app_id_keys = list(_cnmts_db[app_id].keys())
        if len(app_id_keys):
            app = _cnmts_db[app_id][app_id_keys[-1]]
            
            if app['titleType'] == 128:
                app_type = APP_TYPE_BASE
                title_id = app_id.upper()
            elif app['titleType'] == 129:
                app_type = APP_TYPE_UPD
                if 'otherApplicationId' in app:
                    title_id = app['otherApplicationId'].upper()
                else:
                    title_id = get_title_id_from_app_id(app_id, app_type)
            elif app['titleType'] == 130:
                app_type = APP_TYPE_DLC
                if 'otherApplicationId' in app:
                    title_id = app['otherApplicationId'].upper()
                else:
                    title_id = get_title_id_from_app_id(app_id, app_type)
        else:
            logger.warning(f'{app_id} has no keys in cnmts_db, fallback to default identification.')
            if app_id.endswith('000'):
                app_type = APP_TYPE_BASE
                title_id = app_id
            elif app_id.endswith('800'):
                app_type = APP_TYPE_UPD
                title_id = get_title_id_from_app_id(app_id, app_type)
            else:
                app_type = APP_TYPE_DLC
                title_id = get_title_id_from_app_id(app_id, app_type)
    else:
        logger.warning(f'{app_id} not in cnmts_db, fallback to default identification.')
        if app_id.endswith('000'):
            app_type = APP_TYPE_BASE
            title_id = app_id
        elif app_id.endswith('800'):
            app_type = APP_TYPE_UPD
            title_id = get_title_id_from_app_id(app_id, app_type)
        else:
            app_type = APP_TYPE_DLC
            title_id = get_title_id_from_app_id(app_id, app_type)
    
    return title_id.upper(), app_type

def load_titledb():
    global _cnmts_db
    global _titles_db
    global _versions_db
    global _versions_txt_db
    global identification_in_progress_count
    global _titles_db_loaded

    identification_in_progress_count += 1
    if not _titles_db_loaded:
        logger.info("Loading TitleDBs into memory...")
        app_settings = load_settings()
        with open(os.path.join(TITLEDB_DIR, 'cnmts.json'), "r", encoding="utf-8") as f:
            _cnmts_db = json.load(f)

        with open(os.path.join(TITLEDB_DIR, titledb.get_region_titles_file(app_settings)), "r", encoding="utf-8") as f:
            _titles_db = json.load(f)

        with open(os.path.join(TITLEDB_DIR, 'versions.json'), "r", encoding="utf-8") as f:
            _versions_db = json.load(f)

        _versions_txt_db = {}
        with open(os.path.join(TITLEDB_DIR, 'versions.txt'), "r", encoding="utf-8") as f:
            for line in f:
                line_strip = line.rstrip("\n")
                app_id, rightsId, version = line_strip.split('|')
                if not version:
                    version = "0"
                _versions_txt_db[app_id] = version
        _titles_db_loaded = True
        logger.info("TitleDBs loaded.")

@debounce(30)
def unload_titledb():
    global _cnmts_db
    global _titles_db
    global _versions_db
    global _versions_txt_db
    global identification_in_progress_count
    global _titles_db_loaded

    if identification_in_progress_count:
        logger.debug('Identification still in progress, not unloading TitleDB.')
        return

    logger.info("Unloading TitleDBs from memory...")
    _cnmts_db = None
    _titles_db = None
    _versions_db = None
    _versions_txt_db = None
    _titles_db_loaded = False
    logger.info("TitleDBs unloaded.")

def identify_file_from_filename(filename):
    title_id = None
    app_id = None
    app_type = None
    version = None
    errors = []

    app_id = get_app_id_from_filename(filename)
    if app_id is None:
        errors.append('Could not determine App ID from filename, pattern [APPID] not found. Title ID and Type cannot be derived.')
    else:
        title_id, app_type = identify_appId(app_id)

    version = get_version_from_filename(filename)
    if version is None:
        errors.append('Could not determine version from filename, pattern [vVERSION] not found.')
    
    error = ' '.join(errors)
    return app_id, title_id, app_type, version, error

def get_cnmts(container):
    cnmts = []
    if isinstance(container, Nsp.Nsp):
        try:
            cnmt = container.cnmt()
            cnmts.append(cnmt)
        except Exception as e:
            logger.warning('CNMT section not found in Nsp.')

    elif isinstance(container, Xci.Xci):
        container = container.hfs0['secure']
        for nspf in container:
            if isinstance(nspf, Nca.Nca) and nspf.header.contentType == Type.Content.META:
                cnmts.append(nspf)

    return cnmts

def extract_meta_from_cnmt(cnmt_sections):
    contents = []
    for section in cnmt_sections:
        if isinstance(section, Pfs0.Pfs0):
            Cnmt = section.getCnmt()
            titleType = APP_TYPE_MAP[Cnmt.titleType]
            titleId = Cnmt.titleId.upper()
            version = Cnmt.version
            contents.append((titleType, titleId, version))
    return contents

def identify_file_from_cnmt(filepath):
    contents = []
    container = factory(Path(filepath).resolve())
    try:
        container.open(filepath, 'rb', meta_only=True)
        for cnmt_sections in get_cnmts(container):
            contents += extract_meta_from_cnmt(cnmt_sections)
    except OSError as e:
        # Check if the error is due to a missing master_key
        match = re.search(r"master_key_([0-9a-fA-F]{2}) missing from", str(e))
        if match:
            key_index = match.group(1)
            raise ValueError(f"Missing valid master_key_{key_index} from keys file.") from e
        else:
            raise # Re-raise other OSErrors
    finally:
        container.close()

    return contents

def identify_file(filepath):
    filename = os.path.split(filepath)[-1]
    contents = []
    success = True
    error = ''
    if Keys.keys_loaded:
        identification = 'cnmt'
        try:
            cnmt_contents = identify_file_from_cnmt(filepath)
            if not cnmt_contents:
                error = 'No content found in NCA containers.'
                success = False
            else:
                for content in cnmt_contents:
                    app_type, app_id, version = content
                    if app_type != APP_TYPE_BASE:
                        # need to get the title ID from cnmts
                        title_id, app_type = identify_appId(app_id)
                    else:
                        title_id = app_id
                    contents.append((title_id, app_type, app_id, version))
        except Exception as e:
            logger.error(f'Could not identify file {filepath} from metadata: {e}')
            error = str(e)
            success = False

        # Fall back to filename identification if CNMT failed
        if not success:
            logger.info(f'CNMT identification failed for {filename}, falling back to filename identification.')
            app_id, title_id, app_type, version, fn_error = identify_file_from_filename(filename)
            if not fn_error:
                identification = 'filename'
                contents.append((title_id, app_type, app_id, version))
                success = True
                error = ''

    else:
        identification = 'filename'
        app_id, title_id, app_type, version, error = identify_file_from_filename(filename)
        if not error:
            contents.append((title_id, app_type, app_id, version))
        else:
            success = False

    if contents:
        contents = [{
            'title_id': c[0],
            'app_id': c[2],
            'type': c[1],
            'version': c[3],
            } for c in contents]
    return identification, success, contents, error


def get_game_info(title_id):
    global _titles_db
    if _titles_db is None:
        logger.error("titles_db is not loaded. Call load_titledb first.")
        return None

    # O(1) dict lookup instead of linear scan — keys are title IDs
    title_info = _titles_db.get(title_id.upper()) or _titles_db.get(title_id.lower())
    if title_info:
        return {
            'name': title_info.get('name', ''),
            'bannerUrl': title_info.get('bannerUrl', ''),
            'iconUrl': title_info.get('iconUrl', ''),
            'id': title_info.get('id', title_id),
            'category': title_info.get('category', ''),
            'description': title_info.get('description', ''),
            'intro': title_info.get('intro', ''),
            'publisher': title_info.get('publisher', ''),
            'releaseDate': title_info.get('releaseDate', ''),
        }
    else:
        logger.error(f"Title ID not found in titledb: {title_id}")
        return {
            'name': 'Unrecognized',
            'bannerUrl': '//placehold.it/400x200',
            'iconUrl': '',
            'id': title_id + ' not found in titledb',
            'category': '',
            'description': '',
            'intro': '',
            'publisher': '',
            'releaseDate': '',
        }

def get_update_number(version):
    return int(version)//65536


def extract_display_version(filepath):
    """Extract the displayVersion string from an NSP/NSZ control NCA.

    Uses the nsz library for proper NCA decryption. Finds the control NCA
    (contentType == 2), reads its RomFS section, and extracts the NACP
    displayVersion field at offset 0x17260.
    Returns the display version string (e.g. "1.3.1") or None.
    """
    try:
        ext = os.path.splitext(filepath)[1].lower()
        if ext not in ('.nsp', '.nsz'):
            return None

        if not Keys.keys_loaded:
            return None

        container = factory(Path(filepath).resolve())
        container.open(filepath, 'rb')

        try:
            for f in container:
                if not (hasattr(f, 'header') and hasattr(f.header, 'contentType')):
                    continue
                if f.header.contentType != 2:  # Control NCA
                    continue

                for section in f:
                    section.seek(0)
                    data = section.read(min(section.size, 0x18000))

                    # displayVersion is at offset 0x17260 in the RomFS section
                    offset = 0x17260
                    if offset + 0x10 <= len(data):
                        raw = data[offset:offset + 0x10]
                        null_pos = raw.find(b'\x00')
                        if null_pos > 0:
                            raw = raw[:null_pos]
                        version_str = raw.decode('ascii', errors='ignore').strip()
                        if re.match(r'^\d+\.\d+', version_str):
                            return version_str
                break
        finally:
            container.close()

        return None
    except Exception as e:
        logger.debug(f"Could not extract display version from {filepath}: {e}")
        return None

def get_game_latest_version(all_existing_versions):
    return max(v['version'] for v in all_existing_versions)

def get_all_existing_versions(titleid):
    global _versions_db
    if _versions_db is None:
        logger.error("versions_db is not loaded. Call load_titledb first.")
        return []

    titleid = titleid.lower()
    if titleid not in _versions_db:
        # print(f'Title ID not in versions.json: {titleid.upper()}')
        return []

    versions_from_db = _versions_db[titleid].keys()
    return [
        {
            'version': int(version_from_db),
            'update_number': get_update_number(version_from_db),
            'release_date': _versions_db[titleid][str(version_from_db)],
        }
        for version_from_db in versions_from_db
    ]

def get_all_app_existing_versions(app_id):
    global _cnmts_db
    if _cnmts_db is None:
        logger.error("cnmts_db is not loaded. Call load_titledb first.")
        return None

    app_id = app_id.lower()
    if app_id in _cnmts_db:
        versions_from_cnmts_db = _cnmts_db[app_id].keys()
        if len(versions_from_cnmts_db):
            return sorted(versions_from_cnmts_db)
        else:
            logger.warning(f'No keys in cnmts.json for app ID: {app_id.upper()}')
            return None
    else:
        # print(f'DLC app ID not in cnmts.json: {app_id.upper()}')
        return None
    
def get_app_id_version_from_versions_txt(app_id):
    global _versions_txt_db
    if _versions_txt_db is None:
        logger.error("versions_txt_db is not loaded. Call load_titledb first.")
        return None
    return _versions_txt_db.get(app_id, None)
    
def get_suggested_content(owned_title_ids):
    """Build suggestions directly from TitleDB, filter owned games, return all candidates."""
    from datetime import datetime, timedelta

    global _titles_db
    if _titles_db is None:
        logger.error("titles_db is not loaded. Call load_titledb first.")
        return {'suggestions': []}

    candidates = []
    now = datetime.now()
    ninety_days_ago = now - timedelta(days=90)

    for tid, entry in _titles_db.items():
        # Base games only (ID ends in 000)
        entry_id = entry.get('id', tid)
        if not entry_id or not entry_id.upper().endswith('000'):
            continue
        tid_upper = entry_id.upper()

        # Skip owned games
        if tid_upper in owned_title_ids or tid_upper.lower() in owned_title_ids:
            continue

        name = entry.get('name', '')
        if not name:
            continue

        # Filter out demos
        name_lower = name.lower()
        if 'demo' in name_lower or 'trial ver' in name_lower:
            continue
        cat_str = str(entry.get('category', '')).lower()
        if 'demo' in cat_str:
            continue

        release_date = str(entry.get('releaseDate', '') or '')
        # Parse release date
        parsed_date = None
        if release_date:
            for fmt in ('%Y%m%d', '%Y-%m-%d', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S'):
                try:
                    parsed_date = datetime.strptime(release_date[:19], fmt)
                    break
                except (ValueError, TypeError):
                    continue
            # Normalize to YYYY-MM-DD for display
            if parsed_date:
                release_date = parsed_date.strftime('%Y-%m-%d')

        # Build intro text (prefer intro, fall back to description)
        intro = entry.get('intro', '') or ''
        if not intro:
            desc = entry.get('description', '') or ''
            if desc:
                intro = desc[:200]

        raw_cat = entry.get('category', '') or ''
        # Normalize category to a comma-separated string
        if isinstance(raw_cat, list):
            category = ', '.join(str(c) for c in raw_cat if c)
        else:
            category = str(raw_cat)
        # Filter out Japanese-only category strings
        if category and not any(c.isascii() and c.isalpha() for c in category):
            category = ''

        # Extract first screenshot URL if available
        screenshots = entry.get('screenshots', []) or []
        screenshot = screenshots[0] if screenshots else ''

        candidates.append({
            'id': tid_upper,
            'name': name,
            'icon_url': entry.get('iconUrl', '') or '',
            'banner_url': entry.get('bannerUrl', '') or '',
            'screenshot': screenshot,
            'release_date': release_date,
            'parsed_date': parsed_date,
            'publisher': entry.get('publisher', '') or '',
            'category': category,
            'intro': intro,
            'is_new': parsed_date is not None and parsed_date >= ninety_days_ago and parsed_date <= now,
            'is_unreleased': parsed_date is not None and parsed_date > now,
        })

    # Sort: new releases first (newest first), then the rest alphabetically
    # Unreleased games go at the end, sorted by date ascending (soonest first)
    new_releases = [c for c in candidates if c['is_new'] and not c['is_unreleased']]
    new_releases.sort(key=lambda x: x['parsed_date'], reverse=True)
    rest = [c for c in candidates if not c['is_new'] and not c['is_unreleased']]
    rest.sort(key=lambda x: x['name'].lower())
    upcoming = [c for c in candidates if c['is_unreleased']]
    upcoming.sort(key=lambda x: x['parsed_date'])

    all_suggestions = new_releases + rest + upcoming

    # Strip non-serializable fields
    for item in all_suggestions:
        del item['parsed_date']

    return {'suggestions': all_suggestions}


def get_all_existing_dlc(title_id):
    global _cnmts_db
    if _cnmts_db is None:
        logger.error("cnmts_db is not loaded. Call load_titledb first.")
        return []

    title_id = title_id.lower()
    dlcs = []
    for app_id in _cnmts_db.keys():
        for version, version_description in _cnmts_db[app_id].items():
            if version_description.get('titleType') == 130 and version_description.get('otherApplicationId') == title_id:
                if app_id.upper() not in dlcs:
                    dlcs.append(app_id.upper())
    return dlcs
