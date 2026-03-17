import os
import sys
import json
import hashlib
import tempfile
import subprocess
import logging

from constants import COMBINE_STATE_FILE, APP_TYPE_BASE, APP_TYPE_UPD, APP_TYPE_DLC
from db import Apps, Titles, Files, db

logger = logging.getLogger('main')

ACORN_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'acorn.py')


def get_combinable_files(title_id):
    """Get all owned file paths for a title (base + latest update + all DLC).
    Returns list of file paths. Skips if only 1 file."""
    title = Titles.query.filter_by(title_id=title_id).first()
    if not title:
        return []

    owned_apps = Apps.query.filter_by(title_id=title.id, owned=True).all()
    if not owned_apps:
        return []

    file_paths = []
    seen_files = set()

    # Collect base game files
    base_apps = [a for a in owned_apps if a.app_type == APP_TYPE_BASE]
    for app in base_apps:
        for f in app.files:
            if f.id not in seen_files:
                seen_files.add(f.id)
                file_paths.append(f.filepath)

    # Collect latest owned update only
    update_apps = [a for a in owned_apps if a.app_type == APP_TYPE_UPD]
    if update_apps:
        # Sort by version descending, pick latest
        update_apps.sort(key=lambda a: int(a.app_version) if a.app_version and a.app_version.isdigit() else 0, reverse=True)
        latest_update = update_apps[0]
        for f in latest_update.files:
            if f.id not in seen_files:
                seen_files.add(f.id)
                file_paths.append(f.filepath)

    # Collect all owned DLC
    dlc_apps = [a for a in owned_apps if a.app_type == APP_TYPE_DLC]
    for app in dlc_apps:
        for f in app.files:
            if f.id not in seen_files:
                seen_files.add(f.id)
                file_paths.append(f.filepath)

    # Nothing to combine if only 1 file
    if len(file_paths) <= 1:
        return []

    return file_paths


def get_combine_state():
    """Load combine state from disk."""
    if os.path.exists(COMBINE_STATE_FILE):
        try:
            with open(COMBINE_STATE_FILE, 'r') as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_combine_state(state):
    """Save combine state to disk."""
    os.makedirs(os.path.dirname(COMBINE_STATE_FILE), exist_ok=True)
    with open(COMBINE_STATE_FILE, 'w') as f:
        json.dump(state, f)


def compute_files_hash(file_paths):
    """Compute a hash of file paths and sizes for change detection."""
    items = []
    for p in sorted(file_paths):
        try:
            size = os.path.getsize(p)
        except OSError:
            size = 0
        items.append(f"{p}:{size}")
    return hashlib.sha256('\n'.join(items).encode()).hexdigest()


def _find_title_xcis(title_id, output_dir):
    """Find XCI files for a title_id in output_dir using exact bracket matching."""
    needle = f'[{title_id}]'
    try:
        return [
            os.path.join(output_dir, f)
            for f in os.listdir(output_dir)
            if f.endswith('.xci') and needle in f
        ]
    except OSError:
        return []


def get_combined_file(title_id, output_dir):
    """Find the combined XCI file for a title_id in output_dir."""
    matches = _find_title_xcis(title_id, output_dir)
    if matches:
        return matches[0]
    return None


def combine_title(title_id, output_dir, keys_path=None):
    """Combine all owned files for a title into a multi-content XCI.

    Returns dict with 'status' ('success', 'skipped', 'error') and optional 'message'.
    """
    file_paths = get_combinable_files(title_id)
    if not file_paths:
        return {'status': 'skipped', 'message': 'Not enough files to combine'}

    # Check state hash for change detection
    files_hash = compute_files_hash(file_paths)
    state = get_combine_state()
    title_state = state.get(title_id, {})
    if title_state.get('files_hash') == files_hash:
        existing = get_combined_file(title_id, output_dir)
        if existing:
            return {'status': 'skipped', 'message': 'Already up to date'}

    # Ensure output dir exists
    os.makedirs(output_dir, exist_ok=True)

    # Delete old combined XCI for this title before creating new one
    for old_file in _find_title_xcis(title_id, output_dir):
        try:
            os.remove(old_file)
            logger.info(f"Removed old combined XCI: {old_file}")
        except OSError as e:
            logger.warning(f"Failed to remove old combined XCI {old_file}: {e}")

    # Write file list to temp file
    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as tmp:
            for p in file_paths:
                tmp.write(p + '\n')
            tmp_path = tmp.name

        # Build ACORN command
        cmd = [sys.executable, ACORN_PATH, '-tfile', tmp_path, '-o', output_dir]

        logger.info(f"Running ACORN for {title_id} with {len(file_paths)} files")
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600,  # 1 hour timeout
            cwd=os.path.dirname(ACORN_PATH)  # /app, where prod.keys symlink lives
        )

        if result.stdout:
            logger.debug(f"ACORN stdout for {title_id}: {result.stdout[:1000]}")
        if result.returncode != 0:
            logger.error(f"ACORN failed for {title_id}: {result.stderr}")
            return {'status': 'error', 'message': result.stderr or 'ACORN process failed'}

        logger.info(f"ACORN completed for {title_id}")

        # Update state
        state[title_id] = {
            'files_hash': files_hash,
            'output_dir': output_dir
        }
        save_combine_state(state)

        combined = get_combined_file(title_id, output_dir)
        return {
            'status': 'success',
            'message': os.path.basename(combined) if combined else 'Combined XCI created'
        }

    except subprocess.TimeoutExpired:
        logger.error(f"ACORN timed out for {title_id}")
        return {'status': 'error', 'message': 'ACORN process timed out'}
    except Exception as e:
        logger.error(f"Error combining {title_id}: {e}")
        return {'status': 'error', 'message': str(e)}
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass


def combine_all_titles(app_context, output_dir, keys_path=None):
    """Combine all titles that have multiple owned files.
    Must be called with app context."""
    with app_context:
        from library import generate_grouped_library
        games = generate_grouped_library()

        results = {'success': 0, 'skipped': 0, 'error': 0}
        for game in games:
            title_id = game['title_id']
            result = combine_title(title_id, output_dir, keys_path)
            results[result['status']] = results.get(result['status'], 0) + 1
            if result['status'] == 'success':
                logger.info(f"Combined {title_id}: {result.get('message', '')}")
            elif result['status'] == 'error':
                logger.error(f"Failed to combine {title_id}: {result.get('message', '')}")

        logger.info(f"Combine all complete - Success: {results['success']}, Skipped: {results['skipped']}, Errors: {results['error']}")
        return results
