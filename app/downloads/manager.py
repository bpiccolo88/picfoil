import os
import json
import time
import shutil
import threading
import logging

from constants import DOWNLOAD_STATE_FILE, ALLOWED_EXTENSIONS
from downloads import prowlarr, torrent_client

logger = logging.getLogger('main')

_state_lock = threading.Lock()


def _load_state():
    if os.path.exists(DOWNLOAD_STATE_FILE):
        try:
            with open(DOWNLOAD_STATE_FILE, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.error(f"Failed to load download state from {DOWNLOAD_STATE_FILE}: {e}")
    return {"downloads": []}


def _save_state(state):
    # Write to temp file then atomic rename to prevent corruption if the process
    # crashes mid-write (os.replace is atomic on POSIX)
    os.makedirs(os.path.dirname(DOWNLOAD_STATE_FILE), exist_ok=True)
    tmp = DOWNLOAD_STATE_FILE + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(state, f)
    os.replace(tmp, DOWNLOAD_STATE_FILE)


def _get_client(settings):
    """Create a Transmission client from settings."""
    tc = settings.get('downloads', {}).get('torrent_client', {})
    return torrent_client.TransmissionClient(
        url=tc.get('url', ''),
        username=tc.get('username', ''),
        password=tc.get('password', '')
    )


def search_content(settings, query):
    """Search Prowlarr with a query string. Returns list of results."""
    p = settings.get('downloads', {}).get('prowlarr', {})
    if not p.get('enabled') or not p.get('url') or not p.get('api_key'):
        return []

    return prowlarr.search(
        url=p['url'],
        api_key=p['api_key'],
        query=query,
        categories=p.get('categories'),
        indexer_ids=p.get('indexer_ids')
    )


def start_download(settings, download_url, title_id='', content_type=''):
    """Add a torrent to Transmission and track it in state."""
    tc_settings = settings.get('downloads', {}).get('torrent_client', {})
    client = _get_client(settings)

    labels = []
    category = tc_settings.get('category', 'picfoil')
    if category:
        labels = [category]

    torrent_hash = client.add_torrent(
        download_url=download_url,
        labels=labels
    )

    if not torrent_hash:
        raise Exception("Failed to add torrent - no hash returned")

    with _state_lock:
        state = _load_state()
        # Don't add duplicate entries for the same hash
        existing_hashes = {d["hash"] for d in state["downloads"]}
        if torrent_hash not in existing_hashes:
            state["downloads"].append({
                "hash": torrent_hash,
                "title_id": title_id,
                "content_type": content_type,
                "status": "downloading",
                "added_at": int(time.time())
            })
            _save_state(state)

    logger.info(f"Started download for {title_id}: {torrent_hash}")
    return torrent_hash


def get_active_downloads(settings):
    """Get all tracked downloads with live status from Transmission."""
    tc_settings = settings.get('downloads', {}).get('torrent_client', {})
    if not tc_settings.get('url'):
        return []

    try:
        client = _get_client(settings)
        category = tc_settings.get('category', 'picfoil')
        torrents = client.get_torrents(labels=[category] if category else None)
    except Exception as e:
        logger.error(f"Failed to get torrents from Transmission: {e}")
        return []

    # Merge with our state for title_id info
    with _state_lock:
        state = _load_state()

    state_map = {d["hash"]: d for d in state.get("downloads", [])}
    torrent_hashes = {t["hash"] for t in torrents}

    result = []
    for t in torrents:
        tracked = state_map.get(t["hash"])
        if not tracked:
            if not t["completed"]:
                continue
            # Orphan: completed in Transmission with our label, not yet in state.
            # Show it while awaiting verification on the next check cycle.
            result.append({
                "hash": t["hash"],
                "name": t["name"],
                "title_id": "",
                "content_type": "",
                "status": "completed",
                "progress": t["progress"],
                "size": t["size"],
                "eta": t["eta"],
                "download_speed": t["download_speed"],
                "added_at": t.get("added_date", 0),
                "failed_reason": "",
                "source_files": [],
                "copied_to": [],
            })
            continue
        # Use tracked status if it's a terminal state (verified/failed)
        tracked_status = tracked.get("status", "")
        if tracked_status in ("verified", "failed"):
            status = tracked_status
        else:
            status = "completed" if t["completed"] else "downloading"
        result.append({
            "hash": t["hash"],
            "name": t["name"],
            "title_id": tracked.get("title_id", ""),
            "content_type": tracked.get("content_type", ""),
            "status": status,
            "progress": t["progress"],
            "size": t["size"],
            "eta": t["eta"],
            "download_speed": t["download_speed"],
            "added_at": tracked.get("added_at", t.get("added_date", 0)),
            "failed_reason": tracked.get("failed_reason", ""),
            "source_files": tracked.get("source_files", []),
            "copied_to": tracked.get("copied_to", []),
        })

    # Include tracked downloads no longer in Transmission (orphaned or failed)
    state_changed = False
    for d in state.get("downloads", []):
        if d["hash"] not in torrent_hashes:
            if d.get("status") == "downloading":
                # Torrent removed from Transmission while we were tracking it
                d["status"] = "failed"
                d["failed_reason"] = "torrent no longer in Transmission"
                state_changed = True
            if d.get("status") == "failed":
                result.append({
                    "hash": d["hash"],
                    "name": "",
                    "title_id": d.get("title_id", ""),
                    "content_type": d.get("content_type", ""),
                    "status": d["status"],
                    "progress": 100,
                    "size": 0,
                    "eta": -1,
                    "download_speed": 0,
                    "added_at": d.get("added_at", 0),
                    "failed_reason": d.get("failed_reason", ""),
                    "source_files": d.get("source_files", []),
                    "copied_to": d.get("copied_to", []),
                })

    if state_changed:
        with _state_lock:
            _save_state(state)

    return result


def cancel_download(settings, torrent_hash, delete_files=False):
    """Cancel a download and remove from state. Returns (success, error_message)."""
    try:
        client = _get_client(settings)
        client.delete_torrent(torrent_hash, delete_files=delete_files)
    except Exception as e:
        logger.error(f"Failed to remove torrent {torrent_hash}: {e}")
        return False, str(e)

    with _state_lock:
        state = _load_state()
        state["downloads"] = [d for d in state["downloads"] if d["hash"] != torrent_hash]
        _save_state(state)

    return True, ""


def dismiss_download(torrent_hash):
    """Dismiss a download from the UI (remove from state only)."""
    with _state_lock:
        state = _load_state()
        state["downloads"] = [d for d in state["downloads"] if d["hash"] != torrent_hash]
        _save_state(state)


def delete_download(settings, torrent_hash):
    """Delete a download: remove from state and from Transmission (with files)."""
    with _state_lock:
        state = _load_state()
        state["downloads"] = [d for d in state["downloads"] if d["hash"] != torrent_hash]
        _save_state(state)

    try:
        client = _get_client(settings)
        client.delete_torrent(torrent_hash, delete_files=True)
    except Exception as e:
        logger.warning(f"Could not remove torrent {torrent_hash} from Transmission: {e}")
        return False, str(e)
    return True, ""


def recheck_download(torrent_hash):
    """Reset a failed download to 'downloading' so the next check cycle re-verifies it."""
    with _state_lock:
        state = _load_state()
        for d in state["downloads"]:
            if d["hash"] == torrent_hash:
                d["status"] = "downloading"
                d.pop("failed_reason", None)
                d.pop("source_files", None)
                d.pop("copied_to", None)
                break
        _save_state(state)


def _find_file_extensions(directory):
    """Walk a directory and return a sorted set of file extensions found."""
    extensions = set()
    if not os.path.exists(directory):
        return extensions
    for root, _dirs, files in os.walk(directory):
        for f in files:
            if '.' in f:
                extensions.add('.' + f.rsplit('.', 1)[-1].lower())
    return sorted(extensions)


def _find_switch_files(directory):
    """Walk a directory and return paths to files with allowed Switch extensions."""
    switch_files = []
    if not os.path.exists(directory):
        return switch_files
    for root, _dirs, files in os.walk(directory):
        for f in files:
            ext = f.rsplit('.', 1)[-1].lower() if '.' in f else ''
            if ext in ALLOWED_EXTENSIONS:
                switch_files.append(os.path.join(root, f))
    return switch_files


def _find_rar_files(directory):
    """Walk a directory and return paths to .rar files."""
    rar_files = []
    if not os.path.exists(directory):
        return rar_files
    for root, _dirs, files in os.walk(directory):
        for f in files:
            if f.lower().endswith('.rar'):
                rar_files.append(os.path.join(root, f))
    return rar_files


def _extract_archive(archive_path, dest_dir):
    """Extract an archive (RAR, 7z, zip, etc.) to dest_dir using bsdtar. Returns (success, error_message)."""
    import subprocess
    try:
        os.makedirs(dest_dir, exist_ok=True)
        result = subprocess.run(
            ['bsdtar', 'xf', archive_path, '-C', dest_dir],
            capture_output=True, text=True, timeout=600
        )
        if result.returncode != 0:
            return False, result.stderr.strip() or f"bsdtar exit code {result.returncode}"
        return True, ""
    except FileNotFoundError:
        return False, "bsdtar not installed (libarchive-tools)"
    except subprocess.TimeoutExpired:
        return False, "extraction timed out"
    except Exception as e:
        return False, str(e)


def verify_and_copy_download(torrent_info, tracked_state, settings):
    """Verify a completed download contains valid Switch content and copy to library.

    Returns (status, updates_dict) where status is 'verified' or 'failed',
    and updates_dict contains fields to merge into the tracked state.
    """
    import titles as titles_lib

    # Ensure titledb is loaded for identification
    try:
        titles_lib.load_titledb()
    except Exception as e:
        logger.warning(f"Could not load titledb for verification: {e}")

    extract_dir = None

    try:
        torrent_name = torrent_info.get("name", "")
        # Use the configured save_path from settings rather than Transmission's downloadDir,
        # since Transmission runs in a different container with different mount paths
        tc_settings = settings.get('downloads', {}).get('torrent_client', {})
        save_path = tc_settings.get('save_path', '') or torrent_info.get("save_path", "")
        if not save_path:
            return "failed", {"failed_reason": "no save path configured"}

        # 1. Find Switch files in the torrent's content
        # Multi-file torrents: files are in save_path/torrent_name/
        # Single-file torrents: file is save_path/torrent_name (the file itself)
        torrent_dir = os.path.join(save_path, torrent_name) if torrent_name else ""
        if torrent_dir and os.path.isdir(torrent_dir):
            search_dir = torrent_dir
            switch_files = _find_switch_files(torrent_dir)
        elif torrent_dir and os.path.isfile(torrent_dir):
            # Single file torrent — check if it's a Switch file
            ext = torrent_name.rsplit('.', 1)[-1].lower() if '.' in torrent_name else ''
            switch_files = [torrent_dir] if ext in ALLOWED_EXTENSIONS else []
            search_dir = save_path
        else:
            # Fallback: scan entire save_path
            search_dir = save_path
            switch_files = _find_switch_files(save_path)

        # If no Switch files found, try extracting RAR archives
        if not switch_files:
            rar_files = _find_rar_files(search_dir)
            if not rar_files:
                found_exts = _find_file_extensions(search_dir)
                ext_hint = f" (found: {', '.join(found_exts)})" if found_exts else ""
                return "failed", {"failed_reason": f"no valid Switch files found in download{ext_hint}"}

            extract_dir = os.path.join(save_path, '.picfoil_extracted', torrent_name or 'unknown')
            extraction_errors = []
            for rar_path in rar_files:
                logger.info(f"Extracting archive: {rar_path}")
                success, error = _extract_archive(rar_path, extract_dir)
                if not success:
                    extraction_errors.append(error)

            switch_files = _find_switch_files(extract_dir)
            if not switch_files:
                if extraction_errors:
                    return "failed", {"failed_reason": f"RAR extraction failed: {extraction_errors[0]}"}
                found_exts = _find_file_extensions(extract_dir)
                ext_hint = f" (found: {', '.join(found_exts)})" if found_exts else ""
                return "failed", {"failed_reason": f"no valid Switch files found after extracting RAR{ext_hint}"}
            if extraction_errors:
                logger.warning(f"RAR extraction had errors but found valid files, continuing: {extraction_errors[0]}")

        source_filenames = [os.path.basename(f) for f in switch_files]
        requested_title_id = tracked_state.get("title_id", "")

        # 2. Identify and verify each file
        verified_files = []
        for filepath in switch_files:
            try:
                identification, success, contents, error = titles_lib.identify_file(filepath)
            except Exception as e:
                logger.error(f"Exception identifying {filepath}: {e}")
                return "failed", {
                    "failed_reason": f"error identifying file: {e}",
                    "source_files": source_filenames,
                }

            if not success or not contents:
                return "failed", {
                    "failed_reason": f"could not identify file: {error or 'unknown error'}",
                    "source_files": source_filenames,
                }

            # 3. Verify title_id match (if we have a requested title_id)
            if requested_title_id:
                file_title_id = contents[0].get("title_id") or ""
                # Compare first 13 chars (base title ID portion)
                if not file_title_id or file_title_id[:13].upper() != requested_title_id[:13].upper():
                    return "failed", {
                        "failed_reason": f"title_id mismatch: expected {requested_title_id}, got {file_title_id}",
                        "source_files": source_filenames,
                    }

            verified_files.append(filepath)

        # 4. Copy to library
        library_paths = settings.get('library', {}).get('paths', [])
        if not library_paths:
            return "failed", {
                "failed_reason": "no library path configured",
                "source_files": source_filenames,
            }

        dest_dir = library_paths[0]
        copied_to = []
        for filepath in verified_files:
            dest_path = os.path.join(dest_dir, os.path.basename(filepath))
            if os.path.exists(dest_path):
                logger.info(f"File already exists at {dest_path}, skipping copy")
                copied_to.append(dest_path)
                continue
            try:
                os.makedirs(dest_dir, exist_ok=True)
                shutil.copy2(filepath, dest_path)
                # Verify copy is complete
                src_size = os.path.getsize(filepath)
                dst_size = os.path.getsize(dest_path)
                if src_size != dst_size:
                    os.remove(dest_path)
                    return "failed", {
                        "failed_reason": f"copy incomplete: {dst_size}/{src_size} bytes",
                        "source_files": source_filenames,
                    }
                logger.info(f"Copied {filepath} to {dest_path}")
                copied_to.append(dest_path)
            except Exception as e:
                # Clean up partial file if it exists
                if os.path.exists(dest_path):
                    try:
                        os.remove(dest_path)
                    except OSError:
                        pass
                logger.error(f"Failed to copy {filepath} to {dest_path}: {e}")
                return "failed", {
                    "failed_reason": f"copy failed: {e}",
                    "source_files": source_filenames,
                }

        status = "verified"
        updates = {
            "source_files": source_filenames,
            "copied_to": copied_to,
            "verified_at": int(time.time()),
        }

        # Clean up extraction directory after successful verification
        if extract_dir and os.path.isdir(extract_dir):
            shutil.rmtree(extract_dir, ignore_errors=True)
            logger.info(f"Cleaned up extraction directory: {extract_dir}")

        return status, updates

    finally:
        # load_titledb() increments identification_in_progress_count and unload_titledb()
        # only frees memory when the count reaches 0. Every load must be paired with a
        # decrement + unload call, otherwise the TitleDB stays in memory forever.
        titles_lib.identification_in_progress_count -= 1
        titles_lib.unload_titledb()


_check_lock = threading.Lock()

def check_completed_downloads(app, settings):
    """Check for completed downloads, verify them, and copy to library."""
    if not _check_lock.acquire(blocking=False):
        logger.debug("Skipping check_completed_downloads: previous run still active")
        return  # previous run still active
    try:
        _check_completed_downloads_inner(app, settings)
    finally:
        _check_lock.release()

def _check_completed_downloads_inner(app, settings):
    tc_settings = settings.get('downloads', {}).get('torrent_client', {})
    if not tc_settings.get('url'):
        return

    try:
        client = _get_client(settings)
        category = tc_settings.get('category', 'picfoil')
        torrents = client.get_torrents(labels=[category] if category else None)
    except Exception as e:
        logger.error(f"Failed to check completed downloads: {e}")
        return

    completed = [t for t in torrents if t["completed"]]
    if not completed:
        return

    with _state_lock:
        state = _load_state()
        state_map = {d["hash"]: d for d in state.get("downloads", [])}

    newly_completed = []
    orphaned_completed = []
    for t in completed:
        tracked = state_map.get(t["hash"])
        if tracked and tracked.get("status") == "downloading":
            newly_completed.append((t, tracked))
        elif not tracked:
            # Completed torrent with our label but no state entry — reconcile it
            orphaned_completed.append((t, {"title_id": "", "content_type": ""}))

    if not newly_completed and not orphaned_completed:
        return

    if newly_completed:
        logger.info(f"{len(newly_completed)} download(s) completed, verifying...")
    if orphaned_completed:
        logger.info(f"{len(orphaned_completed)} orphaned torrent(s) found in Transmission, verifying...")

    any_verified = False
    for torrent_info, tracked in newly_completed + orphaned_completed:
        status, updates = verify_and_copy_download(torrent_info, tracked, settings)
        logger.info(f"Download {torrent_info['hash']}: {status}" +
                     (f" - {updates.get('failed_reason', '')}" if status == 'failed' else ''))

        with _state_lock:
            state = _load_state()
            found = False
            for d in state["downloads"]:
                if d["hash"] == torrent_info["hash"]:
                    d["status"] = status
                    d.update(updates)
                    found = True
                    break
            if not found:
                state["downloads"].append({
                    "hash": torrent_info["hash"],
                    "title_id": tracked.get("title_id", ""),
                    "content_type": tracked.get("content_type", ""),
                    "status": status,
                    "added_at": torrent_info.get("added_date", int(time.time())),
                    **updates,
                })
            _save_state(state)

        if status == "verified":
            any_verified = True

    if any_verified:
        from app import post_library_change
        post_library_change()
