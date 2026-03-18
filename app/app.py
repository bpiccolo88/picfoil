from flask import Flask, render_template, request, redirect, url_for, jsonify, send_from_directory, Response
from flask_login import LoginManager
from scheduler import init_scheduler, validate_interval_string
from functools import wraps
from file_watcher import Watcher
import threading
import logging
import sys
import copy
import flask.cli
from datetime import timedelta
flask.cli.show_server_banner = lambda *args: None
from constants import *
from settings import *
from db import *
from shop import *
from auth import *
import titles as titles_lib
from utils import *
from library import *
from combine import combine_title, combine_all_titles, get_combined_file
from downloads import prowlarr as prowlarr_client, torrent_client, manager as download_manager, suggested as suggested_content
import titledb
import os

def init():
    global watcher
    global watcher_thread
    # Create and start the file watcher
    logger.info('Initializing File Watcher...')
    watcher = Watcher(on_library_change)
    watcher_thread = threading.Thread(target=watcher.run)
    watcher_thread.daemon = True
    watcher_thread.start()

    # Load initial configuration
    logger.info('Loading initial configuration...')
    reload_conf()

    # init libraries
    library_paths = app_settings['library']['paths']
    init_libraries(app, watcher, library_paths)

    # Initialize and schedule jobs
    logger.info('Initializing Scheduler...')
    init_scheduler(app)
    scan_interval_str = app_settings.get('scheduler', {}).get('scan_interval', '12h')
    schedule_update_and_scan_job(app, scan_interval_str, run_first=True, run_once=True)

    # Check for completed downloads every 30 seconds
    from datetime import timedelta
    app.scheduler.add_job(
        job_id='check_completed_downloads',
        func=lambda: download_manager.check_completed_downloads(app, load_settings()),
        interval=timedelta(seconds=30),
        run_first=False
    )

os.makedirs(CONFIG_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

## Global variables
app_settings = {}
watcher = None
watcher_thread = None
# Create a global variable and lock for scan_in_progress
scan_in_progress = False
scan_lock = threading.Lock()
# Global flag for titledb update status
is_titledb_update_running = False
titledb_update_lock = threading.Lock()

# Configure logging
formatter = ColoredFormatter(
    '[%(asctime)s.%(msecs)03d] %(levelname)s (%(module)s) %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)

logging.basicConfig(
    level=logging.INFO,
    handlers=[handler]
)

# Create main logger
logger = logging.getLogger('main')
logger.setLevel(logging.DEBUG)

# Apply filter to hide date from http access logs
logging.getLogger('werkzeug').addFilter(FilterRemoveDateFromWerkzeugLogs())

# Suppress specific Alembic INFO logs
logging.getLogger('alembic.runtime.migration').setLevel(logging.WARNING)

@login_manager.user_loader
def load_user(user_id):
    # since the user_id is just the primary key of our user table, use it in the query for the user
    return User.query.filter_by(id=user_id).first()

def reload_conf():
    global app_settings
    global watcher
    app_settings = load_settings()

def on_library_change(events):
    # TODO refactor: group modified and created together
    with app.app_context():
        created_events = [e for e in events if e.type == 'created']
        modified_events = [e for e in events if e.type != 'created']

        for event in modified_events:
            if event.type == 'moved':
                if file_exists_in_db(event.src_path):
                    # update the path
                    update_file_path(event.directory, event.src_path, event.dest_path)
                else:
                    # add to the database
                    event.src_path = event.dest_path
                    created_events.append(event)

            elif event.type == 'deleted':
                # delete the file from library if it exists
                delete_file_by_filepath(event.src_path)

            elif event.type == 'modified':
                # can happen if file copy has started before the app was running
                add_files_to_library(event.directory, [event.src_path])

        if created_events:
            directories = list(set(e.directory for e in created_events))
            for library_path in directories:
                new_files = [e.src_path for e in created_events if e.directory == library_path]
                add_files_to_library(library_path, new_files)

    post_library_change()

def create_app():
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = OWNFOIL_DB
    # TODO: generate random secret_key
    app.config['SECRET_KEY'] = '8accb915665f11dfa15c2db1a4e8026905f57716'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    db.init_app(app)
    migrate.init_app(app, db)
    login_manager.init_app(app)

    app.register_blueprint(auth_blueprint)

    return app

# Create app
app = create_app()


def tinfoil_error(error):
    return jsonify({
        'error': error
    })

def tinfoil_access(f):
    @wraps(f)
    def _tinfoil_access(*args, **kwargs):
        reload_conf()
        hauth_success = None
        auth_success = None
        request.verified_host = None
        # Host verification to prevent hotlinking
        #Tinfoil doesn't send Hauth for file grabs, only directories, so ignore get_game endpoints.
        host_verification = "/api/get_game" not in request.path and (request.is_secure or request.headers.get("X-Forwarded-Proto") == "https")
        if host_verification:
            request_host = request.host
            request_hauth = request.headers.get('Hauth')
            logger.info(f"Secure Tinfoil request from remote host {request_host}, proceeding with host verification.")
            shop_host = app_settings["shop"].get("host")
            shop_hauth = app_settings["shop"].get("hauth")
            if not shop_host:
                logger.error("Missing shop host configuration, Host verification is disabled.")

            elif request_host != shop_host:
                logger.warning(f"Incorrect URL referrer detected: {request_host}.")
                error = f"Incorrect URL `{request_host}`."
                hauth_success = False

            elif not shop_hauth:
                # Try authentication, if an admin user is logging in then set the hauth
                auth_success, auth_error, auth_is_admin =  basic_auth(request)
                if auth_success and auth_is_admin:
                    shop_settings = app_settings['shop']
                    shop_settings['hauth'] = request_hauth
                    set_shop_settings(shop_settings)
                    logger.info(f"Successfully set Hauth value for host {request_host}.")
                    hauth_success = True
                else:
                    logger.warning(f"Hauth value not set for host {request_host}, Host verification is disabled. Connect to the shop from Tinfoil with an admin account to set it.")

            elif request_hauth != shop_hauth:
                logger.warning(f"Incorrect Hauth detected for host: {request_host}.")
                error = f"Incorrect Hauth for URL `{request_host}`."
                hauth_success = False

            else:
                hauth_success = True
                request.verified_host = shop_host

            if hauth_success is False:
                return tinfoil_error(error)
        
        # Now checking auth if shop is private
        if not app_settings['shop']['public']:
            # Shop is private
            if auth_success is None:
                auth_success, auth_error, _ = basic_auth(request)
            if not auth_success:
                return tinfoil_error(auth_error)
        # Auth success
        return f(*args, **kwargs)
    return _tinfoil_access

def access_shop():
    return render_template('index.html', title='Library', admin_account_created=admin_account_created())

@access_required('shop')
def access_shop_auth():
    return access_shop()

@app.route('/')
def index():

    @tinfoil_access
    def access_tinfoil_shop():
        shop = {
            "success": app_settings['shop']['motd']
        }
        
        if request.verified_host is not None:
            # enforce client side host verification
            shop["referrer"] = f"https://{request.verified_host}"
            
        shop["files"] = gen_shop_files(db)

        if app_settings['shop']['encrypt']:
            return Response(encrypt_shop(shop), mimetype='application/octet-stream')

        return jsonify(shop)
    
    if all(header in request.headers for header in TINFOIL_HEADERS):
    # if True:
        logger.info(f"Tinfoil connection from {request.remote_addr}")
        return access_tinfoil_shop()
    
    if not app_settings['shop']['public']:
        return access_shop_auth()
    return access_shop()

@app.route('/settings')
@access_required('admin')
def settings_page():
    with open(os.path.join(TITLEDB_DIR, 'languages.json')) as f:
        languages = json.load(f)
        languages = dict(sorted(languages.items()))
    return render_template(
        'settings.html',
        title='Settings',
        languages_from_titledb=languages,
        admin_account_created=admin_account_created())

@app.get('/api/settings')
@access_required('admin')
def get_settings_api():
    reload_conf()
    settings = copy.deepcopy(app_settings)
    if settings['shop'].get('hauth'):
        settings['shop']['hauth'] = True
    else:
        settings['shop']['hauth'] = False
    return jsonify(settings)

@app.post('/api/settings/titles')
@access_required('admin')
def set_titles_settings_api():
    reload_conf()
    title_settings = request.json
    region = title_settings['region']
    language = title_settings['language']
    with open(os.path.join(TITLEDB_DIR, 'languages.json')) as f:
        languages = json.load(f)
        languages = dict(sorted(languages.items()))

    if region not in languages or language not in languages[region]:
        resp = {
            'success': False,
            'errors': [{
                    'path': 'titles',
                    'error': f"The region/language pair {region}/{language} is not available."
                }]
        }
        return jsonify(resp)
    
    if region != app_settings['titles']['region'] or language != app_settings['titles']['language']:
        set_titles_settings(region, language)
        reload_conf()
        titledb.update_titledb(app_settings)
        post_library_change()

    resp = {
        'success': True,
        'errors': []
    } 
    return jsonify(resp)

@app.post('/api/settings/shop')
def set_shop_settings_api():
    data = request.json
    set_shop_settings(data)
    reload_conf()
    resp = {
        'success': True,
        'errors': []
    } 
    return jsonify(resp)

@app.get('/api/browse-directories')
@access_required('admin')
def browse_directories_api():
    """List directories under a given path prefix for autocomplete."""
    prefix = request.args.get('prefix', '/')
    if not prefix.startswith('/'):
        prefix = '/' + prefix

    parent = prefix if prefix.endswith('/') else os.path.dirname(prefix)
    partial = '' if prefix.endswith('/') else os.path.basename(prefix)

    # Resolve symlinks and .. to prevent traversal
    parent = os.path.realpath(parent)

    try:
        if not os.path.isdir(parent):
            return jsonify({'directories': []})
        entries = []
        for name in sorted(os.listdir(parent)):
            if partial and not name.lower().startswith(partial.lower()):
                continue
            full = os.path.join(parent, name)
            if os.path.isdir(full):
                entries.append(full + '/')
        return jsonify({'directories': entries})
    except PermissionError:
        return jsonify({'directories': []})


@app.route('/api/settings/library/paths', methods=['GET', 'POST', 'DELETE'])
@access_required('admin')
def library_paths_api():
    global watcher
    if request.method == 'POST':
        data = request.json
        success, errors = add_library_complete(app, watcher, data['path'])
        if success:
            reload_conf()
            post_library_change()
        resp = {
            'success': success,
            'errors': errors
        }
    elif request.method == 'GET':
        reload_conf()
        resp = {
            'success': True,
            'errors': [],
            'paths': app_settings['library']['paths']
        }    
    elif request.method == 'DELETE':
        data = request.json
        success, errors = remove_library_complete(app, watcher, data['path'])
        if success:
            reload_conf()
            post_library_change()
        resp = {
            'success': success,
            'errors': errors
        }
    return jsonify(resp)

@app.post('/api/settings/library/management')
@access_required('admin')
def set_library_management_settings_api():
    data = request.json
    set_library_management_settings(data)
    reload_conf()
    post_library_change()
    resp = {
        'success': True,
        'errors': []
    }
    return jsonify(resp)

@app.post('/api/settings/scheduler')
@access_required('admin')
def set_scheduler_settings_api():
    data = request.json
    scan_interval_str = data.get('scan_interval')
    
    if scan_interval_str is not None:
        is_valid, error_msg = validate_interval_string(scan_interval_str)
        if not is_valid:
            return jsonify({
                'success': False,
                'errors': [{'path': 'scheduler/scan_interval', 'error': error_msg}]
            })
    
    set_scheduler_settings(data)
    reload_conf()
    
    if scan_interval_str is not None:
        try:
            current_interval_str = app_settings.get('scheduler', {}).get('scan_interval', '12h')
            schedule_update_and_scan_job(app, current_interval_str, run_first=False)
        except Exception as e:
            logger.error(f"Error updating scheduler: {e}")
            return jsonify({
                'success': False,
                'errors': [{'path': 'scheduler', 'error': str(e)}]
            })
    
    return jsonify({'success': True, 'errors': []})

@app.post('/api/upload')
@access_required('admin')
def upload_file():
    errors = []
    success = False
    valid_keys = None
    try:
        file = request.files['file']
        if file and allowed_file(file.filename):
            # filename = secure_filename(file.filename)
            file.save(KEYS_FILE)
            logger.info(f'Validating {file.filename}...')
            valid_keys, missing_keys, corrupt_keys = load_keys(KEYS_FILE)
            if valid_keys:
                post_library_change()
            else:
                logger.warning(f'Invalid keys from {file.filename}')
            success = True
            logger.info('Successfully saved keys.txt')

    except Exception as e:
        logger.error(f'Failed to upload console keys file: {e}')
        os.remove(KEYS_FILE)
        success = False
        errors.append(str(e))

    resp = {
        'success': success,
        'errors': errors,
        'data': {}
    }

    if valid_keys is not None:
        resp['data']['valid_keys'] = valid_keys
        resp['data']['missing_keys'] = missing_keys
        resp['data']['corrupt_keys'] = corrupt_keys
    
    return jsonify(resp)


@app.route('/api/titles', methods=['GET'])
@access_required('shop')
def get_all_titles_api():
    titles_library = generate_library()

    return jsonify({
        'total': len(titles_library),
        'games': titles_library
    })

@app.route('/api/titles/grouped', methods=['GET'])
@access_required('shop')
def get_grouped_titles_api():
    games = generate_grouped_library()
    # Strip internal filepath from update entries before sending to client
    for g in games:
        for u in g.get('updates', []):
            u.pop('filepath', None)
    return jsonify({
        'total': len(games),
        'games': games
    })

@app.route('/title/<title_id>')
@access_required('shop')
def title_detail_page(title_id):
    return render_template('title_detail.html', title='Title Detail', title_id=title_id, admin_account_created=admin_account_created())

@app.route('/api/title/<title_id>', methods=['GET'])
@access_required('shop')
def get_title_detail_api(title_id):
    # Find this title in the grouped library
    games = generate_grouped_library()
    game = next((g for g in games if g['title_id'] == title_id), None)
    if not game:
        return jsonify({'error': 'Title not found'}), 404

    game = copy.deepcopy(game)

    # Add file info for owned apps
    title_apps = get_all_title_apps(title_id)
    files_info = []
    seen_files = set()
    for app_data in title_apps:
        if app_data.get('owned'):
            app_obj = get_app_by_id_and_version(app_data['app_id'], app_data['app_version'])
            if app_obj:
                for f in app_obj.files:
                    if f.id not in seen_files:
                        seen_files.add(f.id)
                        files_info.append({
                            'id': f.id,
                            'filename': f.filename,
                            'filepath': f.filepath,
                            'size': f.size,
                            'extension': f.extension,
                        })

    game['files'] = files_info

    # Resolve display versions for this title's updates (lazy, cached)
    for u in game.get('updates', []):
        filepath = u.pop('filepath', None)
        if u.get('display_version'):
            pass  # already resolved (e.g. from old cache format)
        elif filepath and u.get('owned'):
            u['display_version'] = get_display_version_cached(filepath)
        else:
            u.setdefault('display_version', None)

    # Set owned_display_version for the highest owned update
    owned_updates = [u for u in game.get('updates', []) if u.get('owned') and u.get('display_version')]
    game['owned_display_version'] = owned_updates[-1]['display_version'] if owned_updates else None

    # Add required firmware version
    titles_lib.load_titledb()
    game['required_firmware'] = titles_lib.get_title_required_firmware(title_id)
    titles_lib.unload_titledb()

    return jsonify(game)

@app.route('/api/get_game/<int:id>')
@tinfoil_access
def serve_game(id):
    # TODO add download count increment
    filepath = db.session.query(Files.filepath).filter_by(id=id).first()[0]
    filedir, filename = os.path.split(filepath)
    return send_from_directory(filedir, filename)


@app.route('/api/download/file/<int:id>')
@access_required('shop')
def download_file(id):
    file_entry = db.session.query(Files).filter_by(id=id).first()
    if not file_entry:
        return jsonify({'error': 'File not found'}), 404
    filedir, filename = os.path.split(file_entry.filepath)
    return send_from_directory(filedir, filename, as_attachment=True)


@app.route('/api/download/combined/<title_id>')
@access_required('shop')
def download_combined(title_id):
    reload_conf()
    output_dir = app_settings['library']['management'].get('combine_xci', {}).get('output_path', '/combined')
    combined = get_combined_file(title_id, output_dir)
    if not combined:
        return jsonify({'error': 'No combined XCI found'}), 404
    filedir, filename = os.path.split(combined)
    return send_from_directory(filedir, filename, as_attachment=True)


@app.post('/api/combine/<title_id>')
@access_required('shop')
def combine_title_api(title_id):
    reload_conf()
    output_dir = app_settings['library']['management'].get('combine_xci', {}).get('output_path', '/combined')
    keys_path = KEYS_FILE if os.path.exists(KEYS_FILE) else None

    def run_combine():
        with app.app_context():
            combine_title(title_id, output_dir, keys_path)

    t = threading.Thread(target=run_combine)
    t.daemon = True
    t.start()
    return jsonify({'status': 'started'})


@app.post('/api/combine/all')
@access_required('admin')
def combine_all_api():
    reload_conf()
    output_dir = app_settings['library']['management'].get('combine_xci', {}).get('output_path', '/combined')
    keys_path = KEYS_FILE if os.path.exists(KEYS_FILE) else None

    def run_combine_all():
        combine_all_titles(app.app_context(), output_dir, keys_path)

    t = threading.Thread(target=run_combine_all)
    t.daemon = True
    t.start()
    return jsonify({'status': 'started'})


@app.get('/api/combine/status/<title_id>')
@access_required('shop')
def combine_status_api(title_id):
    reload_conf()
    output_dir = app_settings['library']['management'].get('combine_xci', {}).get('output_path', '/combined')
    # Only report exists if the combine state confirms it's done (not mid-write)
    from combine import get_combine_state
    state = get_combine_state()
    if title_id in state:
        combined = get_combined_file(title_id, output_dir)
        if combined and os.path.exists(combined):
            return jsonify({
                'exists': True,
                'filename': os.path.basename(combined),
                'size': os.path.getsize(combined)
            })
    return jsonify({'exists': False})


## Downloads routes

@app.post('/api/settings/downloads')
@access_required('admin')
def set_downloads_settings_api():
    data = request.json
    set_downloads_settings(data)
    reload_conf()
    return jsonify({'success': True, 'errors': []})


@app.post('/api/downloads/test/prowlarr')
@access_required('admin')
def test_prowlarr_api():
    data = request.json
    result = prowlarr_client.test_connection(data.get('url', ''), data.get('api_key', ''))
    return jsonify(result)


@app.post('/api/downloads/test/torrent')
@access_required('admin')
def test_torrent_api():
    data = request.json
    result = torrent_client.test_connection(
        url=data.get('url', ''),
        username=data.get('username', ''),
        password=data.get('password', '')
    )
    return jsonify(result)


@app.get('/api/downloads/indexers')
@access_required('admin')
def get_indexers_api():
    reload_conf()
    url = request.args.get('url', '')
    api_key = request.args.get('api_key', '')
    if not url or not api_key:
        p = app_settings.get('downloads', {}).get('prowlarr', {})
        url = url or p.get('url', '')
        api_key = api_key or p.get('api_key', '')
    indexers = prowlarr_client.get_indexers(url, api_key)
    return jsonify(indexers)


@app.get('/api/downloads/search')
@access_required('shop')
def search_downloads_api():
    reload_conf()
    query = request.args.get('query', '')
    if not query:
        return jsonify({'results': []})
    try:
        results = download_manager.search_content(app_settings, query)
    except Exception as e:
        return jsonify({'results': [], 'error': str(e)}), 500
    return jsonify({'results': results})


@app.post('/api/downloads/add')
@access_required('shop')
def add_download_api():
    reload_conf()
    data = request.json
    try:
        torrent_hash = download_manager.start_download(
            settings=app_settings,
            download_url=data.get('download_url', ''),
            title_id=data.get('title_id', ''),
            content_type=data.get('content_type', '')
        )
        return jsonify({'status': 'added', 'hash': torrent_hash})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.get('/api/downloads/active')
@access_required('shop')
def active_downloads_api():
    reload_conf()
    downloads = download_manager.get_active_downloads(app_settings)
    return jsonify({'downloads': downloads})


@app.delete('/api/downloads/<torrent_hash>')
@access_required('shop')
def cancel_download_api(torrent_hash):
    reload_conf()
    success, error = download_manager.cancel_download(app_settings, torrent_hash)
    if not success:
        return jsonify({'status': 'error', 'message': f'Failed to remove torrent from Transmission: {error}'}), 500
    return jsonify({'status': 'removed'})


@app.post('/api/downloads/<torrent_hash>/dismiss')
@access_required('shop')
def dismiss_download_api(torrent_hash):
    download_manager.dismiss_download(torrent_hash)
    return jsonify({'status': 'dismissed'})


@app.post('/api/downloads/<torrent_hash>/recheck')
@access_required('shop')
def recheck_download_api(torrent_hash):
    download_manager.recheck_download(torrent_hash)
    return jsonify({'status': 'rechecking'})


@app.delete('/api/downloads/<torrent_hash>/delete')
@access_required('shop')
def delete_download_api(torrent_hash):
    reload_conf()
    success, error = download_manager.delete_download(app_settings, torrent_hash)
    if not success:
        return jsonify({'status': 'error', 'message': error}), 500
    return jsonify({'status': 'deleted'})


@app.get('/api/titles/suggested')
@access_required('shop')
def get_suggested_titles_api():
    titles_lib.load_titledb()
    owned_ids = get_owned_base_title_ids()
    suggestions = suggested_content.build_suggestions_from_titledb(
        None,  # titles_db accessed via global in titles_lib
        owned_ids
    )
    return jsonify({'suggestions': suggestions})


@app.route('/downloads')
@access_required('shop')
def downloads_page():
    return render_template('downloads.html', title='Downloads', admin_account_created=admin_account_created())


@debounce(10, key='post_library_change')
def post_library_change():
    with app.app_context():
        titles_lib.load_titledb()
        process_library_identification(app)
        add_missing_apps_to_db()
        # remove missing files
        remove_missing_files_from_db()
        update_titles() # Ensure titles are updated after identification
        process_library_organization(app, watcher) # Pass the watcher instance to skip organizer move/delete events
        # The process_library_identification already handles updating titles and generating library
        # So, we just need to ensure titles_library is updated from the generated library
        generate_library()
        generate_grouped_library()
        titles_lib.identification_in_progress_count -= 1
        titles_lib.unload_titledb()

        # Auto-combine if enabled
        combine_settings = load_settings().get('library', {}).get('management', {}).get('combine_xci', {})
        if combine_settings.get('enabled'):
            output_dir = combine_settings.get('output_path', '/combined')
            keys_path = KEYS_FILE if os.path.exists(KEYS_FILE) else None
            def run_auto_combine():
                combine_all_titles(app.app_context(), output_dir, keys_path)
            t = threading.Thread(target=run_auto_combine)
            t.daemon = True
            t.start()

@app.post('/api/library/scan')
@access_required('admin')
def scan_library_api():
    data = request.json
    path = data['path']
    success = True
    errors = []

    global scan_in_progress
    with scan_lock:
        if scan_in_progress:
            logger.info('Skipping scan_library_api call: Scan already in progress')
            return {'success': False, 'errors': []}
    # Set the scan status to in progress
    scan_in_progress = True

    try:
        if path is None:
            scan_library()
        else:
            scan_library_path(path)
    except Exception as e:
        errors.append(e)
        success = False
        logger.error(f"Error during library scan: {e}")
    finally:
        with scan_lock:
            scan_in_progress = False

    post_library_change()
    resp = {
        'success': success,
        'errors': errors
    } 
    return jsonify(resp)

def scan_library():
    logger.info(f'Scanning whole library ...')
    libraries = get_libraries()
    for library in libraries:
        scan_library_path(library.path) # Only scan, identification will be done globally

def update_and_scan_job():
    """Combined job: updates TitleDB then scans library"""
    logger.info("Running update job (TitleDB update and library scan)...")
    global scan_in_progress
    
    # Update TitleDB with locking
    with titledb_update_lock:
        is_titledb_update_running = True
    
    # Invalidate suggested content cache so it rebuilds from fresh TitleDB
    try:
        suggested_content.refresh_cache()
    except Exception as e:
        logger.error(f"Error refreshing suggested content cache: {e}")

    logger.info("Starting TitleDB update...")
    try:
        settings = load_settings()
        titledb.update_titledb(settings)
        logger.info("TitleDB update completed.")
    except Exception as e:
        logger.error(f"Error during TitleDB update: {e}")
    finally:
        with titledb_update_lock:
            is_titledb_update_running = False
    
    # Check if update is still running before scanning
    with titledb_update_lock:
        if is_titledb_update_running:
            logger.info("Skipping library scan: TitleDB update still in progress.")
            return
    
    # Scan library with locking
    logger.info("Starting library scan...")
    with scan_lock:
        if scan_in_progress:
            logger.info('Skipping library scan: scan already in progress.')
            return
        scan_in_progress = True
    
    try:
        scan_library()
        post_library_change()
        logger.info("Library scan completed.")
    except Exception as e:
        logger.error(f"Error during library scan: {e}")
    finally:
        with scan_lock:
            scan_in_progress = False
    
    logger.info("Update job completed.")

def schedule_update_and_scan_job(app: Flask, interval_str: str, run_first: bool = True, run_once: bool = False):
    """Schedule or update the update_and_scan job"""
    app.scheduler.update_job_interval(
        job_id='update_db_and_scan',
        interval_str=interval_str,
        func=update_and_scan_job,
        run_first=run_first,
        run_once=run_once
    )

if __name__ == '__main__':
    logger.info('Starting initialization of Ownfoil...')
    init_db(app)
    init_users(app)
    init()
    logger.info('Initialization steps done, starting server...')
    app.run(debug=False, use_reloader=False, host="0.0.0.0", port=8465)
    # Shutdown server
    logger.info('Shutting down server...')
    watcher.stop()
    watcher_thread.join()
    logger.debug('Watcher thread terminated.')
    # Shutdown scheduler
    app.scheduler.shutdown()
    logger.debug('Scheduler terminated.')
