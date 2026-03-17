import os
import json
import time
import logging

from constants import DATA_DIR

logger = logging.getLogger('main')

SUGGESTED_CACHE_FILE = os.path.join(DATA_DIR, 'suggested_titles.json')
CACHE_MAX_AGE = 86400  # 24 hours


def build_suggestions_from_titledb(titles_db, owned_ids):
    """Build and cache suggestions from TitleDB data.

    This is called from the API route — titles_db must already be loaded.
    Returns the cached result if fresh, otherwise rebuilds from titles_db.
    """
    # Check cache first
    if os.path.exists(SUGGESTED_CACHE_FILE):
        try:
            with open(SUGGESTED_CACHE_FILE, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            age = time.time() - cache_data.get('timestamp', 0)
            if age < CACHE_MAX_AGE:
                return cache_data.get('suggestions', [])
        except Exception as e:
            logger.warning(f'Failed to read suggested titles cache: {e}')

    # Rebuild from TitleDB — import here to avoid circular imports
    import titles as titles_lib
    result = titles_lib.get_suggested_content(owned_ids)
    suggestions = result.get('suggestions', [])

    # Save to cache
    cache_data = {
        'timestamp': time.time(),
        'suggestions': suggestions,
    }
    try:
        os.makedirs(os.path.dirname(SUGGESTED_CACHE_FILE), exist_ok=True)
        with open(SUGGESTED_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f)
        logger.info(f'Cached {len(suggestions)} suggested titles from TitleDB.')
    except Exception as e:
        logger.error(f'Failed to save suggested titles cache: {e}')

    return suggestions


def refresh_cache():
    """Invalidate the cache so it gets rebuilt on next request."""
    if os.path.exists(SUGGESTED_CACHE_FILE):
        try:
            os.remove(SUGGESTED_CACHE_FILE)
            logger.info('Suggested titles cache invalidated.')
        except Exception as e:
            logger.error(f'Failed to remove suggested titles cache: {e}')
