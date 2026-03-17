import re

import requests
import logging

logger = logging.getLogger('main')


def test_connection(url, api_key):
    """Test connection to Prowlarr. Returns {success, message}."""
    try:
        resp = requests.get(
            f"{url.rstrip('/')}/api/v1/health",
            headers={"X-Api-Key": api_key},
            timeout=10
        )
        if resp.status_code == 200:
            return {"success": True, "message": "Connected to Prowlarr"}
        elif resp.status_code == 401:
            return {"success": False, "message": "Invalid API key"}
        else:
            return {"success": False, "message": f"HTTP {resp.status_code}: {resp.text[:200]}"}
    except requests.ConnectionError:
        return {"success": False, "message": f"Could not connect to {url}"}
    except Exception as e:
        return {"success": False, "message": str(e)}


def get_indexers(url, api_key):
    """Get available indexers from Prowlarr."""
    try:
        resp = requests.get(
            f"{url.rstrip('/')}/api/v1/indexer",
            headers={"X-Api-Key": api_key},
            timeout=10
        )
        resp.raise_for_status()
        return [
            {"id": idx["id"], "name": idx["name"], "enable": idx.get("enable", True)}
            for idx in resp.json()
        ]
    except Exception as e:
        logger.error(f"Failed to get Prowlarr indexers: {e}")
        return []


def normalize_query(query):
    """Normalize search query for better Prowlarr matching."""
    # Replace & with space (Prowlarr handles fuzzy matching)
    q = query.replace('&', ' ')
    # Remove other special characters that cause search issues
    q = re.sub(r'[^\w\s\-]', ' ', q)
    # Collapse whitespace
    q = re.sub(r'\s+', ' ', q).strip()
    return q


def search(url, api_key, query, categories=None, indexer_ids=None):
    """Search Prowlarr for content. Returns list of results."""
    try:
        params = [("query", normalize_query(query)), ("type", "search")]
        if categories:
            for c in categories:
                params.append(("categories", int(c)))
        if indexer_ids:
            for i in indexer_ids:
                params.append(("indexerIds", int(i)))

        resp = requests.get(
            f"{url.rstrip('/')}/api/v1/search",
            headers={"X-Api-Key": api_key},
            params=params,
            timeout=30
        )
        resp.raise_for_status()

        results = []
        for item in resp.json():
            results.append({
                "title": item.get("title", ""),
                "size": item.get("size", 0),
                "seeders": item.get("seeders", 0),
                "leechers": item.get("leechers", 0),
                "indexer": item.get("indexer", ""),
                "downloadUrl": item.get("downloadUrl", ""),
                "guid": item.get("guid", ""),
                "age": item.get("age", 0),
            })

        # Sort by seeders descending
        results.sort(key=lambda r: r["seeders"], reverse=True)
        return results

    except Exception as e:
        logger.error(f"Prowlarr search failed: {e}")
        raise
