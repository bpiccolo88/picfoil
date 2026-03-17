import requests
import logging

logger = logging.getLogger('main')

COMPLETED_STATUSES = {6}  # 6 = seeding (download complete)
STOPPED_STATUS = 0


class TransmissionClient:
    """Transmission RPC client."""

    def __init__(self, url, username='', password=''):
        self.url = url.rstrip('/') + '/transmission/rpc'
        self.session_id = ''
        self.auth = (username, password) if username else None

    def _request(self, method, arguments=None, retry=True):
        """Make a Transmission RPC request, handling session-id CSRF."""
        payload = {"method": method}
        if arguments:
            payload["arguments"] = arguments

        headers = {"X-Transmission-Session-Id": self.session_id}
        try:
            resp = requests.post(
                self.url, json=payload, headers=headers,
                auth=self.auth, timeout=15
            )
            if resp.status_code == 409 and retry:
                # Get new session ID from response header
                self.session_id = resp.headers.get("X-Transmission-Session-Id", "")
                return self._request(method, arguments, retry=False)

            resp.raise_for_status()
            data = resp.json()
            if data.get("result") != "success":
                raise Exception(f"Transmission error: {data.get('result')}")
            return data.get("arguments", {})

        except requests.ConnectionError:
            raise Exception(f"Could not connect to Transmission at {self.url}")

    def add_torrent(self, download_url, labels=None):
        """Add a torrent by URL/magnet link. Returns torrent hash."""
        args = {"filename": download_url, "paused": False}
        if labels:
            args["labels"] = labels

        result = self._request("torrent-add", args)
        # Transmission returns torrent-added or torrent-duplicate
        torrent = result.get("torrent-added") or result.get("torrent-duplicate", {})
        torrent_hash = torrent.get("hashString", "")

        # For duplicates, Transmission ignores labels from the add request,
        # so apply them explicitly
        if result.get("torrent-duplicate") and labels and torrent_hash:
            try:
                self._request("torrent-set", {
                    "ids": [torrent_hash],
                    "labels": labels
                })
            except Exception as e:
                logger.warning(f"Failed to apply labels to duplicate torrent {torrent_hash}: {e}")

        return torrent_hash

    def get_torrents(self, labels=None):
        """Get torrents, optionally filtered by label. Returns list of torrent info dicts."""
        fields = [
            "hashString", "name", "status", "percentDone",
            "totalSize", "downloadDir", "labels", "eta",
            "rateDownload", "addedDate", "doneDate"
        ]
        result = self._request("torrent-get", {"fields": fields})
        torrents = result.get("torrents", [])

        if labels:
            label_set = set(labels) if isinstance(labels, list) else {labels}
            torrents = [
                t for t in torrents
                if label_set & set(t.get("labels", []))
            ]

        return [
            {
                "hash": t["hashString"],
                "name": t["name"],
                "status": t["status"],
                "progress": round(t["percentDone"] * 100, 1),
                "size": t["totalSize"],
                "save_path": t["downloadDir"],
                "completed": t["percentDone"] >= 1.0,
                "eta": t.get("eta", -1),
                "download_speed": t.get("rateDownload", 0),
                "added_date": t.get("addedDate", 0),
                "done_date": t.get("doneDate", 0),
            }
            for t in torrents
        ]

    def delete_torrent(self, torrent_hash, delete_files=False):
        """Remove a torrent by hash."""
        self._request("torrent-remove", {
            "ids": [torrent_hash],
            "delete-local-data": delete_files
        })


def test_connection(url, username='', password=''):
    """Test connection to Transmission. Returns {success, message}."""
    try:
        client = TransmissionClient(url, username, password)
        client._request("session-get")
        return {"success": True, "message": "Connected to Transmission"}
    except Exception as e:
        return {"success": False, "message": str(e)}
