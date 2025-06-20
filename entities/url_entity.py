from datetime import datetime, timezone
from urllib.parse import urlparse, unquote

class IdeaURL:
    def __init__(self, url: str, status: str = "unprocessed", info=None, processed_at=None):
        self.url = url
        self.created_at = datetime.now(timezone.utc)
        self.status = status
        self.info = info
        self.processed_at = processed_at

        # Extract id and name from url
        parsed = urlparse(url)
        # Example path: /ideas/фотограф-портфолио/919325369379/
        parts = parsed.path.strip('/').split('/')
        self.id = parts[-1] if len(parts) >= 3 else None
        self.name = unquote(parts[-2]) if len(parts) >= 3 else None

    def to_dict(self):
        return {
            "url": self.url,
            "created_at": self.created_at,
            "status": self.status,
            "id": self.id,
            "name": self.name,
            "info": self.info,
            "processed_at": self.processed_at
        }

    def to_mongo_dict(self):
        d = self.to_dict()
        # Remove None fields for MongoDB cleanliness
        return {k: v for k, v in d.items() if v is not None}
