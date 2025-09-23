from datetime import datetime, timezone
import logging
from typing import Dict, Any, Optional
from project.libs.supabase_client import get_client

class StorageClient:
    def __init__(self):
        self.client = get_client()

    def insert_business_page(self, page: Dict[str, Any]):
        """
        Insert a record into business_pages.
        page dict must include: business_id, url, page_type, summary, email, page_speed_score, time_to_interactive_ms
        """
        try:
            response = (
                self.client.table("business_pages")
                .upsert(page, on_conflict="business_id,url")
                .execute()
            )
            if getattr(response, "error", None):
                logging.error(f"Failed to upsert business page for {page.get('business_id')}: {response.error}")
        except Exception as e:
            logging.exception(f"Exception during insert_business_page for {page.get('business_id')}: {e}")