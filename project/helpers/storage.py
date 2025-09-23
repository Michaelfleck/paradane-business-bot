from datetime import datetime, timezone, timedelta
import logging
from typing import Dict, Any, Optional
from project.libs.supabase_client import get_client

TWENTY_FOUR_HOURS = timedelta(hours=24)
SEVEN_DAYS = timedelta(days=7)

class StorageClient:
    def __init__(self):
        self.client = get_client()

    # --------------- Business-level gating (enrichment) -----------------

    def business_recently_updated(self, business_id: str) -> bool:
        """
        Return True if businesses.updated_at within last 24 hours for given business_id.
        """
        try:
            resp = (
                self.client.table("businesses")
                .select("updated_at")
                .eq("id", business_id)
                .limit(1)
                .execute()
            )
            rows = getattr(resp, "data", None) or []
            if not rows:
                return False
            ts = rows[0].get("updated_at")
            if not ts:
                return False
            # Supabase returns RFC3339 string; parse via fromisoformat (strip Z if present)
            ts_str = str(ts).replace("Z", "+00:00")
            last = datetime.fromisoformat(ts_str)
            # Normalize to timezone-aware UTC
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            else:
                last = last.astimezone(timezone.utc)
            now = datetime.now(timezone.utc)
            return (now - last) < TWENTY_FOUR_HOURS
        except Exception as e:
            logging.warning(f"business_recently_updated failed for {business_id}: {e}")
            return False

    def touch_business_updated_at(self, business_id: str) -> None:
        """
        Set/update businesses.updated_at to now() for given business_id.
        """
        try:
            now_iso = datetime.now(timezone.utc).isoformat()
            _ = (
                self.client.table("businesses")
                .update({"updated_at": now_iso})
                .eq("id", business_id)
                .execute()
            )
        except Exception as e:
            logging.warning(f"touch_business_updated_at failed for {business_id}: {e}")

    # --------------- Page-level gating (pipeline/page processing) -----------------

    def business_pages_recently_updated(self, business_id: str) -> bool:
        """
        Return True if any business_pages for business_id has updated_at within last 24 hours.
        We use the most recent page timestamp to decide.
        """
        try:
            resp = (
                self.client.table("business_pages")
                .select("updated_at")
                .eq("business_id", business_id)
                .order("updated_at", desc=True)
                .limit(1)
                .execute()
            )
            rows = getattr(resp, "data", None) or []
            if not rows:
                return False
            ts = rows[0].get("updated_at")
            if not ts:
                return False
            ts_str = str(ts).replace("Z", "+00:00")
            last = datetime.fromisoformat(ts_str)
            # Ensure timezone-aware UTC for both sides to avoid naive/aware subtraction errors
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            else:
                last = last.astimezone(timezone.utc)
            now = datetime.now(timezone.utc)
            return (now - last) < TWENTY_FOUR_HOURS
        except Exception as e:
            logging.warning(f"business_pages_recently_updated failed for {business_id}: {e}")
            return False

    def get_business_page(self, business_id: str, url: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a single business_pages row for (business_id, url).
        Returns a dict with fields including updated_at or None if not found.
        """
        try:
            resp = (
                self.client.table("business_pages")
                .select("business_id,url,summary,page_type,email,page_speed_score,time_to_interactive_ms,seo_score,seo_explanation,updated_at")
                .eq("business_id", business_id)
                .eq("url", url)
                .limit(1)
                .execute()
            )
            rows = getattr(resp, "data", None) or []
            return rows[0] if rows else None
        except Exception as e:
            logging.warning(f"get_business_page failed for {business_id} {url}: {e}")
            return None

    def insert_business_page(self, page: Dict[str, Any]):
        """
        Insert or update a record in business_pages via upsert.
        page dict must include at minimum: business_id, url.
        We intentionally allow selective fields: summary/page_type may be omitted to preserve prior values.
        """
        try:
            # Ensure updated_at is set to now (so upsert writes bump the timestamp)
            page = dict(page)
            page["updated_at"] = datetime.now(timezone.utc).isoformat()

            response = (
                self.client.table("business_pages")
                .upsert(page, on_conflict="business_id,url")
                .execute()
            )
            if getattr(response, "error", None):
                logging.error(f"Failed to upsert business page for {page.get('business_id')}: {response.error}")
        except Exception as e:
            logging.exception(f"Exception during insert_business_page for {page.get('business_id')}: {e}")