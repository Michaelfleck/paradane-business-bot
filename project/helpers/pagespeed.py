import os
import requests
from typing import Dict, Any, Optional


class PageSpeedClient:
    """
    Wrapper for Google PageSpeed Insights API.
    Requires an environment variable: PAGESPEED_API_KEY
    """

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("PAGESPEED_API_KEY")
        if not self.api_key:
            raise ValueError("PAGESPEED_API_KEY is missing. Please add it to your environment variables.")
        self.base_url = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"

    def analyze_page(self, url: str, strategy: str = "desktop") -> Dict[str, Any]:
        """
        Get PageSpeed analysis of a URL.
        Returns score and time-to-interactive in ms.
        """
        params = {
            "url": url,
            "strategy": strategy,
            "key": self.api_key,
        }
        response = requests.get(self.base_url, params=params, timeout=360)
        response.raise_for_status()
        data = response.json()

        lighthouse = data.get("lighthouseResult", {})
        categories = lighthouse.get("categories", {})
        performance = categories.get("performance", {})
        score = int(performance.get("score", 0) * 100) if performance.get("score") is not None else None

        audits = lighthouse.get("audits", {})
        tti = audits.get("interactive", {}).get("numericValue")

        return {
            "page_speed_score": score,
            "time_to_interactive_ms": int(tti) if tti is not None else None,
        }