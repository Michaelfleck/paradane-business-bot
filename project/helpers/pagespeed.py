import os
import time
import random
import requests
from typing import Dict, Any, Optional


class PageSpeedClient:
    """
    Wrapper for Google PageSpeed Insights API.
    Requires an environment variable: GOOGLE_API_KEY
    """

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("GOOGLE_API_KEY")
        if not self.api_key:
            raise ValueError("GOOGLE_API_KEY is missing. Please add it to your environment variables.")
        self.base_url = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"

    def analyze_page(self, url: str, strategy: str = "desktop") -> Dict[str, Any]:
        """
        Get PageSpeed analysis of a URL.
        Returns score and time-to-interactive in ms.
        Retries the request up to 3 times on transient failures (network issues, 5xx) and 429 rate limit.
        """
        params = {
            "url": url,
            "strategy": strategy,
            "key": self.api_key,
        }

        last_err: Optional[Exception] = None
        data: Optional[Dict[str, Any]] = None
        # Exponential backoff with jitter: 0.5s, 1s, 2s (+/- up to 200ms)
        for attempt in range(3):
            try:
                response = requests.get(self.base_url, params=params, timeout=360)
                status = response.status_code
                # Treat 5xx and 429 as retryable
                if status == 429 or 500 <= status < 600:
                    raise requests.HTTPError(f"{status} Retryable Error", response=response)
                # For other statuses, raise_for_status handles non-2xx.
                response.raise_for_status()
                data = response.json()
                break
            except requests.HTTPError as http_err:
                status = getattr(getattr(http_err, "response", None), "status_code", None)
                # If it's a 4xx other than 429, don't retry.
                if status is not None and 400 <= status < 500 and status != 429:
                    raise
                last_err = http_err
            except (requests.ConnectionError, requests.Timeout) as net_err:
                last_err = net_err
            except Exception as e:
                # Unknown error; keep for last but retry as it may be transient.
                last_err = e

            if attempt < 2:
                base_delay = 0.5 * (2 ** attempt)
                jitter = random.uniform(-0.2, 0.2)
                delay = max(0.1, base_delay + jitter)
                time.sleep(delay)
            else:
                # Exhausted retries
                if last_err:
                    raise last_err
                raise RuntimeError("Unknown error calling PageSpeed API with no exception captured.")

        lighthouse = (data or {}).get("lighthouseResult", {})
        categories = lighthouse.get("categories", {})
        performance = categories.get("performance", {})
        score = int(performance.get("score", 0) * 100) if performance.get("score") is not None else None

        audits = lighthouse.get("audits", {})
        tti = audits.get("interactive", {}).get("numericValue")

        return {
            "page_speed_score": score,
            "time_to_interactive_ms": int(tti) if tti is not None else None,
        }