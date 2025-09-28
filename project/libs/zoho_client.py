import os
import time
import requests
from typing import Dict, Any, Optional, List
import logging

logger = logging.getLogger(__name__)

class ZohoAuth:
    """Handles Zoho OAuth2 authentication and token management."""

    def __init__(self, client_id: str, client_secret: str, data_center: str = "us"):
        self.client_id = client_id
        self.client_secret = client_secret
        self.data_center = data_center.lower()
        if self.data_center == "us":
            self.base_url = "https://www.zohoapis.com"
            self.auth_url = "https://accounts.zoho.com/oauth/v2/token"
        else:
            self.base_url = f"https://www.zohoapis{self.data_center}.com"
            self.auth_url = f"https://accounts.zoho{self.data_center}.com/oauth/v2/token"
        self._access_token: Optional[str] = None
        self._token_expires_at: Optional[float] = None

    def _get_access_token(self) -> str:
        """Get a valid access token, refreshing if necessary."""
        if self._access_token and self._token_expires_at and time.time() < self._token_expires_at - 60:  # Refresh 1 min early
            return self._access_token

        data = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }

        try:
            response = requests.post(self.auth_url, data=data, timeout=30)
            response.raise_for_status()
            token_data = response.json()

            self._access_token = token_data['access_token']
            expires_in = token_data.get('expires_in', 3600)  # Default 1 hour
            self._token_expires_at = time.time() + expires_in

            logger.info("Successfully obtained Zoho access token")
            return self._access_token
        except requests.RequestException as e:
            logger.error(f"Failed to get Zoho access token: {e}")
            raise

    def get_headers(self) -> Dict[str, str]:
        """Get headers with valid access token."""
        token = self._get_access_token()
        return {
            'Authorization': f'Zoho-oauthtoken {token}',
            'Content-Type': 'application/json'
        }


class ZohoCRMClient:
    """Client for Zoho CRM API operations."""

    def __init__(self, client_id: str, client_secret: str, data_center: str = "us"):
        self.auth = ZohoAuth(client_id, client_secret, data_center)
        self.base_url = self.auth.base_url

    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, files: Optional[Dict] = None) -> Dict[str, Any]:
        """Make authenticated request to Zoho API."""
        url = f"{self.base_url}{endpoint}"
        headers = self.auth.get_headers()

        if files:
            # For file uploads, don't set Content-Type
            headers.pop('Content-Type', None)

        try:
            response = requests.request(method, url, headers=headers, json=data, files=files, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Zoho API request failed: {method} {url} - {e}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response: {e.response.text}")
            raise

    def create_lead(self, lead_data: Dict[str, Any]) -> str:
        """Create a new lead in Zoho CRM. Returns the lead ID."""
        endpoint = "/crm/v2/Leads"
        response = self._make_request("POST", endpoint, {"data": [lead_data]})

        if 'data' in response and response['data']:
            lead_id = response['data'][0]['details']['id']
            logger.info(f"Created Zoho lead: {lead_id}")
            return lead_id
        else:
            raise ValueError("Failed to create lead - no ID returned")

    def update_lead(self, lead_id: str, lead_data: Dict[str, Any]) -> bool:
        """Update an existing lead in Zoho CRM."""
        endpoint = f"/crm/v2/Leads/{lead_id}"
        response = self._make_request("PUT", endpoint, lead_data)

        if 'data' in response and response['data']:
            logger.info(f"Updated Zoho lead: {lead_id}")
            return True
        return False

    def create_contact(self, contact_data: Dict[str, Any]) -> str:
        """Create a new contact in Zoho CRM. Returns the contact ID."""
        endpoint = "/crm/v2/Contacts"
        response = self._make_request("POST", endpoint, {"data": [contact_data]})

        if 'data' in response and response['data']:
            contact_id = response['data'][0]['details']['id']
            logger.info(f"Created Zoho contact: {contact_id}")
            return contact_id
        else:
            raise ValueError("Failed to create contact - no ID returned")

    def attach_document(self, module: str, record_id: str, file_path: str, file_name: str) -> bool:
        """Attach a document to a record (lead/contact)."""
        endpoint = f"/crm/v2/{module}/{record_id}/Attachments"

        with open(file_path, 'rb') as f:
            files = {'file': (file_name, f, 'application/pdf')}
            response = self._make_request("POST", endpoint, files=files)

        if 'data' in response and response['data']:
            logger.info(f"Attached document {file_name} to {module}/{record_id}")
            return True
        return False

    def search_leads(self, criteria: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Search for leads based on criteria."""
        endpoint = "/crm/v2/Leads/search"
        params = "&".join([f"{k}={v}" for k, v in criteria.items()])
        if params:
            endpoint += f"?{params}"

        response = self._make_request("GET", endpoint)
        return response.get('data', [])


def get_zoho_client() -> ZohoCRMClient:
    """Factory function to create Zoho client from environment variables."""
    client_id = os.getenv("ZOHO_CLIENT_ID")
    client_secret = os.getenv("ZOHO_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise ValueError("ZOHO_CLIENT_ID and ZOHO_CLIENT_SECRET must be set in environment")

    # Default to US data center, can be overridden with ZOHO_DATA_CENTER env var
    data_center = os.getenv("ZOHO_DATA_CENTER", "us")

    return ZohoCRMClient(client_id, client_secret, data_center)