import os
import time
import requests
import urllib.parse
from typing import Dict, Any, Optional, List
import logging

logger = logging.getLogger(__name__)

class ZohoAuth:
    """Handles Zoho OAuth2 authentication and token management."""

    # Class-level cache for shared token state across instances
    _shared_access_token: Optional[str] = None
    _shared_token_expires_at: Optional[float] = None

    def __init__(self, client_id: str, client_secret: str, redirect_uri: str, refresh_token: Optional[str] = None, data_center: str = "us"):
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.refresh_token = refresh_token
        self.data_center = data_center.lower()
        if self.data_center == "us":
            self.base_url = "https://www.zohoapis.com"
            self.auth_url = "https://accounts.zoho.com/oauth/v2/token"
            self.auth_base_url = "https://accounts.zoho.com/oauth/v2/auth"
        else:
            self.base_url = f"https://www.zohoapis{self.data_center}.com"
            self.auth_url = f"https://accounts.zoho{self.data_center}.com/oauth/v2/token"
            self.auth_base_url = f"https://accounts.zoho{self.data_center}.com/oauth/v2/auth"

    def _get_access_token(self) -> str:
        """Get a valid access token, refreshing if necessary."""
        if ZohoAuth._shared_access_token and ZohoAuth._shared_token_expires_at and time.time() < ZohoAuth._shared_token_expires_at - 60:  # Refresh 1 min early
            return ZohoAuth._shared_access_token

        if not self.refresh_token:
            raise ValueError("No refresh token available. Please complete OAuth authorization first.")

        data = {
            'grant_type': 'refresh_token',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'refresh_token': self.refresh_token
        }

        try:
            response = requests.post(self.auth_url, data=data, timeout=30)
            response.raise_for_status()
            token_data = response.json()

            ZohoAuth._shared_access_token = token_data['access_token']
            expires_in = token_data.get('expires_in', 3600)  # Default 1 hour
            ZohoAuth._shared_token_expires_at = time.time() + expires_in

            logger.info("Successfully refreshed Zoho access token")
            return ZohoAuth._shared_access_token
        except requests.RequestException as e:
            logger.error(f"Failed to refresh Zoho access token: {e}")
            raise

    def get_authorization_url(self, scope: str = "ZohoCRM.modules.ALL") -> str:
        """Generate the authorization URL for OAuth flow."""
        params = {
            'client_id': self.client_id,
            'redirect_uri': self.redirect_uri,
            'response_type': 'code',
            'scope': scope,
            'access_type': 'offline'
        }
        query = "&".join([f"{k}={requests.utils.quote(str(v))}" for k, v in params.items()])
        return f"{self.auth_base_url}?{query}"

    def exchange_code_for_tokens(self, code: str) -> Dict[str, Any]:
        """Exchange authorization code for access and refresh tokens."""
        data = {
            'grant_type': 'authorization_code',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'redirect_uri': self.redirect_uri,
            'code': code
        }

        logger.info(f"Exchanging code for tokens with data: {data}")
        logger.info(f"Authorization code (first 10 chars): {code[:10]}...")
        logger.info(f"Attempting connection to {self.auth_url} for token exchange")

        try:
            response = requests.post(self.auth_url, data=data, timeout=30)
            logger.info(f"Token exchange response status: {response.status_code}")
            if response.status_code != 200:
                logger.error(f"Token exchange failed: {response.text}")
            response.raise_for_status()
            token_data = response.json()

            if 'access_token' not in token_data:
                logger.error(f"Access token not found in response. Full response: {token_data}")
                raise ValueError(f"Invalid response from Zoho: missing access_token. Response: {token_data}")

            ZohoAuth._shared_access_token = token_data['access_token']
            self.refresh_token = token_data['refresh_token']
            expires_in = token_data.get('expires_in', 3600)
            ZohoAuth._shared_token_expires_at = time.time() + expires_in

            logger.info("Successfully exchanged code for Zoho tokens")
            return token_data
        except requests.ConnectionError as e:
            logger.error(f"Connection error during token exchange: {e}")
            if isinstance(e, requests.exceptions.ConnectionError) and hasattr(e, 'args') and e.args:
                inner_exc = e.args[0]
                if isinstance(inner_exc, ConnectionResetError):
                    logger.error("Connection was reset by remote host - possible firewall blocking or invalid request")
            raise
        except requests.RequestException as e:
            logger.error(f"Failed to exchange code for tokens: {e}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response: {e.response.text}")
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

    def __init__(self, client_id: str, client_secret: str, redirect_uri: str, refresh_token: Optional[str] = None, data_center: str = "us"):
        self.auth = ZohoAuth(client_id, client_secret, redirect_uri, refresh_token, data_center)
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
            # Handle 204 No Content (e.g., search with no results)
            if response.status_code == 204:
                return {"data": []}
            try:
                return response.json()
            except ValueError as e:
                logger.error(f"Failed to parse JSON response: {e}. Response body: {response.text}")
                raise
        except requests.RequestException as e:
            logger.error(f"Zoho API request failed: {method} {url} - {e}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response headers: {dict(e.response.headers)}")
                logger.error(f"Response body: {e.response.text}")
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
        response = self._make_request("PUT", endpoint, {"data": [lead_data]})

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

    def update_contact(self, contact_id: str, contact_data: Dict[str, Any]) -> bool:
        """Update an existing contact in Zoho CRM."""
        endpoint = f"/crm/v2/Contacts/{contact_id}"
        response = self._make_request("PUT", endpoint, {"data": [contact_data]})

        if 'data' in response and response['data']:
            logger.info(f"Updated Zoho contact: {contact_id}")
            return True
        return False

    def create_note(self, module: str, record_id: str, note_data: Dict[str, Any]) -> str:
        """Create a new note for a record in Zoho CRM. Returns the note ID."""
        endpoint = f"/crm/v2/{module}/{record_id}/Notes"
        response = self._make_request("POST", endpoint, {"data": [note_data]})

        if 'data' in response and response['data']:
            note_id = response['data'][0]['details']['id']
            logger.info(f"Created Zoho note: {note_id}")
            return note_id
        else:
            raise ValueError("Failed to create note - no ID returned")

    def get_notes(self, module: str, record_id: str) -> List[Dict[str, Any]]:
        """Get list of notes for a record."""
        endpoint = f"/crm/v2/{module}/{record_id}/Notes"
        response = self._make_request("GET", endpoint)
        return response.get('data', [])

    def update_note(self, module: str, record_id: str, note_id: str, note_data: Dict[str, Any]) -> bool:
        """Update an existing note in Zoho CRM."""
        endpoint = f"/crm/v2/{module}/{record_id}/Notes/{note_id}"
        response = self._make_request("PUT", endpoint, {"data": [note_data]})

        if 'data' in response and response['data']:
            logger.info(f"Updated Zoho note: {note_id}")
            return True
        return False
    def get_contact(self, contact_id: str) -> Optional[Dict[str, Any]]:
        """Get a contact by ID from Zoho CRM."""
        endpoint = f"/crm/v2/Contacts/{contact_id}"
        response = self._make_request("GET", endpoint)

        if 'data' in response and response['data']:
            return response['data'][0]
        return None

    def get_lead(self, lead_id: str) -> Optional[Dict[str, Any]]:
        """Get a lead by ID from Zoho CRM."""
        endpoint = f"/crm/v2/Leads/{lead_id}"
        response = self._make_request("GET", endpoint)

        if 'data' in response and response['data']:
            return response['data'][0]
        return None


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

    def get_attachments(self, module: str, record_id: str) -> List[Dict[str, Any]]:
        """Get list of attachments for a record."""
        endpoint = f"/crm/v2/{module}/{record_id}/Attachments"
        response = self._make_request("GET", endpoint)
        return response.get('data', [])

    def search_leads(self, criteria: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Search for leads based on criteria."""
        endpoint = "/crm/v2/Leads/search"
        # Zoho CRM search format: criteria=(field:operator:value)
        criteria_parts = []
        for field, value in criteria.items():
            criteria_parts.append(f"({field}:equals:{value})")

        if criteria_parts:
            criteria_str = "or".join(criteria_parts) if len(criteria_parts) > 1 else criteria_parts[0]
            criteria_str = urllib.parse.quote(criteria_str)
            endpoint += f"?criteria={criteria_str}"

        response = self._make_request("GET", endpoint)
        return response.get('data', [])

    def search_contacts(self, criteria: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Search for contacts based on criteria."""
        endpoint = "/crm/v2/Contacts/search"
        # Zoho CRM search format: criteria=(field:operator:value)
        criteria_parts = []
        for field, value in criteria.items():
            criteria_parts.append(f"({field}:equals:{value})")

        if criteria_parts:
            criteria_str = "or".join(criteria_parts) if len(criteria_parts) > 1 else criteria_parts[0]
            criteria_str = urllib.parse.quote(criteria_str)
            endpoint += f"?criteria={criteria_str}"

        response = self._make_request("GET", endpoint)
        return response.get('data', [])

    def create_account(self, account_data: Dict[str, Any]) -> str:
        """Create a new account in Zoho CRM. Returns the account ID."""
        endpoint = "/crm/v2/Accounts"
        response = self._make_request("POST", endpoint, {"data": [account_data]})

        if 'data' in response and response['data']:
            account_id = response['data'][0]['details']['id']
            logger.info(f"Created Zoho account: {account_id}")
            return account_id
        else:
            raise ValueError("Failed to create account - no ID returned")

    def update_account(self, account_id: str, account_data: Dict[str, Any]) -> bool:
        """Update an existing account in Zoho CRM."""
        endpoint = f"/crm/v2/Accounts/{account_id}"
        response = self._make_request("PUT", endpoint, {"data": [account_data]})

        if 'data' in response and response['data']:
            logger.info(f"Updated Zoho account: {account_id}")
            return True
        return False

    def get_account(self, account_id: str) -> Optional[Dict[str, Any]]:
        """Get an account by ID from Zoho CRM."""
        endpoint = f"/crm/v2/Accounts/{account_id}"
        response = self._make_request("GET", endpoint)

        if 'data' in response and response['data']:
            return response['data'][0]
        return None

    def search_accounts(self, criteria: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Search for accounts based on criteria."""
        endpoint = "/crm/v2/Accounts/search"
        # Zoho CRM search format: criteria=(field:operator:value)
        criteria_parts = []
        for field, value in criteria.items():
            criteria_parts.append(f"({field}:equals:{value})")

        if criteria_parts:
            criteria_str = "or".join(criteria_parts) if len(criteria_parts) > 1 else criteria_parts[0]
            criteria_str = urllib.parse.quote(criteria_str)
            endpoint += f"?criteria={criteria_str}"

        response = self._make_request("GET", endpoint)
        return response.get('data', [])


def get_zoho_client() -> ZohoCRMClient:
    """Factory function to create Zoho client from environment variables."""
    client_id = os.getenv("ZOHO_CLIENT_ID")
    client_secret = os.getenv("ZOHO_CLIENT_SECRET")
    redirect_uri = os.getenv("ZOHO_REDIRECT_URI")
    refresh_token = os.getenv("ZOHO_REFRESH_TOKEN")

    if not client_id or not client_secret or not redirect_uri:
        raise ValueError("ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, and ZOHO_REDIRECT_URI must be set in environment")

    # Default to US data center, can be overridden with ZOHO_DATA_CENTER env var
    data_center = os.getenv("ZOHO_DATA_CENTER", "us")

    return ZohoCRMClient(client_id, client_secret, redirect_uri, refresh_token, data_center)