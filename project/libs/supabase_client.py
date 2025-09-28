import os
from supabase import create_client, Client
from dotenv import load_dotenv
import httpx


# Load environment variables from .env
load_dotenv()

_SUPABASE_URL = os.getenv("SUPABASE_URL")
_SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

_client: Client | None = None


def _init_client() -> Client:
    """
    Initialize and return a Supabase client using env variables.
    Raises RuntimeError if credentials are missing.
    """
    global _client

    if not _SUPABASE_URL or not _SUPABASE_SERVICE_KEY:
        raise RuntimeError(
            "Supabase credentials are missing. Ensure SUPABASE_URL and SUPABASE_SERVICE_KEY are set in the environment."
        )

    if _client is None:
        _client = create_client(_SUPABASE_URL, _SUPABASE_SERVICE_KEY)
        # Set longer timeouts on the underlying httpx session to handle slow connections
        _client.postgrest.session.timeout = httpx.Timeout(30.0, connect=30.0)

    return _client


def get_client() -> Client:
    """
    Retrieve the singleton Supabase client instance.
    """
    return _init_client()


def check_connection() -> bool:
    """
    Validate Supabase connection by performing a trivial query.
    Returns True if successful, raises RuntimeError otherwise.
    """
    try:
        client = get_client()
        # Example: fetch just one row from any available system table
        response = client.table("pg_tables").select("*").limit(1).execute()
        # If no exception, connection works
        return True
    except Exception as e:
        raise RuntimeError(f"Supabase connection failed: {e}")


def _businesses_table_schema() -> dict:
    """
    Define schema for the 'businesses' table.
    """
    return {
        "name": "businesses",
        "columns": {
            # Primary key
            "id": "text primary key",

            # Core text fields
            "alias": "text",
            "name": "text",
            "image_url": "text",
            "url": "text",
            "phone": "text",
            "display_phone": "text",
            "price": "text",
            "state": "text",
            "country": "text",
            "zip_code": "text",
            "city": "text",
            "address1": "text",
            "address2": "text",
            "address3": "text",
            "cross_streets": "text",
            "date_opened": "text",
            "date_closed": "text",
            "yelp_menu_url": "text",
            "cbsa": "text",
            "primary_category": "text",
            "score": "text",
            "distance": "text",

            # Integer fields
            "review_count": "integer",
            "photo_count": "integer",

            # Boolean
            "is_closed": "boolean",
            "is_claimed": "boolean",

            # Floats
            "rating": "float",
            "latitude": "float",
            "longitude": "float",
            "response_rate": "float",

            # JSONB fields
            "categories": "jsonb",
            "coordinates": "jsonb",
            "transactions": "jsonb",
            "location": "jsonb",
            "display_address": "jsonb",
            "attributes": "jsonb",
            "photos": "jsonb",
            "special_hours": "jsonb",
            "messaging": "jsonb",
            "photo_details": "jsonb",
            "popularity_score": "jsonb",
            "rapc": "jsonb",
            "hours": "jsonb",
        }
    }


def ensure_table_exists(table_schema: dict | None = None) -> None:
    """
    Ensure a table exists in the Supabase database.
    If it does not exist, attempt to create it using the provided schema.
    
    Args:
        table_schema (dict): Dictionary with `name` and `columns` definitions.
            Example:
                {
                  "name": "businesses",
                  "columns": {
                      "id": "uuid primary key",
                      "name": "text",
                      "created_at": "timestamp default now()"
                  }
                }
    """
    client = get_client()
    table_name = table_schema.get("name")

    if not table_name:
        raise ValueError("table_schema must include a 'name' field.")

    try:
        # Try running a count query to check existence
        _ = client.table(table_name).select("id").limit(1).execute()
    except Exception:
        raise RuntimeError(
            f"Table '{table_name}' does not exist in Supabase. Please create it via migrations or the dashboard."
        )