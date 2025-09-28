import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from flask import Flask, request, redirect, jsonify
from project.libs.zoho_client import ZohoAuth
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route('/oauth/start')
def oauth_start():
    """Start the OAuth flow by redirecting to Zoho authorization URL."""
    client_id = os.getenv("ZOHO_CLIENT_ID")
    client_secret = os.getenv("ZOHO_CLIENT_SECRET")
    redirect_uri = os.getenv("ZOHO_REDIRECT_URI")

    if not client_id or not client_secret or not redirect_uri:
        return jsonify({"error": "Missing Zoho credentials in environment"}), 500


    auth = ZohoAuth(client_id, client_secret, redirect_uri)
    auth_url = auth.get_authorization_url()
    return redirect(auth_url)

@app.route('/oauth/callback')
def oauth_callback():
    """Handle the OAuth callback from Zoho."""
    logger.info("OAuth callback triggered")
    code = request.args.get('code')
    error = request.args.get('error')

    if error:
        logger.error(f"OAuth error received: {error}")
        return f"OAuth error: {error}", 400

    if not code:
        logger.error("No authorization code received in callback")
        return "No authorization code received", 400

    logger.info(f"Authorization code received (first 10 chars): {code[:10]}...")

    client_id = os.getenv("ZOHO_CLIENT_ID")
    client_secret = os.getenv("ZOHO_CLIENT_SECRET")
    redirect_uri = os.getenv("ZOHO_REDIRECT_URI")

    if not client_id or not client_secret or not redirect_uri:
        return jsonify({"error": "Missing Zoho credentials in environment"}), 500


    auth = ZohoAuth(client_id, client_secret, redirect_uri)

    try:
        token_data = auth.exchange_code_for_tokens(code)
        refresh_token = token_data.get('refresh_token')

        if refresh_token:
            # Update .env file with the refresh token
            env_file = os.path.join(os.path.dirname(__file__), '..', '.env')
            with open(env_file, 'r') as f:
                lines = f.readlines()

            updated = False
            for i, line in enumerate(lines):
                if line.startswith('ZOHO_REFRESH_TOKEN='):
                    lines[i] = f'ZOHO_REFRESH_TOKEN={refresh_token}\n'
                    updated = True
                    break

            if not updated:
                lines.append(f'ZOHO_REFRESH_TOKEN={refresh_token}\n')

            with open(env_file, 'w') as f:
                f.writelines(lines)

            logger.info("Successfully saved Zoho refresh token to .env")
            return "OAuth authorization successful! You can close this window."
        else:
            return "Failed to obtain refresh token", 500

    except Exception as e:
        logger.error(f"OAuth callback error: {e}")
        return f"OAuth callback error: {e}", 500

if __name__ == '__main__':
    # For development, run on HTTP. For production, use HTTPS with proper SSL.
    app.run(host='0.0.0.0', port=5000, debug=True)