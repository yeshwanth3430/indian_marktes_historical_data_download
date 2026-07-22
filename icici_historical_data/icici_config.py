"""
ICICI Direct Breeze API credentials and session helper.

Secrets are read from the environment so nothing sensitive is ever committed.
Copy .env.example to .env, fill in your own values, and load_dotenv() picks
them up at import time.

BREEZE_SESSION_TOKEN expires every day. To refresh it: open the login URL that
connect() prints, log in, then copy the `API_Session` value out of the URL you
are redirected to and paste it into .env.
"""

import os
import urllib.parse

from breeze_connect import BreezeConnect
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ.get("BREEZE_API_KEY", "")
API_SECRET = os.environ.get("BREEZE_API_SECRET", "")
SESSION_TOKEN = os.environ.get("BREEZE_SESSION_TOKEN", "")

LOGIN_URL = ("https://api.icicidirect.com/apiuser/login?api_key="
             + urllib.parse.quote_plus(API_KEY))


def connect() -> BreezeConnect:
    """Return a BreezeConnect client with an active session."""
    missing = [name for name, value in (
        ("BREEZE_API_KEY", API_KEY),
        ("BREEZE_API_SECRET", API_SECRET),
        ("BREEZE_SESSION_TOKEN", SESSION_TOKEN),
    ) if not value]
    if missing:
        raise SystemExit(
            "Missing credentials: " + ", ".join(missing)
            + "\nCopy .env.example to .env and fill it in (see README.md)."
        )

    breeze = BreezeConnect(api_key=API_KEY)

    # Printed so a stale session token can be regenerated without digging.
    print(f"Login URL (for a fresh session token): {LOGIN_URL}")

    breeze.generate_session(api_secret=API_SECRET, session_token=SESSION_TOKEN)
    print("Connected to ICICI Direct API successfully.")
    return breeze
