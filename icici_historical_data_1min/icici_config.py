"""
ICICI Direct Breeze API credentials and session helper.

Secrets are read from the environment so nothing sensitive is ever committed.
Copy .env.example to .env, replace the ****** with your own values, and
load_dotenv() picks them up at import time.

BREEZE_SESSION_TOKEN expires every day. To refresh it: run login_url(), open
the result, log in, then copy the `API_Session` value out of the URL you are
redirected to and paste it into .env.

Nothing here prints a credential in full — startup logs show masked values, so
pasting terminal output somewhere does not leak the key.
"""

import os
import urllib.parse

from breeze_connect import BreezeConnect

# Always resolve .env next to this file, so the scripts work from any cwd.
_ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")

try:
    from dotenv import load_dotenv
except ImportError:  # python-dotenv is optional — fall back to a tiny reader
    def load_dotenv(dotenv_path: str, **_kwargs) -> bool:
        if not os.path.exists(dotenv_path):
            return False
        with open(dotenv_path) as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                # real environment variables win over .env
                os.environ.setdefault(key.strip(), value.strip().strip("\"'"))
        return True

load_dotenv(_ENV_PATH)

API_KEY = os.environ.get("BREEZE_API_KEY", "")
API_SECRET = os.environ.get("BREEZE_API_SECRET", "")
SESSION_TOKEN = os.environ.get("BREEZE_SESSION_TOKEN", "")


def mask(secret: str, keep: int = 4) -> str:
    """Render a secret for logs: first/last few characters, rest starred out."""
    if not secret:
        return "<unset>"
    if len(secret) <= keep * 2:
        return "*" * len(secret)
    return f"{secret[:keep]}{'*' * 6}{secret[-keep:]}"


def login_url() -> str:
    """The ICICI login URL used to mint a fresh session token.

    This embeds the real API key, so it is returned rather than printed —
    call it only when you actually need to log in, and do not paste the
    result into a shared log.
    """
    return ("https://api.icicidirect.com/apiuser/login?api_key="
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

    print(f"Breeze API key    : {mask(API_KEY)}")
    print(f"Breeze session    : {mask(SESSION_TOKEN)}")

    breeze = BreezeConnect(api_key=API_KEY)
    try:
        breeze.generate_session(api_secret=API_SECRET, session_token=SESSION_TOKEN)
    except Exception as exc:
        # Almost always an expired session token — point at the fix.
        raise SystemExit(
            f"Breeze session failed: {exc}\n"
            "The session token expires daily. Get a fresh one by running:\n"
            "  python -c \"import icici_config; print(icici_config.login_url())\"\n"
            "then log in and copy API_Session from the redirect URL into .env."
        )

    print("Connected to ICICI Direct API successfully.")
    return breeze
