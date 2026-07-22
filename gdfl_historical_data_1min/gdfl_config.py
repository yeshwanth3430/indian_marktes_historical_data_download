"""
GDFL API configuration.

Each API key is tied to specific exchanges — do not mix them:

    GDFL_NSE_API_KEY   NFO + NSE_IDX   NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY
    GDFL_BSE_API_KEY   BFO + BSE_IDX   SENSEX, BANKEX

Keys are read from the environment so nothing sensitive is committed. Copy
.env.example to .env, replace the ****** with your own keys, and load_dotenv()
picks them up.

Both keys are single-session: one websocket at a time per key. The NSE and BSE
keys are separate sessions, so an NSE and a BSE script may run concurrently —
but never two scripts using the same key.

Importing a key that is not set raises immediately with a usable message, so a
missing key fails at startup rather than as an opaque auth error later.
"""

import os

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

# Public endpoint — not a secret, overridable for a different GDFL host.
WS_ENDPOINT = os.environ.get("GDFL_WS_ENDPOINT",
                             "ws://nimblewebstream.lisuns.com:4575/")

_KEYS = {
    "NSE_API_KEY": "GDFL_NSE_API_KEY",
    "BSE_API_KEY": "GDFL_BSE_API_KEY",
}


def mask(secret: str, keep: int = 4) -> str:
    """Render a key for logs: first/last few characters, rest starred out."""
    if not secret:
        return "<unset>"
    if len(secret) <= keep * 2:
        return "*" * len(secret)
    return f"{secret[:keep]}{'*' * 6}{secret[-keep:]}"


def __getattr__(name: str) -> str:
    """Resolve NSE_API_KEY / BSE_API_KEY from the environment on first access.

    Defined as a module __getattr__ (PEP 562) rather than plain constants so
    that a script importing only the NSE key is not blocked by a missing BSE
    key, and vice versa.
    """
    env_var = _KEYS.get(name)
    if env_var is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    value = os.environ.get(env_var, "")
    if not value:
        raise SystemExit(
            f"{env_var} is not set.\n"
            "Copy .env.example to .env and add your GDFL key (see README.md)."
        )
    return value
