"""Smoke-test the Snowflake credentials in `.env`.

Run it before touching Airflow — it fails in one place, with one error message,
instead of somewhere inside a scheduler log:

    ../.venv/bin/python check_connection.py
"""

from __future__ import annotations

import sys
from pathlib import Path

from dotenv import dotenv_values

HERE = Path(__file__).resolve().parent
ENV_FILE = HERE / ".env"

# The context query is one round-trip that proves four things at once: the
# handshake worked, the role resolved, a warehouse is attached (NULL here is
# why COUNT(*) queries later fail with "no active warehouse"), and the database
# and schema defaults landed.
CONTEXT_QUERY = """
SELECT CURRENT_VERSION(),
       CURRENT_ACCOUNT(),
       CURRENT_USER(),
       CURRENT_ROLE(),
       CURRENT_WAREHOUSE(),
       CURRENT_DATABASE(),
       CURRENT_SCHEMA()
"""


def load_env() -> dict[str, str]:
    """Read `.env`, keeping only the keys that were actually filled in."""
    if not ENV_FILE.exists():
        sys.exit(f"No {ENV_FILE} — copy .env.example to .env and fill it in.")
    # dotenv_values rather than load_dotenv: reading the file directly keeps a
    # stale exported shell variable from silently overriding what is on disk.
    return {k: v.strip() for k, v in dotenv_values(ENV_FILE).items() if v and v.strip()}


def read_private_key(env: dict[str, str]) -> bytes:
    """Load the PEM key and hand back the DER bytes the connector wants."""
    from cryptography.hazmat.primitives import serialization

    key_path = Path(env["SNOWFLAKE_PRIVATE_KEY_PATH"])
    if not key_path.is_absolute():
        key_path = HERE / key_path
    if not key_path.exists():
        sys.exit(f"SNOWFLAKE_PRIVATE_KEY_PATH points at {key_path}, which does not exist.")

    passphrase = env.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    private_key = serialization.load_pem_private_key(
        key_path.read_bytes(),
        password=passphrase.encode() if passphrase else None,
    )
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def build_connect_kwargs(env: dict[str, str]) -> dict[str, object]:
    for required in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER"):
        if required not in env:
            sys.exit(f"{required} is empty in .env — fill it in first.")

    kwargs: dict[str, object] = {
        "account": env["SNOWFLAKE_ACCOUNT"],
        "user": env["SNOWFLAKE_USER"],
        # Shows up in Snowflake's QUERY_HISTORY, so these runs are easy to spot.
        "application": "innowise_setup_check",
        "login_timeout": 20,
    }

    # An empty authenticator is not equivalent to the default: the connector
    # reads it as a request for an SSO handshake and never sends the password.
    authenticator = env.get("SNOWFLAKE_AUTHENTICATOR", "snowflake")
    kwargs["authenticator"] = authenticator

    # Ordered most-specific first, so a leftover value in a block the user is
    # no longer using cannot quietly take precedence over the current one.
    if "SNOWFLAKE_PAT" in env:
        # A PAT travels in the password field with the ordinary "snowflake"
        # authenticator — it substitutes for the password rather than
        # accompanying it. The connector also defines a PROGRAMMATIC_ACCESS_TOKEN
        # authenticator, but that is a different server-side flow and is
        # rejected outright ("token is invalid") on a standard account, so it is
        # deliberately not what this maps to.
        kwargs["password"] = env["SNOWFLAKE_PAT"]
        kwargs["_auth_label"] = "PAT"
        expected = "snowflake"
    elif "SNOWFLAKE_OAUTH_TOKEN" in env:
        kwargs["token"] = env["SNOWFLAKE_OAUTH_TOKEN"]
        expected = "oauth"
    elif "SNOWFLAKE_PRIVATE_KEY_PATH" in env:
        kwargs["private_key"] = read_private_key(env)
        expected = "snowflake"
    elif "SNOWFLAKE_PASSWORD" in env:
        kwargs["password"] = env["SNOWFLAKE_PASSWORD"]
        expected = "snowflake"
    else:
        sys.exit(
            "No credential in .env — set one of SNOWFLAKE_PAT, "
            "SNOWFLAKE_OAUTH_TOKEN, SNOWFLAKE_PRIVATE_KEY_PATH or "
            "SNOWFLAKE_PASSWORD."
        )

    # Mismatched authenticator and credential fails deep inside the handshake
    # with an error that names neither, so it is worth catching up front.
    if authenticator.lower() != expected.lower():
        sys.exit(
            f"SNOWFLAKE_AUTHENTICATOR is '{authenticator}', but the credential "
            f"you filled in needs '{expected}'. Fix one of the two in .env."
        )

    for key in ("ROLE", "WAREHOUSE", "DATABASE", "SCHEMA"):
        if f"SNOWFLAKE_{key}" in env:
            kwargs[key.lower()] = env[f"SNOWFLAKE_{key}"]

    return kwargs


def describe_auth(kwargs: dict[str, object]) -> str:
    if "_auth_label" in kwargs:
        return str(kwargs["_auth_label"])
    if "private_key" in kwargs:
        return "key pair"
    if "token" in kwargs:
        return "OAuth token"
    if kwargs.get("authenticator", "").lower() == "programmatic_access_token":
        return "PAT"
    return "password"


def main() -> int:
    import snowflake.connector

    env = load_env()
    kwargs = build_connect_kwargs(env)
    auth = describe_auth(kwargs)
    # Marker only — the connector would reject an unknown keyword argument.
    kwargs.pop("_auth_label", None)
    print(f"Connecting to {kwargs['account']} as {kwargs['user']} ({auth} auth)...")

    try:
        with snowflake.connector.connect(**kwargs) as conn:
            with conn.cursor() as cur:
                row = cur.execute(CONTEXT_QUERY).fetchone()
    except snowflake.connector.errors.Error as exc:
        sys.stdout.flush()  # keep the "Connecting to..." line above the error
        print(f"\nConnection FAILED: {exc}", file=sys.stderr)
        print(explain(exc), file=sys.stderr)
        return 1

    labels = ["Version", "Account", "User", "Role", "Warehouse", "Database", "Schema"]
    print("\nConnected.\n")
    for label, value in zip(labels, row):
        print(f"  {label:<10} {value if value is not None else '<not set>'}")

    if row[4] is None:
        print(
            "\nNote: no active warehouse. The handshake works, but any query that "
            "reads data will fail until SNOWFLAKE_WAREHOUSE names one you can use."
        )
    return 0


def explain(exc: Exception) -> str:
    """Map the handful of errors that actually show up here to their cause."""
    text = str(exc)
    if any(code in text for code in ("290404", "250001")) or "Could not connect" in text:
        return "Hint: SNOWFLAKE_ACCOUNT is almost certainly wrong — it is the account identifier (myorg-my_account), not the full URL and not the e-mail domain."
    if "390100" in text or "Incorrect username or password" in text:
        return "Hint: bad user or password. If the account enforces MFA, password auth is blocked entirely — use key-pair auth instead."
    if "394400" in text or "Invalid OAuth access token" in text or "programmatic access token" in text.lower():
        return "Hint: the token is expired, revoked, or issued for a different user. PATs are also refused if the user's network policy does not permit them."
    if "390144" in text or "JWT token is invalid" in text:
        return "Hint: the public key is not registered on the user, or SNOWFLAKE_USER is not spelled the way Snowflake stores it (usually uppercase)."
    if "does not exist or not authorized" in text:
        return "Hint: the role, warehouse, database or schema in .env is misspelled or not granted to this user."
    return "Hint: re-check the values in .env against Snowsight."


if __name__ == "__main__":
    raise SystemExit(main())
