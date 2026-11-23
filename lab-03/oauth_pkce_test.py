import base64
import hashlib
import json
import os
import secrets
import threading
import urllib.error
import urllib.parse
import urllib.request
import webbrowser

from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path


ISSUER = "http://localhost:8082/realms/calorie-system"
CLIENT_ID = "calorie-web"
REDIRECT_URI = "http://localhost:8090/callback"

SCOPES = [
    "openid",
    "profile",
    "email",
    "profiles:read",
    "profiles:write",
    "foods:read",
    "foods:write",
    "meals:read",
    "meals:write",
    "analytics:read",
]

TOKEN_FILE = (
    Path.home()
    / ".cache"
    / "calorie-system"
    / "access_token"
)


def base64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


code_verifier = base64url(secrets.token_bytes(32))

code_challenge = base64url(
    hashlib.sha256(code_verifier.encode("ascii")).digest()
)

state = secrets.token_urlsafe(32)

authorization_params = {
    "client_id": CLIENT_ID,
    "response_type": "code",
    "redirect_uri": REDIRECT_URI,
    "scope": " ".join(SCOPES),
    "state": state,
    "code_challenge": code_challenge,
    "code_challenge_method": "S256",
}

authorization_url = (
    f"{ISSUER}/protocol/openid-connect/auth?"
    + urllib.parse.urlencode(authorization_params)
)


class CallbackHandler(BaseHTTPRequestHandler):

    authorization_code = None
    authorization_error = None
    received_state = None

    def do_GET(self):
        parsed_url = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed_url.query)

        CallbackHandler.authorization_code = (
            params.get("code", [None])[0]
        )

        CallbackHandler.authorization_error = (
            params.get("error", [None])[0]
        )

        CallbackHandler.received_state = (
            params.get("state", [None])[0]
        )

        if CallbackHandler.authorization_code:
            message = (
                "Authorization code received. "
                "You can close this browser tab."
            )
            status = 200
        else:
            message = (
                "Authorization failed. "
                "Return to the terminal for details."
            )
            status = 400

        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(message.encode("utf-8"))

    def log_message(self, format, *args):
        pass


print("Starting local OAuth callback server...")

server = HTTPServer(
    ("127.0.0.1", 8090),
    CallbackHandler,
)

server.timeout = 180

print("Opening Keycloak authorization page...")
print("Sign in using Google when prompted.")

webbrowser.open(authorization_url)

server.handle_request()
server.server_close()

authorization_code = CallbackHandler.authorization_code

if CallbackHandler.received_state != state:
    raise RuntimeError(
        "OAuth state validation failed. Do not use the received code."
    )

if CallbackHandler.authorization_error:
    raise RuntimeError(
        f"Keycloak authorization error: "
        f"{CallbackHandler.authorization_error}"
    )

if not authorization_code:
    raise RuntimeError(
        "No authorization code received within 180 seconds."
    )

print("Authorization code received successfully.")
print("Exchanging authorization code for access token...")

token_url = (
    f"{ISSUER}/protocol/openid-connect/token"
)

token_data = {
    "grant_type": "authorization_code",
    "client_id": CLIENT_ID,
    "code": authorization_code,
    "redirect_uri": REDIRECT_URI,
    "code_verifier": code_verifier,
}

request = urllib.request.Request(
    token_url,
    data=urllib.parse.urlencode(token_data).encode("utf-8"),
    headers={
        "Content-Type": "application/x-www-form-urlencoded",
    },
    method="POST",
)

try:
    with urllib.request.urlopen(request, timeout=15) as response:
        token_response = json.loads(response.read().decode("utf-8"))

except urllib.error.HTTPError as exc:
    error_body = exc.read().decode("utf-8", errors="replace")
    raise RuntimeError(
        f"Token exchange failed: HTTP {exc.code}: {error_body}"
    ) from exc

access_token = token_response.get("access_token")

if not access_token:
    raise RuntimeError(
        "Keycloak response does not contain an access token."
    )

TOKEN_FILE.parent.mkdir(
    parents=True,
    exist_ok=True,
)

TOKEN_FILE.parent.chmod(0o700)

file_descriptor = os.open(
    TOKEN_FILE,
    os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
    0o600,
)

with os.fdopen(file_descriptor, "w") as token_file:
    token_file.write(access_token)

print()
print("OAuth 2.0 Authorization Code Flow completed successfully.")
print(f"Token type: {token_response.get('token_type')}")
print(f"Expires in: {token_response.get('expires_in')} seconds")
print(f"Access token saved to: {TOKEN_FILE}")
print("The token itself has not been printed.")
