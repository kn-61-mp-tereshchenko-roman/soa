import os

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt import PyJWKClient
from jwt.exceptions import PyJWKClientError


ISSUER = os.getenv(
    "KEYCLOAK_ISSUER",
    "http://localhost:8082/realms/calorie-system",
)

JWKS_URL = os.getenv(
    "KEYCLOAK_JWKS_URL",
    "http://keycloak:8080/realms/calorie-system/protocol/openid-connect/certs",
)

AUDIENCE = os.getenv("JWT_AUDIENCE", "calorie-api")

ALGORITHMS = ["RS256"]

bearer_scheme = HTTPBearer(auto_error=False)

jwks_client = PyJWKClient(
    JWKS_URL,
    cache_keys=True,
)


def get_current_token(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
):
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials

    try:
        signing_key = jwks_client.get_signing_key_from_jwt(token)

        payload = jwt.decode(
            token,
            signing_key.key,
            algorithms=ALGORITHMS,
            audience=AUDIENCE,
            issuer=ISSUER,
            options={
                "require": ["exp", "iss", "aud", "sub"],
            },
        )

    except PyJWKClientError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not retrieve Keycloak signing keys",
        ) from exc

    except jwt.InvalidTokenError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired access token",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc

    return payload


def require_scopes(*required_scopes: str):
    def scope_dependency(
        payload: dict = Depends(get_current_token),
    ):
        token_scopes = payload.get("scope", "")

        if isinstance(token_scopes, str):
            available_scopes = set(token_scopes.split())
        else:
            available_scopes = set(token_scopes)

        missing_scopes = set(required_scopes) - available_scopes

        if missing_scopes:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={
                    "message": "Insufficient permissions",
                    "missing_scopes": sorted(missing_scopes),
                },
            )

        return payload

    return scope_dependency


def require_scope(required_scope: str):
    return require_scopes(required_scope)