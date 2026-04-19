from __future__ import annotations

import logging
import time
from typing import Any

import httpx
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt

from app.config import settings

logger = logging.getLogger(__name__)

_bearer = HTTPBearer(auto_error=False)


class EntraTokenValidator:
    def __init__(self, tenant_id: str, api_client_id: str, api_scope: str = "") -> None:
        self._tenant_id = tenant_id
        self._api_client_id = api_client_id
        self._issuer = f"https://login.microsoftonline.com/{tenant_id}/v2.0"
        self._valid_issuers: set[str] = {
            f"https://login.microsoftonline.com/{tenant_id}/v2.0",
            f"https://login.microsoftonline.com/{tenant_id}/",
            f"https://sts.windows.net/{tenant_id}/",
        }
        self._jwks_url = f"https://login.microsoftonline.com/{tenant_id}/discovery/v2.0/keys"
        self._jwks_cache: dict[str, dict[str, Any]] = {}
        self._jwks_expiry = 0.0
        # Accept common Entra audience formats.
        self._valid_audiences: set[str] = {api_client_id, f"api://{api_client_id}"}
        if api_scope:
            self._valid_audiences.add(api_scope)
            # If scope is api://<app-id>/<scope-name>, audience is usually api://<app-id>.
            if "/" in api_scope:
                self._valid_audiences.add(api_scope.rsplit("/", 1)[0])
        logger.info("EntraTokenValidator configured with valid audiences: %s", sorted(self._valid_audiences))

    async def validate_token(self, token: str) -> dict[str, Any]:
        if not token:
            logger.error("Token validation: no token provided")
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing bearer token")

        try:
            header = jwt.get_unverified_header(token)
            logger.info(f"Token header: {header}")
        except JWTError as exc:
            logger.error(f"Invalid token header: {exc}")
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token header") from exc

        kid = header.get("kid")
        if not kid:
            logger.error("Token missing key id (kid)")
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token missing key id")

        logger.info(f"Token kid: {kid}")
        jwk = await self._get_signing_key(kid)
        try:
            # Validate signature and issuer only; audience is checked manually below.
            claims = jwt.decode(
                token,
                jwk,
                algorithms=["RS256"],
                options={"verify_aud": False, "verify_iss": False},
            )

            token_aud = claims.get("aud")
            if isinstance(token_aud, str):
                audience_ok = token_aud in self._valid_audiences
            elif isinstance(token_aud, list):
                audience_ok = any(isinstance(a, str) and a in self._valid_audiences for a in token_aud)
            else:
                audience_ok = False

            if not audience_ok:
                logger.error("Invalid audience: %r. Valid audiences: %s", token_aud, sorted(self._valid_audiences))
                raise JWTError("Invalid audience")

            token_iss = claims.get("iss")
            if not isinstance(token_iss, str) or token_iss not in self._valid_issuers:
                logger.error("Invalid issuer: %r. Valid issuers: %s", token_iss, sorted(self._valid_issuers))
                raise JWTError("Invalid issuer")

            logger.info(
                "Token validated successfully. Claims: aud=%r, iss=%r, sub=%r",
                claims.get("aud"),
                claims.get("iss"),
                claims.get("sub"),
            )
        except JWTError as exc:
            # Try to decode without validation
            try:
                unverified = jwt.decode(token, options={"verify_signature": False})
                logger.error(
                    "Token validation failed: %s. Unverified: aud=%r, iss=%r, valid_audiences=%s",
                    exc,
                    unverified.get("aud"),
                    unverified.get("iss"),
                    sorted(self._valid_audiences),
                )
            except Exception:
                logger.error("Token validation failed: %s", exc)
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token validation failed") from exc

        return claims

    async def _get_signing_key(self, kid: str) -> dict[str, Any]:
        now = time.time()
        if now >= self._jwks_expiry or not self._jwks_cache:
            await self._refresh_jwks()

        key = self._jwks_cache.get(kid)
        if key:
            return key

        await self._refresh_jwks()
        key = self._jwks_cache.get(kid)
        if not key:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unknown signing key")

        return key

    async def _refresh_jwks(self) -> None:
        timeout = httpx.Timeout(connect=5.0, read=10.0, write=5.0, pool=5.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(self._jwks_url)
            response.raise_for_status()
            payload = response.json()

        keys = payload.get("keys", [])
        if not isinstance(keys, list) or not keys:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Entra keys unavailable")

        self._jwks_cache = {
            key.get("kid"): key
            for key in keys
            if isinstance(key, dict) and key.get("kid")
        }
        self._jwks_expiry = time.time() + 3600


async def require_auth(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer),
) -> dict[str, Any] | None:
    if not settings.entra_auth_enabled:
        return None

    validator = getattr(request.app.state, "entra_validator", None)
    if validator is None:
        logger.error("Entra auth enabled but validator not initialised")
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Auth service unavailable")

    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required")

    return await validator.validate_token(credentials.credentials)
