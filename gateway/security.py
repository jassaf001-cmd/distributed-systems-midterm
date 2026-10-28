
import os
import secrets

from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader


api_key_header = APIKeyHeader(
    name="X-API-Key",
    auto_error=False
)


def require_api_key(key: str | None = Security(api_key_header)):
    expected = os.getenv("STORAGE_API_KEY")

    if not expected:
        raise HTTPException(
            status_code=503,
            detail="Server API key is not configured"
        )

    if not key or not secrets.compare_digest(key, expected):
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing API key"
        )

    return True
