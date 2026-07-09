"""Repository for API key management."""

import secrets
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from taxora.data.model.api_key import ApiKey
from taxora.utility.security import hash_secure_value, verify_secure_value

KEY_PREFIX = "txra_"
PREFIX_LENGTH = 8  # length of key_prefix stored in DB (first 8 chars of full key)


def generate_api_key() -> str:
    """Generate a new API key: txra_ + 32 random bytes hex-encoded."""
    return KEY_PREFIX + secrets.token_hex(32)


class ApiKeyRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create(self, name: str, is_superuser: bool = False) -> tuple[ApiKey, str]:
        """Create a new API key. Returns (model, plaintext_key)."""
        plaintext = generate_api_key()
        key = ApiKey(
            name=name,
            key_prefix=plaintext[:PREFIX_LENGTH],
            key_hash=hash_secure_value(plaintext),
            is_superuser=is_superuser,
            is_active=True,
        )
        self.session.add(key)
        await self.session.commit()
        await self.session.refresh(key)
        return key, plaintext

    async def verify_key(self, plaintext: str) -> ApiKey | None:
        """Look up by prefix then verify with Argon2. Returns key or None."""
        prefix = plaintext[:PREFIX_LENGTH]
        result = await self.session.execute(
            select(ApiKey).where(ApiKey.key_prefix == prefix)
        )
        candidates = result.scalars().all()
        for key in candidates:
            if key.is_active and verify_secure_value(plaintext, key.key_hash):
                return key
        return None

    async def get_by_id(self, key_id: int) -> ApiKey | None:
        result = await self.session.execute(select(ApiKey).where(ApiKey.id == key_id))
        return result.scalar_one_or_none()

    async def list_all(self) -> list[ApiKey]:
        result = await self.session.execute(
            select(ApiKey).order_by(ApiKey.created_at.desc())
        )
        return list(result.scalars().all())

    async def toggle_active(self, key_id: int) -> ApiKey | None:
        key = await self.get_by_id(key_id)
        if key is None:
            return None
        key.is_active = not key.is_active
        await self.session.commit()
        await self.session.refresh(key)
        return key

    async def delete(self, key_id: int) -> bool:
        key = await self.get_by_id(key_id)
        if key is None:
            return False
        await self.session.delete(key)
        await self.session.commit()
        return True

    async def update_last_used(self, key_id: int) -> None:
        key = await self.get_by_id(key_id)
        if key:
            key.last_used_at = datetime.utcnow()
            await self.session.commit()
