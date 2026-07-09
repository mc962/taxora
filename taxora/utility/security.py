"""Cryptographic utilities for API key hashing."""

from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")


def hash_secure_value(value: str) -> str:
    """Hash a value using Argon2 (memory-hard algorithm for API keys)."""
    return str(pwd_context.hash(value))


def verify_secure_value(value: str, hashed: str) -> bool:
    """Verify a value against an Argon2 hash."""
    return bool(pwd_context.verify(value, hashed))
