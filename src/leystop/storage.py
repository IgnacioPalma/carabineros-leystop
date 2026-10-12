"""Object storage: Cloudflare R2 (S3 API) in CI, a local directory for development."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Protocol


class Store(Protocol):
    def get(self, key: str) -> bytes | None: ...
    def put(self, key: str, data: bytes, content_type: str, cache_control: str | None = None) -> None: ...
    def delete(self, key: str) -> None: ...
    def list(self, prefix: str) -> list[str]: ...


class LocalStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def get(self, key: str) -> bytes | None:
        p = self.root / key
        return p.read_bytes() if p.exists() else None

    def put(self, key: str, data: bytes, content_type: str, cache_control: str | None = None) -> None:
        p = self.root / key
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_bytes(data)
        tmp.replace(p)

    def delete(self, key: str) -> None:
        (self.root / key).unlink(missing_ok=True)

    def list(self, prefix: str) -> list[str]:
        base = self.root / prefix
        if not base.exists():
            return []
        return sorted(str(p.relative_to(self.root)) for p in base.rglob("*") if p.is_file() and p.suffix != ".tmp")


class R2Store:
    def __init__(self, bucket: str, account_id: str, access_key_id: str, secret_access_key: str) -> None:
        import boto3
        from botocore.config import Config

        self.bucket = bucket
        self.s3 = boto3.client(
            "s3",
            endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name="auto",
            config=Config(retries={"max_attempts": 10, "mode": "standard"}),
        )

    def get(self, key: str) -> bytes | None:
        try:
            return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        except self.s3.exceptions.NoSuchKey:
            return None

    def put(self, key: str, data: bytes, content_type: str, cache_control: str | None = None) -> None:
        extra = {"CacheControl": cache_control} if cache_control else {}
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType=content_type, **extra)

    def delete(self, key: str) -> None:
        self.s3.delete_object(Bucket=self.bucket, Key=key)

    def list(self, prefix: str) -> list[str]:
        keys: list[str] = []
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix):
            keys.extend(o["Key"] for o in page.get("Contents", []))
        return sorted(keys)


def store_from_env(local_dir: str | None) -> Store:
    if local_dir:
        return LocalStore(local_dir)
    try:
        return R2Store(
            bucket=os.environ["R2_BUCKET"],
            account_id=os.environ["R2_ACCOUNT_ID"],
            access_key_id=os.environ["R2_ACCESS_KEY_ID"],
            secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        )
    except KeyError as e:
        raise SystemExit(f"Missing env var {e}; set R2_* variables or pass --local DIR") from None
