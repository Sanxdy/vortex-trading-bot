import json
import os
from datetime import datetime, timezone
from typing import Optional
from redis import asyncio as aioredis

_redis: Optional[aioredis.Redis] = None

def init_activity(redis_client: aioredis.Redis):
    global _redis
    _redis = redis_client

async def push_activity(message: str, msg_type: str = "info"):
    if _redis is None:
        return
    try:
        entry = json.dumps({
            "t": datetime.now(timezone.utc).timestamp(),
            "m": message,
            "type": msg_type,
        })
        await _redis.lpush("vortex:activity", entry)
        await _redis.ltrim("vortex:activity", 0, 499)
    except Exception:
        pass

async def get_activity(limit: int = 50) -> list:
    if _redis is None:
        return []
    try:
        raw = await _redis.lrange("vortex:activity", 0, limit - 1)
        return [json.loads(e) for e in raw]
    except Exception:
        return []
