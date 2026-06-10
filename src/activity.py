import json
from datetime import datetime, timezone
from typing import Optional
from redis import asyncio as aioredis

_redis: Optional[aioredis.Redis] = None
_activity_key: str = "vortex:activity"

def init_activity(redis_client: aioredis.Redis, activity_key: str = "vortex:activity"):
    global _redis, _activity_key
    _redis = redis_client
    _activity_key = activity_key

async def push_activity(message: str, msg_type: str = "info"):
    if _redis is None:
        return
    try:
        entry = json.dumps({
            "t": datetime.now(timezone.utc).timestamp(),
            "m": message,
            "type": msg_type,
        })
        await _redis.lpush(_activity_key, entry)
        await _redis.ltrim(_activity_key, 0, 499)
    except Exception:
        pass

async def get_activity(limit: int = 50) -> list:
    if _redis is None:
        return []
    try:
        raw = await _redis.lrange(_activity_key, 0, limit - 1)
        return [json.loads(e) for e in raw]
    except Exception:
        return []
