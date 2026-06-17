import json, time
from typing import Optional

class AgentMemory:
    def __init__(self, redis, prefix: str = "vortex:agents:memory"):
        self.redis = redis
        self.prefix = prefix
        self._max_entries = 20

    async def record(self, symbol: str, role: str, decision: str, outcome: str, pnl: float):
        key = f"{self.prefix}:{symbol}"
        entry = {
            "role": role, "decision": decision, "outcome": outcome,
            "pnl": round(pnl, 2), "timestamp": time.time()
        }
        try:
            await self.redis.lpush(key, json.dumps(entry))
            await self.redis.ltrim(key, 0, self._max_entries - 1)
            await self.redis.expire(key, 604800)
        except Exception:
            pass

    async def get_context(self, symbol: str, limit: int = 3) -> str:
        key = f"{self.prefix}:{symbol}"
        try:
            raw = await self.redis.lrange(key, 0, limit - 1)
            if not raw:
                return ""
            lines = []
            for r in raw:
                e = json.loads(r)
                lines.append(f"  {e['role']} {e['decision']} → {e['outcome']} (${e['pnl']})")
            return "Recent decisions on this pair:\n" + "\n".join(lines)
        except Exception:
            return ""

    async def get_streak(self, symbol: str) -> int:
        key = f"{self.prefix}:{symbol}"
        try:
            raw = await self.redis.lrange(key, 0, 4)
            streak = 0
            for r in raw:
                e = json.loads(r)
                if e["outcome"] == "loss":
                    streak += 1
                else:
                    break
            return streak
        except Exception:
            return 0
