"""Bounded payment emulator: random latency, reproducible business outcome."""

from asyncio import sleep
from hashlib import sha256
from random import uniform
from typing import Literal
from uuid import UUID


class Gateway:
    async def process(self, payment_id: UUID) -> Literal["succeeded", "failed"]:
        await sleep(uniform(2, 5))
        # Hash all UUID bits, not UUID version/timestamp bits or Python's salted hash().
        bucket = int.from_bytes(sha256(payment_id.bytes).digest()) % 10
        return "failed" if bucket == 0 else "succeeded"
