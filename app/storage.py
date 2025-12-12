import asyncio
from typing import Dict, List

_batches: Dict[str, List[str]] = {}
_lock = asyncio.Lock()

async def save_batch(batch_id: str, hospital_ids: List[str]):
    async with _lock:
        _batches[batch_id] = hospital_ids

async def get_batch(batch_id: str):
    async with _lock:
        return _batches.get(batch_id)

async def get_all_batches():
    async with _lock:
        return dict(_batches)

async def remove_batch(batch_id: str):
    async with _lock:
        if batch_id in _batches:
            del _batches[batch_id]
            return True
        return False
