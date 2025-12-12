import csv
import io
import time
import uuid
import asyncio
from typing import List, Dict, Tuple
import httpx
from .config import settings
from .schemas import HospitalResult

async def parse_csv_bytes(file_bytes: bytes) -> List[Dict]:
    """
    Parse CSV bytes into list of dicts with keys: name, address, phone.
    Accepts files with or without header. Skips empty rows.
    """
    text = file_bytes.decode("utf-8-sig")
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)

    # If header appears to be present (first cell equals 'name'), drop it.
    if rows and rows[0] and rows[0][0].strip().lower() == "name":
        rows = rows[1:]

    parsed = []
    for row in rows:
        # allow rows with 2+ columns (name,address,[phone])
        if not row or all((not c.strip() for c in row)):
            continue
        name = row[0].strip() if len(row) > 0 else ""
        address = row[1].strip() if len(row) > 1 else ""
        phone = row[2].strip() if len(row) > 2 else None
        parsed.append({"name": name, "address": address, "phone": phone})
    return parsed

async def create_hospital(client: httpx.AsyncClient, row_idx: int, row: Dict, batch_id: str) -> HospitalResult:
    """
    Create a single hospital by calling external API.
    Returns a HospitalResult dataclass instance.
    """
    payload = {
        "name": row.get("name"),
        "address": row.get("address"),
        "phone": row.get("phone") or None,
        "creation_batch_id": batch_id
    }

    # Basic validation
    if not payload["name"] or not payload["address"]:
        return HospitalResult(row=row_idx, hospital_id=None, name=payload["name"] or "", status="validation_failed", error="name or address missing")

    url = f"{settings.HOSPITAL_API_BASE}/hospitals/"
    try:
        # Try twice for transient errors
        for attempt in range(2):
            resp = await client.post(url, json=payload, timeout=settings.HTTP_TIMEOUT)
            if resp.status_code in (200, 201):
                data = resp.json()
                return HospitalResult(row=row_idx, hospital_id=data.get("id"), name=payload["name"], status="created")
            if 400 <= resp.status_code < 500:
                # client error, don't retry
                return HospitalResult(row=row_idx, hospital_id=None, name=payload["name"], status="failed", error=f"HTTP {resp.status_code}: {resp.text}")
            # otherwise treat as transient and retry
        return HospitalResult(row=row_idx, hospital_id=None, name=payload["name"], status="failed", error=f"HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        return HospitalResult(row=row_idx, hospital_id=None, name=payload["name"], status="failed", error=str(e))

async def process_rows(rows: List[Dict], batch_id: str) -> Tuple[List[HospitalResult], bool, float]:
    """
    Process the rows concurrently (bounded by settings.CONCURRENCY).
    Returns: (results list, activated_bool, processing_time_seconds)
    """
    start = time.time()
    sem = asyncio.Semaphore(settings.CONCURRENCY)
    results: List[HospitalResult] = []

    async with httpx.AsyncClient() as client:
        async def worker(i: int, r: Dict):
            async with sem:
                return await create_hospital(client, i, r, batch_id)
        tasks = [asyncio.create_task(worker(i + 1, r)) for i, r in enumerate(rows)]
        for coro in asyncio.as_completed(tasks):
            res = await coro
            results.append(res)

    # Attempt activation if at least one created
    created_any = any(r.status == "created" for r in results)
    activated = False
    if created_any:
        activate_url = f"{settings.HOSPITAL_API_BASE}/hospitals/batch/{batch_id}/activate"
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.patch(activate_url, timeout=settings.HTTP_TIMEOUT)
                activated = resp.status_code in (200, 204)
        except Exception:
            activated = False

    processing_time = time.time() - start
    return results, activated, processing_time
