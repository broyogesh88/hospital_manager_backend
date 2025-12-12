import uuid
from fastapi import FastAPI, UploadFile, File, HTTPException
from .processor import parse_csv_bytes, process_rows
from .storage import save_batch, get_batch, get_all_batches, remove_batch
from .schemas import BulkResponse, HospitalResult
from .config import settings
import httpx

app = FastAPI(title="Hospital Management Backend", version="1.0")

RENDER_BASE = settings.HOSPITAL_API_BASE


@app.get("/")
def root():
    return {"message": "Backend (8001) running successfully"}


# ---------------------------------------------------------
# LIST HOSPITALS — PROXY TO RENDER BACKEND
# ---------------------------------------------------------

@app.get("/hospitals")
async def list_hospitals():
    async with httpx.AsyncClient() as client:
        res = await client.get(f"{RENDER_BASE}/hospitals/")
        return res.json()


# ---------------------------------------------------------
# HOSPITAL OPERATIONS — PROXY TO RENDER BACKEND
# ---------------------------------------------------------


@app.delete("/hospitals/{hospital_id}")
async def delete_hospital(hospital_id: str):
    async with httpx.AsyncClient() as client:
        res = await client.delete(f"{RENDER_BASE}/hospitals/{hospital_id}")
        if res.status_code == 404:
            raise HTTPException(404, "Hospital not found")
        return {"deleted": True}


# ---------------------------------------------------------
# BULK CSV UPLOAD — USE PROVIDED PROCESSOR
# ---------------------------------------------------------

@app.post("/hospitals/bulk/upload", response_model=BulkResponse)
async def bulk_upload(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files allowed.")

    file_bytes = await file.read()
    rows = await parse_csv_bytes(file_bytes)

    if len(rows) == 0:
        raise HTTPException(400, "CSV contains no valid rows")

    if len(rows) > settings.MAX_CSV_ROWS:
        raise HTTPException(400, f"Max rows allowed: {settings.MAX_CSV_ROWS}")

    batch_id = str(uuid.uuid4())

    results, activated, processing_time = await process_rows(rows, batch_id)

    # Extract created hospital IDs
    created_ids = [r.hospital_id for r in results if r.status == "created"]

    # Save batch mapping
    await save_batch(batch_id, created_ids)

    # Build API response
    return BulkResponse(
        batch_id=batch_id,
        total_hospitals=len(rows),
        processed_hospitals=len(created_ids),
        failed_hospitals=len(rows) - len(created_ids),
        processing_time_seconds=processing_time,
        batch_activated=activated,
        hospitals=[r for r in results]
    )


# ---------------------------------------------------------
# BATCH DETAILS — FETCH FROM RENDER BACKEND
# ---------------------------------------------------------

@app.get("/hospitals/batch/{batch_id}")
async def batch_details(batch_id: str):
    ids = await get_batch(batch_id)
    if not ids:
        raise HTTPException(404, "Batch not found")

    hospitals = []
    async with httpx.AsyncClient() as client:
        for hid in ids:
            res = await client.get(f"{RENDER_BASE}/hospitals/{hid}")
            if res.status_code == 200:
                hospitals.append(res.json())

    return hospitals


# ---------------------------------------------------------
# BATCH ACTIVATE
# ---------------------------------------------------------

@app.patch("/hospitals/batch/{batch_id}/activate")
async def activate_batch(batch_id: str):
    async with httpx.AsyncClient() as client:
        res = await client.patch(f"{RENDER_BASE}/hospitals/batch/{batch_id}/activate")
        return {"batch_id": batch_id, "activated": res.status_code in (200, 204)}


# ---------------------------------------------------------
# BATCH DELETE
# ---------------------------------------------------------

@app.delete("/hospitals/batch/{batch_id}")
async def delete_batch(batch_id: str):
    ids = await get_batch(batch_id)
    if not ids:
        raise HTTPException(404, "Batch not found")

    # delete hospitals in Render
    async with httpx.AsyncClient() as client:
        for hid in ids:
            await client.delete(f"{RENDER_BASE}/hospitals/{hid}")

    # remove batch locally
    await remove_batch(batch_id)

    return {"batch_id": batch_id, "deleted": True}


@app.get("/hospitals/batches")
async def list_batches():
    batches = await get_all_batches()

    response = []

    async with httpx.AsyncClient() as client:
        for batch_id, hospital_ids in batches.items():
            # Get 1 hospital from batch to determine active status
            active_status = False

            if hospital_ids:  # ensure batch not empty
                first_hid = hospital_ids[0]
                res = await client.get(f"{RENDER_BASE}/hospitals/{first_hid}")

                if res.status_code == 200:
                    hospital = res.json()
                    active_status = hospital.get("active", False)

            response.append({
                "batch_id": batch_id,
                "total_hospitals": len(hospital_ids),
                "active": active_status,
            })

    return {"count": len(response), "batches": response}


@app.patch("/hospitals/batch/{batch_id}/activate")
async def deactivate_batch(batch_id: str):
    """
    Deactivate batch by calling the same activate endpoint but sending active=false.
    """
    payload = {"active": False}

    async with httpx.AsyncClient() as client:
        res = await client.patch(
            f"{RENDER_BASE}/hospitals/batch/{batch_id}/activate",
            json=payload
        )

    if res.status_code not in (200, 204):
        raise HTTPException(400, f"Failed to deactivate batch: {res.text}")

    return {"batch_id": batch_id, "deactivated": True}
