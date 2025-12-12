from pydantic import BaseModel
from typing import Optional, List, Any

class HospitalCreate(BaseModel):
    name: str
    address: str
    phone: Optional[str] = None
    creation_batch_id: Optional[str] = None

class HospitalResult(BaseModel):
    row: int
    hospital_id: Optional[Any] = None
    name: str
    status: str
    error: Optional[str] = None

class BulkResponse(BaseModel):
    batch_id: str
    total_hospitals: int
    processed_hospitals: int
    failed_hospitals: int
    processing_time_seconds: float
    batch_activated: bool
    hospitals: List[HospitalResult]
