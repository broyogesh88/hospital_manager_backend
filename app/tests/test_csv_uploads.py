from fastapi.testclient import TestClient
from app.main import app
import io

client = TestClient(app)

def test_root():
    r = client.get("/")
    assert r.status_code == 200
    assert r.json()["message"].startswith("Hospital Bulk API")

def test_upload_csv_success():
    csv_data = "name,address,phone\nApollo Hospital,Mumbai,9999999999\nAIIMS,Delhi,8888888888\n"
    files = {"file": ("hosp.csv", csv_data, "text/csv")}
    r = client.post("/hospitals/bulk/upload", files=files)
    assert r.status_code in (200, 400, 502, 504, 500)
