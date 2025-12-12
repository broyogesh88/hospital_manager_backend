from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Render backend API
    HOSPITAL_API_BASE: str = "https://hospital-directory.onrender.com"

    MAX_CSV_ROWS: int = 20
    CONCURRENCY: int = 5
    HTTP_TIMEOUT: int = 10  # seconds

settings = Settings()
