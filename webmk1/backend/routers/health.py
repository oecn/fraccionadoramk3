from datetime import datetime, timezone

from fastapi import APIRouter


router = APIRouter(prefix="/health", tags=["health"])


@router.get("")
def health_check() -> dict[str, str]:
    return {
        "status": "ok",
        "service": "webmk1-backend",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

