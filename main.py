from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import traceback

from analytics import router as analytics_router
from regions import router as regions_router

app = FastAPI()

app.include_router(analytics_router)
app.include_router(regions_router)

# Разрешаем доступ с фронта
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    tb = traceback.format_exc()
    print("[ERROR]", tb)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal Server Error", "trace": tb},
        headers={"Access-Control-Allow-Origin": "*"},
    )
