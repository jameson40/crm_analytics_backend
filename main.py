from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import traceback

from api.csv_analytics import router as csv_analytics_router
from api.excel_analytics import router as excel_analytics_router

app = FastAPI()

app.include_router(csv_analytics_router)
app.include_router(excel_analytics_router)

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
