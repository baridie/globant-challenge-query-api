from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from .routers import metrics
from .utils.auth import verify_api_key
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Globant Query API",
    description="Data Query API for Globant Challenge - Handles metrics and data queries",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(
    metrics.router, 
    prefix="/api/v1",
    tags=["Metrics"],
    dependencies=[Depends(verify_api_key)]
)

@app.get("/")
def read_root():
    return {
        "service": "Globant Query API",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "docs": "/docs",
            "health": "/health",
            "metrics": "/api/v1/metrics"
        }
    }

@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "service": "query-api"
    }

@app.on_event("startup")
async def startup_event():
    logger.info("Query API starting up...")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Query API shutting down...")
