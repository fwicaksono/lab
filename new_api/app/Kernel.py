from fastapi import FastAPI
from config.setting import env
from contextlib import asynccontextmanager
from app.services.MedicalSearchService import medical_search_service
from app.services.SalesService import sales_service

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Starting Medical Search API...")
    await medical_search_service.initialize()
    await sales_service.initialize()
    print("✅ Medical Search API started successfully with ClickHouse")
    
    yield
    
    print("🔄 Shutting down Medical Search API...")
    await medical_search_service.shutdown()
    await sales_service.shutdown()
    print("✅ Medical Search API shutdown complete")

app = FastAPI(
    title=env.app_name,
    description="Advanced medical records search with ClickHouse progressive matching and sales analysis",
    version=env.app_version,
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)