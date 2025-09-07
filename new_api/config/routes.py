from fastapi import FastAPI
from routes.api import router as api_router
from config.setting import env

def setup_routes(app: FastAPI):
    app.include_router(
        api_router,
        prefix="/api/v1",
        tags=["Medical Search API"]
    )
    
    @app.get("/")
    async def root():
        return {
            "app_name": env.app_name,
            "app_version": env.app_version,
            "docs": "/docs",
            "health": "/api/v1/health-check"
        }