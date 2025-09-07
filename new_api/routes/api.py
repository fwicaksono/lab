from fastapi import APIRouter, Query
from app.schemas.searchSchema import SearchRequest, UnifiedSearchRequest
from app.controllers.MedicalSearchController import medical_controller
from app.controllers.HealthController import health_controller
from app.utils.HttpResponseUtils import response_success, response_error
from config.setting import env

router = APIRouter()

@router.post("/medical/search/unified")
async def unified_search(payload: UnifiedSearchRequest):
    return await medical_controller.unified_search_with_billing(payload)

@router.get("/medical/search/unified")
async def unified_search_get(
    q: str = Query(..., description="Search query"),
    max_results: int = Query(default=env.default_max_results, ge=1, le=env.max_search_results)
):
    payload = SearchRequest(query=q, max_results=max_results)
    return await medical_controller.unified_search_with_billing(payload)

@router.get("/health-check")
async def health_check():
    return await health_controller.check_health()

@router.post("/estimation")
async def estimation_search(payload: UnifiedSearchRequest):
    return await medical_controller.billing_analysis_search(payload)

@router.get("/estimation") 
async def estimation_search_get(
    q: str = Query(..., description="Search query"),
    max_results: int = Query(default=env.default_max_results, ge=1, le=env.max_search_results)
):
    payload = SearchRequest(query=q, max_results=max_results)
    return await medical_controller.billing_analysis_search(payload)

