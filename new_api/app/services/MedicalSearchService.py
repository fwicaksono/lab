from app.services.ClickHouseMedicalSearchService import clickhouse_medical_search_service
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)

class MedicalSearchService:
    def __init__(self):
        self.searcher = None
    
    async def initialize(self):
        self.searcher = clickhouse_medical_search_service
        await self.searcher.initialize()
        logger.info("✅ Medical Search Service initialized with ClickHouse direct queries")
    
    async def shutdown(self):
        if self.searcher:
            await self.searcher.shutdown()
    
    async def search(self, query: str, target_results: int = 10, show_details: bool = True) -> Dict[str, Any]:
        """Simple query search - parse query into structured format first"""
        # For now, return empty results for simple queries
        # This can be enhanced later to parse the query into structured data
        logger.warning(f"Simple query search not yet implemented for ClickHouse: {query}")
        return {
            'found': 0,
            'results': [],
            'search_time_ms': 0,
            'page': 1
        }
    
    async def search_structured(self, structured_data: Dict, target_results: int = 10) -> Dict[str, Any]:
        """Structured medical search using ClickHouse progressive search"""
        try:
            raw_results = await self.searcher.search_similar_admissions(structured_data, target_results)
            formatted_results = self.searcher.format_results_for_api(raw_results)
            return formatted_results
        except Exception as e:
            logger.error(f"Structured search failed: {e}")
            return {
                'found': 0,
                'results': [],
                'search_time_ms': 0,
                'page': 1,
                'error': str(e)
            }
    
    async def search_with_filter(self, query: str, max_results: int = 10, filter_by: str = None, 
                               query_by: str = None, query_by_weights: str = None) -> Dict[str, Any]:
        """Filtered search - not implemented for ClickHouse"""
        logger.warning("Filtered search not implemented for ClickHouse")
        return {
            'found': 0,
            'results': [],
            'search_time_ms': 0,
            'page': 1
        }

medical_search_service = MedicalSearchService()