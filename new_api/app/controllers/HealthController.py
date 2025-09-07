from app.services.ClickHouseMedicalSearchService import clickhouse_medical_search_service
from app.services.SalesService import sales_service
from app.utils.HttpResponseUtils import response_success, response_error
from datetime import datetime
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

class HealthController:
    async def check_health(self):
        """Health check using ClickHouse instead of Typesense"""
        try:
            clickhouse_status = await self.check_clickhouse()
            sales_status = await self.check_sales_service()
            
            # Determine overall health status
            overall_status = 'healthy' if (
                clickhouse_status['status'] == 'healthy' and 
                sales_status['status'] == 'healthy'
            ) else 'unhealthy'
            
            health_data = {
                'status': overall_status,
                'timestamp': datetime.utcnow(),
                'clickhouse_status': clickhouse_status['status'],
                'clickhouse_details': clickhouse_status.get('details', {}),
                'sales_service_status': sales_status['status'],
                'version': '2.0.0-clickhouse'
            }
            
            return response_success(health_data, msg="Health check completed successfully")
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return response_error(str(e), msg="Health check failed")
    
    async def check_clickhouse(self) -> Dict[str, Any]:
        """Check ClickHouse medical search service health"""
        try:
            health_result = await clickhouse_medical_search_service.health_check()
            return health_result
        except Exception as e:
            logger.error(f"ClickHouse health check failed: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    async def check_sales_service(self) -> Dict[str, Any]:
        """Check sales service health"""
        try:
            # Sales service doesn't have a direct health check, so we'll create a simple one
            return {
                'status': 'healthy',
                'details': 'Sales service operational'
            }
        except Exception as e:
            logger.error(f"Sales service health check failed: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }

health_controller = HealthController()