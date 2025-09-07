import clickhouse_connect
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Any, Optional
from config.setting import env
import logging

logger = logging.getLogger(__name__)

class ClickHouseService:
    def __init__(self):
        self.clickhouse_client = None
        self.thread_pool = None
        
    async def initialize(self):
        """Initialize ClickHouse client"""
        self.thread_pool = ThreadPoolExecutor(max_workers=4)
        
        try:
            await self._initialize_clickhouse()
            logger.info("✅ ClickHouse client initialized successfully")
        except Exception as e:
            logger.error(f"❌ ClickHouse initialization failed: {e}")
            raise
    
    
    async def _initialize_clickhouse(self):
        """Initialize ClickHouse client"""
        loop = asyncio.get_event_loop()
        
        def _create_clickhouse_client():
            return clickhouse_connect.get_client(
                host=env.clickhouse_host,
                port=env.clickhouse_port,
                database=env.clickhouse_database,
                username=env.clickhouse_username,
                password=env.clickhouse_password
            )
        
        self.clickhouse_client = await loop.run_in_executor(
            self.thread_pool, _create_clickhouse_client
        )
        
        # Test connection
        await self._test_clickhouse_connection()
    
    
    async def _test_clickhouse_connection(self):
        """Test ClickHouse connection"""
        loop = asyncio.get_event_loop()
        
        def _test_query():
            return self.clickhouse_client.query("SELECT 1 as test").result_rows
        
        result = await loop.run_in_executor(self.thread_pool, _test_query)
        logger.info(f"ClickHouse test successful: {result}")
    
    async def get_sales_items_for_admission(self, admission_id: int, limit: int = None) -> List[Dict[str, Any]]:
        """Get sales items for a specific admission ID from ClickHouse"""
        return await self._get_sales_items_clickhouse(admission_id, limit)
    
    
    async def _get_sales_items_clickhouse(self, admission_id: int, limit: int = None) -> List[Dict[str, Any]]:
        """Get sales items from ClickHouse"""
        loop = asyncio.get_event_loop()
        
        def _execute_query():
            query = f"""
            SELECT 
                AdmissionId,
                sales_item_id,
                item_type,
                item_name,
                Quantity,
                ItemNetAmount
            FROM sales_item_filtered
            WHERE AdmissionId = {{admission_id:Int64}}
            ORDER BY ItemNetAmount DESC
            {"LIMIT {limit:Int32}" if limit else ""}
            """
            
            parameters = {'admission_id': admission_id}
            if limit:
                parameters['limit'] = limit
            
            result = self.clickhouse_client.query(query, parameters=parameters)
            
            results = []
            for row in result.result_rows:
                results.append({
                    'AdmissionId': row[0],
                    'SalesItemId': str(row[1]) if row[1] else '',
                    'ItemType': row[2] or '',
                    'ItemName': row[3] or '',
                    'Quantity': row[4] or 0,
                    'ItemNetAmount': float(row[5] or 0),
                    'PatientId': '',  # Not available in this table
                    'OrganizationCode': ''  # Not available in this table
                })
            
            return results
        
        try:
            return await loop.run_in_executor(self.thread_pool, _execute_query)
        except Exception as e:
            logger.error(f"Error querying ClickHouse for admission {admission_id}: {e}")
            return []
    
    
    
    
    async def get_uom_id_for_sales_items(self, sales_item_ids: List[str]) -> Dict[str, str]:
        """Get UOM ID mapping for sales items from ClickHouse sales_item_filtered table"""
        if not sales_item_ids:
            return {}
        
        return await self._get_uom_id_clickhouse(sales_item_ids)
    
    async def _get_uom_id_clickhouse(self, sales_item_ids: List[str]) -> Dict[str, str]:
        """Get UOM ID mapping from ClickHouse sales_item_filtered table"""
        loop = asyncio.get_event_loop()
        
        def _execute_query():
            # Create a comma-separated string of sales item IDs with quotes
            sales_item_ids_str = ','.join([f"'{item_id}'" for item_id in sales_item_ids])
            
            query = f"""
            SELECT DISTINCT
                sales_item_id,
                uom_id
            FROM sales_item_filtered
            WHERE sales_item_id IN ({sales_item_ids_str})
            AND uom_id IS NOT NULL
            AND uom_id > 0
            """
            
            print(f"🔍 UOM Query: {query}")
            print(f"🔍 Looking for sales_item_ids: {sales_item_ids[:5]}...")  # Show first 5
            
            result = self.clickhouse_client.query(query)
            
            print(f"🔍 UOM Query returned {len(result.result_rows)} rows")
            
            # Create mapping dictionary
            uom_mapping = {}
            for row in result.result_rows:
                sales_item_id = str(row[0]) if row[0] else ''
                uom_id = str(row[1]) if row[1] else ''
                if sales_item_id and uom_id:
                    uom_mapping[sales_item_id] = uom_id
                print(f"🔍 UOM mapping: {sales_item_id} -> {uom_id}")
            
            print(f"🔍 Final UOM mapping: {len(uom_mapping)} items mapped")
            return uom_mapping
        
        try:
            return await loop.run_in_executor(self.thread_pool, _execute_query)
        except Exception as e:
            logger.error(f"Error querying ClickHouse for UOM IDs: {e}")
            return {}
    

    async def shutdown(self):
        """Shutdown the service"""
        if self.thread_pool:
            self.thread_pool.shutdown(wait=True)
        
        if self.clickhouse_client:
            self.clickhouse_client.close()
        
        logger.info("ClickHouse service shutdown complete")

# Global service instance
clickhouse_service = ClickHouseService()