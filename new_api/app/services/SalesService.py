from app.services.ClickHouseService import clickhouse_service
from app.models.SalesModel import SalesItem, AdmissionSalesSummary
from config.setting import env
from typing import Dict, Any, List, Optional
import asyncio
import logging

logger = logging.getLogger(__name__)

class SalesService:
    def __init__(self):
        self.clickhouse = clickhouse_service
    
    async def initialize(self):
        """Initialize sales service with ClickHouse"""
        try:
            await self.clickhouse.initialize()
            logger.info("✅ Sales Service initialized - using ClickHouse gold_ETBS.sales_item_filtered")
        except Exception as e:
            logger.error(f"Failed to initialize Sales Service: {e}")
            raise
    
    
    async def get_sales_for_admission(self, admission_id: int) -> Optional[AdmissionSalesSummary]:
        """Get all sales items for a specific admission from ClickHouse"""
        try:
            if not admission_id or admission_id == 0:
                logger.warning(f"Invalid admission_id: {admission_id}")
                return None
            
            return await self._get_sales_for_admission_clickhouse(admission_id)
        except Exception as e:
            logger.error(f"Error getting sales for admission {admission_id}: {e}")
            return None
    
    async def _get_sales_for_admission_clickhouse(self, admission_id: int) -> Optional[AdmissionSalesSummary]:
        """Get sales items for admission using ClickHouse/BigQuery"""
        try:
            sales_data = await self.clickhouse.get_sales_items_for_admission(admission_id)
            
            if not sales_data:
                return None
            
            # Process results
            sales_items = []
            total_amount = 0.0
            item_types = set()
            
            for doc in sales_data:
                try:
                    item_net_amount = float(doc.get('ItemNetAmount', 0))
                    
                    sales_item = SalesItem(
                        admission_id=doc['AdmissionId'],
                        sales_item_id=doc['SalesItemId'],
                        item_type=doc['ItemType'],
                        item_name=doc['ItemName'],
                        quantity=doc.get('Quantity', 0),
                        item_net_amount=item_net_amount
                    )
                    
                    sales_items.append(sales_item)
                    total_amount += item_net_amount
                    item_types.add(doc['ItemType'])
                except (KeyError, ValueError, TypeError) as e:
                    logger.warning(f"Error processing ClickHouse sales item for admission {admission_id}: {e}")
                    continue
            
            return AdmissionSalesSummary(
                admission_id=admission_id,
                total_items=len(sales_items),
                total_amount=round(total_amount, 2),
                item_types=list(item_types),
                items=sales_items
            )
        except Exception as e:
            logger.error(f"Error getting ClickHouse sales for admission {admission_id}: {e}")
            return None
    
    
    
    
    
    async def shutdown(self):
        """Shutdown sales service"""
        try:
            logger.info("🔄 Sales Service shutting down...")
            await self.clickhouse.shutdown()
        except Exception as e:
            logger.error(f"Error during Sales Service shutdown: {e}")

sales_service = SalesService()