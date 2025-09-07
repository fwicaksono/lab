from typing import List
from pydantic import BaseModel, Field

class SalesItem(BaseModel):
    admission_id: int = Field(..., description="Admission ID")
    sales_item_id: str = Field(..., description="Sales item ID")
    item_type: str = Field(..., description="Type of item (Drugs, Consultation, etc.)")
    item_name: str = Field(..., description="Name of the item")
    quantity: int = Field(..., description="Quantity")
    item_net_amount: float = Field(..., description="Net amount for this item")

class AdmissionSalesSummary(BaseModel):
    admission_id: int = Field(..., description="Admission ID")
    total_items: int = Field(..., description="Total number of items")
    total_amount: float = Field(..., description="Total amount spent")
    item_types: List[str] = Field(..., description="List of item types")
    items: List[SalesItem] = Field(..., description="List of sales items")

