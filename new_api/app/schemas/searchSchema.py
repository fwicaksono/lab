from pydantic import BaseModel, Field, validator, model_validator
from typing import List, Optional

class SearchRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=500, description="Medical search query")
    max_results: int = Field(default=3, ge=1, le=20, description="Maximum number of results")
    organization_filter: Optional[str] = Field(None, description="Filter by specific organization")
    
    @validator('query')
    def validate_query(cls, v):
        if not v.strip():
            raise ValueError('Query cannot be empty or whitespace only')
        return v.strip()



class UnifiedSearchRequest(BaseModel):
    # Simple query format (existing)
    query: Optional[str] = Field(None, min_length=1, max_length=500, description="Medical search query")
    max_results: int = Field(default=3, ge=1, le=20, description="Maximum number of results")
    organization_filter: Optional[str] = Field(None, description="Filter by specific organization")
    
    # Structured format (new)
    class_id: Optional[str] = Field(None, description="Class ID")
    class_name: Optional[str] = Field(None, description="Class name (e.g., VIP)")
    hospital_code: Optional[str] = Field(None, description="Hospital code (e.g., SHLV)")
    hospital_name: Optional[str] = Field(None, description="Hospital name")
    archetype: Optional[str] = Field(None, description="Hospital archetype")
    hospital_region: Optional[str] = Field(None, description="Hospital region")
    birthdate: Optional[str] = Field(None, description="Patient birthdate (YYYY-MM-DD)")
    gender: Optional[str] = Field(None, description="Patient gender")
    icd10: Optional[List[str]] = Field(None, description="ICD10 codes")
    icd9: Optional[List[str]] = Field(None, description="ICD9 codes")
    primary_doctor: Optional[str] = Field(None, description="Primary doctor name")
    primary_doctor_id: Optional[int] = Field(None, description="Primary doctor ID")
    admission_type: Optional[str] = Field(None, description="Admission type")
    admission_id: Optional[int] = Field(None, description="Admission ID")
    admission_date: Optional[str] = Field(None, description="Admission date")
    discharge_date: Optional[str] = Field(None, description="Discharge date")
    anesthesia_doctor: Optional[str] = Field(None, description="Anesthesia doctor name")
    anesthesia_type: Optional[str] = Field(None, description="Anesthesia type")
    surgery_nature: Optional[str] = Field(None, description="Surgery nature")
    payer_type: Optional[str] = Field(None, description="Payer type")
    payer_name: Optional[str] = Field(None, description="Payer name")
    payer_id: Optional[int] = Field(None, description="Payer ID")
    patient_category: Optional[int] = Field(None, description="Patient category (default: 1)")
    
    @model_validator(mode='before')
    @classmethod
    def validate_unified_fields(cls, values):
        # Check if either query or at least one structured field is provided
        if not values.get('query') and not any([
            values.get('class_id'), values.get('class_name'), values.get('hospital_code'),
            values.get('hospital_name'), values.get('archetype'), values.get('hospital_region'),
            values.get('birthdate'), values.get('gender'), values.get('icd10'), values.get('icd9'),
            values.get('primary_doctor'), values.get('admission_type'), values.get('anesthesia_doctor'),
            values.get('anesthesia_type'), values.get('payer_type'), values.get('payer_name')
        ]):
            raise ValueError('Either query or at least one structured field must be provided')
        return values

