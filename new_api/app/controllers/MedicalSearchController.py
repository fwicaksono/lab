from app.services.MedicalSearchService import medical_search_service
from app.schemas.searchSchema import UnifiedSearchRequest
from app.utils.HttpResponseUtils import response_success, response_error
from datetime import datetime
from typing import Dict, Any, List
from app.services.BillingService import billing_service
import logging

logger = logging.getLogger(__name__)

class MedicalSearchController:
    async def unified_search_with_billing(self, request: UnifiedSearchRequest):
        """
        Unified search using ClickHouse with progressive 17-step matching and billing integration
        """
        try:
            # For simple query format, return limited functionality for now
            if request.query:
                logger.warning(f"Simple query format not fully supported with ClickHouse yet: {request.query}")
                return response_error("Simple query format not yet supported", 
                                    msg="Please use structured fields for medical search")

            # Use structured search with ClickHouse progressive matching
            structured_data = self._extract_structured_data(request)
            if not any(structured_data.values()):
                return response_error("No search criteria provided", 
                                    msg="At least one structured field must be provided")
            
            logger.info(f"🔍 UNIFIED ENDPOINT: Structured search with {len(structured_data)} fields")
            logger.debug(f"Structured data: {structured_data}")
            
            # Use ClickHouse structured search (17-step progressive matching)
            search_result = await medical_search_service.search_structured(
                structured_data,
                request.max_results
            )
            
            # Enhance with billing data
            enhanced_results = []
            for admission in search_result.get('results', []):
                # Extract data from document field (ClickHouse format)
                doc = admission.get('document', {})
                admission_id = doc.get('AdmissionId')
                
                # Get billing data
                billing_data = None
                has_billing = False
                
                if admission_id:
                    try:
                        # Use SalesService directly for billing data
                        from app.services.SalesService import sales_service
                        billing_summary = await sales_service.get_sales_for_admission(admission_id)
                        
                        if billing_summary:
                            # Convert AdmissionSalesSummary to billing_data format
                            billing_data = {
                                "total_items": billing_summary.total_items,
                                "total_amount": billing_summary.total_amount,
                                "items": [
                                    {
                                        "sales_item_id": item.sales_item_id,
                                        "item_type": item.item_type,
                                        "item_name": item.item_name,
                                        "quantity": item.quantity,
                                        "item_net_amount": item.item_net_amount
                                    }
                                    for item in billing_summary.items
                                ]
                            }
                            has_billing = True
                    except Exception as e:
                        logger.warning(f"Billing fetch failed for admission {admission_id}: {e}")
                        pass  # Continue without billing data
                
                # Create clean unified result
                clean_admission = {
                    'patient_id': doc.get('PatientId'),
                    'admission_id': doc.get('AdmissionId'),
                    'doctor': doc.get('PrimaryDoctor'),
                    'specialty': doc.get('Specialty'),
                    'organization': doc.get('OrganizationCode'),
                    'disease': doc.get('DiseaseClassification'),
                    'procedure': doc.get('ProcedureClassification'),
                    'birth_date': doc.get('BirthDate'),
                    'sex': doc.get('Sex'),
                    'payer_name': doc.get('PayerName'),
                    'payer_type': doc.get('PayerType'),
                    'archetype': doc.get('Archetype'),
                    'region': doc.get('Region'),
                    'age': doc.get('Age'),
                    'length_of_stay': doc.get('LengthOfStay'),
                    'admission_date': doc.get('AdmissionDate'),
                    'admission_type': doc.get('AdmissionTypeName'),
                    'anesthesia_doctor': doc.get('AnesthesiaDoctor'),
                    'anesthesia_type': doc.get('AnesthesiaType'),
                    'billing_data': billing_data,
                    'has_billing': has_billing,
                    'found_in_step': doc.get('result_step', 'UNKNOWN')  # ClickHouse search step
                }
                enhanced_results.append(clean_admission)
            
            # Prepare clean response data
            response_data = {
                'results': enhanced_results,
                'total_found': search_result.get('found', len(enhanced_results)),
                'search_meta': {
                    'max_results': request.max_results,
                    'includes_billing': True,
                    'search_method': 'clickhouse_progressive_17_step'
                }
            }
            
            return response_success(response_data, msg="Unified search completed successfully")
            
        except Exception as e:
            logger.error(f"Unified search failed: {e}")
            return response_error(str(e), msg="Failed to complete unified search")

    async def billing_analysis_search(self, request: UnifiedSearchRequest) -> Dict[str, Any]:
        """
        Billing-focused analysis using ClickHouse search with AI-generated summaries
        Returns billing items from top admission and references from all admissions
        """
        try:
            # For simple query format, return limited functionality
            if request.query:
                logger.warning(f"Simple query format not fully supported with ClickHouse yet: {request.query}")
                return {
                    "success": False,
                    "message": "Simple query format not yet supported",
                    "status_code": 400,
                    "data": {
                        "billing_items": [],
                        "billing_summary": [],
                        "billing_references": [],
                        "matched_unmatched_params": {
                            "matched_params": [],
                            "unmatched_params": []
                        }
                    }
                }

            # Extract structured data
            structured_data = self._extract_structured_data(request)
            if not any(structured_data.values()):
                return {
                    "success": False,
                    "message": "No search criteria provided",
                    "status_code": 400,
                    "data": {
                        "billing_items": [],
                        "billing_summary": [],
                        "billing_references": [],
                        "matched_unmatched_params": {
                            "matched_params": [],
                            "unmatched_params": []
                        }
                    }
                }

            logger.info(f"🔍 BILLING ANALYSIS: Structured search with {len(structured_data)} fields")
            
            # Use ClickHouse structured search (17-step progressive matching)
            search_result = await medical_search_service.search_structured(
                structured_data,
                request.max_results
            )

            if search_result.get('found', 0) == 0:
                return {
                    "success": False,
                    "message": "No results found for billing analysis",
                    "status_code": 404,
                    "data": {
                        "billing_items": [],
                        "billing_summary": [],
                        "billing_references": [],
                        "matched_unmatched_params": {
                            "matched_params": [],
                            "unmatched_params": []
                        }
                    }
                }

            # Get first (top) admission for detailed billing
            results = search_result.get('results', [])
            if not results:
                return {
                    "success": False,
                    "message": "No admission results found",
                    "status_code": 404,
                    "data": {
                        "billing_items": [],
                        "billing_summary": [],
                        "billing_references": [],
                        "matched_unmatched_params": {
                            "matched_params": [],
                            "unmatched_params": []
                        }
                    }
                }

            top_admission = results[0].get('document', {})
            top_admission_id = top_admission.get('AdmissionId')
            
            # Get detailed billing for top admission with real-time pricing
            billing_items = []
            if top_admission_id:
                try:
                    from app.services.SalesService import sales_service
                    billing_summary = await sales_service.get_sales_for_admission(top_admission_id)
                    
                    if billing_summary and billing_summary.items:
                        # Get real-time pricing from pricing engine API
                        real_time_pricing = await self._get_real_time_pricing(top_admission, billing_summary.items, request)
                        
                        if real_time_pricing and real_time_pricing.get('success') and real_time_pricing.get('sales_items'):
                            # HYBRID APPROACH: Use historical data for item details, real-time API for pricing
                            logger.info(f"Creating hybrid billing items (historical details + real-time prices)")
                            
                            # Create lookup map from real-time pricing by sales_item_id
                            realtime_price_map = {}
                            for rt_item in real_time_pricing['sales_items']:
                                item_id = str(rt_item.get('sales_item_id', ''))
                                if item_id:
                                    realtime_price_map[item_id] = rt_item.get('calculated_price', 0)
                            
                            logger.info(f"Real-time pricing map created for {len(realtime_price_map)} items")
                            
                            # Merge historical details with real-time prices
                            for historical_item in billing_summary.items:
                                historical_id = str(historical_item.sales_item_id)
                                realtime_price = realtime_price_map.get(historical_id, historical_item.item_net_amount)
                                
                                billing_item = {
                                    "admission_id": historical_item.admission_id,
                                    "sales_item_id": historical_item.sales_item_id,
                                    "item_type": historical_item.item_type,           # From historical
                                    "item_name": historical_item.item_name,           # From historical  
                                    "quantity": historical_item.quantity,            # From historical
                                    "item_net_amount": realtime_price                # From real-time API
                                }
                                billing_items.append(billing_item)
                                
                                # Debug first few items to show the hybrid approach
                                if len(billing_items) <= 3:
                                    price_source = "real-time" if historical_id in realtime_price_map else "historical"
                                    logger.info(f"Item {historical_id}: {historical_item.item_name} = {realtime_price} ({price_source})")
                                    
                            logger.info(f"Hybrid billing: {len(billing_items)} items with historical details + real-time prices")
                        else:
                            logger.warning("Real-time pricing failed, using historical data")
                            # Fallback to historical data
                            billing_items = [
                                {
                                    "admission_id": historical_item.admission_id,
                                    "sales_item_id": historical_item.sales_item_id,
                                    "item_type": historical_item.item_type,
                                    "item_name": historical_item.item_name,
                                    "quantity": historical_item.quantity,
                                    "item_net_amount": historical_item.item_net_amount
                                }
                                for historical_item in billing_summary.items
                            ]
                except Exception as e:
                    logger.error(f"Failed to fetch billing for admission {top_admission_id}: {e}")

            # Create billing references from all admissions (just admission IDs)
            billing_references = []
            for result in results:
                doc = result.get('document', {})
                admission_id = doc.get('AdmissionId')
                if admission_id:
                    billing_references.append(admission_id)

            # Generate AI summary if we have billing items
            ai_summary = ""
            if billing_items:
                try:
                    # Convert structured data back to query-like format for AI context
                    query_context = self._format_search_context(structured_data)
                    
                    # Extract item types from billing items for AI summary
                    item_types = list(set([item.get('item_type') for item in billing_items if item.get('item_type')]))
                    ai_summary = await billing_service.generate_billing_summary(item_types)
                except Exception as e:
                    logger.error(f"AI summary generation failed: {e}")
                    ai_summary = "AI billing analysis temporarily unavailable."

            # Analyze matched/unmatched params using top result
            query_for_analysis = self._format_search_context(structured_data)
            matched_unmatched = self._analyze_search_match({"results": results}, query_for_analysis)

            return {
                "success": True,
                "message": "Billing analysis completed successfully",
                "status_code": 200,
                "data": {
                    "billing_items": billing_items,
                    "billing_summary": [ai_summary] if ai_summary else [],
                    "billing_references": billing_references,
                    "matched_unmatched_params": matched_unmatched,
                    "search_meta": {
                        "total_admissions_found": len(billing_references),
                        "top_admission_id": top_admission_id,
                        "search_method": "clickhouse_progressive_17_step"
                    }
                }
            }

        except Exception as e:
            logger.error(f"Billing analysis search failed: {e}")
            return {
                "success": False,
                "message": f"Billing analysis failed: {str(e)}",
                "status_code": 500,
                "data": {
                    "billing_items": [],
                    "billing_summary": [],
                    "billing_references": [],
                    "matched_unmatched_params": {
                        "matched_params": [],
                        "unmatched_params": []
                    }
                }
            }

    def _extract_structured_data(self, request: UnifiedSearchRequest) -> Dict:
        """Extract non-empty structured data fields from request"""
        structured_fields = {
            'class_id': request.class_id,
            'class_name': request.class_name,
            'hospital_code': request.hospital_code,
            'hospital_name': request.hospital_name,
            'archetype': request.archetype,
            'hospital_region': request.hospital_region,
            'birthdate': request.birthdate,
            'gender': request.gender,
            'icd10': request.icd10,
            'icd9': request.icd9,
            'primary_doctor': request.primary_doctor,
            'admission_type': request.admission_type,
            'admission_date': request.admission_date,
            'discharge_date': request.discharge_date,
            'anesthesia_doctor': request.anesthesia_doctor,
            'anesthesia_type': request.anesthesia_type,
            'surgery_nature': request.surgery_nature,
            'payer_type': request.payer_type,
            'payer_name': request.payer_name
        }
        
        # Filter out None and empty values
        return {k: v for k, v in structured_fields.items() if v is not None and v != "" and v != []}

    def _format_search_context(self, structured_data: Dict) -> str:
        """Format structured data as context string for AI analysis"""
        context_parts = []
        
        # Add ICD codes with priority
        if structured_data.get('icd10'):
            context_parts.append(f"ICD-10: {', '.join(structured_data['icd10'])}")
        if structured_data.get('icd9'):
            context_parts.append(f"ICD-9: {', '.join(structured_data['icd9'])}")
            
        # Add other important fields
        important_fields = [
            'hospital_code', 'primary_doctor', 'payer_name', 'archetype', 
            'hospital_region', 'admission_type', 'gender', 'birthdate'
        ]
        
        for field in important_fields:
            if structured_data.get(field):
                context_parts.append(f"{field.replace('_', ' ').title()}: {structured_data[field]}")
        
        return "; ".join(context_parts)

    def _analyze_search_match(self, search_result: Dict, query: str) -> Dict[str, List[str]]:
        """
        Analyze which search parameters matched vs didn't match in top result
        """
        try:
            if not search_result.get('results'):
                return {"matched_params": [], "unmatched_params": []}
            
            # Get top result
            top_result = search_result['results'][0]
            doc = top_result.get('document', {})
            
            # Define searchable fields and their display names
            searchable_fields = {
                'PayerName': 'payer name',
                'DiseaseClassification': 'disease classification',
                'ProcedureClassification': 'procedure classification',
                'Sex': 'gender',
                'BirthDate': 'birth date',
                'PrimaryDoctor': 'doctor name',
                'OrganizationCode': 'organization',
                'Region': 'region',
                'Archetype': 'archetype',
                'AdmissionTypeName': 'admission type',
                'AnesthesiaDoctor': 'anesthesia doctor',
                'AnesthesiaType': 'anesthesia type'
            }
            
            matched_params = []
            unmatched_params = []
            
            query_lower = query.lower()
            
            for field, display_name in searchable_fields.items():
                field_value = doc.get(field)
                
                if field_value:
                    field_value_str = str(field_value).lower()
                    
                    if field == 'PrimaryDoctor' or field == 'AnesthesiaDoctor':
                        # For doctor names, require exact match of the full name
                        # Field: "dr. Bernard Agung B.S., SpB, K. Onk"
                        # Query must contain the EXACT string to match
                        
                        # Check if the exact field value appears in the query
                        is_matched = field_value_str in query_lower
                        
                        if is_matched:
                            matched_params.append(display_name)
                        else:
                            unmatched_params.append(display_name)
                    
                    else:
                        # For all other fields, use the simple working logic
                        import re
                        query_parts = set(re.findall(r'\b\w+[\w.-]*\w*\b', query_lower))
                        field_parts = set(re.findall(r'\b\w+[\w.-]*\w*\b', field_value_str))
                        
                        # Check if there's any overlap between query parts and field parts
                        if query_parts & field_parts:  # Set intersection
                            matched_params.append(display_name)
                        else:
                            unmatched_params.append(display_name)
                else:
                    # Field has no value
                    unmatched_params.append(display_name)
            
            return {
                "matched_params": matched_params,
                "unmatched_params": unmatched_params
            }
            
        except Exception as e:
            logger.error(f"Error analyzing search match: {e}")
            return {"matched_params": [], "unmatched_params": []}

    async def _get_real_time_pricing(self, admission_doc: Dict, historical_items: List, request = None) -> Dict[str, Any]:
        """
        Get real-time pricing from pricing engine API using admission data and historical items as template
        """
        try:
            logger.info(f"Getting real-time pricing for admission: {admission_doc.get('AdmissionId')}")
            
            # Map admission document fields to pricing API request format
            # Get discharge date or use present day as fallback
            discharge_date = admission_doc.get('DischargeDate')
            if not discharge_date:
                from datetime import datetime
                discharge_date = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z')
                logger.warning(f"No discharge date found, using present day: {discharge_date}")
            else:
                # Convert ClickHouse DateTime to ISO format with Z suffix for pricing API
                if hasattr(discharge_date, 'strftime'):
                    discharge_date = discharge_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                else:
                    discharge_date = str(discharge_date)
            
            # Get patient_category from request or default to 1
            patient_category = 1  # Default
            if request and hasattr(request, 'patient_category') and request.patient_category is not None:
                patient_category = request.patient_category
            
            pricing_request = {
                "sales_items": [],
                "class_id": admission_doc.get('InvoiceClassId'),
                "admission_type": admission_doc.get('AdmissionTypeId'),
                "transaction_date": discharge_date,  # Use discharge date as transaction date
                "patient_type_id": admission_doc.get('PatientTypeId'),
                "patient_category": patient_category,  # From request or default 1
                "payer_id": admission_doc.get('PayerId'),
                "organization_id": admission_doc.get('OrganizationId'),
                "previewed": True  # Required field
            }
            
            # Check for null values and warn
            null_fields = [k for k, v in pricing_request.items() if v is None and k != 'sales_items']
            if null_fields:
                logger.warning(f"WARNING - Null fields in pricing request: {null_fields}")
                logger.warning(f"This might be why the API returns empty data!")
            
            # Collect all sales item IDs to get UOM mapping
            sales_item_ids = []
            valid_items = []
            
            for item in historical_items:
                sales_item_id = getattr(item, 'sales_item_id', None)
                if sales_item_id:
                    sales_item_ids.append(str(sales_item_id))
                    valid_items.append(item)
                else:
                    logger.warning(f"Skipping item with no sales_item_id: {item}")
            
            # Get UOM ID mapping from ClickHouse
            uom_mapping = {}
            if sales_item_ids:
                try:
                    logger.info(f"About to lookup UOM for {len(sales_item_ids)} sales items")
                    logger.info(f"Sample sales_item_ids: {sales_item_ids[:10]}")
                    
                    from app.services.ClickHouseService import clickhouse_service
                    uom_mapping = await clickhouse_service.get_uom_id_for_sales_items(sales_item_ids)
                    
                    logger.info(f"Retrieved UOM mapping for {len(uom_mapping)} items")
                    
                    # Check which items got UOM mapping vs which didn't
                    mapped_items = set(uom_mapping.keys())
                    unmapped_items = set(sales_item_ids) - mapped_items
                    
                    if mapped_items:
                        logger.info(f"Items WITH UOM mapping: {list(mapped_items)[:5]}...")
                        logger.info(f"Sample UOM values: {dict(list(uom_mapping.items())[:5])}")
                    
                    if unmapped_items:
                        logger.warning(f"Items WITHOUT UOM mapping: {list(unmapped_items)[:10]}...")
                        logger.warning(f"These will default to UOM '0'")
                        
                except Exception as e:
                    logger.warning(f"Failed to get UOM mapping: {e}")
                    import traceback
                    logger.warning(f"Error details: {traceback.format_exc()}")
            
            # Determine is_cito based on surgery_nature from request
            is_cito = False
            if request and hasattr(request, 'surgery_nature') and request.surgery_nature:
                is_cito = request.surgery_nature.upper() == "CITO"
                logger.info(f"Surgery nature: '{request.surgery_nature}' -> is_cito: {is_cito}")
            else:
                logger.info(f"No surgery_nature provided, defaulting to is_cito: False")
            
            # Convert historical items to pricing API format with UOM ID
            for item in valid_items:
                sales_item_id = str(getattr(item, 'sales_item_id'))
                uom_id = uom_mapping.get(sales_item_id, '0')  # Default to '0' if not found
                
                pricing_item = {
                    "sales_item_id": int(sales_item_id) if sales_item_id.isdigit() else 0,
                    "package_sales_item_id": 0,
                    "package_price_id": 0,
                    "checkup_package_price_id": 0,
                    "quantity": getattr(item, 'quantity', 1),
                    "uom_id": int(uom_id) if uom_id.isdigit() else 0,
                    "start_date": discharge_date,  # Already in ISO format
                    "end_date": discharge_date,    # Already in ISO format
                    "doctor_user_id": admission_doc.get('PrimaryDoctorUserId', 0),
                    "is_cito": is_cito,  # Dynamic based on surgery_nature
                    "edited_sales_price": float(getattr(item, 'item_net_amount', 0)),
                    "is_default_sales_price": True
                }
                
                # Debug a few items to see UOM assignment
                if len(pricing_request["sales_items"]) < 3:
                    logger.info(f"Item {sales_item_id}: UOM {uom_id} (from mapping: {sales_item_id in uom_mapping})")

                pricing_request["sales_items"].append(pricing_item)
            
            logger.info(f"Pricing API request prepared with {len(pricing_request['sales_items'])} items")
            
            # Validate that we have sales items before making the API call
            if not pricing_request["sales_items"]:
                logger.error(f"No valid sales items found - cannot call pricing API")
                return {
                    "success": False,
                    "error": "No valid sales items found for pricing calculation"
                }
            
            # Call pricing engine API
            import aiohttp
            import asyncio
            
            pricing_api_url = "https://preprd-his-kairos-api.siloamhospitals.com:7777/api/v2/recalculate/get-price-preview"
            
            timeout = aiohttp.ClientTimeout(total=30)  # 30 second timeout for debugging
            
            async with aiohttp.ClientSession(timeout=timeout) as session:
                logger.info(f"Calling pricing API: {pricing_api_url}")
                
                # Debug: Print complete request being sent
                logger.info(f"\n" + "="*80)
                logger.info(f"COMPLETE PRICING API REQUEST DEBUG")
                logger.info(f"="*80)
                logger.info(f"URL: {pricing_api_url}")
                logger.info(f"Method: POST")
                logger.info(f"Headers: {{'Content-Type': 'application/json', 'Accept': 'application/json'}}")
                logger.info(f"\nRequest Body (JSON):")
                import json
                logger.info(json.dumps(pricing_request, indent=2, default=str))
                logger.info(f"="*80)
                logger.info(f"Request Summary:")
                logger.info(f"   - organization_id: {pricing_request.get('organization_id')}")
                logger.info(f"   - patient_type_id: {pricing_request.get('patient_type_id')}")
                logger.info(f"   - payer_id: {pricing_request.get('payer_id')}")
                logger.info(f"   - transaction_date: {pricing_request.get('transaction_date')}")
                logger.info(f"   - patient_category: {pricing_request.get('patient_category')}")
                logger.info(f"   - total_sales_items: {len(pricing_request.get('sales_items', []))}")
                logger.info(f"   - previewed: {pricing_request.get('previewed')}")
                
                # Show first few sales items for debugging
                sales_items = pricing_request.get('sales_items', [])
                if sales_items:
                    logger.info(f"Sales Items Details (first 3):")
                    for i, item in enumerate(sales_items[:3]):
                        logger.info(f"   [{i+1}] ID: {item.get('sales_item_id')}, "
                                  f"Qty: {item.get('quantity')}, "
                                  f"UOM: {item.get('uom_id')}, "
                                  f"Price: {item.get('edited_sales_price')}")
                logger.info(f"="*80 + "\n")
                
                # Try the main request format first
                async with session.post(
                    pricing_api_url,
                    json=pricing_request,
                    headers={
                        'Content-Type': 'application/json',
                        'Accept': 'application/json'
                    }
                ) as response:
                    if response.status == 200:
                        pricing_result = await response.json()
                        logger.info(f"Pricing API success: {pricing_result.get('message', 'No message') if isinstance(pricing_result, dict) else 'Ok'}")
                        
                        # Map pricing API response back to our format
                        mapped_result = {
                            "success": True,
                            "sales_items": []
                        }
                        
                        # Handle different response structures
                        api_items = []
                        
                        if isinstance(pricing_result, list):
                            # If response is a list, use it directly
                            api_items = pricing_result
                        elif isinstance(pricing_result, dict):
                            # If response is a dict, look for items in various locations
                            data_field = pricing_result.get('data', [])
                            
                            if isinstance(data_field, list):
                                # data is directly a list of items
                                api_items = data_field
                            elif isinstance(data_field, dict):
                                # data is an object containing items
                                api_items = (
                                    data_field.get('sales_items', []) or
                                    data_field.get('items', [])
                                )
                            else:
                                # Try other locations
                                api_items = (
                                    pricing_result.get('sales_items', []) or
                                    pricing_result.get('items', [])
                                )
                        
                        # Check if we have any items to process
                        if not api_items or len(api_items) == 0:
                            logger.warning(f"Pricing API returned empty data - no items to process")
                            return {
                                "success": False,
                                "error": "Pricing API returned empty data"
                            }
                        
                        # Process the items
                        for i, api_item in enumerate(api_items):
                            if isinstance(api_item, dict):
                                # Extract price with detailed debugging
                                sales_item_id = api_item.get('sales_item_id') or api_item.get('id') or api_item.get('ItemId')
                                
                                mapped_item = {
                                    "sales_item_id": sales_item_id,
                                    "item_type": api_item.get('item_type', api_item.get('ItemType', 'Unknown')),
                                    "item_name": api_item.get('item_name', api_item.get('ItemName', 'Real-time Item')),
                                    "quantity": api_item.get('quantity', api_item.get('Quantity', 1)),
                                    "calculated_price": (
                                        api_item.get('sales_price') or 
                                        api_item.get('calculated_price') or 
                                        api_item.get('final_price') or 
                                        api_item.get('CalculatedPrice') or 
                                        api_item.get('FinalPrice') or 
                                        api_item.get('price') or 
                                        0
                                    )
                                }
                                
                                mapped_result["sales_items"].append(mapped_item)
                            else:
                                logger.warning(f"Unexpected item format: {api_item}")
                        
                        logger.info(f"Real-time pricing calculated for {len(mapped_result['sales_items'])} items")
                        return mapped_result
                        
                    else:
                        error_text = await response.text()
                        logger.error(f"Pricing API error {response.status}: {error_text}")
                        return {
                            "success": False,
                            "error": f"Pricing API returned {response.status}: {error_text}"
                        }
                        
        except asyncio.TimeoutError:
            logger.warning(f"Pricing API timeout - using historical data as fallback")
            return {
                "success": False,
                "error": "Pricing API timeout"
            }
        except Exception as e:
            logger.error(f"Error calling pricing API: {e}")
            return {
                "success": False,
                "error": f"Pricing API error: {str(e)}"
            }

# Global controller instance
medical_controller = MedicalSearchController()