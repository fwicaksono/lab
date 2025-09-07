import clickhouse_connect
from typing import List, Dict, Any, Optional
from datetime import datetime
from config.setting import env
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging

logger = logging.getLogger(__name__)

class ClickHouseMedicalSearchService:
    """Medical search service using direct ClickHouse queries with 17-step progressive matching"""
    
    def __init__(self):
        self.client = None
        self.thread_pool = None
        self.found_admission_ids = set()
        self.results = []
        
    async def initialize(self):
        """Initialize ClickHouse connection"""
        self.thread_pool = ThreadPoolExecutor(max_workers=4)
        
        try:
            # Initialize ClickHouse client using existing service settings
            self.client = clickhouse_connect.get_client(
                host=env.clickhouse_host,
                port=env.clickhouse_port,
                username=env.clickhouse_username,
                password=env.clickhouse_password,
                database=env.clickhouse_database
            )
            
            # Test connection
            await self.health_check()
            logger.info("✅ ClickHouse Medical Search Service initialized successfully")
            
        except Exception as e:
            logger.error(f"❌ ClickHouse Medical Search Service initialization failed: {e}")
            raise

    async def shutdown(self):
        """Cleanup resources"""
        if self.thread_pool:
            self.thread_pool.shutdown(wait=True)

    async def health_check(self) -> Dict[str, Any]:
        """Check ClickHouse connection health"""
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                self.thread_pool,
                lambda: self.client.query("SELECT 1").result_rows
            )
            return {
                'status': 'healthy',
                'clickhouse': 'connected'
            }
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e)
            }

    def _calculate_icd_scores(self, input_data: Dict, step_type: str = "exact") -> tuple:
        """Calculate ICD match scores - exact matching uses = operator, partial uses LIKE"""
        icd10_codes = input_data.get('icd10', [])
        icd9_codes = input_data.get('icd9', [])
        
        def build_enhanced_score(codes, field_name):
            """Helper function to build score with penalty (for partial matching steps only)"""
            if not codes:
                return "0"
            
            # Base score (match counting)
            base_score = " + ".join([f"CASE WHEN {field_name} LIKE '%{code}%' THEN 1 ELSE 0 END" for code in codes])
            
            # Code count penalty
            input_code_count = len(codes)
            code_count_penalty = f"CASE WHEN length({field_name}) - length(replace({field_name}, ';', '')) + 1 > {input_code_count} THEN -0.1 ELSE 0 END"
            
            return f"({base_score}) + ({code_count_penalty})"
        
        def build_exact_codes_string(codes):
            """Build exact match string for multiple codes"""
            if not codes:
                return ""
            # Keep original order to match database storage
            return "; ".join(codes) #ini kalau buat prod, kalau pre-prod hapus " "
        
        if step_type == "exact":
            # Steps 1-13: TRUE EXACT MATCHING - use = operator
            if icd10_codes:
                exact_icd10_string = build_exact_codes_string(icd10_codes)
                icd10_filter = f"DiseaseClassification = '{exact_icd10_string}'"
            else:
                icd10_filter = "1=1"
                
            if icd9_codes:
                exact_icd9_string = build_exact_codes_string(icd9_codes)
                icd9_filter = f"ProcedureClassification = '{exact_icd9_string}'"
            else:
                icd9_filter = "1=1"
            
            # NO SCORING: Just return fixed values
            icd10_score = "1" if icd10_codes else "0"
            icd9_score = "1" if icd9_codes else "0"
            
        elif step_type == "partial":
            # Step 14: Use LIKE for partial matching
            if icd10_codes:
                icd10_conditions = [f"DiseaseClassification LIKE '%{code}%'" for code in icd10_codes]
                icd10_filter = "(" + " OR ".join(icd10_conditions) + ")"
            else:
                icd10_filter = "1=1"
                
            if icd9_codes:
                icd9_conditions = [f"ProcedureClassification LIKE '%{code}%'" for code in icd9_codes]
                icd9_filter = "(" + " OR ".join(icd9_conditions) + ")"
            else:
                icd9_filter = "1=1"
            
            # FULL SCORING: Count matches + penalty
            icd10_score = build_enhanced_score(icd10_codes, "DiseaseClassification")
            icd9_score = build_enhanced_score(icd9_codes, "ProcedureClassification")
            
        elif step_type == "mixed":
            # Step 15: Exact ICD9 + Partial ICD10
            if icd10_codes:
                icd10_conditions = [f"DiseaseClassification LIKE '%{code}%'" for code in icd10_codes]
                icd10_filter = "(" + " OR ".join(icd10_conditions) + ")"  # OR for partial
            else:
                icd10_filter = "1=1"
                
            if icd9_codes:
                exact_icd9_string = build_exact_codes_string(icd9_codes)
                icd9_filter = f"ProcedureClassification = '{exact_icd9_string}'"  # = for exact
            else:
                icd9_filter = "1=1"
            
            # FULL SCORING: Count matches + penalty
            icd10_score = build_enhanced_score(icd10_codes, "DiseaseClassification")
            icd9_score = build_enhanced_score(icd9_codes, "ProcedureClassification")
            
        elif step_type == "icd9_only_exact":
            # Step 16: Exact ICD9 only
            icd10_filter = "1=1"  # Ignore ICD10 completely
            icd10_score = "0"     # No ICD10 scoring
                
            if icd9_codes:
                exact_icd9_string = build_exact_codes_string(icd9_codes)
                icd9_filter = f"ProcedureClassification = '{exact_icd9_string}'"
            else:
                icd9_filter = "1=1"
            
            # NO SCORING: Just fixed value
            icd9_score = "1" if icd9_codes else "0"
            
        elif step_type == "icd9_only_partial":
            # Step 17: Partial ICD9 only
            icd10_filter = "1=1"  # Ignore ICD10 completely
            icd10_score = "0"     # No ICD10 scoring
                
            if icd9_codes:
                icd9_conditions = [f"ProcedureClassification LIKE '%{code}%'" for code in icd9_codes]
                icd9_filter = "(" + " OR ".join(icd9_conditions) + ")"
            else:
                icd9_filter = "1=1"
            
            # FULL SCORING: Count matches + penalty
            icd9_score = build_enhanced_score(icd9_codes, "ProcedureClassification")
        
        return icd10_filter, icd9_filter, icd10_score, icd9_score

    def _calculate_age_and_los(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate age at admission and length of stay from input data"""
        processed_data = input_data.copy()

        try:
            # Parse dates - handle both ISO and MySQL datetime formats
            def parse_datetime(date_str):
                # Try ISO format first
                if 'T' in date_str or 'Z' in date_str:
                    return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                # Try MySQL datetime format
                else:
                    return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
            
            admission_date = parse_datetime(input_data['admission_date'])
            birth_date = datetime.strptime(input_data['birthdate'], '%Y-%m-%d')

            # Calculate age at admission (in years)
            age_at_admission = admission_date.year - birth_date.year
            if (admission_date.month, admission_date.day) < (birth_date.month, birth_date.day):
                age_at_admission -= 1

            # Handle length of stay calculation
            if not input_data.get('discharge_date') or input_data['discharge_date'] == '':
                length_of_stay = ''
            else:
                discharge_date = parse_datetime(input_data['discharge_date'])
                # Use day difference instead of 24-hour periods
                length_of_stay = (discharge_date.date() - admission_date.date()).days

            # Add calculated values to processed data
            processed_data['calculated_age'] = age_at_admission
            processed_data['calculated_los'] = length_of_stay

            # Format dates for ClickHouse (MySQL format)
            processed_data['formatted_admission_date'] = admission_date.strftime('%Y-%m-%d %H:%M:%S')
            processed_data['formatted_birth_date'] = birth_date.strftime('%Y-%m-%d')

        except Exception as e:
            logger.error(f"Error calculating age/LOS: {e}")
            # Set defaults if calculation fails
            processed_data['calculated_age'] = 0
            processed_data['calculated_los'] = ''
            processed_data['formatted_admission_date'] = input_data.get('admission_date', '')
            processed_data['formatted_birth_date'] = input_data.get('birthdate', '')

        return processed_data

    def _get_los_diff_calculation(self, input_data: Dict) -> str:
        """Generate LOS difference calculation, handling empty calculated_los"""
        calculated_los = input_data.get('calculated_los', '')
        
        if calculated_los == '' or calculated_los is None:
            # If input has no LOS, just return 0 (no difference penalty)
            return "0 as los_diff"
        else:
            # Normal LOS difference calculation
            return f"abs(toInt32OrZero(LengthOfStay) - {calculated_los}) as los_diff"

    def _get_exclusion_clause(self) -> str:
        """Generate exclusion clause for previous admission IDs"""
        if not self.found_admission_ids:
            return ""
        ids_str = ",".join(map(str, self.found_admission_ids))
        return f"AND AdmissionId NOT IN ({ids_str})"

    def _build_condition(self, field_name: str, value: str) -> str:
        """Build WHERE condition, skip if value is empty"""
        if not value or value.strip() == "":
            return ""
        return f"{field_name} = '{value}'"

    def _build_conditions(self, conditions: list) -> str:
        """Combine multiple conditions, filtering out empty ones"""
        valid_conditions = [cond.strip() for cond in conditions if cond and cond.strip()]
        return " AND ".join(valid_conditions)

    def _build_where_clause(self, input_data: dict, base_conditions: list, extra_conditions: list = None) -> str:
        """Build complete WHERE clause with common fields, skipping empty ones"""
        conditions = base_conditions.copy()
        
        # Add common conditional fields
        conditions.extend([
            self._build_condition('OrganizationCode', input_data.get('hospital_code', '')),
            self._build_condition('PayerName', input_data.get('payer_name', '')),
            self._build_condition('PrimaryDoctor', input_data.get('primary_doctor', '')),
            self._build_condition('AdmissionTypeName', input_data.get('admission_type', '')),
            self._build_condition('Sex', input_data.get('gender', '')),
            self._build_condition('AnesthesiaDoctor', input_data.get('anesthesia_doctor', '')),
            self._build_condition('AnesthesiaType', input_data.get('anesthesia_type', ''))
        ])
        
        # Add extra conditions if provided
        if extra_conditions:
            conditions.extend(extra_conditions)
        
        # Filter out empty conditions BEFORE adding exclusion
        valid_conditions = [cond.strip() for cond in conditions if cond and cond.strip()]
        
        # Add exclusion clause if it exists
        exclusion = self._get_exclusion_clause()
        if exclusion:
            # Remove the 'AND' from exclusion since we'll add it in join
            exclusion_clean = exclusion.replace('AND ', '', 1)
            valid_conditions.append(exclusion_clean)
        
        # Build the final WHERE clause
        if not valid_conditions:
            return "WHERE 1=1"
        else:
            return "WHERE " + " AND ".join(valid_conditions)

    async def search_similar_admissions(self, input_data: Dict[str, Any], max_results: int = 50) -> List[Dict]:
        """Execute progressive search until we have enough results"""
        # Reset for new search
        self.found_admission_ids = set()
        self.results = []
        
        processed_data = self._calculate_age_and_los(input_data)
        
        logger.info(f"Calculated age at admission: {processed_data['calculated_age']} years")
        logger.info(f"Calculated length of stay: {processed_data['calculated_los']} days")

        steps = [
            self._step_1, self._step_2, self._step_3, self._step_4, self._step_5,
            self._step_6, self._step_7, self._step_8, self._step_9, self._step_10,
            self._step_11, self._step_12, self._step_13, self._step_14, self._step_15,
            self._step_16, self._step_17
        ]

        for i, step_func in enumerate(steps, 1):
            if len(self.results) >= max_results:
                break

            logger.info(f"Executing Step {i}...")
            try:
                step_results = await step_func(processed_data)

                if step_results:
                    logger.info(f"Step {i} found {len(step_results)} results")
                    self.results.extend(step_results)
                    for result in step_results:
                        self.found_admission_ids.add(result[0])  # AdmissionId is first column
                else:
                    logger.info(f"Step {i} found no results")
            except Exception as e:
                logger.error(f"Error in Step {i}: {str(e)}")
                continue

        # Sort final results by found_in_step (ascending - earlier steps first)
        final_results = self.results[:max_results]
        
        # Extract step numbers for sorting
        def get_step_number(result):
            step_str = result[-1] if result else ''
            if isinstance(step_str, str) and step_str.startswith('STEP_'):
                try:
                    return int(step_str.split('_')[1])
                except (IndexError, ValueError):
                    return 999
            return 999
        
        final_results.sort(key=get_step_number)
        return final_results

    # Progressive search step implementations
    async def _step_1(self, input_data: Dict) -> List[Dict]:
        """STEP 1: All exact matches with ICD prioritization"""
        icd10_filter, icd9_filter, icd10_score, icd9_score = self._calculate_icd_scores(input_data, "exact")

        conditions = [
            icd10_filter,
            icd9_filter,
            self._build_condition('OrganizationCode', input_data.get('hospital_code', '')),
            self._build_condition('PayerName', input_data.get('payer_name', '')),
            self._build_condition('PrimaryDoctor', input_data.get('primary_doctor', '')),
            self._build_condition('LengthOfStay', str(input_data.get('calculated_los', ''))),
            self._build_condition('AdmissionTypeName', input_data.get('admission_type', '')),
            self._build_condition('Sex', input_data.get('gender', '')),
            self._build_condition('AnesthesiaDoctor', input_data.get('anesthesia_doctor', '')),
            self._build_condition('AnesthesiaType', input_data.get('anesthesia_type', ''))
        ]
        
        where_clause = self._build_conditions(conditions)
        if not where_clause.strip():
            where_clause = "WHERE 1=1"
        elif not where_clause.startswith('WHERE'):
            where_clause = "WHERE " + where_clause

        icd10_count = len(input_data.get('icd10', []))
        icd9_count = len(input_data.get('icd9', []))

        query = f"""
        SELECT 
            *,
            {icd10_count} as icd10_match_count,
            {icd9_count} as icd9_match_count,
            abs(dateDiff('year', BirthDate, toDate('{input_data['formatted_birth_date']}'))) as age_diff,
            abs(dateDiff('day', AdmissionDate, parseDateTime('{input_data['formatted_admission_date']}'))) as date_diff,
            'STEP_1' as result_step
        FROM {env.clickhouse_table_name}
        {where_clause}
        ORDER BY 
            age_diff ASC,
            date_diff ASC
        LIMIT 50
        """

        return await self._execute_query(query)

    async def _step_2(self, input_data: Dict) -> List[Dict]:
        """STEP 2: Remove Anesthesia Doctor"""
        icd10_filter, icd9_filter, icd10_score, icd9_score = self._calculate_icd_scores(input_data, "exact")
        exclusion = self._get_exclusion_clause()

        query = f"""
        SELECT 
            *,
            abs(dateDiff('year', BirthDate, toDate('{input_data['formatted_birth_date']}'))) as age_diff,
            abs(dateDiff('day', AdmissionDate, parseDateTime('{input_data['formatted_admission_date']}'))) as date_diff,
            'STEP_2' as result_step
        FROM {env.clickhouse_table_name}
        WHERE 
            {icd10_filter}
            AND {icd9_filter}
            AND OrganizationCode = '{input_data.get('hospital_code', '')}'
            AND PayerName = '{input_data.get('payer_name', '')}'
            AND PrimaryDoctor = '{input_data.get('primary_doctor', '')}'
            AND LengthOfStay = '{input_data.get('calculated_los', '')}'
            AND AdmissionTypeName = '{input_data.get('admission_type', '')}'
            AND Sex = '{input_data.get('gender', '')}'
            AND AnesthesiaType = '{input_data.get('anesthesia_type', '')}'
            {exclusion}
        ORDER BY 
            age_diff ASC,
            date_diff ASC
        LIMIT 50
        """
        
        return await self._execute_query(query)

    async def _step_3(self, input_data: Dict) -> List[Dict]:
        """STEP 3: Remove Anesthesia Type"""
        icd10_filter, icd9_filter, icd10_score, icd9_score = self._calculate_icd_scores(input_data, "exact")
        exclusion = self._get_exclusion_clause()

        query = f"""
        SELECT 
            *,
            abs(dateDiff('year', BirthDate, toDate('{input_data['formatted_birth_date']}'))) as age_diff,
            abs(dateDiff('day', AdmissionDate, parseDateTime('{input_data['formatted_admission_date']}'))) as date_diff,
            'STEP_3' as result_step
        FROM {env.clickhouse_table_name}
        WHERE 
            {icd10_filter}
            AND {icd9_filter}
            AND OrganizationCode = '{input_data.get('hospital_code', '')}'
            AND PayerName = '{input_data.get('payer_name', '')}'
            AND PrimaryDoctor = '{input_data.get('primary_doctor', '')}'
            AND LengthOfStay = '{input_data.get('calculated_los', '')}'
            AND AdmissionTypeName = '{input_data.get('admission_type', '')}'
            AND Sex = '{input_data.get('gender', '')}'
            {exclusion}
        ORDER BY 
            age_diff ASC,
            date_diff ASC
        LIMIT 50
        """
        return await self._execute_query(query)

    async def _step_4(self, input_data: Dict) -> List[Dict]:
        """STEP 4: Remove Gender"""
        icd10_filter, icd9_filter, icd10_score, icd9_score = self._calculate_icd_scores(input_data, "exact")
        exclusion = self._get_exclusion_clause()

        query = f"""
        SELECT 
            *,
            abs(dateDiff('year', BirthDate, toDate('{input_data['formatted_birth_date']}'))) as age_diff,
            abs(dateDiff('day', AdmissionDate, parseDateTime('{input_data['formatted_admission_date']}'))) as date_diff,
            'STEP_4' as result_step
        FROM {env.clickhouse_table_name}
        WHERE 
            {icd10_filter}
            AND {icd9_filter}
            AND OrganizationCode = '{input_data.get('hospital_code', '')}'
            AND PayerName = '{input_data.get('payer_name', '')}'
            AND PrimaryDoctor = '{input_data.get('primary_doctor', '')}'
            AND LengthOfStay = '{input_data.get('calculated_los', '')}'
            AND AdmissionTypeName = '{input_data.get('admission_type', '')}'
            {exclusion}
        ORDER BY 
            age_diff ASC,
            date_diff ASC
        LIMIT 50
        """
        return await self._execute_query(query)

    async def _step_5(self, input_data: Dict) -> List[Dict]:
        """STEP 5: Remove Admission Type, prioritize same gender"""
        icd10_filter, icd9_filter, icd10_score, icd9_score = self._calculate_icd_scores(input_data, "exact")
        exclusion = self._get_exclusion_clause()

        query = f"""
        SELECT 
            *,
            abs(dateDiff('year', BirthDate, toDate('{input_data['formatted_birth_date']}'))) as age_diff,
            abs(dateDiff('day', AdmissionDate, parseDateTime('{input_data['formatted_admission_date']}'))) as date_diff,
            {self._get_los_diff_calculation(input_data)},
            CASE WHEN Sex = '{input_data.get('gender', '')}' THEN 0 ELSE 1 END as gender_priority,
            'STEP_5' as result_step
        FROM {env.clickhouse_table_name}
        WHERE 
            {icd10_filter}
            AND {icd9_filter}
            AND OrganizationCode = '{input_data.get('hospital_code', '')}'
            AND PayerName = '{input_data.get('payer_name', '')}'
            AND PrimaryDoctor = '{input_data.get('primary_doctor', '')}'
            {exclusion}
        ORDER BY 
            gender_priority ASC,
            age_diff ASC,
            los_diff ASC,
            date_diff ASC
        LIMIT 50
        """
        return await self._execute_query(query)

    async def _step_6(self, input_data: Dict) -> List[Dict]:
        """STEP 6: Remove Length of Stay, add length of stay difference"""
        icd10_filter, icd9_filter, icd10_score, icd9_score = self._calculate_icd_scores(input_data, "exact")
        exclusion = self._get_exclusion_clause()

        query = f"""
        SELECT 
            *,
            abs(dateDiff('year', BirthDate, toDate('{input_data['formatted_birth_date']}'))) as age_diff,
            abs(dateDiff('day', AdmissionDate, parseDateTime('{input_data['formatted_admission_date']}'))) as date_diff,
            {self._get_los_diff_calculation(input_data)},
            CASE WHEN AdmissionTypeName = '{input_data.get('admission_type', '')}' THEN 0 ELSE 1 END as admission_type_priority,
            CASE WHEN Sex = '{input_data.get('gender', '')}' THEN 0 ELSE 1 END as gender_priority,
            'STEP_6' as result_step
        FROM {env.clickhouse_table_name}
        WHERE 
            {icd10_filter}
            AND {icd9_filter}
            AND OrganizationCode = '{input_data.get('hospital_code', '')}'
            AND PayerName = '{input_data.get('payer_name', '')}'
            AND PrimaryDoctor = '{input_data.get('primary_doctor', '')}'
            {exclusion}
        ORDER BY 
            admission_type_priority ASC,
            gender_priority ASC,
            age_diff ASC,
            los_diff ASC,
            date_diff ASC
        LIMIT 50
        """
        return await self._execute_query(query)

    async def _step_7(self, input_data: Dict) -> List[Dict]:
        """STEP 7: Use Doctor Specialty instead of Doctor Name"""
        icd10_filter, icd9_filter, icd10_score, icd9_score = self._calculate_icd_scores(input_data, "exact")
        exclusion = self._get_exclusion_clause()

        doctor_specialty = input_data.get('doctor_specialty', '')
        query = f"""
        SELECT 
            *,
            abs(dateDiff('year', BirthDate, toDate('{input_data['formatted_birth_date']}'))) as age_diff,
            abs(dateDiff('day', AdmissionDate, parseDateTime('{input_data['formatted_admission_date']}'))) as date_diff,
            {self._get_los_diff_calculation(input_data)},
            CASE WHEN AdmissionTypeName = '{input_data.get('admission_type', '')}' THEN 0 ELSE 1 END as admission_type_priority,
            CASE WHEN Sex = '{input_data.get('gender', '')}' THEN 0 ELSE 1 END as gender_priority,
            'STEP_7' as result_step
        FROM {env.clickhouse_table_name}
        WHERE 
            {icd10_filter}
            AND {icd9_filter}
            AND OrganizationCode = '{input_data.get('hospital_code', '')}'
            AND PayerName = '{input_data.get('payer_name', '')}'
            AND Specialty = '{input_data.get('doctor_specialty', '')}'
            {exclusion}
        ORDER BY 
            admission_type_priority ASC,
            gender_priority ASC,
            age_diff ASC,
            los_diff ASC,
            date_diff ASC
        LIMIT 50
        """
        return await self._execute_query(query)

    async def _step_8(self, input_data: Dict) -> List[Dict]:
        """STEP 8: Remove Doctor Name/Specialty"""
        icd10_filter, icd9_filter, icd10_score, icd9_score = self._calculate_icd_scores(input_data, "exact")
        exclusion = self._get_exclusion_clause()

        query = f"""
        SELECT 
            *,
            abs(dateDiff('year', BirthDate, toDate('{input_data['formatted_birth_date']}'))) as age_diff,
            abs(dateDiff('day', AdmissionDate, parseDateTime('{input_data['formatted_admission_date']}'))) as date_diff,
            {self._get_los_diff_calculation(input_data)},
            CASE WHEN AdmissionTypeName = '{input_data.get('admission_type', '')}' THEN 0 ELSE 1 END as admission_type_priority,
            CASE WHEN Sex = '{input_data.get('gender', '')}' THEN 0 ELSE 1 END as gender_priority,
            'STEP_8' as result_step
        FROM {env.clickhouse_table_name}
        WHERE 
            {icd10_filter}
            AND {icd9_filter}
            AND OrganizationCode = '{input_data.get('hospital_code', '')}'
            AND PayerName = '{input_data.get('payer_name', '')}'
            {exclusion}
        ORDER BY 
            admission_type_priority ASC,
            gender_priority ASC,
            age_diff ASC,
            los_diff ASC,
            date_diff ASC
        LIMIT 50
        """
        return await self._execute_query(query)

    async def _step_9(self, input_data: Dict) -> List[Dict]:
        """STEP 9: Use Payer Type instead of Payer Name"""
        icd10_filter, icd9_filter, icd10_score, icd9_score = self._calculate_icd_scores(input_data, "exact")
        exclusion = self._get_exclusion_clause()

        query = f"""
        SELECT 
            *,
            abs(dateDiff('year', BirthDate, toDate('{input_data['formatted_birth_date']}'))) as age_diff,
            abs(dateDiff('day', AdmissionDate, parseDateTime('{input_data['formatted_admission_date']}'))) as date_diff,
            {self._get_los_diff_calculation(input_data)},
            CASE WHEN PrimaryDoctor = '{input_data.get('primary_doctor', '')}' THEN 0 ELSE 1 END as doctor_priority,
            CASE WHEN AdmissionTypeName = '{input_data.get('admission_type', '')}' THEN 0 ELSE 1 END as admission_type_priority,
            CASE WHEN Sex = '{input_data.get('gender', '')}' THEN 0 ELSE 1 END as gender_priority,
            'STEP_9' as result_step
        FROM {env.clickhouse_table_name}
        WHERE 
            {icd10_filter}
            AND {icd9_filter}
            AND OrganizationCode = '{input_data.get('hospital_code', '')}'
            AND PayerType = '{input_data.get('payer_type', '')}'
            {exclusion}
        ORDER BY 
            doctor_priority ASC,
            admission_type_priority ASC,
            gender_priority ASC,
            age_diff ASC,
            los_diff ASC,
            date_diff ASC
        LIMIT 50
        """
        return await self._execute_query(query)

    async def _step_10(self, input_data: Dict) -> List[Dict]:
        """STEP 10: Remove Payer Type, keep Hospital Name"""
        icd10_filter, icd9_filter, icd10_score, icd9_score = self._calculate_icd_scores(input_data, "exact")
        exclusion = self._get_exclusion_clause()

        query = f"""
        SELECT 
            *,
            abs(dateDiff('year', BirthDate, toDate('{input_data['formatted_birth_date']}'))) as age_diff,
            abs(dateDiff('day', AdmissionDate, parseDateTime('{input_data['formatted_admission_date']}'))) as date_diff,
            {self._get_los_diff_calculation(input_data)},
            CASE WHEN PrimaryDoctor = '{input_data.get('primary_doctor', '')}' THEN 0 ELSE 1 END as doctor_priority,
            CASE WHEN AdmissionTypeName = '{input_data.get('admission_type', '')}' THEN 0 ELSE 1 END as admission_type_priority,
            CASE WHEN Sex = '{input_data.get('gender', '')}' THEN 0 ELSE 1 END as gender_priority,
            'STEP_10' as result_step
        FROM {env.clickhouse_table_name}
        WHERE 
            {icd10_filter}
            AND {icd9_filter}
            AND OrganizationCode = '{input_data.get('hospital_code', '')}'
            {exclusion}
        ORDER BY 
            doctor_priority ASC,
            admission_type_priority ASC,
            gender_priority ASC,
            age_diff ASC,
            los_diff ASC,
            date_diff ASC
        LIMIT 50
        """
        return await self._execute_query(query)

    async def _step_11(self, input_data: Dict) -> List[Dict]:
        """STEP 11: Use Hospital Archetype instead of Hospital Name"""
        icd10_filter, icd9_filter, icd10_score, icd9_score = self._calculate_icd_scores(input_data, "exact")
        exclusion = self._get_exclusion_clause()

        query = f"""
        SELECT 
            *,
            abs(dateDiff('year', BirthDate, toDate('{input_data['formatted_birth_date']}'))) as age_diff,
            abs(dateDiff('day', AdmissionDate, parseDateTime('{input_data['formatted_admission_date']}'))) as date_diff,
            {self._get_los_diff_calculation(input_data)},
            CASE WHEN PayerName = '{input_data.get('payer_name', '')}' THEN 0 ELSE 1 END as payer_name_priority,
            CASE WHEN PayerType = '{input_data.get('payer_type', '')}' THEN 0 ELSE 1 END as payer_type_priority,
            CASE WHEN PrimaryDoctor = '{input_data.get('primary_doctor', '')}' THEN 0 ELSE 1 END as doctor_priority,
            CASE WHEN AdmissionTypeName = '{input_data.get('admission_type', '')}' THEN 0 ELSE 1 END as admission_type_priority,
            CASE WHEN Sex = '{input_data.get('gender', '')}' THEN 0 ELSE 1 END as gender_priority,
            'STEP_11' as result_step
        FROM {env.clickhouse_table_name}
        WHERE 
            {icd10_filter}
            AND {icd9_filter}
            AND Archetype = '{input_data.get('archetype', '')}'
            {exclusion}
        ORDER BY 
            payer_name_priority ASC,
            payer_type_priority ASC,
            doctor_priority ASC,
            admission_type_priority ASC,
            gender_priority ASC,
            age_diff ASC,
            los_diff ASC,
            date_diff ASC
        LIMIT 50
        """
        return await self._execute_query(query)

    async def _step_12(self, input_data: Dict) -> List[Dict]:
        """STEP 12: Same as Step 11 but different sorting"""
        icd10_filter, icd9_filter, icd10_score, icd9_score = self._calculate_icd_scores(input_data, "exact")
        exclusion = self._get_exclusion_clause()

        query = f"""
        SELECT 
            *,
            abs(dateDiff('year', BirthDate, toDate('{input_data['formatted_birth_date']}'))) as age_diff,
            abs(dateDiff('day', AdmissionDate, parseDateTime('{input_data['formatted_admission_date']}'))) as date_diff,
            {self._get_los_diff_calculation(input_data)},
            CASE WHEN PrimaryDoctor = '{input_data.get('primary_doctor', '')}' THEN 0 ELSE 1 END as doctor_priority,
            CASE WHEN AdmissionTypeName = '{input_data.get('admission_type', '')}' THEN 0 ELSE 1 END as admission_type_priority,
            CASE WHEN Sex = '{input_data.get('gender', '')}' THEN 0 ELSE 1 END as gender_priority,
            'STEP_12' as result_step
        FROM {env.clickhouse_table_name}
        WHERE 
            {icd10_filter}
            AND {icd9_filter}
            AND Archetype = '{input_data.get('archetype', '')}'
            {exclusion}
        ORDER BY 
            doctor_priority ASC,
            admission_type_priority ASC,
            gender_priority ASC,
            age_diff ASC,
            los_diff ASC,
            date_diff ASC
        LIMIT 50
        """
        return await self._execute_query(query)

    async def _step_13(self, input_data: Dict) -> List[Dict]:
        """STEP 13: Use Hospital Region instead of Archetype"""
        icd10_filter, icd9_filter, icd10_score, icd9_score = self._calculate_icd_scores(input_data, "exact")
        exclusion = self._get_exclusion_clause()

        query = f"""
        SELECT 
            *,
            abs(dateDiff('year', BirthDate, toDate('{input_data['formatted_birth_date']}'))) as age_diff,
            abs(dateDiff('day', AdmissionDate, parseDateTime('{input_data['formatted_admission_date']}'))) as date_diff,
            {self._get_los_diff_calculation(input_data)},
            CASE WHEN PrimaryDoctor = '{input_data.get('primary_doctor', '')}' THEN 0 ELSE 1 END as doctor_priority,
            CASE WHEN AdmissionTypeName = '{input_data.get('admission_type', '')}' THEN 0 ELSE 1 END as admission_type_priority,
            CASE WHEN Sex = '{input_data.get('gender', '')}' THEN 0 ELSE 1 END as gender_priority,
            'STEP_13' as result_step
        FROM {env.clickhouse_table_name}
        WHERE 
            {icd10_filter}
            AND {icd9_filter}
            AND Region = '{input_data.get('hospital_region', '')}'
            {exclusion}
        ORDER BY 
            doctor_priority ASC,
            admission_type_priority ASC,
            gender_priority ASC,
            age_diff ASC,
            los_diff ASC,
            date_diff ASC
        LIMIT 50
        """
        return await self._execute_query(query)

    async def _step_14(self, input_data: Dict) -> List[Dict]:
        """STEP 14: Only exact ICD codes (no other constraints)"""
        icd10_filter, icd9_filter, icd10_score, icd9_score = self._calculate_icd_scores(input_data, "exact")
        exclusion = self._get_exclusion_clause()

        query = f"""
        SELECT 
            *,
            abs(dateDiff('year', BirthDate, toDate('{input_data['formatted_birth_date']}'))) as age_diff,
            abs(dateDiff('day', AdmissionDate, parseDateTime('{input_data['formatted_admission_date']}'))) as date_diff,
            {self._get_los_diff_calculation(input_data)},
            CASE WHEN OrganizationCode = '{input_data.get('hospital_code', '')}' THEN 0 ELSE 1 END as hospital_priority,
            CASE WHEN Region = '{input_data.get('hospital_region', '')}' THEN 0 ELSE 1 END as region_priority,
            CASE WHEN Archetype = '{input_data.get('archetype', '')}' THEN 0 ELSE 1 END as archetype_priority,
            CASE WHEN PrimaryDoctor = '{input_data.get('primary_doctor', '')}' THEN 0 ELSE 1 END as doctor_priority,
            CASE WHEN AdmissionTypeName = '{input_data.get('admission_type', '')}' THEN 0 ELSE 1 END as admission_type_priority,
            CASE WHEN Sex = '{input_data.get('gender', '')}' THEN 0 ELSE 1 END as gender_priority,
            'STEP_14' as result_step
        FROM {env.clickhouse_table_name}
        WHERE 
            {icd10_filter}
            AND {icd9_filter}
            {exclusion}
        ORDER BY 
            hospital_priority ASC,
            region_priority ASC,
            archetype_priority ASC,
            doctor_priority ASC,
            admission_type_priority ASC,
            gender_priority ASC,
            age_diff ASC,
            los_diff ASC,
            date_diff ASC
        LIMIT 50
        """
        return await self._execute_query(query)

    async def _step_15(self, input_data: Dict) -> List[Dict]:
        """STEP 15: Exact ICD9 + Partial ICD10 (no other constraints)"""
        icd10_filter, icd9_filter, icd10_score, icd9_score = self._calculate_icd_scores(input_data, "mixed")
        exclusion = self._get_exclusion_clause()

        query = f"""
        SELECT 
            *,
            ({icd10_score}) as icd10_match_count,
            ({icd9_score}) as icd9_match_count,
            abs(dateDiff('year', BirthDate, toDate('{input_data['formatted_birth_date']}'))) as age_diff,
            abs(dateDiff('day', AdmissionDate, parseDateTime('{input_data['formatted_admission_date']}'))) as date_diff,
            {self._get_los_diff_calculation(input_data)},
            'STEP_15' as result_step
        FROM {env.clickhouse_table_name}
        WHERE 
            {icd10_filter}
            AND {icd9_filter}
            {exclusion}
        ORDER BY 
            icd9_match_count DESC,   -- Exact ICD9 matches first
            icd10_match_count DESC,  -- Then partial ICD10 matches
            age_diff ASC,
            los_diff ASC,
            date_diff ASC
        LIMIT 50
        """
        return await self._execute_query(query)

    async def _step_16(self, input_data: Dict) -> List[Dict]:
        """STEP 16: Only exact ICD9 (ignore ICD10 completely)"""
        icd10_filter, icd9_filter, icd10_score, icd9_score = self._calculate_icd_scores(input_data, "icd9_only_exact")
        exclusion = self._get_exclusion_clause()

        query = f"""
        SELECT 
            *,
            ({icd9_score}) as icd9_match_count,
            abs(dateDiff('year', BirthDate, toDate('{input_data['formatted_birth_date']}'))) as age_diff,
            abs(dateDiff('day', AdmissionDate, parseDateTime('{input_data['formatted_admission_date']}'))) as date_diff,
            {self._get_los_diff_calculation(input_data)},
            'STEP_16' as result_step
        FROM {env.clickhouse_table_name}
        WHERE 
            {icd9_filter}
            {exclusion}
        ORDER BY 
            icd9_match_count DESC,
            age_diff ASC,
            los_diff ASC,
            date_diff ASC
        LIMIT 50
        """
        return await self._execute_query(query)

    async def _step_17(self, input_data: Dict) -> List[Dict]:
        """STEP 17: Only partial ICD9 (ignore ICD10 completely)"""
        icd10_filter, icd9_filter, icd10_score, icd9_score = self._calculate_icd_scores(input_data, "icd9_only_partial")
        exclusion = self._get_exclusion_clause()

        query = f"""
        SELECT 
            *,
            ({icd9_score}) as icd9_match_count,
            abs(dateDiff('year', BirthDate, toDate('{input_data['formatted_birth_date']}'))) as age_diff,
            abs(dateDiff('day', AdmissionDate, parseDateTime('{input_data['formatted_admission_date']}'))) as date_diff,
            {self._get_los_diff_calculation(input_data)},
            'STEP_17' as result_step
        FROM {env.clickhouse_table_name}
        WHERE 
            {icd9_filter}
            {exclusion}
        ORDER BY 
            icd9_match_count DESC,
            age_diff ASC,
            los_diff ASC,
            date_diff ASC
        LIMIT 50
        """
        return await self._execute_query(query)
    
    async def _execute_query(self, query: str) -> List[tuple]:
        """Execute ClickHouse query asynchronously"""
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                self.thread_pool,
                lambda: self.client.query(query)
            )
            return result.result_rows
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return []

    def format_results_for_api(self, raw_results: List[tuple]) -> Dict[str, Any]:
        """Format raw ClickHouse results for API response"""
        formatted_results = []
        
        # Column mapping for admission data
        column_mapping = [
            'AdmissionId', 'AdmissionTypeId', 'AdmissionTypeName', 'OrganizationCode',
            'OrganizationId', 'AdmissionDate', 'DischargeDate', 'PatientId', 'BirthDate',
            'Sex', 'PatientType', 'PatientTypeId', 'PrimaryDoctor', 'PrimaryDoctorUserId',
            'Specialty', 'SpecialtyGroup', 'Region', 'Archetype', 'DiseaseClassification',
            'ProcedureClassification', 'InvoiceClass', 'InvoiceClassId', 'PayerName',
            'PayerId', 'PayerType', 'InvoiceNetAmount', 'Age', 'LengthOfStay',
            'AnesthesiaDoctor', 'AnesthesiaType'
        ]
        
        for result_row in raw_results:
            result_dict = {}
            
            # Map base columns
            for i, col_name in enumerate(column_mapping):
                if i < len(result_row):
                    result_dict[col_name] = result_row[i]
            
            # Add calculated fields from the end of the result
            if len(result_row) > len(column_mapping):
                remaining_fields = result_row[len(column_mapping):]
                if len(remaining_fields) >= 3:
                    result_dict['age_diff'] = remaining_fields[-3]
                    result_dict['date_diff'] = remaining_fields[-2]
                    result_dict['result_step'] = remaining_fields[-1]
            
            formatted_results.append({
                'document': result_dict,
                'highlights': {},
                'text_match': 100
            })
        
        return {
            'found': len(formatted_results),
            'results': formatted_results,
            'search_time_ms': 0,
            'page': 1
        }

# Global service instance
clickhouse_medical_search_service = ClickHouseMedicalSearchService()