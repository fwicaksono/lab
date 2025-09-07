from typing import List
import json
from app.services.GeminiService import gemini_service
from app.prompts.summary_prompt import billing_summary

class BillingService:
    def __init__(self):
        pass
    
    async def generate_billing_summary(self, item_types: List[str]) -> List[str]:
        """
        Generate AI summary for billing item types using Gemini
        """
        try:
            if not item_types:
                return []
            
            # Get prompt and combine with item types
            base_prompt = billing_summary()
            prompt = f"{base_prompt}\n\nItem types to explain: {', '.join(item_types)}"
            
            # Get Gemini client
            llm = gemini_service.gemini_20_flash()
            
            # Call Gemini
            response = llm.invoke(prompt)
            
            # Extract the content from response
            if hasattr(response, 'content'):
                response_text = response.content
            else:
                response_text = str(response)
            
            # Try to parse as JSON array
            try:
                summaries = json.loads(response_text)
                if isinstance(summaries, list):
                    return summaries[:len(item_types)]
                else:
                    # If not a list, return as single item
                    return [response_text] if response_text else []
            except json.JSONDecodeError:
                # Clean up the response by removing markdown and JSON artifacts
                cleaned_text = response_text.replace('```json', '').replace('```', '').strip()
                
                # Try parsing the cleaned text
                try:
                    summaries = json.loads(cleaned_text)
                    if isinstance(summaries, list):
                        return summaries[:len(item_types)]
                except:
                    # If still not valid JSON, split by lines and clean up
                    lines = cleaned_text.strip().split('\n')
                    cleaned_lines = []
                    for line in lines:
                        line = line.strip(' -"[]').rstrip(',').strip()
                        if line and not line.startswith('[') and not line.startswith('{'):
                            # Remove the field names if present (like "Drugs:")
                            if ':' in line:
                                line = line.split(':', 1)[1].strip(' "')
                            cleaned_lines.append(line)
                    
                    return cleaned_lines if cleaned_lines else [response_text] if response_text else []
                
        except Exception as e:
            print(f"Error generating billing summary: {e}")
            return []

billing_service = BillingService()