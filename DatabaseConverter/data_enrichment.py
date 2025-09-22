from typing import Dict, List, Optional, Any
import streamlit as st
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from api_clients import ClearbitClient, LondonPlanningClient

class DataEnrichmentManager:
    """Manages multiple data enrichment providers"""
    
    def __init__(self):
        self.providers = {}
        self.active_providers = []
        self.api_keys = {}
        
        # Initialize provider clients (will be updated with API keys)
        self._initialize_providers()
        
        # Initialize London Planning Portal (no API key needed)
        self.planning_portal = LondonPlanningClient()
    
    def _initialize_providers(self):
        """Initialize all provider clients"""
        self.providers = {
            'clearbit': None,
            'planning_portal': True  # Always available, no API key needed
        }
    
    def update_api_keys(self, api_keys: Dict[str, str]):
        """Update API keys and reinitialize providers"""
        self.api_keys = api_keys
        
        # Initialize providers with API keys
        if api_keys.get('clearbit'):
            self.providers['clearbit'] = ClearbitClient(api_keys['clearbit'])
    
    def get_available_providers(self) -> Dict[str, bool]:
        """Get list of providers and their availability status"""
        return {
            provider: client is not None 
            for provider, client in self.providers.items()
        }
    
    def set_active_providers(self, provider_names: List[str]):
        """Set which providers to use for enrichment"""
        self.active_providers = [
            name for name in provider_names 
            if name in self.providers and self.providers[name] is not None
        ]
    
    def enrich_company(self, company_data: Dict, max_workers: int = 3) -> Dict[str, Any]:
        """Enrich company data using all active providers"""
        if not self.active_providers:
            return {}
        
        results = {}
        
        # Use ThreadPoolExecutor for concurrent API calls
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit enrichment tasks
            future_to_provider = {}
            
            for provider_name in self.active_providers:
                client = self.providers[provider_name]
                if client:
                    future = executor.submit(self._safe_enrich, client, company_data, provider_name)
                    future_to_provider[future] = provider_name
            
            # Collect results as they complete
            for future in as_completed(future_to_provider, timeout=30):
                provider_name = future_to_provider[future]
                try:
                    result = future.result()
                    results[provider_name] = result
                except Exception as e:
                    results[provider_name] = None
                    st.warning(f"Error enriching with {provider_name}: {str(e)}")
        
        return results
    
    def _safe_enrich(self, client, company_data: Dict, provider_name: str) -> Optional[Dict]:
        """Safely call enrichment API with error handling"""
        try:
            # Add small delay between API calls to respect rate limits
            time.sleep(0.1)
            return client.enrich_company(company_data)
        except Exception as e:
            st.warning(f"{provider_name} enrichment failed: {str(e)}")
            return None
    
    def enrich_company_sequential(self, company_data: Dict) -> Dict[str, Any]:
        """Enrich company data sequentially (fallback method)"""
        results = {}
        
        for provider_name in self.active_providers:
            client = self.providers[provider_name]
            if client:
                try:
                    result = client.enrich_company(company_data)
                    results[provider_name] = result
                    
                    # Rate limiting between providers
                    time.sleep(1)
                    
                except Exception as e:
                    results[provider_name] = None
                    st.warning(f"Error enriching with {provider_name}: {str(e)}")
        
        return results
    
    def get_provider_info(self) -> Dict[str, Dict[str, Any]]:
        """Get information about each provider"""
        return {
            'clearbit': {
                'name': 'Clearbit',
                'description': 'Premium B2B data platform with 95% accuracy',
                'features': ['Real-time enrichment', 'Visitor identification', 'Technographics'],
                'pricing': 'Subscription-based, custom pricing',
                'accuracy': '95%',
                'best_for': 'Enterprise teams, high accuracy needs'
            }
        }
    
    def validate_enrichment_data(self, enrichment_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and clean enrichment data"""
        validation_results = {}
        
        for provider, data in enrichment_data.items():
            if not data:
                validation_results[provider] = {
                    'valid': False,
                    'issues': ['No data returned']
                }
                continue
            
            issues = []
            
            # Check for required fields
            if not data.get('name'):
                issues.append('Missing company name')
            
            if not data.get('domain'):
                issues.append('Missing domain')
            
            # Check data quality
            if data.get('employee_count'):
                try:
                    emp_count = int(data['employee_count'])
                    if emp_count < 0:
                        issues.append('Invalid employee count')
                except (ValueError, TypeError):
                    issues.append('Invalid employee count format')
            
            # Check for suspicious data patterns
            if data.get('domain') and not '.' in data['domain']:
                issues.append('Invalid domain format')
            
            validation_results[provider] = {
                'valid': len(issues) == 0,
                'issues': issues,
                'quality_score': self._calculate_quality_score(data)
            }
        
        return validation_results
    
    def _calculate_quality_score(self, data: Dict) -> float:
        """Calculate data quality score (0-100)"""
        score = 0
        max_score = 0
        
        # Check for presence of key fields
        key_fields = ['name', 'domain', 'industry', 'employee_count', 'description']
        for field in key_fields:
            max_score += 20
            if data.get(field):
                score += 20
        
        # Bonus points for additional data
        bonus_fields = ['annual_revenue', 'founded_year', 'technologies', 'social_profiles']
        for field in bonus_fields:
            if data.get(field):
                score += 5
        
        return min(100, (score / max_score * 100) if max_score > 0 else 0)
    
    def merge_enrichment_data(self, enrichment_results: Dict[str, Any]) -> Dict[str, Any]:
        """Merge data from multiple providers into a single enriched profile"""
        merged_data = {}
        
        # Priority order for data sources (most trusted first)
        priority_order = ['clearbit']
        
        # Base fields to merge
        fields_to_merge = [
            'name', 'domain', 'industry', 'employee_count', 'annual_revenue',
            'description', 'founded_year', 'location'
        ]
        
        # Merge based on priority
        for field in fields_to_merge:
            for provider in priority_order:
                if provider in enrichment_results and enrichment_results[provider]:
                    provider_data = enrichment_results[provider]
                    if field in provider_data and provider_data[field] and field not in merged_data:
                        merged_data[field] = provider_data[field]
                        merged_data[f'{field}_source'] = provider
                        break
        
        # Merge technologies from all providers
        all_technologies = set()
        for provider, data in enrichment_results.items():
            if data and 'technologies' in data and data['technologies']:
                if isinstance(data['technologies'], list):
                    all_technologies.update(data['technologies'])
                elif isinstance(data['technologies'], str):
                    all_technologies.add(data['technologies'])
        
        if all_technologies:
            merged_data['technologies'] = list(all_technologies)
        
        # Merge social profiles
        social_profiles = {}
        for provider, data in enrichment_results.items():
            if data and 'social_profiles' in data and data['social_profiles']:
                for platform, url in data['social_profiles'].items():
                    if url and platform not in social_profiles:
                        social_profiles[platform] = url
        
        if social_profiles:
            merged_data['social_profiles'] = social_profiles
        
        # Add metadata about the enrichment
        merged_data['enrichment_metadata'] = {
            'providers_used': list(enrichment_results.keys()),
            'successful_providers': [k for k, v in enrichment_results.items() if v],
            'enrichment_timestamp': time.time(),
            'data_quality_score': self._calculate_quality_score(merged_data)
        }
        
        return merged_data
    
    def enrich_with_planning_data(self, company_data: Dict, application_type: str = None, decision_date: str = None) -> Dict[str, Any]:
        """Enrich company data with planning information using specific filters"""
        if not self.planning_portal:
            return {}
        
        company_name = company_data.get('company_name', '')
        postcode = self._extract_postcode(company_data.get('registered_office_address', {}))
        
        planning_data = self.planning_portal.search_planning_applications(
            company_name, postcode, application_type, decision_date
        )
        
        if planning_data and not planning_data.get('error'):
            return planning_data  # Return the structured data with applications array
        
        return {'total_applications': 0, 'applications': []}
    
    def _extract_postcode(self, address_data) -> str:
        """Extract postcode from address data"""
        if isinstance(address_data, dict):
            return address_data.get('postal_code', '')
        elif isinstance(address_data, str) and address_data:
            # Try to extract postcode from address string
            import re
            postcode_pattern = r'[A-Z]{1,2}\d[A-Z\d]? \d[A-Z]{2}'
            match = re.search(postcode_pattern, address_data.upper())
            return match.group() if match else ''
        return ''
