import requests
import re
import time
import urllib.parse
from typing import List, Dict, Optional
import trafilatura


class LinkedInScraper:
    """
    DEPRECATED: LinkedIn scraper class - Google search disabled to avoid rate limits.
    Use Bright Data API instead for LinkedIn profile searches.
    """
    
    def __init__(self):
        # Disabled to avoid rate limiting issues
        pass
    
    def clean_name(self, name: str) -> str:
        """Clean and format name for search"""
        if not name:
            return ""
        
        # Remove common titles and suffixes
        titles_to_remove = [
            'Mr', 'Mrs', 'Ms', 'Miss', 'Dr', 'Prof', 'Sir', 'Dame',
            'Jr', 'Sr', 'III', 'IV', 'Jr.', 'Sr.', 'III.', 'IV.',
            'OBE', 'MBE', 'CBE', 'KBE', 'GBE'
        ]
        
        # Split name into parts
        name_parts = name.strip().split()
        cleaned_parts = []
        
        for part in name_parts:
            # Remove punctuation and check if it's a title
            clean_part = re.sub(r'[^\w]', '', part)
            if clean_part not in titles_to_remove:
                cleaned_parts.append(part.strip(','))
        
        return ' '.join(cleaned_parts)
    
    def build_search_query(self, person_name: str, company_name: str) -> str:
        """Build Google search query for LinkedIn profile"""
        clean_person = self.clean_name(person_name)
        clean_company = company_name.strip()
        
        # Build search query targeting LinkedIn
        query = f'site:linkedin.com/in/ "{clean_person}" "{clean_company}"'
        return query
    
    def search_google(self, query: str, max_results: int = 5) -> List[str]:
        """
        DISABLED: Google search removed to avoid rate limiting.
        Use Bright Data API instead.
        """
        return []
    
    def validate_linkedin_url(self, url: str) -> bool:
        """Validate if URL is a proper LinkedIn profile URL"""
        if not url:
            return False
        
        # Check if it's a LinkedIn profile URL
        linkedin_profile_pattern = r'^https://[a-z]{2,3}\.linkedin\.com/in/[a-zA-Z0-9\-_%]+$'
        return bool(re.match(linkedin_profile_pattern, url))
    
    def search_officer_linkedin(self, officer_name: str, company_name: str) -> Optional[str]:
        """
        DISABLED: Officer LinkedIn search removed to avoid rate limiting.
        Use Bright Data API instead.
        """
        return None
        
    
    def search_company_officers_linkedin(self, officers: List[str], company_name: str) -> Dict[str, str]:
        """
        DISABLED: Company officers LinkedIn search removed to avoid rate limiting.
        Use Bright Data API instead.
        """
        return {}
    
    def format_linkedin_results(self, linkedin_data: Dict[str, str]) -> str:
        """Format LinkedIn results for display in table"""
        if not linkedin_data:
            return "No LinkedIn profiles found"
        
        formatted_results = []
        for name, url in linkedin_data.items():
            formatted_results.append(f"{name}: {url}")
        
        return "; ".join(formatted_results)


# Utility functions for integration
def extract_officer_names(officer_details: str) -> List[str]:
    """Extract individual officer names from the officer details string"""
    if not officer_details or officer_details == "No officers found":
        return []
    
    # Split by semicolon and clean names
    names = []
    for name in officer_details.split(';'):
        clean_name = name.strip()
        if clean_name and not clean_name.startswith('+'):  # Skip "+ X more" entries
            names.append(clean_name)
    
    return names


def search_company_linkedin_profiles(company_name: str, officer_details: str) -> str:
    """
    Main function to search LinkedIn profiles for a company's officers using professional APIs
    Returns formatted string of LinkedIn URLs
    """
    if not company_name or not officer_details:
        return "Insufficient data for LinkedIn search"
    
    # Try to get LinkedIn data from enrichment providers first
    linkedin_url = get_company_linkedin_from_enrichment(company_name)
    if linkedin_url:
        return f"Company LinkedIn: {linkedin_url}"
    
    # Try Bright Data API for officer LinkedIn profiles
    officer_names = extract_officer_names(officer_details)
    if officer_names:
        bright_data_results = search_officers_with_bright_data(officer_names, company_name)
        if bright_data_results:
            return format_bright_data_results(bright_data_results)
        else:
            return "No LinkedIn profiles found - Bright Data API required"
    
    return "No officer names found"


def get_company_linkedin_from_enrichment(company_name: str) -> str:
    """
    Get company LinkedIn URL from enrichment providers (Apollo, Clearbit)
    This is faster and more reliable than web scraping
    """
    import streamlit as st
    
    try:
        # Check if enrichment manager is available
        if 'enrichment_manager' not in st.session_state:
            return ""
        
        enrichment_manager = st.session_state.enrichment_manager
        
        # Create mock company data for enrichment
        company_data = {
            'company_name': company_name,
            'title': company_name
        }
        
        # Get enrichment data from providers
        enrichment_results = enrichment_manager.enrich_company(company_data)
        
        # Extract LinkedIn URLs from enrichment results
        linkedin_urls = []
        
        for provider, data in enrichment_results.items():
            if data and isinstance(data, dict):
                # Check social profiles for LinkedIn
                social_profiles = data.get('social_profiles', {})
                if social_profiles and 'linkedin' in social_profiles:
                    linkedin_url = social_profiles['linkedin']
                    if linkedin_url and linkedin_url != 'N/A':
                        # Clean and validate LinkedIn URL
                        if 'linkedin.com' in linkedin_url:
                            if not linkedin_url.startswith('http'):
                                linkedin_url = f"https://{linkedin_url}"
                            linkedin_urls.append(f"{provider.title()}: {linkedin_url}")
        
        if linkedin_urls:
            return "; ".join(linkedin_urls)
        
        return ""
        
    except Exception as e:
        return f"Enrichment error: {str(e)[:50]}..."


def search_officers_with_bright_data(officer_names: List[str], company_name: str, company_address: str = None) -> Dict[str, str]:
    """
    Search for LinkedIn profiles using Bright Data API with GB filtering and city prioritization
    Returns dict mapping officer names to LinkedIn URLs
    """
    import streamlit as st
    
    try:
        # Check if Bright Data client is available
        if 'brightdata_client' not in st.session_state or not st.session_state.brightdata_client:
            return {}
        
        client = st.session_state.brightdata_client
        
        # Prepare officer data for Bright Data search
        officers_data = []
        for name in officer_names:
            name_parts = name.strip().split()
            if len(name_parts) >= 2:
                officers_data.append({
                    'name': name,
                    'first_name': name_parts[0],
                    'last_name': ' '.join(name_parts[1:])
                })
        
        if not officers_data:
            return {}
        
        # Use batch search for multiple officers with company address for city matching
        results = client.search_multiple_profiles(officers_data, company_name, company_address)
        return results
        
    except Exception as e:
        pass  # Silent error handling
        return {}


def format_bright_data_results(results: Dict[str, str]) -> str:
    """Format Bright Data LinkedIn results for display"""
    if not results:
        return "No LinkedIn profiles found via Bright Data"
    
    formatted_results = []
    for name, url in results.items():
        formatted_results.append(f"{name}: {url}")
    
    return "; ".join(formatted_results)