#!/usr/bin/env python3
"""
Debug script to understand Companies House API query format
"""
import os
import sys
import requests
from datetime import date

def debug_companies_house_queries():
    """Debug different query formats to understand what works"""
    
    # Get API key
    api_key = os.getenv("COMPANIES_HOUSE_API_KEY")
    if not api_key:
        print("❌ COMPANIES_HOUSE_API_KEY not configured")
        return False
    
    base_url = "https://api.company-information.service.gov.uk"
    session = requests.Session()
    session.auth = (api_key, '')
    session.headers.update({
        'User-Agent': 'UK-Company-Enrichment-App/1.0'
    })
    
    print(f"✅ Testing different query formats with API key: {api_key[:8]}...")
    
    test_queries = [
        # Test 1: Just SIC code in different formats
        {"q": "sic_codes:4110", "items_per_page": 5},
        {"q": "sic:4110", "items_per_page": 5},
        {"q": "4110", "items_per_page": 5},
        
        # Test 2: SIC code with status
        {"q": "sic_codes:4110 company_status:active", "items_per_page": 5},
        
        # Test 3: Different SIC codes to see if any work
        {"q": "sic_codes:62020", "items_per_page": 5},  # Computer consulting
        {"q": "sic_codes:70100", "items_per_page": 5},  # Head office activities
        
        # Test 4: Just status
        {"q": "company_status:active", "items_per_page": 5},
        
        # Test 5: Simple general search
        {"q": "limited", "items_per_page": 5},
    ]
    
    endpoint = "/search/companies"
    
    for i, params in enumerate(test_queries):
        print(f"\n🔍 Test {i+1}: Query = '{params['q']}'")
        try:
            url = f"{base_url}{endpoint}"
            response = session.get(url, params=params)
            
            print(f"   Status Code: {response.status_code}")
            print(f"   URL: {response.url}")
            
            if response.status_code == 200:
                data = response.json()
                total_results = data.get('total_results', 0)
                items = data.get('items', [])
                print(f"   ✅ Success! Total Results: {total_results}, Items returned: {len(items)}")
                
                if items:
                    sample = items[0]
                    company_name = sample.get('title', sample.get('company_name', 'N/A'))
                    company_number = sample.get('company_number', 'N/A')
                    sic_codes = sample.get('sic_codes', [])
                    status = sample.get('company_status', 'N/A')
                    print(f"   Sample: {company_name} ({company_number})")
                    print(f"   Status: {status}, SIC Codes: {sic_codes}")
                else:
                    print("   No items returned even though total_results > 0")
                    
            elif response.status_code == 401:
                print("   ❌ Unauthorized - API key issue")
                return False
            elif response.status_code == 429:
                print("   ⚠️ Rate limited - waiting...")
                import time
                time.sleep(1)
            else:
                print(f"   ❌ Failed with status {response.status_code}")
                print(f"   Response: {response.text[:200]}")
                
        except Exception as e:
            print(f"   ❌ Exception: {e}")
    
    print("\n" + "="*60)
    print("Summary: Testing specific queries mentioned in the bug report")
    
    # Test the specific cases from the bug report
    bug_test_queries = [
        {"q": "sic_codes:4110 company_status:active", "items_per_page": 20},
        {"q": "sic_codes:4110", "items_per_page": 20},
    ]
    
    for i, params in enumerate(bug_test_queries):
        print(f"\n🐛 Bug Test {i+1}: Query = '{params['q']}'")
        try:
            url = f"{base_url}{endpoint}"
            response = session.get(url, params=params)
            
            print(f"   Status Code: {response.status_code}")
            print(f"   URL: {response.url}")
            
            if response.status_code == 200:
                data = response.json()
                total_results = data.get('total_results', 0)
                items = data.get('items', [])
                print(f"   Results: {total_results} total, {len(items)} items returned")
                
                if items:
                    for j, item in enumerate(items[:3]):
                        company_name = item.get('title', 'N/A')
                        company_number = item.get('company_number', 'N/A')
                        print(f"   [{j+1}] {company_name} ({company_number})")
            else:
                print(f"   ❌ Failed with status {response.status_code}: {response.text}")
                
        except Exception as e:
            print(f"   ❌ Exception: {e}")
    
    return True

if __name__ == "__main__":
    debug_companies_house_queries()