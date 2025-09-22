#!/usr/bin/env python3
"""Test the fixed Companies House search functionality"""

import os
import sys
from api_clients import CompaniesHouseClient

def test_basic_search():
    """Test basic company search functionality"""
    api_key = os.getenv("COMPANIES_HOUSE_API_KEY")
    if not api_key:
        print("❌ COMPANIES_HOUSE_API_KEY not found")
        return False
    
    # Initialize client
    client = CompaniesHouseClient(api_key)
    
    # Test basic search with simple query
    test_queries = ["construction", "property", "development", "british telecom"]
    
    for query in test_queries:
        print(f"\n🔍 Testing search for: '{query}'")
        try:
            results = client.search_companies(query, items_per_page=5)
            print(f"📊 Results: {len(results)} companies found")
            
            if results:
                print("✅ Sample results:")
                for i, company in enumerate(results[:2]):
                    print(f"  {i+1}. {company.get('title', 'N/A')} ({company.get('company_number', 'N/A')})")
                    print(f"     Status: {company.get('company_status', 'N/A')}")
                    print(f"     Type: {company.get('company_type', 'N/A')}")
            else:
                print("❌ No results found")
                
        except Exception as e:
            print(f"❌ Error: {str(e)}")
    
    return True

if __name__ == "__main__":
    print("🧪 Testing Companies House Search Fix")
    test_basic_search()