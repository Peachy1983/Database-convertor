#!/usr/bin/env python3
"""
Investigate available application types in London Planning API
"""

import requests
import json
from collections import Counter

def get_application_types():
    """Get all unique application types from the API"""
    print("🔍 Investigating Available Application Types")
    print("=" * 50)
    
    base_url = "https://planningdata.london.gov.uk/api-guest/applications/_search"
    headers = {
        'User-Agent': 'UK-Planning-Search/1.0',
        'X-API-AllowRequest': 'be2rmRnt&',
        'Content-Type': 'application/json'
    }
    
    # Use aggregation to get all unique application types
    query_body = {
        "size": 0,  # Don't need the actual documents
        "aggs": {
            "application_types": {
                "terms": {
                    "field": "application_type",
                    "size": 100  # Get top 100 types
                }
            }
        }
    }
    
    try:
        print(f"Querying API for application type aggregation...")
        response = requests.post(base_url, json=query_body, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            aggs = data.get('aggregations', {})
            app_types = aggs.get('application_types', {}).get('buckets', [])
            
            print(f"Found {len(app_types)} unique application types:")
            print("\n📋 Application Types (with counts):")
            print("-" * 40)
            
            for bucket in app_types:
                app_type = bucket['key']
                count = bucket['doc_count']
                print(f"  '{app_type}': {count:,} applications")
            
            # Look for outline-related types
            print(f"\n🎯 Outline-related application types:")
            print("-" * 40)
            outline_types = [
                bucket for bucket in app_types 
                if 'outline' in bucket['key'].lower() or 'reserved' in bucket['key'].lower()
            ]
            
            if outline_types:
                for bucket in outline_types:
                    app_type = bucket['key']
                    count = bucket['doc_count']
                    print(f"  ✅ '{app_type}': {count:,} applications")
            else:
                print(f"  ❌ No types containing 'outline' or 'reserved' found")
                print(f"  💡 Outline applications might use different terminology")
            
            return [bucket['key'] for bucket in app_types]
            
        else:
            print(f"❌ API Error: {response.status_code} - {response.text}")
            return []
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return []

def search_for_outline_applications():
    """Search for applications that might be outline applications using different strategies"""
    print(f"\n🔍 Searching for Outline Applications")
    print("=" * 50)
    
    base_url = "https://planningdata.london.gov.uk/api-guest/applications/_search"
    headers = {
        'User-Agent': 'UK-Planning-Search/1.0',
        'X-API-AllowRequest': 'be2rmRnt&',
        'Content-Type': 'application/json'
    }
    
    # Strategy 1: Search descriptions for outline-related terms
    print(f"\n--- Strategy 1: Description search for 'outline' ---")
    query_body = {
        "query": {
            "bool": {
                "should": [
                    {"match": {"description": "outline"}},
                    {"match": {"development_description": "outline"}},
                    {"match": {"proposal_description": "outline"}},
                    {"match": {"work_description": "outline"}}
                ]
            }
        },
        "size": 10,
        "_source": ["lpa_name", "lpa_app_no", "application_type", "description", "development_description"]
    }
    
    try:
        response = requests.post(base_url, json=query_body, headers=headers, timeout=30)
        if response.status_code == 200:
            data = response.json()
            hits = data.get('hits', {}).get('hits', [])
            total = data.get('hits', {}).get('total', 0)
            
            print(f"Found {total} applications with 'outline' in description")
            
            if hits:
                app_types_found = set()
                for hit in hits[:5]:  # Show first 5
                    source = hit.get('_source', {})
                    app_type = source.get('application_type')
                    if app_type:
                        app_types_found.add(app_type)
                    print(f"  {source.get('lpa_app_no')}: {app_type} - {source.get('description', '')[:100]}...")
                
                print(f"\nApplication types for outline-described applications: {sorted(app_types_found)}")
            else:
                print(f"  ❌ No applications found with 'outline' in description")
        else:
            print(f"❌ Error: {response.status_code}")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Strategy 2: Search for applications with reference patterns like "OUT" or "/OUT"
    print(f"\n--- Strategy 2: Reference pattern search for outline indicators ---")
    query_body = {
        "query": {
            "bool": {
                "should": [
                    {"wildcard": {"lpa_app_no": "*OUT*"}},
                    {"wildcard": {"lpa_app_no": "*/OUT"}},
                    {"wildcard": {"lpa_app_no": "*OUT"}},
                ]
            }
        },
        "size": 10,
        "_source": ["lpa_name", "lpa_app_no", "application_type", "description"]
    }
    
    try:
        response = requests.post(base_url, json=query_body, headers=headers, timeout=30)
        if response.status_code == 200:
            data = response.json()
            hits = data.get('hits', {}).get('hits', [])
            total = data.get('hits', {}).get('total', 0)
            
            print(f"Found {total} applications with 'OUT' pattern in reference")
            
            if hits:
                app_types_found = set()
                for hit in hits[:5]:  # Show first 5
                    source = hit.get('_source', {})
                    app_type = source.get('application_type')
                    if app_type:
                        app_types_found.add(app_type)
                    print(f"  {source.get('lpa_app_no')}: {app_type}")
                
                print(f"\nApplication types for OUT-pattern references: {sorted(app_types_found)}")
            else:
                print(f"  ❌ No applications found with OUT pattern in reference")
        else:
            print(f"❌ Error: {response.status_code}")
    except Exception as e:
        print(f"❌ Error: {e}")

def test_fixed_api():
    """Test the API with the field name fixes"""
    print(f"\n🧪 Testing Fixed API Query")
    print("=" * 50)
    
    base_url = "https://planningdata.london.gov.uk/api-guest/applications/_search"
    headers = {
        'User-Agent': 'UK-Planning-Search/1.0',
        'X-API-AllowRequest': 'be2rmRnt&',
        'Content-Type': 'application/json'
    }
    
    # Test the exact query structure that was failing before (but with fixed field names)
    query_body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"lpa_name": "Barnet"}},  # Fixed: removed .raw
                    {"range": {"valid_date": {"gte": "19/09/2024"}}},
                    {"term": {"application_type": "Householder"}}  # Using known type instead of "Outline"
                ]
            }
        },
        "size": 5,
        "_source": ["lpa_name", "lpa_app_no", "application_type", "valid_date", "description"]
    }
    
    print(f"Testing query: {json.dumps(query_body, indent=2)}")
    
    try:
        response = requests.post(base_url, json=query_body, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            hits = data.get('hits', {}).get('hits', [])
            total = data.get('hits', {}).get('total', 0)
            
            print(f"✅ SUCCESS! Total: {total}, Returned: {len(hits)}")
            
            if hits:
                for i, hit in enumerate(hits):
                    source = hit.get('_source', {})
                    print(f"  Record {i+1}: {source}")
            else:
                print(f"  ⚠️ No results returned")
        else:
            print(f"❌ Error: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    print("🚀 London Planning API Application Type Investigation")
    print("=" * 60)
    
    # Get all application types
    app_types = get_application_types()
    
    # Search for outline applications
    search_for_outline_applications()
    
    # Test the fixed API
    test_fixed_api()
    
    print("\n🏁 Investigation completed!")