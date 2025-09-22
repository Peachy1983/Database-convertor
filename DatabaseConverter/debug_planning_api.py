#!/usr/bin/env python3
"""
Debug script for LondonPlanningClient to identify why searches return 0 results
"""

import requests
import json
from datetime import datetime

def test_api_endpoint_and_auth():
    """Test 1: Verify API endpoint and authentication"""
    print("🔍 TEST 1: API Endpoint and Authentication")
    print("=" * 50)
    
    base_url = "https://planningdata.london.gov.uk/api-guest/applications/_search"
    headers = {
        'User-Agent': 'UK-Planning-Search/1.0',
        'X-API-AllowRequest': 'be2rmRnt&',
        'Content-Type': 'application/json'
    }
    
    # Test with minimal query
    minimal_query = {
        "query": {"match_all": {}},
        "size": 1,
        "_source": ["lpa_name", "lpa_app_no", "application_type"]
    }
    
    try:
        print(f"URL: {base_url}")
        print(f"Headers: {headers}")
        print(f"Query: {json.dumps(minimal_query, indent=2)}")
        
        response = requests.post(base_url, json=minimal_query, headers=headers, timeout=30)
        
        print(f"\nResponse Status: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            data = response.json()
            hits = data.get('hits', {}).get('hits', [])
            print(f"Total hits: {data.get('hits', {}).get('total', 'unknown')}")
            print(f"Returned hits: {len(hits)}")
            
            if hits:
                print(f"First record: {json.dumps(hits[0].get('_source', {}), indent=2)}")
                return True, data
            else:
                print("❌ No records returned even with match_all query!")
                return False, data
        else:
            print(f"❌ API Error: {response.text}")
            return False, None
            
    except Exception as e:
        print(f"❌ Request failed: {e}")
        return False, None

def test_field_names(sample_data):
    """Test 2: Verify Elasticsearch field names"""
    print("\n🔍 TEST 2: Field Names Analysis")
    print("=" * 50)
    
    if not sample_data or not sample_data.get('hits', {}).get('hits'):
        print("❌ No sample data available")
        return
    
    hits = sample_data['hits']['hits']
    
    # Analyze field structure
    all_fields = set()
    authority_fields = set()
    app_type_fields = set()
    date_fields = set()
    
    for hit in hits[:10]:  # Analyze first 10 records
        source = hit.get('_source', {})
        all_fields.update(source.keys())
        
        # Look for authority-related fields
        for field, value in source.items():
            if 'lpa' in field.lower() or 'authority' in field.lower():
                authority_fields.add(field)
            if 'type' in field.lower() or 'application' in field.lower():
                app_type_fields.add(field)
            if 'date' in field.lower():
                date_fields.add(field)
    
    print(f"All available fields: {sorted(all_fields)}")
    print(f"Authority-related fields: {sorted(authority_fields)}")
    print(f"Application type fields: {sorted(app_type_fields)}")
    print(f"Date-related fields: {sorted(date_fields)}")
    
    # Check specific field values
    print(f"\nSample field values:")
    first_record = hits[0].get('_source', {})
    for field in ['lpa_name', 'lpa_name.raw', 'application_type', 'application_type.raw', 'valid_date']:
        value = first_record.get(field)
        print(f"  {field}: {value}")

def test_authority_names(sample_data):
    """Test 3: Check authority name formats"""
    print("\n🔍 TEST 3: Authority Name Formats")
    print("=" * 50)
    
    if not sample_data or not sample_data.get('hits', {}).get('hits'):
        print("❌ No sample data available")
        return
    
    hits = sample_data['hits']['hits']
    
    # Collect unique authority names
    authority_names = set()
    for hit in hits[:100]:  # Check more records for authority patterns
        source = hit.get('_source', {})
        auth_name = source.get('lpa_name')
        if auth_name:
            authority_names.add(auth_name)
    
    print(f"Unique authority names found:")
    for auth in sorted(authority_names):
        print(f"  '{auth}'")
    
    # Check if Barnet exists and in what format
    barnet_variations = [name for name in authority_names if 'barnet' in name.lower()]
    print(f"\nBarnet variations: {barnet_variations}")

def test_simple_queries():
    """Test 4: Try progressively complex queries"""
    print("\n🔍 TEST 4: Simple Query Testing")
    print("=" * 50)
    
    base_url = "https://planningdata.london.gov.uk/api-guest/applications/_search"
    headers = {
        'User-Agent': 'UK-Planning-Search/1.0',
        'X-API-AllowRequest': 'be2rmRnt&',
        'Content-Type': 'application/json'
    }
    
    test_queries = [
        # Test 1: Match all (should return results)
        {
            "name": "Match All",
            "query": {"match_all": {}},
            "size": 5
        },
        
        # Test 2: Simple authority search (without .raw)
        {
            "name": "Authority Search (lpa_name)",
            "query": {"term": {"lpa_name": "Barnet"}},
            "size": 5
        },
        
        # Test 3: Authority search with .raw
        {
            "name": "Authority Search (lpa_name.raw)",
            "query": {"term": {"lpa_name.raw": "Barnet"}},
            "size": 5
        },
        
        # Test 4: Fuzzy authority search
        {
            "name": "Authority Fuzzy Search",
            "query": {"match": {"lpa_name": "Barnet"}},
            "size": 5
        },
        
        # Test 5: Application type search
        {
            "name": "Application Type Search",
            "query": {"term": {"application_type.raw": "Outline"}},
            "size": 5
        },
        
        # Test 6: Date range test (recent dates)
        {
            "name": "Recent Date Range",
            "query": {"range": {"valid_date": {"gte": "01/01/2024"}}},
            "size": 5
        }
    ]
    
    for test in test_queries:
        print(f"\n--- Testing: {test['name']} ---")
        query_body = {
            "query": test["query"],
            "size": test["size"],
            "_source": ["lpa_name", "lpa_app_no", "application_type", "valid_date"]
        }
        
        try:
            response = requests.post(base_url, json=query_body, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                hits = data.get('hits', {}).get('hits', [])
                total = data.get('hits', {}).get('total', 0)
                
                print(f"✅ Status: 200, Total: {total}, Returned: {len(hits)}")
                
                if hits:
                    for i, hit in enumerate(hits[:2]):  # Show first 2 results
                        source = hit.get('_source', {})
                        print(f"  Record {i+1}: {source}")
                else:
                    print("  ⚠️ No results returned")
            else:
                print(f"❌ Status: {response.status_code}, Error: {response.text[:200]}")
                
        except Exception as e:
            print(f"❌ Error: {e}")

def test_date_formats():
    """Test 5: Different date formats"""
    print("\n🔍 TEST 5: Date Format Testing")
    print("=" * 50)
    
    base_url = "https://planningdata.london.gov.uk/api-guest/applications/_search"
    headers = {
        'User-Agent': 'UK-Planning-Search/1.0',
        'X-API-AllowRequest': 'be2rmRnt&',
        'Content-Type': 'application/json'
    }
    
    # Test different date formats
    date_formats = [
        ("DD/MM/YYYY", "19/09/2024"),  # Current format
        ("YYYY-MM-DD", "2024-09-19"),  # ISO format
        ("MM/DD/YYYY", "09/19/2024"),  # US format
        ("DD-MM-YYYY", "19-09-2024"),  # Dash format
    ]
    
    for format_name, date_value in date_formats:
        print(f"\n--- Testing date format: {format_name} ({date_value}) ---")
        
        query_body = {
            "query": {"range": {"valid_date": {"gte": date_value}}},
            "size": 3,
            "_source": ["lpa_name", "lpa_app_no", "valid_date"]
        }
        
        try:
            response = requests.post(base_url, json=query_body, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                hits = data.get('hits', {}).get('hits', [])
                total = data.get('hits', {}).get('total', 0)
                
                print(f"✅ Status: 200, Total: {total}, Returned: {len(hits)}")
                
                if hits:
                    for hit in hits[:1]:  # Show first result
                        source = hit.get('_source', {})
                        print(f"  Sample: {source}")
            else:
                print(f"❌ Status: {response.status_code}")
                
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    print("🚀 London Planning API Debug Script")
    print("=" * 60)
    
    # Run all tests
    success, sample_data = test_api_endpoint_and_auth()
    
    if success and sample_data:
        test_field_names(sample_data)
        test_authority_names(sample_data)
    
    test_simple_queries()
    test_date_formats()
    
    print("\n🏁 Debug tests completed!")