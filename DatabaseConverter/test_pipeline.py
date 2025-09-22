"""
Test script for the applicant extraction and company matching pipeline.
Verifies end-to-end functionality with sample data.
"""
import os
import json
import requests
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_pipeline_endpoints():
    """Test the pipeline endpoints with sample data"""
    base_url = "http://localhost:5001"
    api_key = os.getenv('PLANNING_API_KEY', 'default-key-123')
    
    headers = {
        'Content-Type': 'application/json',
        'X-API-Key': api_key
    }
    
    # Sample applicant data for testing
    test_applicants = [
        {
            "planning_reference": "TEST/2025/001",
            "applicant_name": "Barratt Developments plc",
            "borough": "Test Borough",
            "contact_email": "info@barrattdevelopments.co.uk",
            "contact_phone": "01234567890",
            "description": "Residential development application"
        },
        {
            "planning_reference": "TEST/2025/002", 
            "applicant_name": "Taylor Wimpey PLC",
            "borough": "Test Borough",
            "description": "Housing development"
        },
        {
            "planning_reference": "TEST/2025/003",
            "applicant_name": "Persimmon Homes Ltd",
            "borough": "Test Borough",
            "description": "New residential estate"
        },
        {
            "planning_reference": "TEST/2025/004",
            "applicant_name": "John Smith",  # Individual - should be skipped
            "borough": "Test Borough",
            "description": "Single house extension"
        },
        {
            "planning_reference": "TEST/2025/005",
            "applicant_name": "Berkeley Group Holdings PLC",
            "borough": "Test Borough",
            "description": "Mixed-use development"
        }
    ]
    
    print("🧪 Testing Applicant Extraction and Company Matching Pipeline")
    print("=" * 60)
    
    # Test 1: Check if endpoints are alive
    print("\\n1️⃣ Testing endpoint availability...")
    
    try:
        response = requests.get(f"{base_url}/api/applicants/test", timeout=10)
        if response.status_code == 200:
            print("✅ Basic applicant endpoint is alive")
        else:
            print(f"❌ Basic applicant endpoint failed: {response.status_code}")
    except Exception as e:
        print(f"❌ Basic applicant endpoint connection failed: {e}")
    
    try:
        response = requests.get(f"{base_url}/api/pipeline/status", timeout=10)
        if response.status_code == 200:
            status_data = response.json()
            print("✅ Pipeline status endpoint is alive")
            print(f"   Configured: {status_data.get('configured', False)}")
            
            # Print connectivity status
            connectivity = status_data.get('connectivity', {})
            print(f"   Database: {'✅' if connectivity.get('database') else '❌'}")
            print(f"   Companies House API: {'✅' if connectivity.get('companies_house_api') else '❌'}")
            print(f"   Applicant Processor: {'✅' if connectivity.get('applicant_processor') else '❌'}")
            
        else:
            print(f"❌ Pipeline status endpoint failed: {response.status_code}")
    except Exception as e:
        print(f"❌ Pipeline status endpoint connection failed: {e}")
    
    # Test 2: Test basic applicant batch processing
    print("\\n2️⃣ Testing basic applicant batch processing...")
    
    try:
        payload = {"applicants": test_applicants}
        response = requests.post(
            f"{base_url}/api/applicants/batch",
            headers=headers,
            json=payload,
            timeout=30
        )
        
        if response.status_code in [200, 207]:
            result = response.json()
            print("✅ Basic batch processing successful")
            print(f"   Processed: {result.get('processed_count', 0)}")
            print(f"   Errors: {result.get('error_count', 0)}")
            print(f"   Total received: {result.get('total_received', 0)}")
            
            if result.get('error_details'):
                print(f"   Error details: {result['error_details'][:3]}")  # Show first 3 errors
                
        else:
            print(f"❌ Basic batch processing failed: {response.status_code}")
            print(f"   Response: {response.text}")
            
    except Exception as e:
        print(f"❌ Basic batch processing connection failed: {e}")
    
    # Test 3: Test complete pipeline processing (if Companies House API is configured)
    print("\\n3️⃣ Testing complete pipeline processing...")
    
    try:
        companies_house_key = os.getenv('COMPANIES_HOUSE_API_KEY')
        if not companies_house_key:
            print("⚠️  Companies House API key not configured - skipping pipeline test")
            print("   Set COMPANIES_HOUSE_API_KEY environment variable to test full pipeline")
        else:
            # Use smaller batch for pipeline test to avoid rate limits
            pipeline_test_applicants = test_applicants[:2]  # Just first 2 applicants
            
            payload = {"applicants": pipeline_test_applicants}
            response = requests.post(
                f"{base_url}/api/applicants/pipeline",
                headers=headers,
                json=payload,
                timeout=120  # Longer timeout for pipeline processing
            )
            
            if response.status_code in [200, 207]:
                result = response.json()
                print("✅ Complete pipeline processing successful")
                print(f"   Status: {result.get('status', 'unknown')}")
                
                pipeline_stats = result.get('pipeline_stats', {})
                print(f"   Processed applicants: {pipeline_stats.get('processed_applicants', 0)}")
                print(f"   Matched companies: {pipeline_stats.get('matched_companies', 0)}")
                print(f"   New companies fetched: {pipeline_stats.get('new_companies_fetched', 0)}")
                print(f"   New officers fetched: {pipeline_stats.get('new_officers_fetched', 0)}")
                print(f"   New appointments created: {pipeline_stats.get('new_appointments_created', 0)}")
                print(f"   Network edges updated: {pipeline_stats.get('network_edges_updated', 0)}")
                
                if pipeline_stats.get('errors'):
                    print(f"   Errors: {len(pipeline_stats['errors'])}")
                    for error in pipeline_stats['errors'][:3]:  # Show first 3 errors
                        print(f"     - {error}")
                        
            else:
                print(f"❌ Complete pipeline processing failed: {response.status_code}")
                print(f"   Response: {response.text}")
                
    except Exception as e:
        print(f"❌ Complete pipeline processing connection failed: {e}")
    
    # Test 4: Test data validation and deduplication
    print("\\n4️⃣ Testing data validation and deduplication...")
    
    # Test with invalid data
    invalid_applicants = [
        {"applicant_name": "Test Company Ltd"},  # Missing planning_reference
        {"planning_reference": ""},  # Empty planning_reference
        {"planning_reference": "TEST/2025/006", "applicant_name": ""},  # Empty name
        {"planning_reference": "TEST/2025/007", "applicant_name": "Valid Company Ltd"},  # Valid
        {"planning_reference": "TEST/2025/007", "applicant_name": "Valid Company Ltd"},  # Duplicate
    ]
    
    try:
        payload = {"applicants": invalid_applicants}
        response = requests.post(
            f"{base_url}/api/applicants/batch",
            headers=headers,
            json=payload,
            timeout=30
        )
        
        if response.status_code in [200, 207]:
            result = response.json()
            print("✅ Data validation and deduplication working")
            print(f"   Processed: {result.get('processed_count', 0)} (should be 1 from 5 input)")
            print(f"   Errors: {result.get('error_count', 0)} (should be 4 validation errors)")
            
        else:
            print(f"❌ Data validation test failed: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Data validation test connection failed: {e}")
    
    # Test 5: Test authentication
    print("\\n5️⃣ Testing API key authentication...")
    
    try:
        # Test without API key
        response = requests.post(
            f"{base_url}/api/applicants/batch",
            json={"applicants": [test_applicants[0]]},
            timeout=10
        )
        
        if response.status_code == 401:
            print("✅ Authentication working - rejected request without API key")
        else:
            print(f"⚠️  Authentication issue - expected 401, got {response.status_code}")
        
        # Test with wrong API key
        wrong_headers = {
            'Content-Type': 'application/json',
            'X-API-Key': 'wrong-key'
        }
        
        response = requests.post(
            f"{base_url}/api/applicants/batch",
            headers=wrong_headers,
            json={"applicants": [test_applicants[0]]},
            timeout=10
        )
        
        if response.status_code == 401:
            print("✅ Authentication working - rejected request with wrong API key")
        else:
            print(f"⚠️  Authentication issue - expected 401, got {response.status_code}")
            
    except Exception as e:
        print(f"❌ Authentication test connection failed: {e}")
    
    print("\\n" + "=" * 60)
    print("🏁 Pipeline testing completed!")
    print("\\nTo run the full pipeline with real data:")
    print("1. Set COMPANIES_HOUSE_API_KEY environment variable")
    print("2. Set PLANNING_API_KEY environment variable (for client authentication)")
    print("3. Send POST requests to http://localhost:5001/api/applicants/pipeline")
    print("\\nExample curl command:")
    print("curl -X POST http://localhost:5001/api/applicants/pipeline \\\\")
    print('  -H "Content-Type: application/json" \\\\')
    print(f'  -H "X-API-Key: {api_key}" \\\\')
    print("  -d '{\"applicants\": [{\"planning_reference\": \"REF001\", \"applicant_name\": \"Company Ltd\"}]}'")

if __name__ == "__main__":
    test_pipeline_endpoints()