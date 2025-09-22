#!/usr/bin/env python3
"""Test keyVal resolution with different authorities to find working portals"""

from api_clients import LondonPlanningClient
import time

def test_keyval_resolution():
    print("🔍 TESTING KEYVAL RESOLUTION")
    print("=" * 50)
    
    client = LondonPlanningClient()
    
    # Test different authorities to find working portals
    test_cases = [
        ('Westminster', '23/01234/FULL'),
        ('Camden', '2023/0001/P'), 
        ('Hackney', '2023/0001/FUL'),
        ('Islington', '231234'),
        ('Tower Hamlets', '23/00001/FUL'),
        ('Southwark', '23-AP-0001'),
        ('Lambeth', '23/00001/FUL'),
        ('Wandsworth', '2023/0001'),
    ]
    
    for authority, reference in test_cases:
        print(f"\n🏛️ Testing: {authority} - {reference}")
        print("-" * 30)
        
        try:
            # Test keyVal resolution with shorter timeout
            result = client.resolve_keyval_planning_url(authority, reference, delay=0.2)
            
            print(f"URL: {result.get('url', 'N/A')}")
            print(f"Status: {result.get('status', 'N/A')}")
            print(f"Method: {result.get('method', 'N/A')}")
            
            # Check if this is a direct keyVal link
            url = result.get('url', '')
            if 'keyVal=' in url and 'applicationDetails.do' in url:
                print("✅ SUCCESS: Direct keyVal link generated!")
                return authority, reference, url
            elif result.get('status') == 'resolved':
                print("✅ Portal responsive but no keyVal found")
            else:
                print(f"❌ Failed: {result.get('status')}")
                
        except Exception as e:
            print(f"❌ Error: {str(e)}")
        
        # Small delay between attempts
        time.sleep(1)
    
    print("\n💡 SUMMARY:")
    print("All portals appear to be unresponsive or having issues.")
    print("KeyVal extraction requires working portal connections.")
    
    return None, None, None

if __name__ == "__main__":
    authority, reference, url = test_keyval_resolution()
    
    if url:
        print(f"\n🎯 WORKING KEYVAL EXAMPLE:")
        print(f"Authority: {authority}")
        print(f"Reference: {reference}")  
        print(f"URL: {url}")
    else:
        print(f"\n⚠️ No working portals found for keyVal extraction.")
        print("This is why you're seeing fallback URLs instead of direct keyVal links.")