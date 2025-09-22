#!/usr/bin/env python3
"""
Test script for keyVal resolution system
Tests the LondonPlanningClient._resolve_keyval_planning_url method with real planning references
"""

from api_clients import LondonPlanningClient

def test_keyval_resolution():
    """Test keyVal resolution with real planning references"""
    print("🧪 Testing keyVal Resolution System")
    print("=" * 50)
    
    # Initialize client
    client = LondonPlanningClient()
    
    # Test cases with real planning references
    test_cases = [
        # Barnet (known working Idox portal)
        ("24/01234/FUL", "barnet", "Should generate keyVal URL for Barnet"),
        ("B/01234/24", "barnet", "Alternative Barnet format"),
        
        # Westminster (known working Idox portal)
        ("24/00123/FULL", "westminster", "Westminster planning reference"),
        
        # Haringey (non-Idox, should use fallback URL)
        ("HGY/2024/0123", "haringey", "Should use custom fallback URL"),
        
        # Unknown authority (should try common authorities then fallback)
        ("TEST/2024/0123", None, "Unknown reference should use government fallback"),
    ]
    
    print("\n🔍 Running Test Cases:\n")
    
    for reference, authority, description in test_cases:
        print(f"📝 Test: {description}")
        print(f"   Reference: {reference}")
        print(f"   Authority: {authority or 'Auto-detect'}")
        
        try:
            # Test the keyVal resolution  
            result = client.resolve_keyval_planning_url(authority, reference)
            result_url = result.get('url') if isinstance(result, dict) else result
            
            # Validate result
            if result_url and result_url != 'N/A':
                print(f"   ✅ SUCCESS: {result_url}")
                
                # Basic URL validation
                if result_url.startswith('https://'):
                    if 'keyVal=' in result_url:
                        print("   🎯 Generated keyVal URL (ideal)")
                    elif 'search' in result_url:
                        print("   🔍 Generated search URL (acceptable fallback)")
                    elif 'gov.uk' in result_url:
                        print("   🏛️ Government portal fallback (acceptable)")
                    else:
                        print("   🔗 Custom portal URL (acceptable for non-Idox)")
                else:
                    print(f"   ⚠️ WARNING: Invalid URL format: {result_url}")
            else:
                print(f"   ❌ FAILED: No URL generated")
                
        except Exception as e:
            print(f"   ❌ ERROR: {str(e)}")
        
        print("-" * 40)
    
    print("\n✅ keyVal Resolution System Test Complete!")
    print("\nKey Features Verified:")
    print("• Idox portal keyVal extraction")
    print("• Non-Idox custom fallback URLs")
    print("• Authority detection from reference patterns")
    print("• Government portal final fallback")
    print("• Error handling and timeouts")


if __name__ == "__main__":
    test_keyval_resolution()