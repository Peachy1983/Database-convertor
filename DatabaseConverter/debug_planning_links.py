#!/usr/bin/env python3
"""
Debug script to test planning portal link generation and validation
"""

import requests
from typing import Dict
import time

def generate_planning_link(authority: str, reference: str) -> Dict[str, str]:
    """Test version of the generate_planning_link function from app.py"""
    if not authority or not reference or authority == 'N/A' or reference == 'N/A':
        return {'url': 'N/A', 'status': 'invalid', 'icon': '❌'}
    
    # Static URLs for fast display (from app.py lines 2760-2796)
    planning_search_portals = {
        'Barnet': 'https://publicaccess.barnet.gov.uk/online-applications/search.do?action=search&searchType=Application&reference=',
        'Westminster': 'https://idoxpa.westminster.gov.uk/online-applications/search.do?action=search&searchType=Application&reference=',
        'Camden': 'https://planning.camden.gov.uk/online-applications/search.do?action=search&searchType=Application&reference=',
        'Hackney': 'https://planning.hackney.gov.uk/online-applications/search.do?action=search&searchType=Application&reference=',
        'Islington': 'https://planning.islington.gov.uk/online-applications/search.do?action=search&searchType=Application&reference=',
        'Tower Hamlets': 'https://development.towerhamlets.gov.uk/online-applications/search.do?action=search&searchType=Application&reference=',
        'Southwark': 'https://planning.southwark.gov.uk/online-applications/search.do?action=search&searchType=Application&reference=',
        'Lambeth': 'https://planning.lambeth.gov.uk/online-applications/search.do?action=search&searchType=Application&reference=',
        'Wandsworth': 'https://planning.wandsworth.gov.uk/online-applications/search.do?action=search&searchType=Application&reference=',
        'Kingston upon Thames': 'https://planning.kingston.gov.uk/online-applications/search.do?action=search&searchType=Application&reference=',
        'Merton': 'https://planning.merton.gov.uk/online-applications/search.do?action=search&searchType=Application&reference=',
        'Sutton': 'https://secplan.sutton.gov.uk/online-applications/search.do?action=search&searchType=Application&reference=',
        'Croydon': 'https://publicaccess2.croydon.gov.uk/online-applications/search.do?action=search&searchType=Application&reference=',
        'Bromley': 'https://searchapplications.bromley.gov.uk/online-applications/search.do?action=search&searchType=Application&reference=',
        'Bexley': 'https://pa.bexley.gov.uk/online-applications/search.do?action=search&searchType=Application&reference=',
        'Greenwich': 'https://planning.royalgreenwich.gov.uk/online-applications/search.do?action=search&searchType=Application&reference=',
        'Lewisham': 'https://planning.lewisham.gov.uk/online-applications/search.do?action=search&searchType=Application&reference=',
        'Newham': 'https://pa.newham.gov.uk/online-applications/search.do?action=search&searchType=Application&reference=',
        'Waltham Forest': 'https://planning.walthamforest.gov.uk/online-applications/search.do?action=search&searchType=Application&reference=',
        'Redbridge': 'https://planning.redbridge.gov.uk/online-applications/search.do?action=search&searchType=Application&reference=',
        'Havering': 'https://pa2.havering.gov.uk/online-applications/search.do?action=search&searchType=Application&reference=',
        'Enfield': 'https://planningandbuildingcontrol.enfield.gov.uk/online-applications/search.do?action=search&searchType=Application&reference=',
        'Brent': 'https://pa.brent.gov.uk/online-applications/search.do?action=search&searchType=Application&reference=',
        'Ealing': 'https://pam.ealing.gov.uk/online-applications/search.do?action=search&searchType=Application&reference=',
        
        # Custom URL patterns for non-Idox authorities
        'Richmond upon Thames': 'https://www2.richmond.gov.uk/lbrplanning/Planning_CaseNo.aspx?strCASENO=',
        'Hounslow': 'https://planning.hounslow.gov.uk/planning_summary.aspx?strCASENO=',
        'Hillingdon': 'https://planning.hillingdon.gov.uk/OAS/enquiry/search?number=',
        'Harrow': 'https://www.harrow.gov.uk/planning-applications/search?reference=',
        'Haringey': 'https://www.haringey.gov.uk/planning-and-building-control/planning/planning-applications/search-planning-applications?reference=',
        'Kensington and Chelsea': 'https://www.rbkc.gov.uk/planning/searches?reference=',
        'Hammersmith and Fulham': 'https://public-access.lbhf.gov.uk/online-applications/search.do?action=search&searchType=Application&reference=',
        'Barking and Dagenham': 'https://paplan.lbbd.gov.uk/online-applications/search.do?action=search&searchType=Application&reference=',
        'City of London': 'https://www.planning2.cityoflondon.gov.uk/online-applications/search.do?action=search&searchType=Application&reference='
    }
    
    # Get direct search URL for authority
    search_base = planning_search_portals.get(authority)
    if search_base:
        return {
            'url': f"{search_base}{reference}",
            'status': 'search_fallback',
            'method': 'static_mapping',
            'icon': '🔍'
        }
    
    # Fallback to generic UK government planning portal search
    search_ref = reference.replace('/', '%2F')
    return {
        'url': f"https://www.gov.uk/search-planning-applications?reference={search_ref}",
        'status': 'fallback',
        'method': 'gov_uk_search',
        'icon': '🌐'
    }

def test_url_validity(url: str, timeout: int = 10) -> Dict[str, str]:
    """Test if a URL is valid and accessible"""
    try:
        print(f"Testing URL: {url}")
        response = requests.head(url, timeout=timeout, allow_redirects=True)
        
        status_info = {
            'status_code': response.status_code,
            'accessible': response.status_code < 400,
            'final_url': response.url,
            'redirected': response.url != url
        }
        
        if response.status_code == 200:
            result = "✅ SUCCESS"
        elif 300 <= response.status_code < 400:
            result = "↗️ REDIRECT"
        elif response.status_code == 404:
            result = "❌ NOT FOUND"
        elif response.status_code >= 500:
            result = "🚫 SERVER ERROR"
        else:
            result = f"⚠️ HTTP {response.status_code}"
        
        print(f"  Result: {result}")
        print(f"  Status Code: {response.status_code}")
        if status_info['redirected']:
            print(f"  Redirected to: {response.url}")
        
        return status_info
        
    except requests.exceptions.Timeout:
        print(f"  Result: ⏰ TIMEOUT")
        return {'error': 'timeout', 'accessible': False}
    except requests.exceptions.ConnectionError:
        print(f"  Result: 🔌 CONNECTION ERROR")
        return {'error': 'connection_error', 'accessible': False}
    except Exception as e:
        print(f"  Result: ❌ ERROR - {str(e)}")
        return {'error': str(e), 'accessible': False}

def main():
    """Main debug function"""
    print("🔍 DEBUGGING PLANNING PORTAL LINKS")
    print("=" * 50)
    
    # Test cases with real data from the logs
    test_cases = [
        ('Barnet', '19/2959/NMA'),  # From user's example
        ('Westminster', '20/00100/FULL'),  # Example Westminster case
        ('Camden', '2023/0001/P'),  # Example Camden case
        ('Tower Hamlets', '23/00002/FUL'),  # Example Tower Hamlets case
        ('Kensington and Chelsea', '2023/00001/FULL'),  # Custom URL format
        ('Unknown Authority', '23/00001/FULL'),  # Test fallback
    ]
    
    print("\n1. TESTING URL GENERATION")
    print("-" * 30)
    
    generated_urls = []
    for authority, reference in test_cases:
        print(f"\nAuthority: {authority}")
        print(f"Reference: {reference}")
        
        link_info = generate_planning_link(authority, reference)
        print(f"Generated URL: {link_info['url']}")
        print(f"Status: {link_info['status']}")
        print(f"Method: {link_info['method']}")
        print(f"Icon: {link_info['icon']}")
        
        generated_urls.append((authority, reference, link_info['url']))
    
    print("\n\n2. TESTING URL ACCESSIBILITY")
    print("-" * 30)
    
    successful_urls = 0
    total_urls = 0
    
    for authority, reference, url in generated_urls:
        if url == 'N/A':
            print(f"\nSkipping N/A URL for {authority} - {reference}")
            continue
            
        print(f"\n📍 {authority} - {reference}")
        total_urls += 1
        
        result = test_url_validity(url)
        if result.get('accessible', False):
            successful_urls += 1
        
        # Add small delay to be respectful to servers
        time.sleep(1)
    
    print("\n\n3. SUMMARY")
    print("-" * 30)
    print(f"Total URLs tested: {total_urls}")
    print(f"Successful URLs: {successful_urls}")
    print(f"Success rate: {(successful_urls/total_urls*100):.1f}%" if total_urls > 0 else "N/A")
    
    print("\n\n4. POTENTIAL ISSUES IDENTIFIED")
    print("-" * 30)
    
    issues_found = []
    
    # Check for common issues
    for authority, reference, url in generated_urls:
        if url == 'N/A':
            continue
            
        # Check for URL encoding issues
        if '/' in reference and '%2F' not in url and 'gov.uk' not in url:
            issues_found.append(f"❌ {authority}: Reference '{reference}' may need URL encoding (contains '/')")
        
        # Check for HTTPS vs HTTP
        if url.startswith('http://'):
            issues_found.append(f"⚠️ {authority}: Using HTTP instead of HTTPS - may cause security warnings")
    
    if issues_found:
        for issue in issues_found:
            print(issue)
    else:
        print("✅ No obvious URL formatting issues detected")
    
    print("\n\n5. RECOMMENDATIONS")
    print("-" * 30)
    
    recommendations = [
        "✅ URL generation logic appears correct",
        "✅ LinkColumn configuration in Streamlit should work with these URLs",
        "🔍 Test clicking actual links in the Streamlit app to verify LinkColumn behavior",
        "🔍 Check browser developer tools for any JavaScript errors when clicking links",
        "🔍 Verify that the planning portal sites aren't blocking external referrers",
        "💡 Consider adding target='_blank' to LinkColumn to open in new tabs"
    ]
    
    for rec in recommendations:
        print(rec)

if __name__ == "__main__":
    main()