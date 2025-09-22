import requests
import time
import os
import threading
from typing import Dict, List, Optional, Any

class ResolverClient:
    """Client for batch resolution using resolver service"""
    
    def __init__(self):
        self.resolver_url = os.getenv("RESOLVER_URL")
        self.resolver_token = os.getenv("RESOLVER_TOKEN")
        
        # Councils that work with the current resolver (IDOX-based)
        self.idox_ok = {
            "Barnet", "Brent", "Enfield", "Haringey", "Westminster", "Camden", "Islington", "Hackney",
            "Harrow", "Ealing", "Hounslow", "KensingtonAndChelsea", "Southwark", "Lambeth", "Lewisham",
            "Croydon", "Merton", "Hammersmith and Fulham", "Hillingdon", "Havering", "Newham",
            "Redbridge", "Greenwich", "Barking and Dagenham", "Bexley", "Kingston upon Thames",
            "Sutton", "Tower Hamlets", "Bromley"  # Added Bromley for outline applications
        }
    
    def resolve_batch_items(self, rows):
        """Resolve a batch of items using the resolver service
        
        Args:
            rows: List of dicts with {"ref": "...", "borough": "...", "app_index": ...} structure
        
        Returns:
            List of resolved URLs in same order as input, or empty list if no supported boroughs
        """
        # Only keep rows where borough is in IDOX_OK
        items = [r for r in rows if r["borough"] in self.idox_ok]
        
        if not items:
            print(f"No supported boroughs found in {len(rows)} items")
            return []  # nothing to resolve
        
        # Group by borough since API expects one borough per request
        borough_groups = {}
        for item in items:
            borough = item["borough"]
            if borough not in borough_groups:
                borough_groups[borough] = []
            borough_groups[borough].append(item)
        
        # Ensure URL has proper scheme
        url = self.resolver_url
        if not url.startswith(('http://', 'https://')):
            url = f"https://{url}"
        
        # Results in same order as input
        all_results = [{"url": "N/A"} for _ in items]
        
        # Process each borough separately  
        for borough, borough_items in borough_groups.items():
            try:
                refs = [item["ref"] for item in borough_items]
                timeout = max(180, 20 + 35 * len(refs))
                
                r = requests.post(
                    f"{url}/resolve-batch",
                    headers={
                        "Authorization": f"Bearer {self.resolver_token}",
                        "Content-Type": "application/json"
                    },
                    json={"borough": borough, "refs": refs},
                    timeout=timeout
                )
                r.raise_for_status()
                data = r.json()
                borough_results = data.get("results", data) if isinstance(data, dict) else data
                if borough_results is None:
                    borough_results = []
                
                print(f"Resolver response type: {type(data).__name__}, len={len(borough_results)}")
                
                # Map results back to original positions
                for i, result in enumerate(borough_results):
                    if i < len(borough_items):
                        # Find the position in the original items list
                        original_item = borough_items[i]
                        original_index = next(j for j, item in enumerate(items) if item is original_item)
                        all_results[original_index] = result
                        
                print(f"Successfully resolved {len(borough_results)} URLs from {len(refs)} items in {borough}")
                
            except Exception as e:
                print(f"❌ Failed to resolve {len(refs)} items from {borough}: {e}")
                # Results already initialized as N/A, so no need to change anything
        
        return all_results

class CompaniesHouseClient:
    """Client for interacting with Companies House API with global rate limiting"""
    
    # No rate limiting - back to original working configuration
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.company-information.service.gov.uk"
        self.session = requests.Session()
        self.session.auth = (api_key, '')
        self.session.headers.update({
            'User-Agent': 'UK-Company-Enrichment-App/1.0'
        })
    
    # Removed artificial rate limiting - back to original working design
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None, retry_count: int = 0) -> Optional[Dict]:
        """Make a globally rate-limited request to Companies House API"""
        if not self.api_key:
            print("❌ Companies House API key not configured")
            return None
        
        max_retries = 2
        
        # Optimized rate limiting: 0.5 second delay = 2 requests per second = 600 requests per 5 minutes (full limit)
        time.sleep(0.5)
        
        try:
            url = f"{self.base_url}{endpoint}"
            response = self.session.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                print("❌ Invalid Companies House API key")
                return None
            elif response.status_code == 404:
                return None
            elif response.status_code == 429:
                if retry_count < max_retries:
                    # Respect Retry-After header if present, otherwise short delay
                    retry_after = response.headers.get('Retry-After')
                    if retry_after and retry_after.isdigit():
                        delay = min(int(retry_after), 10)  # Cap at 10 seconds
                    else:
                        delay = 3 + (retry_count * 2)  # Progressive: 3s, 5s
                    
                    print(f"⏳ Rate limited. Retrying in {delay}s...")
                    time.sleep(delay)
                    return self._make_request(endpoint, params, retry_count + 1)
                else:
                    print(f"❌ Rate limit exceeded. Skipping this request.")
                    return None
            else:
                print(f"❌ API request failed with status {response.status_code}")
                return None
        
        except requests.RequestException as e:
            if retry_count < max_retries:
                delay = 2 + retry_count
                print(f"⏳ Connection error. Retrying in {delay}s: {str(e)[:50]}...")
                time.sleep(delay)
                return self._make_request(endpoint, params, retry_count + 1)
            else:
                print(f"❌ Connection failed: {str(e)[:50]}...")
                return None
    
    def search_companies(self, query: str, items_per_page: int = 20) -> List[Dict]:
        """Search for companies by name"""
        endpoint = "/search/companies"
        params = {
            'q': query,
            'items_per_page': items_per_page
        }
        
        response = self._make_request(endpoint, params)
        if response and 'items' in response:
            return response['items']
        return []
    
    def get_company_details(self, company_number: str) -> Optional[Dict]:
        """Get detailed information about a company"""
        endpoint = f"/company/{company_number}"
        return self._make_request(endpoint)
    
    def get_company_officers(self, company_number: str) -> List[Dict]:
        """Get company officers"""
        endpoint = f"/company/{company_number}/officers"
        response = self._make_request(endpoint)
        if response and 'items' in response:
            return response['items']
        return []
    
    def get_company_filing_history(self, company_number: str, items_per_page: int = 20) -> List[Dict]:
        """Get company filing history"""
        endpoint = f"/company/{company_number}/filing-history"
        params = {'items_per_page': items_per_page}
        response = self._make_request(endpoint, params)
        if response and 'items' in response:
            return response['items']
        return []
    
    def get_company_charges(self, company_number: str, items_per_page: int = 25) -> List[Dict]:
        """Get company charges"""
        endpoint = f"/company/{company_number}/charges"
        params = {'items_per_page': items_per_page}
        response = self._make_request(endpoint, params)
        if response and 'items' in response:
            return response['items']
        return []
    
    def search_companies_by_sic(self, sic_code: str, items_per_page: int = 20) -> List[Dict]:
        """Search for companies by SIC code using advanced search"""
        endpoint = "/advanced-search/companies"
        params = {
            'sic_codes': sic_code,
            'size': str(items_per_page)
        }
        
        response = self._make_request(endpoint, params)
        if response and 'items' in response:
            return response['items']
        return []
    
    def search_companies_by_status(self, status: str, items_per_page: int = 20) -> List[Dict]:
        """Search for companies by status using advanced search"""
        endpoint = "/advanced-search/companies"
        params = {
            'company_status': status,
            'size': str(items_per_page)
        }
        
        response = self._make_request(endpoint, params)
        if response and 'items' in response:
            return response['items']
        return []
    
    def search_companies_by_incorporation_date(self, date_from, date_to, location_filter: str = "", items_per_page: int = 20, sic_codes: List[str] = None, max_results: int = 5000) -> List[Dict]:
        """Search for companies by incorporation date range with full pagination support"""
        endpoint = "/advanced-search/companies"
        
        # Format dates for API (YYYY-MM-DD format required)
        date_from_str = date_from.strftime('%Y-%m-%d')
        date_to_str = date_to.strftime('%Y-%m-%d')
        
        all_companies = []
        start_index = 0
        page_size = min(items_per_page, 100)  # Max 100 per page as per API docs
        
        while len(all_companies) < max_results:
            # Use CORRECT Companies House API parameter names from official docs
            params = {
                'incorporated_from': date_from_str,
                'incorporated_to': date_to_str,
                'size': str(page_size),
                'start_index': str(start_index)
            }
            
            if location_filter:
                params['location'] = location_filter
            
            # Add SIC code filtering directly in API call for efficiency
            if sic_codes:
                params['sic_codes'] = ','.join(sic_codes)
            
            response = self._make_request(endpoint, params)
            if not response or 'items' not in response:
                break
                
            page_items = response['items']
            if not page_items:
                break
                
            all_companies.extend(page_items)
            
            # Check if we got fewer items than requested (end of results)
            if len(page_items) < page_size:
                break
                
            start_index += page_size
            
            # Rate limiting between pages
            time.sleep(0.5)
            
            print(f"📄 Fetched page {(start_index // page_size)}, total companies: {len(all_companies)}")
        
        return all_companies[:max_results]
    
    def check_health(self) -> Dict[str, Any]:
        """Check Companies House API client health status"""
        health_status = {
            'healthy': False,
            'api_key_configured': bool(self.api_key and self.api_key.strip()),
            'api_accessible': False,
            'error_message': None
        }
        
        try:
            if not self.api_key or not self.api_key.strip():
                health_status['error_message'] = "COMPANIES_HOUSE_API_KEY not configured"
                return health_status
            
            # Test API access with a simple search
            response = self._make_request("/search/companies", {'q': 'test', 'items_per_page': 1})
            if response is not None:
                health_status['api_accessible'] = True
                health_status['healthy'] = True
            else:
                health_status['error_message'] = "API request failed or returned None"
                
        except Exception as e:
            health_status['error_message'] = str(e)
            
        return health_status

    def search_companies_combined(self, sic_code: str, status: str, date_from, location_filter: str = "", max_results: int = 300) -> List[Dict]:
        """Search for companies using combined criteria: SIC code, status, and incorporation date with pagination using advanced search"""
        endpoint = "/advanced-search/companies"
        
        # Build parameters combining criteria
        params = {}
        
        if sic_code and sic_code.strip():
            params['sic_codes'] = sic_code.strip()
        
        if status and status != "all":
            params['company_status'] = status
        
        if date_from:
            date_from_str = date_from.strftime('%Y-%m-%d')
            params['incorporated_from'] = date_from_str
        
        if location_filter:
            params['location'] = location_filter
        
        # No search criteria provided
        if not params:
            return []
        
        all_results = []
        
        # FIXED: Prevent hanging with smaller page sizes and limits
        page_size = min(100, max_results)  # Much smaller page size to prevent hanging
        start_index = 0
        max_iterations = 5  # Limit to 5 API calls maximum to prevent infinite loops
        iteration_count = 0
        
        while len(all_results) < max_results and iteration_count < max_iterations:
            iteration_count += 1
            
            # Calculate how many more items we need
            remaining = max_results - len(all_results)
            current_page_size = min(page_size, remaining)
            
            # Set pagination parameters
            current_params = params.copy()
            current_params['size'] = str(current_page_size)
            current_params['start_index'] = str(start_index)
            
            try:
                response = self._make_request(endpoint, current_params)
                if response and 'items' in response:
                    items = response['items']
                    
                    # If no items returned, we've reached the end
                    if not items:
                        break
                    
                    # Add items to results
                    all_results.extend(items)
                    
                    # If we got fewer items than requested, we've reached the end
                    if len(items) < current_page_size:
                        break
                    
                    # Update start_index for next page
                    start_index += len(items)
                    
                else:
                    # API request failed, break the loop
                    break
                    
            except Exception as e:
                print(f"Error in search iteration {iteration_count}: {e}")
                break
        
        # Trim to exact number requested
        final_results = all_results[:max_results]
        
        return final_results
    
    def get_companies_batch(self, company_numbers: List[str]) -> Dict[str, Optional[Dict]]:
        """Get details for multiple companies in batch with rate limiting"""
        results = {}
        
        for company_number in company_numbers:
            if not company_number:
                results[company_number] = None
                continue
            
            try:
                company_data = self.get_company_details(company_number)
                results[company_number] = company_data
                
                # Log successful fetch
                if company_data:
                    print(f"✅ Fetched company: {company_number}")
                else:
                    print(f"❌ Company not found: {company_number}")
                    
            except Exception as e:
                print(f"❌ Error fetching company {company_number}: {e}")
                results[company_number] = None
        
        return results
    
    def get_officers_batch(self, company_numbers: List[str]) -> Dict[str, List[Dict]]:
        """Get officers for multiple companies in batch with rate limiting"""
        results = {}
        
        for company_number in company_numbers:
            if not company_number:
                results[company_number] = []
                continue
            
            try:
                officers = self.get_company_officers(company_number)
                results[company_number] = officers
                
                # Log successful fetch
                print(f"✅ Fetched {len(officers)} officers for company: {company_number}")
                    
            except Exception as e:
                print(f"❌ Error fetching officers for {company_number}: {e}")
                results[company_number] = []
        
        return results


class ClearbitClient:
    """Client for Clearbit API"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://company.clearbit.com/v2"
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {api_key}'
        })
    
    def enrich_company(self, company_data: Dict) -> Optional[Dict]:
        """Enrich company data using Clearbit"""
        if not self.api_key:
            return None
        
        try:
            domain = self._extract_domain_from_company(company_data)
            if not domain:
                return None
            
            endpoint = f"{self.base_url}/companies/find"
            params = {"domain": domain}
            
            response = self.session.get(endpoint, params=params)
            
            if response.status_code == 200:
                data = response.json()
                return self._format_clearbit_response(data)
            
            return None
        
        except Exception as e:
            print(f"⚠️ Clearbit enrichment failed: {str(e)}")
            return None
    
    def _extract_domain_from_company(self, company_data: Dict) -> Optional[str]:
        """Extract or guess domain from company data"""
        company_name = company_data.get('company_name', '').lower()
        if not company_name:
            return None
        
        # Simple domain guessing
        domain_guess = company_name.replace(' ', '').replace('ltd', '').replace('limited', '').replace('plc', '')
        domain_guess = ''.join(c for c in domain_guess if c.isalnum())
        return f"{domain_guess}.com"
    
    def _format_clearbit_response(self, data: Dict) -> Dict:
        """Format Clearbit response to standardized format"""
        return {
            'name': data.get('name'),
            'domain': data.get('domain'),
            'industry': data.get('category', {}).get('industry'),
            'employee_count': data.get('metrics', {}).get('employees'),
            'annual_revenue': data.get('metrics', {}).get('annualRevenue'),
            'description': data.get('description'),
            'founded_year': data.get('foundedYear'),
            'location': {
                'city': data.get('geo', {}).get('city'),
                'state': data.get('geo', {}).get('state'),
                'country': data.get('geo', {}).get('country')
            },
            'technologies': data.get('tech', []),
            'social_profiles': {
                'linkedin': data.get('linkedin', {}).get('handle'),
                'twitter': data.get('twitter', {}).get('handle'),
                'facebook': data.get('facebook', {}).get('handle')
            }
        }




class LondonPlanningClient:
    """London Planning Data API client for planning applications across all 35 London boroughs
    
    Uses the official London planning data hub API with direct Elasticsearch integration
    """
    
    def __init__(self):
        self.base_url = "https://planningdata.london.gov.uk/api-guest/applications/_search"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'UK-Planning-Search/1.0',
            'X-API-AllowRequest': 'be2rmRnt&',
            'Content-Type': 'application/json'
        })
        
        # Cache for Reference → URL mapping to avoid repeated requests
        self.keyval_cache = {}
        
        # Available London boroughs
        self.london_boroughs = [
            'Westminster', 'Camden', 'Islington', 'Hackney', 'Tower Hamlets', 
            'Greenwich', 'Lewisham', 'Southwark', 'Lambeth', 'Wandsworth',
            'Hammersmith and Fulham', 'Kensington and Chelsea', 'Brent', 'Ealing',
            'Hounslow', 'Richmond upon Thames', 'Kingston upon Thames', 'Merton',
            'Sutton', 'Croydon', 'Bromley', 'Lewisham', 'Bexley', 'Havering',
            'Barking and Dagenham', 'Redbridge', 'Newham', 'Waltham Forest',
            'Haringey', 'Enfield', 'Barnet', 'Harrow', 'Hillingdon'
        ]
        
        # Web scraping headers for keyVal resolution
        self.scrape_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/126.0.0.0 Safari/537.36"
        }
        
        # Portal mappings for keyVal resolution - all 33 London boroughs
        self.idox_portals = {
            # London Boroughs - Idox Planning Portals (support keyVal resolution)
            'barnet': 'https://publicaccess.barnet.gov.uk/online-applications',
            'westminster': 'https://idoxpa.westminster.gov.uk/online-applications', 
            'camden': 'https://planning.camden.gov.uk/online-applications',
            'hackney': 'https://planning.hackney.gov.uk/online-applications',
            'islington': 'https://planning.islington.gov.uk/online-applications',
            'tower_hamlets': 'https://development.towerhamlets.gov.uk/online-applications',
            'southwark': 'https://planning.southwark.gov.uk/online-applications',
            'lambeth': 'https://planning.lambeth.gov.uk/online-applications',
            'wandsworth': 'https://planning.wandsworth.gov.uk/online-applications',
            'kingston_upon_thames': 'https://planning.kingston.gov.uk/online-applications',
            'merton': 'https://planning.merton.gov.uk/online-applications',
            'sutton': 'https://secplan.sutton.gov.uk/online-applications',
            'croydon': 'https://publicaccess2.croydon.gov.uk/online-applications',
            'bromley': 'https://searchapplications.bromley.gov.uk/online-applications',
            'bexley': 'https://pa.bexley.gov.uk/online-applications',
            'greenwich': 'https://planning.royalgreenwich.gov.uk/online-applications',
            'lewisham': 'https://planning.lewisham.gov.uk/online-applications',
            'newham': 'https://pa.newham.gov.uk/online-applications',
            'waltham_forest': 'https://planning.walthamforest.gov.uk/online-applications',
            'redbridge': 'https://planning.redbridge.gov.uk/online-applications',
            'havering': 'https://pa2.havering.gov.uk/online-applications',
            'enfield': 'https://planningandbuildingcontrol.enfield.gov.uk/online-applications',
            'brent': 'https://pa.brent.gov.uk/online-applications',
            'ealing': 'https://pam.ealing.gov.uk/online-applications',
            'harrow': 'https://planning.harrow.gov.uk/online-applications',
            'hillingdon': 'https://planning.hillingdon.gov.uk/online-applications',
            'haringey': 'https://www.planningservices.haringey.gov.uk/online-applications',
            'hammersmith_and_fulham': 'https://public-access.lbhf.gov.uk/online-applications',
            'barking_and_dagenham': 'https://paplan.lbbd.gov.uk/online-applications',
            'city_of_london': 'https://www.planning2.cityoflondon.gov.uk/online-applications'
        }
        
        print("💾 Reference → URL caching enabled")
    
    def clear_url_cache(self):
        """Clear the cached URLs to force fresh resolution"""
        self.keyval_cache.clear()
        print("🧹 URL cache cleared")
        
        # Web scraping headers for keyVal resolution
        self.scrape_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/126.0.0.0 Safari/537.36"
        }
        
        # Portal mappings for keyVal resolution - all 33 London boroughs
        self.idox_portals = {
            # London Boroughs - Idox Planning Portals (support keyVal resolution)
            'barnet': 'https://publicaccess.barnet.gov.uk/online-applications',
            'westminster': 'https://idoxpa.westminster.gov.uk/online-applications', 
            'camden': 'https://planning.camden.gov.uk/online-applications',
            'hackney': 'https://planning.hackney.gov.uk/online-applications',
            'islington': 'https://planning.islington.gov.uk/online-applications',
            'tower_hamlets': 'https://development.towerhamlets.gov.uk/online-applications',
            'southwark': 'https://planning.southwark.gov.uk/online-applications',
            'lambeth': 'https://planning.lambeth.gov.uk/online-applications',
            'wandsworth': 'https://planning.wandsworth.gov.uk/online-applications',
            'kingston_upon_thames': 'https://planning.kingston.gov.uk/online-applications',
            'merton': 'https://planning.merton.gov.uk/online-applications',
            'sutton': 'https://secplan.sutton.gov.uk/online-applications',
            'croydon': 'https://publicaccess2.croydon.gov.uk/online-applications',
            'bromley': 'https://searchapplications.bromley.gov.uk/online-applications',
            'bexley': 'https://pa.bexley.gov.uk/online-applications',
            'greenwich': 'https://planning.royalgreenwich.gov.uk/online-applications',
            'lewisham': 'https://planning.lewisham.gov.uk/online-applications',
            'newham': 'https://pa.newham.gov.uk/online-applications',
            'waltham_forest': 'https://planning.walthamforest.gov.uk/online-applications',
            'redbridge': 'https://planning.redbridge.gov.uk/online-applications',
            'havering': 'https://pa2.havering.gov.uk/online-applications',
            'enfield': 'https://planningandbuildingcontrol.enfield.gov.uk/online-applications',
            'brent': 'https://pa.brent.gov.uk/online-applications',
            'ealing': 'https://pam.ealing.gov.uk/online-applications',
            'harrow': 'https://planning.harrow.gov.uk/online-applications',
            'hillingdon': 'https://planning.hillingdon.gov.uk/online-applications',
            'haringey': 'https://www.planningservices.haringey.gov.uk/online-applications',
            'hammersmith_and_fulham': 'https://public-access.lbhf.gov.uk/online-applications',
            'barking_and_dagenham': 'https://paplan.lbbd.gov.uk/online-applications',
            'city_of_london': 'https://www.planning2.cityoflondon.gov.uk/online-applications'
        }
        
        # Non-Idox portals - custom URL patterns
        self.custom_portals = {
            'richmond_upon_thames': {
                'base': 'https://www2.richmond.gov.uk/lbrplanning',
                'search_pattern': '/Planning_CaseNo.aspx?strCASENO='
            },
            'hounslow': {
                'base': 'https://planning.hounslow.gov.uk',
                'search_pattern': '/planning_summary.aspx?strCASENO='
            },
            'kensington_and_chelsea': {
                'base': 'https://www.rbkc.gov.uk/planning',
                'search_pattern': '/searches?reference='
            }
        }
        
        # Contact parsing tabs to try for enhanced applicant data
        self.contact_tabs = ["contacts", "people", "neighbourComments"]
    
    def search_planning_applications(self, local_authority: str = None, 
                                   application_type: str = None, 
                                   start_date: str = None, 
                                   limit: int = 50,
                                   decision_status: str = None,
                                   offset: int = 0,
                                   enable_large_search: bool = False,
                                   enable_outline_filter: bool = False) -> List[Dict]:
        """Search London planning applications with filtering
        
        Args:
            local_authority: London borough name (e.g., 'Westminster', 'Camden')
            application_type: Type of planning application 
            start_date: Start date in YYYY-MM-DD format
            limit: Maximum number of results (default: 50)
            decision_status: Decision status filter (e.g., 'Approved', 'Refused', 'Pending')
            offset: Pagination offset (default: 0)
            enable_large_search: Enable smart pagination for >10K results
            enable_outline_filter: Apply server-side outline filtering to eliminate sampling bias
        
        Returns:
            List of planning applications from London API
        """
        print(f"🏙️ SEARCHING LONDON PLANNING APPLICATIONS...")
        print(f"🏛️ Authority: {local_authority or 'All London boroughs'}")
        print(f"📅 From date: {start_date or 'Any date'}")
        print(f"📋 Type: {application_type or 'All types'}")
        
        # Check if outline filtering is enabled
        if enable_outline_filter:
            print(f"🎯 ✅ Server-side outline filtering ENABLED - eliminating sampling bias")
        
        # Build Elasticsearch query according to API documentation
        query = {"bool": {"must": []}}
        
        # Add local authority filter if specified
        if local_authority and local_authority.strip():
            print(f"🏛️ ✅ Adding authority filter: {local_authority}")
            # Use exact term matching with .raw field for precise matching
            query["bool"]["must"].append({
                "term": {"lpa_name.raw": local_authority}
            })
        
        # Add date filter if specified
        if start_date and start_date.strip():
            print(f"📅 ✅ Adding date filter from: {start_date}")
            # Convert to DD/MM/YYYY format as shown in API documentation
            try:
                from datetime import datetime
                date_obj = datetime.strptime(start_date, '%Y-%m-%d')
                formatted_date = date_obj.strftime('%d/%m/%Y')
                query["bool"]["must"].append({
                    "range": {"valid_date": {"gte": formatted_date}}
                })
            except ValueError:
                print(f"⚠️ Invalid date format: {start_date}, skipping date filter")
        
        # Add application type filter if specified  
        if application_type and application_type.strip():
            print(f"📋 ✅ Adding application type filter: {application_type}")
            # Use exact term matching with .raw field for precise matching
            query["bool"]["must"].append({
                "term": {"application_type.raw": application_type}
            })
        
        # Add server-side outline filter if enabled
        if enable_outline_filter:
            from utils import create_outline_elasticsearch_query
            outline_query = create_outline_elasticsearch_query()
            query["bool"]["must"].append(outline_query)
            print(f"🎯 ✅ Added server-side outline filter to Elasticsearch query")
        
        # Add decision status filter if specified
        if decision_status and decision_status.strip() and decision_status != "All Statuses":
            print(f"⚖️ ✅ Adding decision status filter: {decision_status}")
            # Use exact term matching for decision status with .raw field for precise matching
            query["bool"]["must"].append({
                "term": {"decision.raw": decision_status}
            })
        
        # If no filters specified, add a broad match_all query
        if not query["bool"]["must"]:
            query = {"match_all": {}}
            print("🌐 No filters specified - searching all London applications")
        
        # Build request body according to API documentation
        request_body = {
            "query": query,
            "size": limit,
            "from": offset,  # Add offset for pagination
            "_source": [
                "lpa_name", "lpa_app_no", "last_updated", "valid_date", 
                "decision_date", "decision", "decision_status", "status", "id", "application_type", "description", 
                "development_description", "proposal_description", "work_description",
                "applicant", "applicant_name", "organisation", "name"  # Add applicant fields
            ]
        }
        
        # Handle large searches by implementing smart pagination strategies
        if enable_large_search and limit > 10000:
            return self._search_large_dataset(query, limit, start_date, local_authority, application_type, decision_status, enable_outline_filter)
        
        try:
            print(f"🔄 Making API request to London Planning API...")
            print(f"🔍 DEBUG: Request body = {request_body}")
            
            # Try the request with retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = self.session.post(
                        self.base_url,
                        json=request_body,
                        timeout=90  # Increased timeout to 90 seconds
                    )
                    break  # Success, exit retry loop
                except Exception as e:
                    print(f"❌ Attempt {attempt + 1} failed: {str(e)}")
                    if attempt < max_retries - 1:
                        print(f"⏳ Retrying in 2 seconds...")
                        import time
                        time.sleep(2)
                    else:
                        raise  # Re-raise the exception if all retries failed
            
            print(f"📡 API Response Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                hits = data.get('hits', {}).get('hits', [])
                
                print(f"✅ Successfully retrieved {len(hits)} planning applications")
                
                # Debug: Print first record to see actual structure
                if hits:
                    print(f"🔍 DEBUG: First record structure = {hits[0].get('_source', {})}")
                    
                    # Debug: Show unique application types in this result set
                    app_types = set()
                    for hit in hits[:20]:  # Check first 20 records
                        source = hit.get('_source', {})
                        app_type = source.get('application_type')
                        if app_type:
                            app_types.add(app_type)
                    print(f"🔍 DEBUG: Application types found = {sorted(list(app_types))}")
                
                # Format results to standardized format
                applications = []
                outline_detected_count = 0
                detection_methods = []
                
                for hit in hits:
                    source = hit.get('_source', {})
                    # Try multiple possible description fields
                    description = (source.get('description') or 
                                 source.get('development_description') or 
                                 source.get('proposal_description') or 
                                 source.get('work_description') or 
                                 'No description available')
                    
                    # Extract applicant information from multiple possible fields
                    applicant = (source.get('applicant') or 
                               source.get('applicant_name') or 
                               source.get('organisation') or 
                               source.get('name') or 
                               'Not specified')
                    
                    app = {
                        'reference': source.get('lpa_app_no'),
                        'authority': source.get('lpa_name'),
                        'application_type': source.get('application_type'),
                        'description': description,
                        'applicant': applicant,  # Add applicant field
                        'valid_date': source.get('valid_date'),
                        'decision_date': source.get('decision_date'),
                        'decision': source.get('decision'),  # Specific decision outcome (Approved, Refused, etc.)
                        'status': source.get('status'),      # Status field
                        'last_updated': source.get('last_updated'),
                        'id': source.get('id')
                    }
                    
                    # If outline filtering was enabled, log detection details
                    if enable_outline_filter:
                        from utils import is_outline
                        if is_outline(app):
                            outline_detected_count += 1
                            # Determine what triggered the detection
                            ref = str(app.get('reference', '')).upper()
                            app_type = str(app.get('application_type', '')).lower()
                            desc = description.lower()
                            
                            method = []
                            if 'outline' in app_type or 'reserved' in app_type:
                                method.append("app_type")
                            if ref.endswith('OUT') or '/OUT' in ref:
                                method.append("reference_pattern")
                            if any(keyword in desc for keyword in ['outline', 'reserved']):
                                method.append("description_keywords")
                                
                            detection_methods.append(f"{app.get('reference', 'N/A')}: {'+'.join(method) if method else 'fallback'}")
                    
                    applications.append(app)
                
                # Enhanced logging for outline searches
                if enable_outline_filter:
                    print(f"🎯 OUTLINE DETECTION SUMMARY:")
                    print(f"   📊 Total applications retrieved: {len(applications)}")
                    print(f"   ✅ Outline applications detected: {outline_detected_count}")
                    if outline_detected_count > 0:
                        print(f"   🔍 Detection methods used:")
                        for method_detail in detection_methods[:10]:  # Show first 10
                            print(f"      • {method_detail}")
                        if len(detection_methods) > 10:
                            print(f"      • ... and {len(detection_methods) - 10} more")
                    else:
                        print(f"   ⚠️  NO OUTLINE APPLICATIONS found in this search")
                        print(f"   💡 Consider:")
                        print(f"      • Expanding date range (try 2020-2025)")
                        print(f"      • Searching different boroughs")
                        print(f"      • Checking if outline applications use different terminology")
                
                # 🔗 RESOLVER INTEGRATION: Add planning URLs for outline applications
                if enable_outline_filter and applications:
                    print(f"🔗 Resolving planning portal URLs for {len(applications)} outline applications...")
                    
                    # Initialize resolver
                    try:
                        resolver = ResolverClient()
                        
                        # Group applications by borough for separate resolver calls
                        # (resolver fails with mixed-borough batches)
                        from collections import defaultdict
                        borough_groups = defaultdict(list)
                        app_to_index = {}
                        
                        for i, app in enumerate(applications):
                            if app.get('reference') and app.get('authority'):
                                borough = app['authority']
                                borough_groups[borough].append({
                                    'ref': app['reference'], 
                                    'borough': borough,
                                    'app_index': i
                                })
                                app_to_index[f"{borough}_{app['reference']}"] = i
                        
                        if borough_groups:
                            print(f"📋 Grouped {len(applications)} applications into {len(borough_groups)} borough batches")
                            
                            # Initialize all applications with N/A first
                            for app in applications:
                                app['planning_url'] = 'N/A'
                            
                            # Process each borough separately
                            total_resolved = 0
                            for borough, items in borough_groups.items():
                                if len(items) > 0:
                                    print(f"🏛️ Resolving {len(items)} applications from {borough}...")
                                    
                                    # Call resolver service for this borough only
                                    borough_resolved_urls = resolver.resolve_batch_items(items)
                                    
                                    if borough_resolved_urls:
                                        # Process results for this borough
                                        for j, result in enumerate(borough_resolved_urls):
                                            if j < len(items) and result:
                                                item = items[j]
                                                app_index = item['app_index']
                                                planning_url = result.get('url')
                                                
                                                if planning_url and planning_url != 'N/A' and planning_url is not None:
                                                    # Fix HTML encoding issue: &amp; -> &
                                                    planning_url = planning_url.replace('&amp;', '&')
                                                    applications[app_index]['planning_url'] = planning_url
                                                    total_resolved += 1
                                                    print(f"  ✅ {item['ref']}: {planning_url}")
                                                else:
                                                    print(f"  ❌ {item['ref']}: N/A (resolver returned null)")
                                    else:
                                        print(f"  ⚠️ No resolver results for {borough}")
                            
                            print(f"🎯 RESOLVER SUMMARY: {total_resolved} URLs resolved across all boroughs")
                        else:
                            print(f"⚠️  No valid items to resolve")
                            for app in applications:
                                app['planning_url'] = 'N/A'
                                
                    except Exception as e:
                        # Mask the URL to avoid exposing secret endpoints
                        error_msg = str(e)
                        if "https://" in error_msg and "ngrok" in error_msg:
                            print(f"❌ Resolver service connection error (endpoint unavailable)")
                        else:
                            print(f"❌ Resolver error: {error_msg}")
                        # Set all URLs to N/A on resolver failure
                        for app in applications:
                            app['planning_url'] = 'N/A'
                
                return applications
                
            else:
                print(f"❌ API Error {response.status_code}: {response.text[:200]}")
                return []
                
        except Exception as e:
            print(f"❌ Exception occurred: {str(e)}")
            return []
    
    def _search_large_dataset(self, query, target_limit, start_date=None, local_authority=None, application_type=None, decision_status=None, enable_outline_filter=False):
        """Handle large dataset searches using multiple pagination strategies"""
        print(f"🚀 LARGE SEARCH MODE: Target {target_limit:,} results")
        print(f"📊 Using smart pagination to bypass 10K API limit")
        
        all_results = []
        seen_ids = set()
        
        # Strategy 1: Authority-based parallel searches (if no specific authority)
        if local_authority is None or local_authority == "All London Boroughs":
            print(f"📍 STRATEGY 1: Authority-based parallel searches")
            authorities_to_search = self.london_boroughs[:10]  # Start with top 10 boroughs
            
            for authority in authorities_to_search:
                print(f"   Searching {authority}...")
                authority_results = self.search_planning_applications(
                    local_authority=authority,
                    application_type=application_type,
                    start_date=start_date,
                    limit=10000,  # Max per authority
                    decision_status=decision_status,
                    enable_large_search=False,  # Avoid recursion
                    enable_outline_filter=enable_outline_filter
                )
                
                # Add unique results
                for result in authority_results:
                    result_id = result.get('id')
                    if result_id and result_id not in seen_ids:
                        seen_ids.add(result_id)
                        all_results.append(result)
                        
                        # Stop if we reach target
                        if len(all_results) >= target_limit:
                            print(f"🎯 Reached target: {len(all_results):,} results")
                            return all_results[:target_limit]
                
                print(f"   {authority}: {len(authority_results)} results (total: {len(all_results)})")
        
        # Strategy 2: Date-based chunking (if we still need more results)
        if len(all_results) < target_limit and start_date:
            print(f"📅 STRATEGY 2: Date-based chunking (need {target_limit - len(all_results):,} more)")
            
            from datetime import datetime, timedelta
            import calendar
            
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                current_dt = start_dt
                end_dt = datetime.now()
                
                while current_dt < end_dt and len(all_results) < target_limit:
                    # Get month chunk
                    month_start = current_dt.strftime('%Y-%m-%d')
                    last_day = calendar.monthrange(current_dt.year, current_dt.month)[1]
                    month_end = current_dt.replace(day=last_day)
                    
                    print(f"   Searching month: {current_dt.strftime('%Y-%m')}")
                    
                    # Use offset pagination within this month
                    offset = 0
                    while offset < 50000 and len(all_results) < target_limit:  # Max 5 pages per month
                        chunk_results = self.search_planning_applications(
                            local_authority=local_authority,
                            application_type=application_type,
                            start_date=month_start,
                            limit=min(10000, target_limit - len(all_results)),
                            decision_status=decision_status,
                            offset=offset,
                            enable_large_search=False,  # Avoid recursion
                            enable_outline_filter=enable_outline_filter
                        )
                        
                        if not chunk_results:
                            break  # No more results for this month
                        
                        # Add unique results
                        new_results = 0
                        for result in chunk_results:
                            result_id = result.get('id')
                            if result_id and result_id not in seen_ids:
                                seen_ids.add(result_id)
                                all_results.append(result)
                                new_results += 1
                        
                        print(f"     Offset {offset}: {new_results} new results (total: {len(all_results)})")
                        
                        if new_results == 0:
                            break  # No new results, move to next month
                        
                        offset += 10000
                    
                    # Move to next month
                    if current_dt.month == 12:
                        current_dt = current_dt.replace(year=current_dt.year + 1, month=1, day=1)
                    else:
                        current_dt = current_dt.replace(month=current_dt.month + 1, day=1)
                        
            except ValueError:
                print(f"⚠️ Date chunking failed, using simple offset pagination")
        
        # Strategy 3: Simple offset pagination (fallback)
        if len(all_results) < target_limit:
            print(f"🔢 STRATEGY 3: Simple offset pagination (need {target_limit - len(all_results):,} more)")
            
            offset = 0
            max_iterations = 20  # Prevent infinite loops
            
            for i in range(max_iterations):
                if len(all_results) >= target_limit:
                    break
                    
                print(f"   Offset {offset}: requesting batch...")
                batch_results = self.search_planning_applications(
                    local_authority=local_authority,
                    application_type=application_type,
                    start_date=start_date,
                    limit=10000,
                    decision_status=decision_status,
                    offset=offset,
                    enable_large_search=False,  # Avoid recursion
                    enable_outline_filter=enable_outline_filter
                )
                
                if not batch_results:
                    print(f"   No more results at offset {offset}")
                    break
                
                # Add unique results
                new_results = 0
                for result in batch_results:
                    result_id = result.get('id')
                    if result_id and result_id not in seen_ids:
                        seen_ids.add(result_id)
                        all_results.append(result)
                        new_results += 1
                
                print(f"   Added {new_results} new results (total: {len(all_results)})")
                
                if new_results == 0:
                    print(f"   No new unique results, stopping pagination")
                    break
                
                offset += 10000
        
        print(f"🎉 LARGE SEARCH COMPLETE: {len(all_results):,} total results")
        return all_results[:target_limit]

    def get_london_boroughs(self) -> List[str]:
        """Get list of available London boroughs"""
        return self.london_boroughs.copy()
    
    # ========== KEYVAL RESOLUTION SYSTEM (ADAPTED FROM BARNET SCRIPT) ==========
    
    def _normalise_whitespace(self, text: str) -> str:
        """Normalize whitespace in text"""
        import re
        return re.sub(r"\s+", " ", text).strip()
    
    def _absolutise_url(self, base: str, href: str) -> str:
        """Convert relative URL to absolute URL"""
        if href.startswith("http"):
            return href
        if not href.startswith("/"):
            href = "/" + href
        return base.rstrip("/") + href
    
    def _pick_first_app_details_link(self, html: str) -> Optional[str]:
        """Find first applicationDetails.do link in HTML"""
        try:
            from bs4 import BeautifulSoup
            import re
            soup = BeautifulSoup(html, "html.parser")
            a = soup.find("a", href=re.compile(r"applicationDetails\.do"))
            if a and a.get("href"):
                return a["href"]
        except:
            pass
        return None
    
    def _try_direct_reference(self, ref: str, base_url: str) -> Optional[str]:
        """Try direct reference URL with Bright Data proxy"""
        import requests
        import re
        url = f"{base_url}/applicationDetails.do?reference={ref}"
        try:
            # Direct request to planning portal
            session = requests.Session()
            session.headers.update(self.scrape_headers)
            print(f"🔍 Direct request: {url}")
            
            r = session.get(url, allow_redirects=True, timeout=15)
            if r.status_code == 200 and "applicationDetails" in r.url:
                return r.url
            # Fallback: simple content check for ref text
            if r.status_code == 200 and ref.replace(" ", "").lower() in re.sub(r"\s+", "", r.text).lower():
                return r.url
        except Exception as e:
            # Better error logging
            print(f"❌ Direct reference method failed for {ref}: {str(e)}")
            pass
        return None
    
    def _try_search_get(self, ref: str, base_url: str) -> Optional[str]:
        """Try GET search method with Bright Data proxy"""
        import requests
        search_url = f"{base_url}/search.do?action=search&searchType=Application&reference={ref}"
        try:
            # Direct search request to planning portal
            session = requests.Session()
            session.headers.update(self.scrape_headers)
            print(f"🔍 Direct search GET: {search_url}")
            
            r = session.get(search_url, allow_redirects=True, timeout=15)
            if r.status_code != 200:
                print(f"❌ Search GET failed for {ref}: HTTP {r.status_code}")
                return None
            link = self._pick_first_app_details_link(r.text)
            if link:
                resolved_url = self._absolutise_url(base_url, link)
                print(f"✅ Search GET resolved {ref} → {resolved_url}")
                return resolved_url
        except Exception as e:
            print(f"❌ Search GET method failed for {ref}: {str(e)}")
            pass
        return None
    
    def _try_search_post(self, ref: str, base_url: str) -> Optional[str]:
        """Try POST search method with Bright Data proxy"""
        import requests
        try:
            # Initialize session with proper headers
            session = requests.Session()
            session.headers.update(self.scrape_headers)
            print(f"🔍 Direct search POST: {ref}")
            
            # Get advanced search page first to establish session and grab any CSRF tokens
            adv_url = f"{base_url}/search.do?action=advanced"
            init_response = session.get(adv_url, timeout=15)
            if init_response.status_code != 200:
                print(f"❌ POST search init failed for {ref}: HTTP {init_response.status_code}")
                return None
            
            data = {
                "searchType": "Application",
                "searchCriteria.reference": ref,
                "date(applicationValidatedStart)": "",
                "date(applicationValidatedEnd)": "",
                "caseAddressType": "Application",
            }
            
            r = session.post(f"{base_url}/doSearch.do", data=data, allow_redirects=True, timeout=5)
            if r.status_code != 200:
                print(f"❌ POST search failed for {ref}: HTTP {r.status_code}")
                return None
            
            link = self._pick_first_app_details_link(r.text)
            if link:
                resolved_url = self._absolutise_url(base_url, link)
                print(f"✅ Search POST resolved {ref} → {resolved_url}")
                return resolved_url
        except Exception as e:
            print(f"❌ Search POST method failed for {ref}: {str(e)}")
            pass
        return None
    
    def _extract_keyval_from_url(self, url: str) -> Optional[str]:
        """Extract keyVal parameter from URL"""
        import re
        m = re.search(r"[?&]keyVal=([A-Za-z0-9]+)", url)
        return m.group(1) if m else None
    
    def _ensure_summary_url(self, url: str) -> str:
        """Force activeTab=summary for stability"""
        import re
        if "activeTab=" in url:
            url = re.sub(r"activeTab=[^&]+", "activeTab=summary", url)
        elif "?" in url:
            url = url + "&activeTab=summary"
        else:
            url = url + "?activeTab=summary"
        return url
    
    def _normalize_authority_name(self, authority: str) -> str:
        """Normalize authority name for portal mapping"""
        if not authority:
            return ""
        
        normalized = authority.lower().replace(' ', '_').replace('-', '_')
        
        # Handle special cases
        authority_mappings = {
            'tower_hamlets': 'tower_hamlets',
            'kingston_upon_thames': 'kingston_upon_thames',
            'richmond_upon_thames': 'richmond_upon_thames',
            'hammersmith_and_fulham': 'hammersmith_and_fulham',
            'kensington_and_chelsea': 'kensington_and_chelsea',
            'barking_and_dagenham': 'barking_and_dagenham',
            'waltham_forest': 'waltham_forest',
            'city_of_london': 'city_of_london'
        }
        
        return authority_mappings.get(normalized, normalized)
    
    # ========== CONTACT SCRAPING SYSTEM (ADAPTED FROM BARNET SCRIPT) ==========
    
    def _build_contact_urls(self, application_url: str) -> List[str]:
        """Build candidate URLs for contact/people pages"""
        candidates = []
        import re
        
        if "activeTab=" in application_url:
            for tab in self.contact_tabs:
                candidates.append(re.sub(r"activeTab=[^&]+", f"activeTab={tab}", application_url))
        else:
            sep = "&" if "?" in application_url else "?"
            for tab in self.contact_tabs:
                candidates.append(f"{application_url}{sep}activeTab={tab}")
        
        return list(dict.fromkeys(candidates))  # Remove duplicates while preserving order
    
    def _fetch_html(self, url: str, timeout: int = 15) -> Optional[str]:
        """Fetch HTML content from URL using proxy system"""
        try:
            # Use proxy-enabled request method
            response = self._make_request_with_proxy(url, method='GET', max_retries=3)
            if response and response.status_code == 200 and response.text:
                return response.text
        except Exception as e:
            print(f"❌ Failed to fetch HTML from {url}: {str(e)}")
        return None
    
    def _parse_contacts_html(self, html: str) -> Dict[str, str]:
        """Parse contact information from planning application HTML"""
        try:
            from bs4 import BeautifulSoup
            import re
            
            soup = BeautifulSoup(html, "html.parser")
            data = {}
            
            # Look for section headers that indicate applicant/agent sections
            section_labels = {"applicant": ["applicant"], "agent": ["agent"]}
            
            for header_tag in soup.select("h1,h2,h3,h4,strong"):
                label = self._normalise_whitespace(header_tag.get_text(" ")).lower()
                
                for key, needles in section_labels.items():
                    if any(n in label for n in needles):
                        container = None
                        
                        # Find the content container after this header
                        for sib in header_tag.find_all_next():
                            if sib.name in ["h1", "h2", "h3", "h4", "strong"]:
                                break
                            if sib.name in ["ul", "ol", "dl", "table", "div"] and sib.get_text(strip=True):
                                container = sib
                                break
                        
                        if container:
                            text = self._normalise_whitespace(container.get_text(" "))
                            data[f"{key}_block"] = text
            
            # Fallback: look for contact cards
            if not data:
                cards = soup.select(".contact, .contactDetails, .simpleList")
                for c in cards:
                    t = self._normalise_whitespace(c.get_text(" "))
                    if "applicant" in t.lower():
                        data["applicant_block"] = t
                    if "agent" in t.lower():
                        data["agent_block"] = t
            
            # Extract structured fields from blocks
            result = {}
            if "applicant_block" in data:
                applicant_fields = self._extract_contact_fields(data["applicant_block"])
                result.update({f"applicant_{k}": v for k, v in applicant_fields.items()})
            
            if "agent_block" in data:
                agent_fields = self._extract_contact_fields(data["agent_block"])
                result.update({f"agent_{k}": v for k, v in agent_fields.items()})
            
            return result
            
        except Exception as e:
            return {}
    
    def _extract_contact_fields(self, block: str) -> Dict[str, str]:
        """Extract structured contact fields from text block"""
        import re
        
        out = {}
        
        # Extract key-value pairs using regex
        pairs = re.findall(r"([A-Za-z ]{3,30}):\s*([^:]+?)(?=(?:[A-Za-z ]{3,30}:)|$)", block)
        for k, v in pairs:
            key = self._normalise_whitespace(k).lower().replace(" ", "_")
            out[key] = self._normalise_whitespace(v)
        
        # Extract specific fields if not found in key-value pairs
        if "name" not in out:
            m = re.search(r"(?:name|contact)\s*:\s*([^:]+)", block, flags=re.I)
            if m:
                out["name"] = self._normalise_whitespace(m.group(1))
        
        if "company" not in out:
            m = re.search(r"(?:company|organisation)\s*:\s*([^:]+)", block, flags=re.I)
            if m:
                out["company"] = self._normalise_whitespace(m.group(1))
        
        if "address" not in out:
            m = re.search(r"(address)\s*:\s*([^:]+)", block, flags=re.I)
            if m:
                out["address"] = self._normalise_whitespace(m.group(2))
        
        if "telephone" not in out:
            m = re.search(r"(?:tel(?:ephone)?|phone)\s*:\s*([\d +()-]{7,})", block, flags=re.I)
            if m:
                out["telephone"] = self._normalise_whitespace(m.group(1))
        
        if "email" not in out:
            m = re.search(r"([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})", block, flags=re.I)
            if m:
                out["email"] = self._normalise_whitespace(m.group(1))
        
        return out
    
    def scrape_enhanced_applicant_data(self, authority: str, reference: str) -> Dict[str, str]:
        """Scrape enhanced applicant data from planning portal"""
        if not authority or not reference:
            return {}
        
        try:
            # First resolve the planning application URL
            url_result = self.resolve_keyval_planning_url(authority, reference, delay=0.3)
            
            if url_result['status'] != 'resolved':
                # Can't scrape if we don't have a direct application URL
                return {}
            
            application_url = url_result['url']
            
            # Try different contact page URLs
            contact_urls = self._build_contact_urls(application_url)
            
            for candidate_url in contact_urls:
                html = self._fetch_html(candidate_url)
                if html:
                    parsed_contacts = self._parse_contacts_html(html)
                    if parsed_contacts:
                        # Successfully found contact information
                        return {
                            'source_url': candidate_url,
                            'scrape_status': 'success',
                            **parsed_contacts
                        }
            
            # No contact info found
            return {'scrape_status': 'no_contacts_found'}
            
        except Exception as e:
            return {'scrape_status': 'error', 'error': str(e)}
    
    def resolve_keyval_planning_url(self, authority: str, reference: str, delay: float = 2.0) -> Dict[str, str]:
        """Resolve keyVal-based planning application URL with caching and slow requests
        
        Returns:
            Dict with 'url', 'status', and 'method' keys
        """
        if not reference or reference == 'N/A' or not authority:
            return {'url': 'N/A', 'status': 'invalid_input', 'method': 'none'}
        
        # Check cache first - avoid repeated requests
        cache_key = f"{authority}_{reference}"
        if cache_key in self.keyval_cache:
            print(f"💾 Using cached result for {reference}")
            return self.keyval_cache[cache_key]
        
        # Normalize authority name
        normalized_authority = self._normalize_authority_name(authority)
        
        # Check if this authority has an Idox portal (supports keyVal resolution)
        if normalized_authority in self.idox_portals:
            base_url = self.idox_portals[normalized_authority]
            result = self._resolve_idox_portal(reference, base_url, delay)
            # Cache the result
            self.keyval_cache[cache_key] = result
            return result
        
        # Check if this authority has a custom portal
        elif normalized_authority in self.custom_portals:
            custom_info = self.custom_portals[normalized_authority]
            url = custom_info['base'] + custom_info['search_pattern'] + reference
            return {'url': url, 'status': 'custom_portal', 'method': 'direct_url'}
        
        # No fallback for unknown authorities - only keyVal links
        else:
            return {'url': 'N/A', 'status': 'keyval_failed', 'method': 'none'}
    
    def _resolve_idox_portal(self, reference: str, base_url: str, delay: float = 0.5) -> Dict[str, str]:
        """Resolve keyVal for Idox-based portals using multiple strategies"""
        import time
        
        try:
            # Strategy A: Try direct reference
            url = self._try_direct_reference(reference, base_url)
            if url:
                return {'url': self._ensure_summary_url(url), 'status': 'resolved', 'method': 'direct_reference'}
            
            time.sleep(delay)
            
            # Strategy B: Try GET search
            url = self._try_search_get(reference, base_url)
            if url:
                return {'url': self._ensure_summary_url(url), 'status': 'resolved', 'method': 'search_get'}
            
            time.sleep(delay)
            
            # Strategy C: Try POST search
            url = self._try_search_post(reference, base_url)
            if url:
                return {'url': self._ensure_summary_url(url), 'status': 'resolved', 'method': 'search_post'}
            
            # No fallback - return failure if keyVal extraction fails
            return {'url': 'N/A', 'status': 'keyval_failed', 'method': 'none'}
            
        except Exception as e:
            # No fallback - return failure on any error
            return {'url': 'N/A', 'status': 'keyval_failed', 'method': 'none', 'error': str(e)}
    
    def _resolve_keyval_planning_url_old(self, reference: str, organisation_entity: str = None) -> str:
        """Old method - kept for backward compatibility"""
        if not reference or reference == 'N/A':
            return 'N/A'
        
        import requests
        from bs4 import BeautifulSoup
        import re
        
        # Authority-specific planning portal mappings
        # Idox-based portals support keyVal extraction, others get custom fallbacks
        idox_portals = {
            # London Boroughs - Idox Planning Portals
            'barnet': 'https://publicaccess.barnet.gov.uk/online-applications/',
            'westminster': 'https://idoxpa.westminster.gov.uk/online-applications/',
            'camden': 'https://planning.camden.gov.uk/online-applications/',
            'hackney': 'https://planning.hackney.gov.uk/online-applications/',
            'islington': 'https://planning.islington.gov.uk/online-applications/',
            'tower_hamlets': 'https://development.towerhamlets.gov.uk/online-applications/',
            'southwark': 'https://planning.southwark.gov.uk/online-applications/',
            'lambeth': 'https://planning.lambeth.gov.uk/online-applications/',
            'wandsworth': 'https://planning.wandsworth.gov.uk/online-applications/',
            'kingston': 'https://planning.kingston.gov.uk/online-applications/',
            'merton': 'https://planning.merton.gov.uk/online-applications/',
            'sutton': 'https://planning.sutton.gov.uk/online-applications/',
            'croydon': 'https://publicaccess2.croydon.gov.uk/online-applications/',
            'bromley': 'https://searchapplications.bromley.gov.uk/online-applications/',
            'bexley': 'https://pa.bexley.gov.uk/online-applications/',
            'greenwich': 'https://planning.royalgreenwich.gov.uk/online-applications/',
            'lewisham': 'https://planning.lewisham.gov.uk/online-applications/',
            'newham': 'https://pa.newham.gov.uk/online-applications/',
            'waltham_forest': 'https://planning.walthamforest.gov.uk/online-applications/',
            'redbridge': 'https://planning.redbridge.gov.uk/online-applications/',
            'havering': 'https://planning.havering.gov.uk/online-applications/',
            'enfield': 'https://planningandbuildingcontrol.enfield.gov.uk/online-applications/',
            'brent': 'https://pa.brent.gov.uk/online-applications/',
            'ealing': 'https://pam.ealing.gov.uk/online-applications/',
            
            # Major Cities - Idox Portals
            'birmingham': 'https://eplanning.birmingham.gov.uk/Northgate/DocumentApplication/',
            'manchester': 'https://pa.manchester.gov.uk/online-applications/',
            'leeds': 'https://publicaccess.leeds.gov.uk/online-applications/',
            'sheffield': 'https://planning.sheffield.gov.uk/online-applications/',
            'bristol': 'https://planningonline.bristol.gov.uk/online-applications/',
            'newcastle': 'https://publicaccess.newcastle.gov.uk/online-applications/',
            'nottingham': 'https://planningonline.nottinghamcity.gov.uk/online-applications/',
            'leicester': 'https://planning.leicester.gov.uk/online-applications/',
            'bradford': 'https://planning.bradford.gov.uk/online-applications/',
            
            # Other Councils - Idox Portals
            'cherwell': 'https://planningregister.cherwell.gov.uk/Planning/',
            'oxford': 'https://planningregister.oxford.gov.uk/Planning/',
            'west_oxfordshire': 'https://planning.westoxon.gov.uk/Planning/',
        }
        
        # Non-Idox portal fallbacks - these use custom search URLs when keyVal fails
        non_idox_fallbacks = {
            # London Boroughs with custom systems
            'haringey': 'https://www.haringey.gov.uk/planning-and-building-control/planning/planning-applications/search-planning-applications?reference={reference}',
            'richmond': 'https://www2.richmond.gov.uk/lbrplanning/Planning_CaseNo.aspx?strCASENO={reference}',
            'hounslow': 'https://planning.hounslow.gov.uk/planning_summary.aspx?strCASENO={reference}',
            'hillingdon': 'https://planning.hillingdon.gov.uk/OAS/enquiry/search?number={reference}',
            'harrow': 'https://www.harrow.gov.uk/planning-applications/search?reference={reference}',
            
            # Major Cities with custom systems  
            'liverpool': 'https://liverpool.gov.uk/planning-and-building-control/applications/search?reference={reference}',
            'coventry': 'https://planning.coventry.gov.uk/CherwellDC/search.aspx?reference={reference}',
            
            # Councils with custom systems
            'south_oxfordshire': 'https://data.southoxon.gov.uk/ccm/support/search?reference={reference}',
            'vale_of_white_horse': 'https://data.whitehorsedc.gov.uk/java/support/search?reference={reference}',
        }
        
        # Helper function to extract keyVal from search results
        def extract_keyval_from_search(portal_base: str, reference: str) -> Optional[str]:
            try:
                # Build search URL 
                search_url = f"{portal_base}search.do?action=search&searchType=Application&reference={reference}"
                
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
                
                # Perform search request using proxy system
                response = self._make_request_with_proxy(search_url, method='GET', max_retries=2)
                if not response or response.status_code != 200:
                    return None
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Look for keyVal in links - multiple patterns
                keyval_patterns = [
                    r'keyVal=([A-Z0-9]{10,})',  # Standard keyVal pattern
                    r'keyval=([A-Z0-9]{10,})',  # lowercase variant
                    r'applicationDetails\.do\?.*keyVal=([A-Z0-9]{10,})'  # Full URL pattern
                ]
                
                # Search in all links and href attributes
                for link in soup.find_all(['a', 'link'], href=True):
                    href = link.get('href', '')
                    if href:
                        for pattern in keyval_patterns:
                            match = re.search(pattern, href, re.IGNORECASE)
                            if match:
                                keyval = match.group(1)
                                print(f"🔍 Found keyVal {keyval} for {reference}")
                                return keyval
                
                # Also search in JavaScript/text content
                for script in soup.find_all('script'):
                    if hasattr(script, 'string') and script.string:
                        for pattern in keyval_patterns:
                            match = re.search(pattern, script.string, re.IGNORECASE)
                            if match:
                                keyval = match.group(1)
                                print(f"🔍 Found keyVal {keyval} in script for {reference}")
                                return keyval
                
                print(f"⚠️ No keyVal found in search results for {reference}")
                return None
                
            except Exception as e:
                print(f"❌ Error searching for keyVal for {reference}: {str(e)}")
                return None
        
        # Try to identify authority from organisation_entity or reference
        authority_key = None
        if organisation_entity:
            org_lower = str(organisation_entity).lower()
            # Direct match first - check both Idox and non-Idox
            if org_lower in idox_portals:
                authority_key = org_lower
            elif org_lower in non_idox_fallbacks:
                authority_key = org_lower
            else:
                # Partial match
                for auth_key in list(idox_portals.keys()) + list(non_idox_fallbacks.keys()):
                    if auth_key in org_lower or org_lower in auth_key:
                        authority_key = auth_key
                        break
        
        # If no authority identified, try common London authorities for London references
        if not authority_key and reference:
            # Barnet pattern: 24/1234/FUL or B/1234/24
            if re.match(r'(24|23|22|21|20)/\d+/', reference) or re.match(r'B/\d+/', reference):
                authority_key = 'barnet'
            elif 'WM' in reference.upper():
                authority_key = 'westminster'
            elif 'CAM' in reference.upper():
                authority_key = 'camden'
        
        # Try keyVal resolution for identified authority (Idox portals only)
        if authority_key and authority_key in idox_portals:
            portal_base = idox_portals[authority_key]
            keyval = extract_keyval_from_search(portal_base, reference)
            
            if keyval:
                # Build the working URL with keyVal
                working_url = f"{portal_base}applicationDetails.do?activeTab=summary&keyVal={keyval}"
                print(f"✅ Generated working keyVal URL for {reference}: {working_url}")
                return working_url
            else:
                # Fallback to search URL for Idox portals
                search_url = f"{portal_base}search.do?action=search&searchType=Application&reference={reference}"
                print(f"⚠️ Fallback to search URL for {reference}: {search_url}")
                return search_url
        
        # Handle non-Idox portals with custom URLs
        if authority_key and authority_key in non_idox_fallbacks:
            fallback_template = non_idox_fallbacks[authority_key]
            custom_url = fallback_template.format(reference=reference)
            print(f"🔗 Using custom non-Idox URL for {authority_key}: {custom_url}")
            return custom_url
        
        # Try multiple common Idox authorities if no specific match
        common_authorities = ['barnet', 'westminster', 'camden', 'hackney', 'islington']
        for auth in common_authorities:
            if auth in idox_portals:
                portal_base = idox_portals[auth]
                try:
                    keyval = extract_keyval_from_search(portal_base, reference)
                    if keyval:
                        working_url = f"{portal_base}applicationDetails.do?activeTab=summary&keyVal={keyval}"
                        print(f"✅ Found working keyVal URL via {auth} for {reference}: {working_url}")
                        return working_url
                except Exception as e:
                    print(f"⚠️ Authority {auth} failed for {reference}: {str(e)}")
                    continue
        
        # Final fallback - UK Government Planning Portal
        search_ref = reference.replace('/', '%2F')
        fallback_url = f"https://www.gov.uk/search-planning-applications?reference={search_ref}"
        print(f"🏛️ Final fallback to government portal for {reference}: {fallback_url}")
        return fallback_url

    def _build_planning_application_url(self, reference: str, organisation_entity: str = None) -> str:
        """Build URL link to actual planning application page using keyVal resolution"""
        # Use the actual method with correct parameters
        result = self.resolve_keyval_planning_url(authority=organisation_entity or "barnet", reference=reference)
        return result.get('url', 'N/A')


class HunterClient:
    """Client for Hunter.io API for domain search"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.hunter.io"
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json'
        })
    
    def find_company_domain(self, company_name: str) -> Optional[str]:
        """Find domain for a company using Hunter.io API"""
        if not self.api_key or not company_name:
            return None
        
        try:
            # Clean company name
            clean_name = self._clean_company_name(company_name)
            
            # Try v2 API first
            domain = self._call_v2_domain_search(clean_name)
            if domain:
                return domain
            
            # Fallback to v1 API
            domain = self._call_v1_domain_search(clean_name)
            return domain
            
        except Exception as e:
            print(f"Hunter.io domain search failed: {str(e)}")
            return None
    
    def _call_v2_domain_search(self, company_name: str) -> Optional[str]:
        """Call Hunter.io v2 domain search API"""
        try:
            params = {
                'company': company_name,
                'api_key': self.api_key
            }
            
            response = self.session.get(
                f"{self.base_url}/v2/domain-search",
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                domain = data.get('data', {}).get('domain')
                if domain and domain.strip():
                    return domain.strip()
            
            return None
            
        except Exception:
            return None
    
    def _call_v1_domain_search(self, company_name: str) -> Optional[str]:
        """Call Hunter.io v1 search API as fallback"""
        try:
            params = {
                'company': company_name,
                'api_key': self.api_key
            }
            
            response = self.session.get(
                f"{self.base_url}/v1/search",
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                domain = data.get('data', {}).get('domain')
                if domain and domain.strip():
                    return domain.strip()
            
            return None
            
        except Exception:
            return None
    
    def _clean_company_name(self, company_name: str) -> str:
        """Clean company name for better search results"""
        if not company_name:
            return ""
        
        # Remove common suffixes and clean
        suffixes_to_remove = [
            'LTD', 'LTD.', 'LIMITED', 'LIMITED.', 'PLC', 'PLC.', 
            'CORP', 'CORP.', 'CORPORATION', 'CORPORATION.',
            'INC', 'INC.', 'LLC', 'LLC.', 'LLP', 'LLP.',
            '& COMPANY', '& CO', '& CO.', 'AND COMPANY', 'AND CO'
        ]
        
        # Remove non-breaking spaces and normalize whitespace first
        cleaned = company_name.replace('\u00A0', ' ').strip()
        cleaned = ' '.join(cleaned.split())  # Remove multiple spaces
        cleaned = cleaned.upper()
        
        # Remove suffixes (check for both with and without preceding space)
        for suffix in suffixes_to_remove:
            # Check for suffix at the end with space
            if cleaned.endswith(f' {suffix}'):
                cleaned = cleaned[:-len(f' {suffix}')]
            # Check for suffix at the end without space (for names like "CompanyLTD")
            elif cleaned.endswith(suffix) and len(cleaned) > len(suffix):
                cleaned = cleaned[:-len(suffix)]
        
        # Final cleanup
        cleaned = cleaned.strip()
        
        return cleaned.title() if cleaned else ""
    
    def test_api_connection(self) -> bool:
        """Test API connection using stripe.com as reference"""
        if not self.api_key:
            return False
        
        try:
            params = {
                'domain': 'stripe.com',
                'api_key': self.api_key
            }
            
            response = self.session.get(
                f"{self.base_url}/v2/domain-search",
                params=params,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get('data', {}).get('domain') == 'stripe.com'
            
            return False
            
        except Exception:
            return False
    
    def find_emails_by_domain(self, domain: str, first_name: str = None, last_name: str = None, 
                             limit: int = 10) -> List[Dict]:
        """Find email addresses for a specific domain, optionally filtered by name"""
        if not self.api_key or not domain:
            return []
        
        try:
            params = {
                'domain': domain.strip(),
                'api_key': self.api_key,
                'limit': limit
            }
            
            # Add name filters if provided
            if first_name:
                params['first_name'] = first_name.strip()
            if last_name:
                params['last_name'] = last_name.strip()
            
            response = self.session.get(
                f"{self.base_url}/v2/domain-search",
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                emails = data.get('data', {}).get('emails', [])
                
                # Format the email results
                formatted_emails = []
                for email_data in emails:
                    formatted_emails.append({
                        'email': email_data.get('value', ''),
                        'first_name': email_data.get('first_name', ''),
                        'last_name': email_data.get('last_name', ''),
                        'position': email_data.get('position', ''),
                        'department': email_data.get('department', ''),
                        'confidence': email_data.get('confidence', 0),
                        'verification': email_data.get('verification', {}).get('result', 'unknown')
                    })
                
                return formatted_emails
            
            return []
            
        except Exception as e:
            print(f"Hunter email search failed: {str(e)}")
            return []
    
    def verify_email(self, email: str) -> Dict[str, Any]:
        """Verify an email address using Hunter.io"""
        if not self.api_key or not email:
            return {'valid': False, 'confidence': 0}
        
        try:
            params = {
                'email': email.strip(),
                'api_key': self.api_key
            }
            
            response = self.session.get(
                f"{self.base_url}/v2/email-verifier",
                params=params,
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json().get('data', {})
                return {
                    'email': data.get('email', email),
                    'valid': data.get('result') == 'deliverable',
                    'confidence': data.get('score', 0),
                    'result': data.get('result', 'unknown'),
                    'sources': data.get('sources', [])
                }
            
            return {'valid': False, 'confidence': 0, 'result': 'unknown'}
            
        except Exception as e:
            print(f"Hunter email verification failed: {str(e)}")
            return {'valid': False, 'confidence': 0, 'result': 'error'}
    
    def find_officer_emails(self, officer_name: str, company_domain: str) -> List[Dict]:
        """Find email addresses for a specific officer at a company domain"""
        if not officer_name or not company_domain:
            return []
        
        # Parse the officer name
        name_parts = officer_name.strip().split()
        if len(name_parts) < 2:
            return []
        
        first_name = name_parts[0]
        last_name = name_parts[-1]
        
        # Search for emails with name filters
        emails = self.find_emails_by_domain(company_domain, first_name, last_name)
        
        # Filter results for better matches
        filtered_emails = []
        for email_data in emails:
            # Calculate name match confidence
            email_first = email_data.get('first_name', '').lower()
            email_last = email_data.get('last_name', '').lower()
            
            if (first_name.lower() in email_first or email_first in first_name.lower()) and \
               (last_name.lower() in email_last or email_last in last_name.lower()):
                # Add name matching confidence
                email_data['name_match_confidence'] = 0.9
                filtered_emails.append(email_data)
            elif first_name.lower()[0] == email_first[0:1] and last_name.lower() in email_last:
                # First initial + last name match
                email_data['name_match_confidence'] = 0.7
                filtered_emails.append(email_data)
        
        return filtered_emails
    
    def enrich_company(self, company_data: Dict) -> Optional[Dict]:
        """Enrich company data with domain information"""
        company_name = company_data.get('company_name', '')
        domain = self.find_company_domain(company_name)
        
        if domain:
            return {
                'domain': domain,
                'company_name': company_name
            }
        
        return None


class ApolloClient:
    """Client for Apollo.io API for email discovery and enrichment"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.apollo.io/v1"
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Cache-Control': 'no-cache'
        })
    
    def search_people(self, first_name: str, last_name: str, organization_name: str = None,
                      domain: str = None, limit: int = 10) -> List[Dict]:
        """Search for people using Apollo.io People Search API"""
        if not self.api_key:
            return []
        
        try:
            params = {
                'api_key': self.api_key,
                'first_name': first_name.strip() if first_name else '',
                'last_name': last_name.strip() if last_name else '',
                'per_page': min(limit, 25),  # Apollo.io max per page
                'page': 1
            }
            
            # Add organization filters
            if organization_name:
                params['organization_name'] = organization_name.strip()
            if domain:
                params['organization_domains'] = domain.strip()
            
            response = self.session.get(
                f"{self.base_url}/mixed_people/search",
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                people = data.get('people', [])
                
                formatted_results = []
                for person in people:
                    email = person.get('email')
                    if email:  # Only include results with email addresses
                        formatted_results.append({
                            'email': email,
                            'first_name': person.get('first_name', ''),
                            'last_name': person.get('last_name', ''),
                            'title': person.get('title', ''),
                            'organization_name': person.get('organization', {}).get('name', ''),
                            'domain': person.get('organization', {}).get('primary_domain', ''),
                            'linkedin_url': person.get('linkedin_url', ''),
                            'confidence': person.get('email_status', '').lower() == 'verified' and 0.8 or 0.6,
                            'source': 'apollo'
                        })
                
                return formatted_results
            
            return []
            
        except Exception as e:
            print(f"Apollo people search failed: {str(e)}")
            return []
    
    def find_officer_emails(self, officer_name: str, company_name: str, company_domain: str = None) -> List[Dict]:
        """Find email addresses for a specific officer using Apollo.io"""
        if not officer_name:
            return []
        
        # Parse the officer name
        name_parts = officer_name.strip().split()
        if len(name_parts) < 2:
            return []
        
        first_name = name_parts[0]
        last_name = name_parts[-1]
        
        # Search with company name first, then domain if available
        results = []
        
        if company_name:
            results = self.search_people(
                first_name=first_name,
                last_name=last_name,
                organization_name=company_name,
                limit=5
            )
        
        # If no results and we have a domain, try domain search
        if not results and company_domain:
            results = self.search_people(
                first_name=first_name,
                last_name=last_name,
                domain=company_domain,
                limit=5
            )
        
        # Add name matching confidence scores
        for result in results:
            result_first = result.get('first_name', '').lower()
            result_last = result.get('last_name', '').lower()
            
            # Calculate name match confidence
            if (first_name.lower() == result_first and last_name.lower() == result_last):
                result['name_match_confidence'] = 0.95
            elif (first_name.lower() in result_first and last_name.lower() in result_last):
                result['name_match_confidence'] = 0.8
            else:
                result['name_match_confidence'] = 0.6
        
        return results
    
    def verify_email(self, email: str) -> Dict[str, Any]:
        """Verify email address using Apollo.io (if available)"""
        # Apollo.io doesn't have a dedicated email verification endpoint
        # Return a basic response indicating it's from Apollo
        return {
            'email': email,
            'valid': True,  # Assume valid since Apollo provides it
            'confidence': 0.7,  # Medium confidence without dedicated verification
            'result': 'unknown',
            'source': 'apollo'
        }
    
    def enrich_company(self, company_data: Dict) -> Optional[Dict]:
        """Enrich company data using Apollo.io organization search"""
        company_name = company_data.get('company_name', '')
        if not self.api_key or not company_name:
            return None
        
        try:
            params = {
                'api_key': self.api_key,
                'name': company_name.strip(),
                'per_page': 5
            }
            
            response = self.session.get(
                f"{self.base_url}/organizations/search",
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                organizations = data.get('organizations', [])
                
                if organizations:
                    org = organizations[0]  # Take the first (best) match
                    return {
                        'name': org.get('name'),
                        'primary_domain': org.get('primary_domain'),
                        'website_url': org.get('website_url'),
                        'industry': org.get('industry'),
                        'employee_count': org.get('estimated_num_employees'),
                        'founded_year': org.get('founded_year'),
                        'description': org.get('description'),
                        'linkedin_url': org.get('linkedin_url'),
                        'source': 'apollo'
                    }
            
            return None
            
        except Exception as e:
            print(f"Apollo company enrichment failed: {str(e)}")
            return None


class BrightDataClient:
    """Client for Bright Data LinkedIn API"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.brightdata.com/datasets/v3/trigger"
        self.dataset_id = "gd_l1viktl72bvl7bjuj0"  # LinkedIn profiles by name dataset
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        })
    
    def search_linkedin_profile(self, first_name: str, last_name: str, company_name: str) -> Optional[str]:
        """Search for LinkedIn profile using name and company"""
        if not self.api_key:
            return None
        
        try:
            # Correct format for name-based discovery
            params = {
                "dataset_id": self.dataset_id,
                "include_errors": "true",
                "type": "discover_new",
                "discover_by": "name"
            }
            
            # Clean names for better search  
            first_name = self._clean_name(first_name)
            last_name = self._clean_name(last_name)
            
            # Direct JSON array for name-based discovery
            data = [{
                "first_name": first_name,
                "last_name": last_name
            }]
            
            # Debug: Show what we're searching for
            print(f"🔍 Searching LinkedIn for: {first_name} {last_name}")
            
            response = self.session.post(self.base_url, json=data, params=params, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                print(f"📊 API Response: {result}")
                
                # Check if we got a snapshot_id (async job started)
                if 'snapshot_id' in result:
                    snapshot_id = result['snapshot_id']
                    print(f"⏱️ Job started with snapshot ID: {snapshot_id}")
                    print("⏳ Waiting for LinkedIn search results...")
                    
                    # Automatically poll for results
                    results_data = self._fetch_results_by_snapshot(snapshot_id)
                    if results_data:
                        extracted_url = self._extract_linkedin_url(results_data)
                        print(f"🔗 Final LinkedIn URL: {extracted_url}")
                        return extracted_url
                    else:
                        return f"Job timeout: {snapshot_id}"
                else:
                    # Direct results (shouldn't happen with /trigger)
                    extracted_url = self._extract_linkedin_url(result)
                    print(f"🔗 Extracted URL: {extracted_url}")
                    return extracted_url
            
            return None
            
        except Exception as e:
            print(f"Bright Data LinkedIn search failed: {str(e)}")
            return None
    
    def search_multiple_profiles(self, officers: List[Dict], company_name: str, company_address: str = None) -> Dict[str, str]:
        """Search for multiple LinkedIn profiles at once"""
        if not self.api_key or not officers:
            return {}
        
        try:
            # Prepare batch request
            input_data = []
            for officer in officers[:5]:  # Limit to 5 officers to control costs
                if isinstance(officer, str):
                    # If officer is just a name string, parse it
                    name_parts = officer.strip().split()
                    if len(name_parts) >= 2:
                        first_name = name_parts[0]
                        last_name = name_parts[-1]  # Take only the last name part (no middle names)
                    else:
                        continue
                else:
                    # If officer is a dict with structured data
                    full_name = officer.get('name', '')
                    if not full_name:
                        continue
                    name_parts = full_name.strip().split()
                    if len(name_parts) >= 2:
                        first_name = name_parts[0]
                        last_name = name_parts[-1]  # Take only the last name part (no middle names)
                    else:
                        continue
                
                input_data.append({
                    "first_name": self._clean_name(first_name),
                    "last_name": self._clean_name(last_name),
                    "country_code": "GB"  # Only return UK/Great Britain results
                })
            
            if not input_data:
                return {}
            
            # Correct format for name-based discovery
            params = {
                "dataset_id": self.dataset_id,
                "include_errors": "true",
                "type": "discover_new",
                "discover_by": "name"
            }
            
            # Direct JSON array for batch name-based discovery
            data = input_data
            
            # Debug: Show what names we're searching for
            search_names = [f"{item['first_name']} {item['last_name']}" for item in data]
            st.write(f"🔍 Batch searching LinkedIn for: {', '.join(search_names)}")
            
            response = self.session.post(self.base_url, json=data, params=params, timeout=60)
            
            if response.status_code == 200:
                results = response.json()
                st.write(f"📊 Batch API Response: {results}")
                
                # Check if we got a snapshot_id (async job started)
                if 'snapshot_id' in results:
                    snapshot_id = results['snapshot_id']
                    st.write(f"⏱️ Batch job started with snapshot ID: {snapshot_id}")
                    st.write("⏳ Waiting for LinkedIn search results...")
                    
                    # Automatically poll for results
                    results_data = self._fetch_results_by_snapshot(snapshot_id)
                    if results_data:
                        processed_results = self._process_batch_results(results_data, officers, company_address)
                        st.write(f"🔗 Final LinkedIn Results: {processed_results}")
                        return processed_results
                    else:
                        return {"job_timeout": f"Snapshot: {snapshot_id}"}
                else:
                    # Direct results (shouldn't happen with /trigger)
                    processed_results = self._process_batch_results(results, officers, company_address)
                    st.write(f"🔗 Processed Results: {processed_results}")
                    return processed_results
            
            return {}
            
        except Exception as e:
            st.warning(f"Bright Data batch LinkedIn search failed: {str(e)}")
            return {}
    
    def _clean_name(self, name: str) -> str:
        """Clean name for better LinkedIn search accuracy"""
        if not name:
            return ""
        
        # Remove common titles and suffixes (less aggressive cleaning for LinkedIn)
        titles_to_remove = [
            'Mr', 'Mrs', 'Ms', 'Miss', 'Dr', 'Prof', 'Sir', 'Dame'
        ]
        
        # Split and clean
        name_parts = name.strip().split()
        cleaned_parts = []
        
        for part in name_parts:
            clean_part = part.strip('.,()[]')
            # Only remove if it's a clear title, keep professional suffixes
            if clean_part not in titles_to_remove and len(clean_part) > 1:
                # Ensure proper capitalization (first letter uppercase, rest lowercase)
                cleaned_parts.append(clean_part.capitalize())
        
        return ' '.join(cleaned_parts)
    
    def _clean_company_name(self, company_name: str) -> str:
        """Clean company name for better search"""
        if not company_name:
            return ""
        
        # Remove common suffixes
        suffixes_to_remove = [
            'LTD', 'LTD.', 'LIMITED', 'LIMITED.', 'PLC', 'PLC.', 
            'CORP', 'CORP.', 'CORPORATION', 'CORPORATION.',
            'INC', 'INC.', 'LLC', 'LLC.', 'LLP', 'LLP.',
            '& COMPANY', '& CO', '& CO.', 'AND COMPANY', 'AND CO'
        ]
        
        # Remove non-breaking spaces and normalize whitespace first
        cleaned = company_name.replace('\u00A0', ' ').strip()
        cleaned = ' '.join(cleaned.split())  # Remove multiple spaces
        cleaned = cleaned.upper()
        
        # Remove suffixes (check for both with and without preceding space)
        for suffix in suffixes_to_remove:
            # Check for suffix at the end with space
            if cleaned.endswith(f' {suffix}'):
                cleaned = cleaned[:-len(f' {suffix}')]
            # Check for suffix at the end without space (for names like "CompanyLTD")
            elif cleaned.endswith(suffix) and len(cleaned) > len(suffix):
                cleaned = cleaned[:-len(suffix)]
        
        # Final cleanup
        cleaned = cleaned.strip()
        
        return cleaned.title() if cleaned else ""
    
    def _fetch_results_by_snapshot(self, snapshot_id: str, max_wait_time: int = 180) -> Optional[Dict]:
        """Fetch results from Bright Data using snapshot ID with automatic polling"""
        if not snapshot_id:
            return None
        
        # Endpoint to fetch snapshot results
        results_url = f"https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}"
        
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            try:
                response = self.session.get(results_url, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Check if results are ready
                    if isinstance(data, list) and len(data) > 0:
                        st.write(f"✅ Results ready! Found {len(data)} items")
                        return data
                    elif isinstance(data, dict):
                        # Check status
                        if data.get('status') == 'running':
                            st.write("⏳ Job still running, waiting...")
                            time.sleep(5)
                            continue
                        elif data.get('status') == 'completed' and 'data' in data:
                            st.write(f"✅ Results ready! Found {len(data.get('data', []))} items")
                            return data.get('data', [])
                        else:
                            st.write(f"📊 Snapshot response: {data}")
                            return data
                
                elif response.status_code == 202:
                    # Job still processing
                    st.write("⏳ Job still processing, waiting...")
                    time.sleep(5)
                    continue
                    
                else:
                    st.write(f"❌ Error fetching results: HTTP {response.status_code}")
                    break
                    
            except Exception as e:
                st.write(f"❌ Error polling results: {str(e)}")
                break
        
        st.write("⏰ Timeout waiting for results")
        return None
    
    def _extract_linkedin_url(self, result: Dict, company_city: str = None) -> Optional[str]:
        """Extract LinkedIn URL from API response with GB filtering and city prioritization"""
        try:
            if isinstance(result, list) and len(result) > 0:
                # Filter to only GB results
                gb_profiles = [p for p in result if p.get('country_code') == 'GB']
                
                if not gb_profiles:
                    return None
                
                # If we have company city info, prioritize matching cities
                if company_city and len(gb_profiles) > 1:
                    best_match = self._find_best_city_match(gb_profiles, company_city)
                    if best_match:
                        linkedin_url = best_match.get('url')
                        if linkedin_url and 'linkedin.com' in linkedin_url:
                            return linkedin_url
                
                # Default to first GB profile
                profile = gb_profiles[0]
                linkedin_url = profile.get('url')
                if linkedin_url and 'linkedin.com' in linkedin_url:
                    return linkedin_url
                    
            elif isinstance(result, dict):
                # Single result - check if it's GB
                if result.get('country_code') == 'GB':
                    linkedin_url = result.get('url')
                    if linkedin_url and 'linkedin.com' in linkedin_url:
                        return linkedin_url
            
            return None
        except Exception:
            return None
    
    def _process_batch_results(self, results: List[Dict], original_officers: List, company_address: str = None) -> Dict[str, str]:
        """Process batch results and map to officer names with GB filtering and city prioritization"""
        linkedin_data = {}
        
        try:
            # Extract city from company address if available
            company_city = self._extract_city_from_address(company_address) if company_address else None
            
            for i, result in enumerate(results):
                if i < len(original_officers):
                    officer_name = original_officers[i] if isinstance(original_officers[i], str) else original_officers[i].get('name', '')
                    linkedin_url = self._extract_linkedin_url(result, company_city)
                    if linkedin_url:
                        linkedin_data[officer_name] = linkedin_url
            
            return linkedin_data
            
        except Exception:
            return {}
    
    def _find_best_city_match(self, gb_profiles: List[Dict], company_city: str) -> Optional[Dict]:
        """Find the LinkedIn profile with the best city match"""
        try:
            company_city_lower = company_city.lower().strip()
            
            # Direct city name matches
            for profile in gb_profiles:
                profile_city = profile.get('city', '')
                if profile_city:
                    profile_city_lower = profile_city.lower()
                    # Check for city name in the LinkedIn location
                    if company_city_lower in profile_city_lower:
                        return profile
            
            # Fuzzy matching for common UK city variants
            city_aliases = {
                'london': ['london', 'greater london'],
                'manchester': ['manchester', 'greater manchester'],
                'birmingham': ['birmingham', 'west midlands'],
                'leeds': ['leeds', 'west yorkshire'],
                'glasgow': ['glasgow', 'greater glasgow'],
                'edinburgh': ['edinburgh', 'lothian']
            }
            
            # Check aliases
            for canonical_city, aliases in city_aliases.items():
                if company_city_lower in aliases:
                    for profile in gb_profiles:
                        profile_city = profile.get('city', '').lower()
                        for alias in aliases:
                            if alias in profile_city:
                                return profile
            
            return None
            
        except Exception:
            return None
    
    def _extract_city_from_address(self, address: str) -> Optional[str]:
        """Extract city name from company address"""
        try:
            if not address:
                return None
            
            # Common patterns in UK addresses
            address_lower = address.lower()
            
            # Look for major UK cities
            uk_cities = [
                'london', 'birmingham', 'manchester', 'glasgow', 'edinburgh',
                'leeds', 'sheffield', 'bristol', 'liverpool', 'cardiff',
                'coventry', 'leicester', 'sunderland', 'belfast', 'newcastle',
                'nottingham', 'plymouth', 'wolverhampton', 'stoke', 'derby'
            ]
            
            for city in uk_cities:
                if city in address_lower:
                    return city.title()
            
            # Try to extract from comma-separated address parts
            parts = [part.strip() for part in address.split(',')]
            if len(parts) >= 2:
                # Usually city is the second-to-last or third-to-last component
                potential_city = parts[-2] if len(parts) > 1 else parts[0]
                return potential_city.strip()
            
            return None
            
        except Exception:
            return None

    def enrich_company(self, company_data: Dict) -> Optional[Dict]:
        """Enrich company data - not used for LinkedIn search but kept for compatibility"""
        return None
