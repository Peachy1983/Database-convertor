"""
Free Proxy Manager for bypassing network blocks
Uses free proxy services to avoid connection timeouts
"""

import requests
import random
import time
from typing import List, Dict, Optional

class FreeProxyManager:
    """Manages free proxy lists for web scraping"""
    
    def __init__(self):
        self.working_proxies = []
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36", 
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
        ]
        self._load_free_proxies()
    
    def _load_free_proxies(self):
        """Load working proxies from multiple free sources"""
        print("🔍 Loading free proxies...")
        
        # Method 1: ProxyScrape API (free, updated every 5 minutes)
        self._load_from_proxyscrape()
        
        # Method 2: Static list of known working free proxies
        self._load_static_proxies()
        
        print(f"✅ Loaded {len(self.working_proxies)} free proxies")
    
    def _load_from_proxyscrape(self):
        """Load proxies from ProxyScrape free API"""
        try:
            # ProxyScrape free API - HTTP proxies only
            url = "https://api.proxyscrape.com/v2/?request=get&protocol=http&timeout=5000&format=json&country=us,gb,de,nl,fr"
            
            response = requests.get(url, timeout=15)
            if response.status_code == 200:
                proxies_data = response.json()
                
                # Add up to 20 proxies from the API
                for proxy in proxies_data[:20]:
                    if 'ip' in proxy and 'port' in proxy:
                        proxy_url = f"http://{proxy['ip']}:{proxy['port']}"
                        self.working_proxies.append({
                            'http': proxy_url,
                            'https': proxy_url,
                            'source': 'proxyscrape'
                        })
                        
                print(f"🌐 Added {min(len(proxies_data), 20)} proxies from ProxyScrape")
                        
        except Exception as e:
            print(f"⚠️ Could not load ProxyScrape proxies: {str(e)}")
    
    def _load_static_proxies(self):
        """Add backup static free proxies"""
        # These are examples - in reality you'd get current working ones
        static_proxies = [
            "20.206.106.192:80",
            "103.49.202.252:80",
            "143.198.228.250:80",
            "165.227.71.60:80",
            "178.62.200.61:80"
        ]
        
        for proxy_addr in static_proxies:
            proxy_url = f"http://{proxy_addr}"
            self.working_proxies.append({
                'http': proxy_url,
                'https': proxy_url,
                'source': 'static'
            })
    
    def get_random_proxy(self) -> Optional[Dict[str, str]]:
        """Get a random working proxy"""
        if not self.working_proxies:
            return None
        return random.choice(self.working_proxies)
    
    def get_random_headers(self) -> Dict[str, str]:
        """Get randomized headers"""
        return {
            "User-Agent": random.choice(self.user_agents),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Cache-Control": "no-cache"
        }
    
    def make_request(self, url: str, method='GET', data=None, max_retries=3) -> Optional[requests.Response]:
        """Make HTTP request with free proxy rotation"""
        for attempt in range(max_retries):
            try:
                # Get random proxy and headers
                proxy_config = self.get_random_proxy()
                headers = self.get_random_headers()
                
                if proxy_config:
                    print(f"🌐 Attempt {attempt + 1}: Using free proxy {proxy_config['http']}")
                else:
                    print(f"🌐 Attempt {attempt + 1}: Direct connection (no proxies available)")
                
                # Make request
                if method.upper() == 'GET':
                    response = requests.get(
                        url,
                        headers=headers,
                        proxies=proxy_config,
                        timeout=10,
                        allow_redirects=True
                    )
                elif method.upper() == 'POST':
                    response = requests.post(
                        url,
                        headers=headers,
                        data=data,
                        proxies=proxy_config,
                        timeout=10,
                        allow_redirects=True
                    )
                else:
                    raise ValueError(f"Unsupported method: {method}")
                
                # Check if successful
                if response.status_code == 200:
                    print(f"✅ Success with {proxy_config['source'] if proxy_config else 'direct'} connection!")
                    return response
                else:
                    print(f"⚠️ HTTP {response.status_code} - trying next proxy")
                    
            except Exception as e:
                print(f"❌ Attempt {attempt + 1} failed: {str(e)}")
            
            # Wait before retry
            if attempt < max_retries - 1:
                wait_time = random.uniform(1, 3)
                time.sleep(wait_time)
        
        print(f"❌ All {max_retries} attempts failed for {url}")
        return None

# Test the free proxy system
if __name__ == "__main__":
    proxy_manager = FreeProxyManager()
    
    # Test with a simple URL
    test_url = "http://httpbin.org/ip"
    response = proxy_manager.make_request(test_url)
    
    if response:
        print(f"🎉 Proxy system working! Response: {response.text}")
    else:
        print("❌ Proxy system not working")