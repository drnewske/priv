
import requests
import time
import json
import sys

# Test a few different sources
TEST_URLS = [
    ("Bbc2 (SuperSonic)", "http://Supersonictv.live:8080/live/Ramsey123/Ramsey123/12191.ts"),
    ("Bbc2 (StarShare)", "http://tv.starsharetv.com:8080/live/7654321/1234567/148218.ts"),
    ("Itv1 (A1XS)", "https://a1xs.vip/1000011")
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

print(f"Testing {len(TEST_URLS)} streams with User-Agent...")
print("-" * 60)

for name, url in TEST_URLS:
    print(f"Testing: {name}")
    print(f"URL: {url}")
    
    start_time = time.time()
    try:
        # Use stream=True to avoid download, timeout 15s
        response = requests.get(url, headers=HEADERS, stream=True, timeout=15)
        
        if response.status_code == 200:
            # Read a small chunk
            chunk = next(response.iter_content(chunk_size=1024))
            end_time = time.time()
            duration = end_time - start_time
            
            print(f"✅ SUCCESS")
            print(f"   Status: {response.status_code}")
            print(f"   Time: {duration:.4f} seconds")
            print(f"   Type: {response.headers.get('content-type', 'unknown')}")
        else:
            print(f"❌ FAILURE: Status {response.status_code}")
            
    except requests.exceptions.Timeout:
        print("❌ FAILURE: Timed out (15s)")
    except requests.exceptions.ConnectionError:
        print("❌ FAILURE: Connection Error")
    except Exception as e:
        print(f"❌ FAILURE: {e}")
    
    print("-" * 60)
