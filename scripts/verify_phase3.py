import os
import json
import urllib.request
from pprint import pformat

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUTS_DIR = os.path.join(BASE_DIR, 'outputs')

ENDPOINTS = [
    {
        "url": "http://localhost:5000/zones",
        "file": "v4_zone_status.json",
        "name": "V4 Zone Equity (GET /zones)",
        "missing_resp": {"error": "Run V4 first"}
    },
    {
        "url": "http://localhost:5000/alerts/active",
        "file": "v5_alerts.json",
        "name": "V5 Alerts (GET /alerts/active)",
        "missing_resp": {"error": "Run V5 first"}
    },
    {
        "url": "http://localhost:5000/burst-risk/top10",
        "file": "v6_burst_top10.json",
        "name": "V6 Burst Risk (GET /burst-risk/top10)",
        "missing_resp": {"error": "Run V6 first"}
    }
]

def fetch_json(url):
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode())
    except Exception as e:
        return {"error_fetching": str(e)}

def deep_compare(obj1, obj2):
    return json.dumps(obj1, sort_keys=True) == json.dumps(obj2, sort_keys=True)

def verify():
    all_passed = True
    print("====================================")
    print("Phase-3 Integrity Check")
    print("====================================\n")
    
    for ep in ENDPOINTS:
        print(f"Checking: {ep['name']}")
        filepath = os.path.join(OUTPUTS_DIR, ep['file'])
        
        # 1. Fetch from backend
        backend_resp = fetch_json(ep['url'])
        
        # 2. Load from file (or expect missing response)
        if os.path.exists(filepath):
            with open(filepath, 'r', encoding='utf-8') as f:
                expected_data = json.load(f)
        else:
            expected_data = ep['missing_resp']
            
        # 3. Compare
        if deep_compare(backend_resp, expected_data):
            print(f"  [PASS] Endpoint perfectly matches {'file content' if os.path.exists(filepath) else 'missing-file fallback'}")
        else:
            print(f"  [FAIL] Discrepancy detected!")
            print(f"    Expected: {str(expected_data)[:200]}...")
            print(f"    Received: {str(backend_resp)[:200]}...")
            all_passed = False
        print("")
        
    print("====================================")
    if all_passed:
        print("RESULT: ALL CHECKS PASS (OK)")
    else:
        print("RESULT: FAIL - Discrepancies found.")
    print("====================================")

if __name__ == '__main__':
    verify()
