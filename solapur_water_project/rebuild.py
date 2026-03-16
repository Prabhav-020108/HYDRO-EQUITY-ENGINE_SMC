import sys
import os

# Add backend to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from backend.routers.recommendations import rebuild_recommendations

print("Rebuilding recommendations...")
try:
    data = rebuild_recommendations()
    print("Success!")
    print(f"Updated at: {data.get('updated_at')}")
    print(f"Engineer Recs: {len(data.get('engineer_recs', []))}")
    print(f"Ward Recs: {len(data.get('ward_recs', []))}")
    print(f"Commissioner Recs: {len(data.get('commissioner_recs', []))}")
    print(f"Citizen Recs: {len(data.get('citizen_recs', []))}")
except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)
