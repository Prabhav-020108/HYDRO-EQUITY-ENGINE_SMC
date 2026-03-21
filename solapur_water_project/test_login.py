from fastapi.testclient import TestClient
from backend.app import app
import traceback
import sys

try:
    client = TestClient(app)
    response = client.post("/auth/login", json={"username":"engineer1", "password":"demo"})
    print(f"Status: {response.status_code}")
    print(f"Body: {response.text}")
except Exception as e:
    print("Caught Exception:", file=sys.stderr)
    traceback.print_exc(file=sys.stderr)
