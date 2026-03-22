import os
import requests

BASE_URL = "http://localhost:8000"

def test_pdf_download():
    # Login as ward officer
    resp = requests.post(f"{BASE_URL}/auth/login", data={"username": "ward_officer", "password": "password123"})
    if not resp.ok:
        print("Login failed:", resp.text)
        return
    
    token = resp.json().get("access_token")
    print("Logged in, token received.")
    
    # Fetch PDF
    pdf_resp = requests.get(
        f"{BASE_URL}/ward/field-work-log/pdf",
        headers={"Authorization": f"Bearer {token}"}
    )
    
    if pdf_resp.status_code == 200 and pdf_resp.headers.get("content-type") == "application/pdf":
        print("Success! PDF received.")
        out_path = os.path.join(os.path.dirname(__file__), '..', 'outputs', 'test_log.pdf')
        with open(out_path, "wb") as f:
            f.write(pdf_resp.content)
        print(f"Saved to {out_path}")
    else:
        print(f"Failed. Status: {pdf_resp.status_code}, Content-Type: {pdf_resp.headers.get('content-type')}")
        print(pdf_resp.text)

if __name__ == "__main__":
    test_pdf_download()
