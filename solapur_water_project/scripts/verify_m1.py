"""
Dhara M1 — Complete Verification Script (Windows Compatible, Fixed)
scripts/verify_m1.py

Run from project root:
    python scripts/verify_m1.py

Uses only Python standard library (urllib) — zero dependencies.
Auto-detects whether server is in AUTH_DEV_MODE (password: demo123)
or DB mode (password: demo@1234).

Prints PASS / FAIL for every M1 check.
"""

import json
import sys
import urllib.request
import urllib.error

BASE = "http://localhost:8000"

PASS_COUNT = 0
FAIL_COUNT = 0


# ─────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────

def _call(method, path, token=None, body=None):
    """Make an HTTP call. Returns (status_code, response_dict)."""
    url = BASE + path
    data = json.dumps(body).encode("utf-8") if body is not None else None
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    req = urllib.request.Request(
        url, data=data, headers=headers, method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8")
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, {"error": raw}
    except Exception as e:
        return 0, {"error": str(e)}


def check(label, condition, got=None):
    global PASS_COUNT, FAIL_COUNT
    if condition:
        PASS_COUNT += 1
        print(f"  PASS  {label}")
    else:
        FAIL_COUNT += 1
        print(f"  FAIL  {label}")
        if got is not None:
            print(f"        got: {got}")


def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def try_login(username, passwords):
    """Try multiple passwords. Returns (token, role, password_used) or (None, None, None)."""
    for pw in passwords:
        status, resp = _call("POST", "/auth/login", body={
            "username": username,
            "password": pw
        })
        if status == 200 and "access_token" in resp:
            return resp["access_token"], resp.get("role", ""), pw
    return None, None, None


# ─────────────────────────────────────────────────────────────────
# TEST 0 — Server health
# ─────────────────────────────────────────────────────────────────

section("TEST 0 — Server Health")

status, resp = _call("GET", "/health")
print(f"  Response: {resp}")
check("Server is reachable (HTTP 200)", status == 200, got=f"status={status}")

if status != 200:
    print("\n  FATAL: Server is not running.")
    print("  Start it with these commands:")
    print("    $env:AUTH_DEV_MODE = '1'")
    print("    python -m uvicorn backend.app:app --port 8000 --reload")
    sys.exit(1)


# ─────────────────────────────────────────────────────────────────
# TEST 1 — Auto-detect auth mode and login as engineer
# ─────────────────────────────────────────────────────────────────

section("TEST 1 — Login as engineer1 (auto-detect password)")

# AUTH_DEV_MODE uses 'demo123'
# DB mode (seed_users.py) uses 'demo@1234'
PASSWORDS_TO_TRY = ["demo123", "demo@1234"]

ENG_TOKEN, eng_role, eng_pw = try_login("engineer1", PASSWORDS_TO_TRY)

if ENG_TOKEN:
    print(f"  Logged in successfully.")
    print(f"  Password that worked : '{eng_pw}'")
    print(f"  Role returned        : {eng_role}")
    print(f"  Token (first 50 chars): {ENG_TOKEN[:50]}...")
    check("Login returns HTTP 200", True)
    check("Response has access_token", True)
    check("Role is engineer", eng_role == "engineer", got=eng_role)

    if eng_pw == "demo123":
        print("\n  INFO: Server is in AUTH_DEV_MODE=1 (password=demo123)  OK")
    else:
        print("\n  INFO: Server is in DB mode (password=demo@1234)  OK")
else:
    print("\n  BOTH passwords failed for engineer1.")
    print("  Tried: 'demo123' and 'demo@1234'")
    print("\n  ── HOW TO FIX ──────────────────────────────────────────")
    print("  Option A: Restart server with AUTH_DEV_MODE=1")
    print("    Kill the server (Ctrl+C), then in the SERVER window run:")
    print("      $env:AUTH_DEV_MODE = '1'")
    print("      python -m uvicorn backend.app:app --port 8000 --reload")
    print("")
    print("  Option B: Seed real users into PostgreSQL")
    print("      python scripts/seed_users.py")
    print("  ────────────────────────────────────────────────────────")
    check("Login returns HTTP 200", False, got="Both passwords failed")
    check("Response has access_token", False)
    check("Role is engineer", False)
    sys.exit(1)

PW = eng_pw  # Use this same password for all other logins


# ─────────────────────────────────────────────────────────────────
# TEST 2 — Login as field_operator
# ─────────────────────────────────────────────────────────────────

section("TEST 2 — Login as field_op1 (field_operator role)")

FOP_TOKEN, fop_role, _ = try_login("field_op1", [PW])

if not FOP_TOKEN:
    print("  field_op1 not found, trying field_op_z1...")
    FOP_TOKEN, fop_role, _ = try_login("field_op_z1", [PW])

if FOP_TOKEN:
    print(f"  Logged in. Role: {fop_role}")
    check("Field op login HTTP 200", True)
    check("Field op has access_token", True)
    check("Role is field_operator", fop_role == "field_operator", got=fop_role)
else:
    print(f"  Login failed for field_op1 / field_op_z1 with password '{PW}'")
    print("  Field op tests will be SKIPPED.")
    print("  To add field_op users run: python scripts/seed_users.py")
    check("Field op login HTTP 200", False, got="No field_op user found")
    check("Field op has access_token", False)
    check("Role is field_operator", False)


# ─────────────────────────────────────────────────────────────────
# TEST 3 — Existing endpoints MUST NOT break (Bible M1 requirement)
# ─────────────────────────────────────────────────────────────────

section("TEST 3 — Existing Endpoints Must Not Break (Bible M1)")

# /zones
status, resp = _call("GET", "/zones", token=ENG_TOKEN)
print(f"  /zones -> HTTP {status}, type={type(resp).__name__}, len={len(resp) if isinstance(resp, list) else 'N/A'}")
check("/zones returns 200", status == 200, got=f"status={status}")
check("/zones returns a list", isinstance(resp, list), got=type(resp).__name__)

# /alerts/active WITHOUT ?status= param (must behave exactly as before)
status, resp = _call("GET", "/alerts/active?scenario=baseline", token=ENG_TOKEN)
print(f"  /alerts/active (no ?status) -> HTTP {status}, keys={list(resp.keys()) if isinstance(resp, dict) else '?'}")
check("/alerts/active (no ?status) returns 200", status == 200, got=f"status={status}")
check("/alerts/active has 'alerts' key", "alerts" in resp, got=list(resp.keys()))
check("/alerts/active has 'scenario' key", "scenario" in resp, got=list(resp.keys()))
check(
    "/alerts/active has NO 'status_filter' without param (backward compat)",
    "status_filter" not in resp,
    got=list(resp.keys())
)

# /burst-risk/top10
status, resp = _call("GET", "/burst-risk/top10", token=ENG_TOKEN)
print(f"  /burst-risk/top10 -> HTTP {status}")
check("/burst-risk/top10 returns 200", status == 200, got=f"status={status}")

# /recommendations/citizen (public — no token needed)
status, resp = _call("GET", "/recommendations/citizen")
print(f"  /recommendations/citizen (public) -> HTTP {status}")
check("/recommendations/citizen (public) returns 200", status == 200, got=f"status={status}")

# /pipeline (public — no token needed)
status, resp = _call("GET", "/pipeline")
print(f"  /pipeline (public) -> HTTP {status}")
check("/pipeline (public) returns 200", status == 200, got=f"status={status}")

# /auth/login
status, resp = _call("POST", "/auth/login", body={"username": "engineer1", "password": PW})
check("/auth/login still works", status == 200, got=f"status={status}")


# ─────────────────────────────────────────────────────────────────
# TEST 4 — New ?status= query param (M1 addition)
# ─────────────────────────────────────────────────────────────────

section("TEST 4 — New ?status= Query Filter (M1)")

for state in ["new", "acknowledged", "resolve_requested", "resolved"]:
    status, resp = _call("GET", f"/alerts/active?status={state}", token=ENG_TOKEN)
    check(
        f"?status={state} returns 200",
        status == 200,
        got=f"status={status}"
    )
    check(
        f"?status={state} has 'status_filter' key",
        "status_filter" in resp if isinstance(resp, dict) else False,
        got=list(resp.keys()) if isinstance(resp, dict) else resp
    )
    check(
        f"status_filter value equals '{state}'",
        resp.get("status_filter") == state,
        got=resp.get("status_filter")
    )
    print(f"    total={resp.get('total', 0)} alerts with status='{state}'")


# ─────────────────────────────────────────────────────────────────
# Find an alert ID in PostgreSQL for lifecycle tests
# ─────────────────────────────────────────────────────────────────

section("Finding Alert ID for Lifecycle Tests")

ALERT_ID = None

# Look for a 'new' alert in DB first
status, resp = _call("GET", "/alerts/active?status=new", token=ENG_TOKEN)
new_alerts = [a for a in resp.get("alerts", []) if a.get("db_alert_id", 0) > 0]

if new_alerts:
    ALERT_ID = new_alerts[0]["db_alert_id"]
    print(f"  Found {len(new_alerts)} new alert(s) in DB. Using alert_id={ALERT_ID}")
else:
    print("  No 'new' alerts found with db_alert_id > 0.")

    # Check acknowledged
    status, resp = _call("GET", "/alerts/active?status=acknowledged", token=ENG_TOKEN)
    ack_alerts = [a for a in resp.get("alerts", []) if a.get("db_alert_id", 0) > 0]
    if ack_alerts:
        ALERT_ID = ack_alerts[0]["db_alert_id"]
        print(f"  Found acknowledged alert_id={ALERT_ID}. Will use for lifecycle.")

if ALERT_ID is None:
    print("\n  NO alerts with valid db_alert_id found in PostgreSQL.")
    print("  This means db_migrate.py has not been run yet.")
    print("\n  Run this command, then re-run verify_m1.py:")
    print("    python scripts/db_migrate.py")

check(
    "At least one alert in DB for lifecycle test",
    ALERT_ID is not None,
    got="Run: python scripts/db_migrate.py  then re-run this script"
)


# ─────────────────────────────────────────────────────────────────
# TEST 5 — Full Alert Lifecycle (Bible Section 3 M1)
# ─────────────────────────────────────────────────────────────────

section("TEST 5 — Full Alert Lifecycle (Bible Section 3 M1)")

if ALERT_ID is None:
    print("  SKIPPED — no alert_id. Run: python scripts/db_migrate.py")

else:
    print(f"  Using alert_id = {ALERT_ID}")
    print("")

    # ── STEP A: Engineer acknowledges ────────────────────────────
    print(f"  [A] POST /alerts/{ALERT_ID}/acknowledge  (engineer only)")
    status, resp = _call(
        "POST",
        f"/alerts/{ALERT_ID}/acknowledge",
        token=ENG_TOKEN,
        body={"notes": "Dispatching team to zone_3 — M1 verification test"}
    )
    print(f"      HTTP {status}  ->  {resp}")
    check("acknowledge returns HTTP 200", status == 200, got=f"status={status}")
    check("acknowledge success=true", resp.get("success") is True, got=resp)
    check(
        "acknowledge status='acknowledged'",
        resp.get("status") == "acknowledged",
        got=resp.get("status")
    )

    # ── STEP B: Verify ?status=acknowledged contains the alert ───
    print(f"\n  [B] GET /alerts/active?status=acknowledged")
    status, resp = _call("GET", "/alerts/active?status=acknowledged", token=ENG_TOKEN)
    ack_ids = [a.get("db_alert_id") for a in resp.get("alerts", [])]
    print(f"      total={resp.get('total')}  found_ids={ack_ids}")
    check("?status=acknowledged returns 200", status == 200, got=f"status={status}")
    check(
        f"alert_id={ALERT_ID} appears in acknowledged list",
        ALERT_ID in ack_ids,
        got=f"found ids: {ack_ids}"
    )

    # ── STEP C: Field op requests resolution ─────────────────────
    if FOP_TOKEN:
        print(f"\n  [C] POST /alerts/{ALERT_ID}/request-resolution  (field_operator only)")
        status, resp = _call(
            "POST",
            f"/alerts/{ALERT_ID}/request-resolution",
            token=FOP_TOKEN,
            body={"report": "Valve V-ZONE3-01 adjusted by 12%. Pressure HEI now 0.89."}
        )
        print(f"      HTTP {status}  ->  {resp}")
        check("request-resolution returns 200", status == 200, got=f"status={status}")
        check("request-resolution success=true", resp.get("success") is True, got=resp)
        check(
            "status='resolve_requested'",
            resp.get("status") == "resolve_requested",
            got=resp.get("status")
        )

        # ── STEP D: Engineer accepts resolution ──────────────────
        print(f"\n  [D] POST /alerts/{ALERT_ID}/accept-resolution  (engineer only)")
        status, resp = _call(
            "POST",
            f"/alerts/{ALERT_ID}/accept-resolution",
            token=ENG_TOKEN,
            body={"notes": "HEI confirmed improved. Closing alert."}
        )
        print(f"      HTTP {status}  ->  {resp}")
        check("accept-resolution returns 200", status == 200, got=f"status={status}")
        check("accept-resolution success=true", resp.get("success") is True, got=resp)
        check(
            "final status='resolved'",
            resp.get("status") == "resolved",
            got=resp.get("status")
        )

        # ── STEP E: Verify ?status=resolved ──────────────────────
        print(f"\n  [E] GET /alerts/active?status=resolved")
        status, resp = _call("GET", "/alerts/active?status=resolved", token=ENG_TOKEN)
        resolved_ids = [a.get("db_alert_id") for a in resp.get("alerts", [])]
        print(f"      total={resp.get('total')}  resolved_ids={resolved_ids}")
        check("?status=resolved returns 200", status == 200, got=f"status={status}")
        check(
            f"alert_id={ALERT_ID} is in resolved list",
            ALERT_ID in resolved_ids,
            got=f"found: {resolved_ids}"
        )

    else:
        print("\n  [C-E] SKIPPED — no field_operator token")
        print("        Run: python scripts/seed_users.py  then re-run")


# ─────────────────────────────────────────────────────────────────
# TEST 6 — Role Enforcement (403 checks)
# ─────────────────────────────────────────────────────────────────

section("TEST 6 — Role Enforcement (403 Checks)")

if not FOP_TOKEN:
    print("  SKIPPED — need field_op token")
    print("  Run: python scripts/seed_users.py  then re-run")
else:
    # field_op must NOT be able to acknowledge (engineer only)
    status, resp = _call(
        "POST", "/alerts/1/acknowledge",
        token=FOP_TOKEN,
        body={"notes": "should be rejected"}
    )
    print(f"  field_op -> /acknowledge          HTTP {status} (expect 403)")
    check("field_op cannot acknowledge (403)", status == 403, got=f"status={status}")

    # engineer must NOT be able to request-resolution (field_operator only)
    status, resp = _call(
        "POST", "/alerts/1/request-resolution",
        token=ENG_TOKEN,
        body={"report": "should be rejected"}
    )
    print(f"  engineer -> /request-resolution   HTTP {status} (expect 403)")
    check("engineer cannot request-resolution (403)", status == 403, got=f"status={status}")

    # field_op must NOT be able to accept-resolution (engineer only)
    status, resp = _call(
        "POST", "/alerts/1/accept-resolution",
        token=FOP_TOKEN,
        body={}
    )
    print(f"  field_op -> /accept-resolution    HTTP {status} (expect 403)")
    check("field_op cannot accept-resolution (403)", status == 403, got=f"status={status}")

    # field_op must NOT be able to reject-resolution (engineer only)
    status, resp = _call(
        "POST", "/alerts/1/reject-resolution",
        token=FOP_TOKEN,
        body={}
    )
    print(f"  field_op -> /reject-resolution    HTTP {status} (expect 403)")
    check("field_op cannot reject-resolution (403)", status == 403, got=f"status={status}")


# ─────────────────────────────────────────────────────────────────
# TEST 7 — Backward compat: old /resolve alias still exists
# ─────────────────────────────────────────────────────────────────

section("TEST 7 — Backward Compat: POST /alerts/{id}/resolve Still Works")

status, resp = _call(
    "POST", "/alerts/99999/resolve",
    token=ENG_TOKEN,
    body={"notes": "compat test with nonexistent id"}
)
print(f"  /alerts/99999/resolve -> HTTP {status}  (must not be 404 or 405)")
check(
    "POST /alerts/{id}/resolve endpoint exists (not 404/405)",
    status not in (404, 405),
    got=f"status={status}"
)


# ─────────────────────────────────────────────────────────────────
# FINAL SUMMARY
# ─────────────────────────────────────────────────────────────────

section("FINAL SUMMARY")

total = PASS_COUNT + FAIL_COUNT
print(f"\n  Total tests  : {total}")
print(f"  PASSED       : {PASS_COUNT}")
print(f"  FAILED       : {FAIL_COUNT}")

if FAIL_COUNT == 0:
    print("\n  ALL M1 CHECKS PASSED")
    print("  Signal B and C to start M2.")
    sys.exit(0)
else:
    print(f"\n  {FAIL_COUNT} check(s) failed. Common fixes:")
    print("")
    print("  If login failed:")
    print("    Kill the server (Ctrl+C), then restart with:")
    print("      $env:AUTH_DEV_MODE = '1'")
    print("      python -m uvicorn backend.app:app --port 8000 --reload")
    print("")
    print("  If 'no alerts in DB':")
    print("      python scripts/db_migrate.py")
    print("")
    print("  If 'no field_op user':")
    print("      python scripts/seed_users.py")
    print("")
    print("  Then re-run:")
    print("      python scripts/verify_m1.py")
    sys.exit(1)