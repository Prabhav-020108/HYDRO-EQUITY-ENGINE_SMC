#!/usr/bin/env bash
# =============================================================================
# Dhara Water Management System — Full Alert Lifecycle Integration Test
# Backend: http://localhost:8000
# Usage:  bash scripts/integration_test.sh
# =============================================================================

BASE_URL="http://localhost:8000"
PASS=0
FAIL=0

# Reset the alert to 'new' before running tests
PGPASSWORD=admin1234 psql -U postgres -d hydro_equity -c "UPDATE alerts SET status='new', acknowledged_at=NULL, acknowledged_by=NULL, resolution_report=NULL, resolved_at=NULL, rejected_count=0, notes=NULL WHERE alert_id=65;"

# ── Colour helpers ────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "${GREEN}PASS${NC} — $1"; PASS=$((PASS + 1)); }
fail() {
    echo -e "${RED}FAIL${NC} — $1"
    if [ -n "$2" ]; then
        echo "     Raw response: $2"
    fi
    FAIL=$((FAIL + 1))
}

# ── Helper: parse a JSON key from a string using python ─────────────────────
json_get() {
    # json_get '<json_string>' '<key>'
    python -c "
import sys, json
try:
    data = json.loads(sys.argv[1])
    val = data.get(sys.argv[2])
    print(val if val is not None else '')
except Exception as e:
    print('')
" "$1" "$2"
}

# ── Helper: check whether a value appears in a JSON list field ───────────────
json_list_contains_id() {
    # json_list_contains_id '<json_string>' '<list_key>' '<id_value>'
    python -c "
import sys, json
try:
    data = json.loads(sys.argv[1])
    lst  = data if isinstance(data, list) else data.get(sys.argv[2], [])
    target = int(sys.argv[3])
    found = any(
        (item.get('db_alert_id') == target or item.get('alert_id') == target or item.get('id') == target)
        for item in lst
        if isinstance(item, dict)
    )
    print('yes' if found else 'no')
except Exception as e:
    print('no')
" "$1" "$2" "$3"
}

# =============================================================================
# STEP 1 — Login as engineer1
# =============================================================================
STEP="Login as engineer1"
RESP=$(curl -s -X POST "$BASE_URL/auth/login" \
    -H "Content-Type: application/json" \
    -d '{"username":"engineer1","password":"demo123"}')
ENG_TOKEN=$(json_get "$RESP" "access_token")

if [ -n "$ENG_TOKEN" ] && [ "$ENG_TOKEN" != "None" ]; then
    pass "$STEP"
else
    fail "$STEP" "$RESP"
fi

# =============================================================================
# STEP 2 — Login as field_op_z1
# =============================================================================
STEP="Login as field_op_z1"
RESP=$(curl -s -X POST "$BASE_URL/auth/login" \
    -H "Content-Type: application/json" \
    -d '{"username":"field_op_z1","password":"demo123"}')
FOP_TOKEN=$(json_get "$RESP" "access_token")

if [ -n "$FOP_TOKEN" ] && [ "$FOP_TOKEN" != "None" ]; then
    pass "$STEP"
else
    fail "$STEP" "$RESP"
fi

# =============================================================================
# STEP 3 — GET /alerts/active?status=new → at least 1 alert exists
# =============================================================================
STEP="GET /alerts/active?status=new — at least 1 alert exists"
RESP=$(curl -s -X GET "$BASE_URL/alerts/active?status=new" \
    -H "Authorization: Bearer $ENG_TOKEN")
COUNT=$(python -c "
import sys, json
try:
    data = json.loads(sys.argv[1])
    lst = data if isinstance(data, list) else data.get('alerts', data.get('data', []))
    print(len(lst))
except:
    print(0)
" "$RESP")

if [ "$COUNT" -ge 1 ] 2>/dev/null; then
    pass "$STEP"
else
    fail "$STEP" "$RESP"
fi

# =============================================================================
# STEP 4 — POST /alerts/65/acknowledge with engineer token
# =============================================================================
STEP="POST /alerts/65/acknowledge (engineer token)"
RESP=$(curl -s -X POST "$BASE_URL/alerts/65/acknowledge" \
    -H "Authorization: Bearer $ENG_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"notes":"test"}')
SUCCESS=$(json_get "$RESP" "success")

if [ "$SUCCESS" = "True" ] || [ "$SUCCESS" = "true" ]; then
    pass "$STEP"
else
    fail "$STEP" "$RESP"
fi

# =============================================================================
# STEP 5 — GET /alerts/active?status=acknowledged → alert 65 in list
# =============================================================================
STEP="GET /alerts/active?status=acknowledged — alert 65 present"
RESP=$(curl -s -X GET "$BASE_URL/alerts/active?status=acknowledged" \
    -H "Authorization: Bearer $ENG_TOKEN")
HAS=$(json_list_contains_id "$RESP" "alerts" "65")

if [ "$HAS" = "yes" ]; then
    pass "$STEP"
else
    fail "$STEP" "$RESP"
fi

# =============================================================================
# STEP 6 — POST /mobile/alerts/65/resolve with field op token
# =============================================================================
STEP="POST /mobile/alerts/65/resolve (field op token)"
RESP=$(curl -s -X POST "$BASE_URL/mobile/alerts/65/resolve" \
    -H "Authorization: Bearer $FOP_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"report":"test report"}')
SUCCESS=$(json_get "$RESP" "success")

if [ "$SUCCESS" = "True" ] || [ "$SUCCESS" = "true" ]; then
    pass "$STEP"
else
    fail "$STEP" "$RESP"
fi

# =============================================================================
# STEP 7 — GET /alerts/active?status=resolve_requested → alert 65 in list
# =============================================================================
STEP="GET /alerts/active?status=resolve_requested — alert 65 present"
RESP=$(curl -s -X GET "$BASE_URL/alerts/active?status=resolve_requested" \
    -H "Authorization: Bearer $ENG_TOKEN")
HAS=$(json_list_contains_id "$RESP" "alerts" "65")

if [ "$HAS" = "yes" ]; then
    pass "$STEP"
else
    fail "$STEP" "$RESP"
fi

# =============================================================================
# STEP 8 — POST /alerts/65/accept-resolution with engineer token
# =============================================================================
STEP="POST /alerts/65/accept-resolution (engineer token)"
RESP=$(curl -s -X POST "$BASE_URL/alerts/65/accept-resolution" \
    -H "Authorization: Bearer $ENG_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"notes":"test"}')
SUCCESS=$(json_get "$RESP" "success")

if [ "$SUCCESS" = "True" ] || [ "$SUCCESS" = "true" ]; then
    pass "$STEP"
else
    fail "$STEP" "$RESP"
fi

# =============================================================================
# STEP 9 — GET /alerts/active?status=resolved → alert 65 in list
# =============================================================================
STEP="GET /alerts/active?status=resolved — alert 65 present"
RESP=$(curl -s -X GET "$BASE_URL/alerts/active?status=resolved" \
    -H "Authorization: Bearer $ENG_TOKEN")
HAS=$(json_list_contains_id "$RESP" "alerts" "65")

if [ "$HAS" = "yes" ]; then
    pass "$STEP"
else
    fail "$STEP" "$RESP"
fi

# =============================================================================
# STEP 10 — GET /recommendations/citizen (no auth) → "valve" NOT in response
# =============================================================================
STEP="GET /recommendations/citizen — word 'valve' absent"
RESP=$(curl -s -X GET "$BASE_URL/recommendations/citizen")
HAS_VALVE=$(python -c "
import sys
text = sys.argv[1].lower()
print('yes' if 'valve' in text else 'no')
" "$RESP")

if [ "$HAS_VALVE" = "no" ]; then
    pass "$STEP"
else
    fail "$STEP" "$RESP"
fi

# =============================================================================
# Summary
# =============================================================================
TOTAL=$((PASS + FAIL))
echo ""
echo "============================================="
echo " Results: $PASS passed, $FAIL failed  (total $TOTAL)"
echo "============================================="

if [ "$FAIL" -eq 0 ]; then
    exit 0
else
    exit 1
fi
