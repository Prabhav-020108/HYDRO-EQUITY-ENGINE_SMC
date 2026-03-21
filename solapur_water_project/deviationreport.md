Person: C | N2 | Part 3
Bible says: "poll /mobile/alerts every 5 seconds so new acknowledged alerts appear without manual refresh"
I did: Added a _firstPoll flag that suppresses the "New task assigned" toast on the very first load. Without this flag, every alert present at login would trigger the toast immediately, which would be confusing. The polling behaviour after first load is exactly as specified.
Files changed outside my column: No.
Impact on A/B: None. No endpoints changed, no shared files touched.
Status: Done.



Person: B | N2 | Part 1
Bible says: "Add setInterval(renderAlerts, 15000)"
I did: Added inside loadAll() AFTER the initial data load completes,
       not at module level, so the first render finishes before the
       timer fires. Added isRefreshing boolean guard as specified.
Impact on A/C: None. No shared endpoints or HTML touched.
Status: Done.

Person: B | N2 | Part 2
Bible says: "Use the returned alert object from response to update just that card"
I did: Added _updateSingleCard(alertObj) helper that finds the card by
       data-alert-id attribute and replaces the .n2-badge-slot span.
       Called in ackAlert, resolveAlert, acceptResolution, rejectResolution.
       d.alert comes from N1 backend change (all POST endpoints return full alert).
Impact on A/C: None. No backend changes. No shared files touched.
Status: Done.

Person: B | N2 | Part 3
Bible says: "Status badges: NEW=grey, ACKNOWLEDGED=blue,
             RESOLVE_REQUESTED=orange, RESOLVED=green"
I did: Added _statusBadge() helper returning the correct .sbadge class.
       Badge sits in a <span class="n2-badge-slot"> at top-right of
       each card title row (flex row, justified space-between).
       Card has data-status attribute for _scrollToFirstPending().
Impact on A/C: None. CSS classes are scoped to this file only.
Status: Done.

Person: B | N2 | Part 4
Bible says: "Sticky orange banner: X alert(s) pending your review"
I did: Added <div id="pendingBanner"> in HTML above #albody.
       _renderPendingBanner(alertsArray) called at end of renderAlerts().
       _scrollToFirstPending() called on banner click — uses
       data-status="resolve_requested" selector + scrollIntoView +
       temporary orange outline for visual feedback.
Impact on A/C: None. Banner div is inside engAlertBody which is
       Person B's panel only.
Status: Done.

Person: A | N3
Bible says: "All values must come from data_provider.py functions"
I did: All 5 data sources now go through data_provider:
       get_zone_status(), get_alerts("baseline"), get_burst_top10().
       NRW read from v4_equity_minimal.json via _read_nrw() helper.
       datetime.now() for report timestamp.
       Graceful empty-table fallback when output files are missing
       (Bible says "PDF generates even if files missing" — I used
       empty table rows with run-command notes instead of crashing).
Files changed outside my column: No.
Impact on B/C: None. No endpoints, DB schema, or shared files changed.
       B and C call GET /reports/weekly — response shape unchanged
       (still returns a PDF binary). They will now get real data
       instead of an error.
Status: Done and tested.


Person: A | N4 | No deviations — all changes match Bible spec exactly.

Person: C | N4 | Part 1
Bible says: "NRW percentage must come from API, not hardcoded string"
I did: Added public GET /nrw endpoint to infrastructure.py that reads
       from outputs/v4_equity_minimal.json with fallback to '18% (baseline estimate)'.
       Fixed commissioner_dashboard.html to fetch NRW via loadNrw() called in
       loadAll() before parallel data fetch. Replaced both hardcoded '18%' strings
       with _nrwValue variable. Also fixed engineer_dashboard.html (Person B file)
       same way — replaced scenario-based hardcoded calculation with _nrwValue.
Files changed outside my column: engineer_dashboard.html (Person B file) — NRW fix only,
       no other logic touched.
Impact on A/B: None. /nrw is a new public endpoint, does not change any existing
       endpoint response shapes. B's alert/recommendation logic untouched.
Status: Done.

Person: B | N4
Bible says: "Audit engineer_dashboard.html for hardcoded zone names, HEI numbers,
             pressure values. Replace any found with dynamic values fetched from
             /zones and /alerts/active. Confirm auto-refresh is working every 15s."
I did: Audit completed. One real violation found (NRW hardcoded calculation).
       NRW fix was applied in N4 Part 1 (const nrw = _nrwValue, reads from /nrw API).
       Auto-refresh confirmed working — setInterval(renderAlerts, 15000) already
       in loadAll() from N2. Verified in browser DevTools Network tab.
       No other violations found:
         - ESRS array: map config, allowed per Bible
         - formulaP() constants: map rendering only, not shown as analytics text
         - getTLData() base values: already marked "(illustrative)" in UI label
         - Zone status labels: derived from /zones API hei value, not hardcoded
Files changed outside my column: None.
Impact on A/C: None. No endpoints or shared files changed.
Status: Done and tested.

Person: C | N4 | Part 2
Bible says: "Update integration_test.sh to include field operator lifecycle,
             auto-refresh verification, commissioner count test"
I did: Added 3 N4-C test sections to integration_test.sh:
       (1) Full field op mobile lifecycle: engineer ack → poll mobile/alerts
           → mobile resolve → check resolve_requested → accept-resolution
           → check resolved. Uses a fresh ALERT_C fetched dynamically.
       (2) Auto-refresh / no-caching test: acknowledge an alert, immediately
           curl /alerts/active (no sleep), verify status=acknowledged in
           response. Proves N2 Task 1 (no in-memory cache in Branch B).
       (3) Commissioner count test: verify /alerts/active?status=acknowledged
           returns count > 0 after Step 1a runs.
       Did NOT duplicate Person A's ward login, PDF, or mock-data tests.
Files changed outside my column: None. Only scripts/integration_test.sh.
Impact on A/B: None. No backend or frontend code changed.
Status: Done and tested.

Deviation Report: Person:C | F0 | Bible says:Replace every occurrence of 'http://localhost:8000' with API_BASE in fetch calls... remove locally defined API_BASE constant | I did:Replaced const BACK = 'http://localhost:8000' with const BACK = API_BASE (fetch calls already used BACK as the base URL), and did not find any locally defined API_BASE constant to remove | Impact on A/B/D:None. The existing fetch architecture was preserved, simply swapping the hardcoded base for the dynamic config.js variable.

Deviation Report: Person:D | F0 | Bible says:Replace every occurrence of 'http://localhost:8000' with API_BASE in fetch calls... remove const BACK = '...' | I did:Replaced const BACK = 'http://localhost:8000' with const BACK = API_BASE (since the fetch calls already used BACK as their base URL) | Impact on A/B/C:None. The existing API fetching architecture using the 'api' helper object was preserved, just swapping the hardcoded URL string for the dynamic environment variable.




# Deviation Report — Team Devsters · Dhara Hydro-Equity Engine

---

## Person: C | F0
**Bible says:** Replace every occurrence of `http://localhost:8000` with `API_BASE` in fetch calls; remove locally defined `API_BASE` constant.
**I did:** Replaced `const BACK = 'http://localhost:8000'` with `const BACK = API_BASE` since all fetch calls already used `BACK` as base URL. No locally defined `API_BASE` was found to remove.
**Impact on A/B/D:** None. Existing fetch architecture preserved.

---

## Person: C | F2 | Feature 1 — Valve Checklist
**Bible says:** After alerts list, add Valve Checklist section. On login call `GET /mobile/valves`. Render valve cards with state badge, checked-by info, Mark Open / Mark Closed buttons. Update badge without re-fetching full list.
**I did:** Implemented exactly as specified. Used `callOrQueue()` for valve check button clicks so offline queuing works.
**Impact on A/D:** None. No endpoints changed.

---

## Person: C | F2 | Feature 2 — Photo Attachment
**Bible says:** Add file input inside resolve form. Use FileReader to read as base64, strip prefix, store in variable. Show thumbnail preview. Include `photo_b64` in POST body on submit. Clear photo state after submit.
**I did:** Implemented exactly as specified. Used per-alert-id dict `_photoBase64[id]` to store photos independently per card.
**Impact on A/D:** None. Backend `mobile.py` already accepted `photo_b64` field.

---

## Person: C | F2 | Feature 3 — Work History
**Bible says:** After valve checklist, add Work History section. On login call `GET /mobile/history`. Render last 10 resolved alerts with zone, signal, date.
**I did:** Implemented exactly as specified. Date formatted as `"21 Mar 2026"` using `toLocaleDateString`.
**Impact on A/D:** None.

---

## Person: C | F2 | Feature 4 — Offline Queue
**Bible says:** Create `callOrQueue(endpoint, method, body)` helper. If offline, push to `localStorage['dhara_offline_queue']`. Show toast. Add badge in header showing queue count. `window.addEventListener('online', flushOfflineQueue)`. Replace `startWork`, `submitFixedIt`, valve check button fetches with `callOrQueue`.
**I did:** Implemented exactly as specified. Badge hidden when count is 0.
**Impact on A/D:** None. No backend changes.

---

## Person: C | F2 | Feature 5 — Push Notifications
**Bible says:** On login call `Notification.requestPermission()`. In 5-second poll, compare alert IDs against `knownAlertIds` Set. Fire `new Notification(...)` for new alerts. Do not fire on first poll — initialise Set from first response.
**I did:** Implemented exactly as specified. Used `_firstPoll` boolean flag to suppress first-poll notifications. `notificationsGranted` flag cleared on logout.
**Impact on A/D:** None.

---

## Person: C | F2 | Bug Fixes Applied During Testing

| Bug | Root Cause | Fix |
|-----|-----------|-----|
| "Could not load alerts" | Backend returns `{alerts:[...]}` dict; frontend expected raw array | Parse both shapes: `Array.isArray(raw) ? raw : raw.alerts` |
| "Zone zone_3" display | `zone_id="zone_3"` + `"Zone " + zone_id` = double prefix | Strip `zone_` prefix before prepending "Zone " |
| CLPS score showing "—" | Code read `a.clps_score`; backend sends `a.clps` | Changed field name to `a.clps` |
| Start Work always disabled | Condition `!== 'new'` but all mobile alerts have `status='acknowledged'` | Enable for both `new` and `acknowledged` |
| Submit crash: `column "resolution_photo" does not exist` | `db_setup_alerts.py` does not add this column | Fixed by running `db_full_setup.py` (column now exists) |
| Resolved alerts still show `!` map marker | `al.forEach` iterated all alerts including resolved | Filter to `status !== 'resolved'` before adding map markers |

---

## Files Changed — F2

| File | Change |
|------|--------|
| `frontend/field_operator_app.html` | Full F2 rewrite — all 5 features added |
| `frontend/engineer_dashboard.html` | Map marker filter: skip resolved alerts |
| `frontend/ward_dashboard.html` | Map marker filter: skip resolved alerts |
| `scripts/patch_map_fix.py` | One-time patcher script for map marker fix |

**Files NOT changed:** `backend/routers/mobile.py`, `backend/app.py`, any DB schema files.