"""
Hydro-Equity Engine — Phase 4b M3
backend/data_provider.py

Central data access layer for all V4/V5/V6/V7 output files.
All routers import from here instead of opening JSON files directly.

Functions:
    get_zone_status()          → list of zone dicts (v4_zone_status.json)
    get_alerts(scenario)       → list of alert dicts for given scenario (v5_alerts.json)
    get_burst_top10()          → list of top-10 burst risk segments (v6_burst_top10.json)
    get_recommendations(role)  → dict with the correct sub-key for role (v7_recommendations.json)

Design principles:
    - Every function returns a safe default (empty list / empty dict) if file is missing.
    - No exceptions bubble up to routers — all errors are caught and logged here.
    - Files are read fresh on every call (no module-level caching) so hot-reloaded
      outputs from V4/V5/V6/V7 are always reflected immediately.
"""

import os
import json
import logging

logger = logging.getLogger(__name__)

# ── Output directory (two levels up from this file: backend/ → project root → outputs/) ──
_OUTPUTS = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', 'outputs')
)

# ── File paths ────────────────────────────────────────────────────────
_V4_ZONE_STATUS      = os.path.join(_OUTPUTS, 'v4_zone_status.json')
_V5_ALERTS           = os.path.join(_OUTPUTS, 'v5_alerts.json')
_V6_BURST_TOP10      = os.path.join(_OUTPUTS, 'v6_burst_top10.json')
_V7_RECOMMENDATIONS  = os.path.join(_OUTPUTS, 'v7_recommendations.json')


def _read_json(path: str, label: str):
    """
    Internal helper. Reads a JSON file and returns its parsed content.
    Returns None if the file does not exist or cannot be parsed.
    """
    if not os.path.exists(path):
        logger.warning("[data_provider] %s not found: %s", label, path)
        return None
    try:
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    except Exception as exc:
        logger.error("[data_provider] Failed to read %s: %s", label, exc)
        return None


# ══════════════════════════════════════════════════════════════════════
#  PUBLIC API
# ══════════════════════════════════════════════════════════════════════

def get_zone_status() -> list:
    """
    Returns the list of zone equity dicts from v4_zone_status.json.

    Each dict contains at minimum:
        zone_id : str   (e.g. "zone_1")
        hei     : float
        status  : str   ("severe" | "moderate" | "equitable" | "over")
        color   : str   (hex colour)

    Returns [] if file is missing or malformed.
    """
    data = _read_json(_V4_ZONE_STATUS, 'v4_zone_status.json')
    if data is None:
        return []
    if isinstance(data, list):
        return data
    # Unexpected format — try common wrapper keys
    for key in ('zones', 'data', 'results'):
        if isinstance(data.get(key), list):
            return data[key]
    logger.warning("[data_provider] v4_zone_status.json has unexpected format: %s", type(data))
    return []


def get_alerts(scenario: str = 'baseline') -> list:
    """
    Returns the list of alert dicts for the given scenario from v5_alerts.json.

    v5_alerts.json is expected to be a dict keyed by scenario name:
        {
            "baseline": [...],
            "leak":     [...],
            "valve":    [...],
            "surge":    [...]
        }

    Falls back to "baseline" if the requested scenario key is absent.
    Returns [] if file is missing or the scenario has no alerts.
    """
    data = _read_json(_V5_ALERTS, 'v5_alerts.json')
    if data is None:
        return []
    if isinstance(data, dict):
        alerts = data.get(scenario)
        if alerts is None:
            logger.warning(
                "[data_provider] Scenario '%s' not found in v5_alerts.json — falling back to 'baseline'",
                scenario
            )
            alerts = data.get('baseline', [])
        return alerts if isinstance(alerts, list) else []
    if isinstance(data, list):
        # Old flat format — return as-is regardless of scenario
        return data
    return []


def get_burst_top10() -> list:
    """
    Returns the top-10 burst risk pipe segments from v6_burst_top10.json.

    v6_burst_top10.json may be either:
        - A bare list: [{"segment_id": ..., "pss": ...}, ...]
        - A dict with a "segments" or "top10" wrapper

    Returns [] if file is missing or malformed.
    """
    data = _read_json(_V6_BURST_TOP10, 'v6_burst_top10.json')
    if data is None:
        return []
    if isinstance(data, list):
        return data
    # Try common wrapper keys
    for key in ('segments', 'top10', 'data'):
        if isinstance(data.get(key), list):
            return data[key]
    logger.warning("[data_provider] v6_burst_top10.json has unexpected format: %s", type(data))
    return []


def get_recommendations(role: str) -> dict:
    """
    Returns the recommendations sub-section for the given role
    from v7_recommendations.json.

    Role → key mapping:
        "engineer"      → "engineer_recs"
        "field_operator"→ "engineer_recs"   (field ops see same recs as engineers)
        "ward_officer"  → "ward_recs"
        "commissioner"  → "commissioner_recs"
        "citizen"       → "citizen_recs"    (used by /recommendations/citizen public endpoint)
        anything else   → "engineer_recs"   (safe default)

    Returns a dict:
        {
            "recs"      : list,    ← the relevant recs list
            "updated_at": str,
            "source"    : str,
        }

    Returns {"recs": [], "updated_at": None, "source": "missing"} if file is absent.
    """
    _ROLE_KEY_MAP = {
        'engineer':       'engineer_recs',
        'field_operator': 'engineer_recs',
        'ward_officer':   'ward_recs',
        'commissioner':   'commissioner_recs',
        'citizen':        'citizen_recs',
    }
    key = _ROLE_KEY_MAP.get(role, 'engineer_recs')

    data = _read_json(_V7_RECOMMENDATIONS, 'v7_recommendations.json')
    if data is None:
        return {"recs": [], "updated_at": None, "source": "missing"}

    recs       = data.get(key, [])
    updated_at = data.get('updated_at')
    source     = data.get('source', 'json')

    return {
        "recs":       recs if isinstance(recs, list) else [],
        "updated_at": updated_at,
        "source":     source,
    }