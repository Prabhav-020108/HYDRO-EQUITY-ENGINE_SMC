"""
scripts/patch_map_fix.py
Run ONCE from project root:
    python scripts/patch_map_fix.py

Fixes: resolved alerts still showing ! markers on map in engineer + ward dashboards.
Patches exactly 2 lines — does not touch any other code.
"""

import os, sys, shutil

ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FRONT   = os.path.join(ROOT, 'frontend')

ENG  = os.path.join(FRONT, 'engineer_dashboard.html')
WARD = os.path.join(FRONT, 'ward_dashboard.html')

# ── Patch 1: engineer_dashboard.html ─────────────────────────────────────────
# In renderAlerts(), map markers are added with al.forEach(...)
# Change to al.filter(not-resolved).forEach(...)
ENG_OLD = "    al.filter(a=>(a.status||'new').toLowerCase()!=='resolved').forEach(a=>{"
ENG_NEW = "    al.filter(a=>(a.status||'new').toLowerCase()!=='resolved').forEach(a=>{"  # already patched check

ENG_OLD_ORIG = "    al.forEach(a=>{"  # original line to find

def patch_engineer():
    with open(ENG, 'r', encoding='utf-8') as f:
        src = f.read()

    # Check if already patched
    if "al.filter(a=>(a.status||'new').toLowerCase()!=='resolved').forEach" in src:
        print("[engineer_dashboard.html] Already patched — skipping.")
        return

    # The marker block starts with this exact line (inside the if scen!=='baseline' block)
    OLD = "    al.forEach(a=>{\n      const zc=ZONES.find(z=>z.id===(a.zone_id_short||''));"
    NEW = "    al.filter(a=>(a.status||'new').toLowerCase()!=='resolved').forEach(a=>{\n      const zc=ZONES.find(z=>z.id===(a.zone_id_short||''));"

    if OLD not in src:
        print("[engineer_dashboard.html] WARN: target line not found. Trying alternate whitespace...")
        OLD2 = "    al.forEach(a=>{"
        if OLD2 in src:
            src = src.replace(
                OLD2,
                "    al.filter(a=>(a.status||'new').toLowerCase()!=='resolved').forEach(a=>{",
                1  # replace only FIRST occurrence inside renderAlerts (the marker block)
            )
            # But first occurrence might be the card render, not the marker.
            # Let's be safe: find the one inside 'if(scen!=..baseline' block.
            # Revert and do it properly with context.
            with open(ENG, 'r', encoding='utf-8') as f:
                src = f.read()
            # Find the marker section by its surrounding unique context
            MARKER_CONTEXT_OLD = (
                "  // Map alert markers for non-baseline scenarios\n"
                "  if(scen!=='baseline'){\n"
                "    const li=L.divIcon({html:`<div class=\"mlk\">!</div>`,className:'',iconSize:[22,22],iconAnchor:[11,11]});\n"
                "    al.forEach(a=>{"
            )
            MARKER_CONTEXT_NEW = (
                "  // Map alert markers for non-baseline scenarios — skip resolved\n"
                "  if(scen!=='baseline'){\n"
                "    const li=L.divIcon({html:`<div class=\"mlk\">!</div>`,className:'',iconSize:[22,22],iconAnchor:[11,11]});\n"
                "    al.filter(a=>(a.status||'new').toLowerCase()!=='resolved').forEach(a=>{"
            )
            if MARKER_CONTEXT_OLD in src:
                src = src.replace(MARKER_CONTEXT_OLD, MARKER_CONTEXT_NEW, 1)
                shutil.copy(ENG, ENG + '.bak')
                with open(ENG, 'w', encoding='utf-8') as f:
                    f.write(src)
                print("[engineer_dashboard.html] Patched (context match).")
            else:
                print("[engineer_dashboard.html] ERROR: Could not find target. Patch manually (see instructions).")
        return

    shutil.copy(ENG, ENG + '.bak')
    src = src.replace(OLD, NEW, 1)
    with open(ENG, 'w', encoding='utf-8') as f:
        f.write(src)
    print("[engineer_dashboard.html] Patched OK. Backup: engineer_dashboard.html.bak")


# ── Patch 2: ward_dashboard.html ─────────────────────────────────────────────
# In loadAlerts(), map marker is added unconditionally.
# Wrap it with: if (alertStatus !== 'resolved') { ... }

def patch_ward():
    with open(WARD, 'r', encoding='utf-8') as f:
        src = f.read()

    if "// Map marker — skip resolved" in src:
        print("[ward_dashboard.html] Already patched — skipping.")
        return

    OLD = (
        "    // Map marker (only for non-baseline scenarios, consistent with engineer dashboard)\n"
        "    L.marker([myZone.lat, myZone.lon], { icon: li, zIndexOffset: 200 })\n"
        "      .bindPopup(`<div class=\"ptit2\" style=\"color:${col}\">⚠ ${a.title}</div>\n"
        "        <div class=\"prow\"><span class=\"pkey\">CLPS</span><span class=\"pval\">${clps.toFixed(3)}</span></div>\n"
        "        <div class=\"prow\"><span class=\"pkey\">Signal</span><span class=\"pval\">${a.dominant_signal}</span></div>`)\n"
        "      .addTo(lLG);"
    )
    NEW = (
        "    // Map marker — skip resolved alerts so they disappear from map\n"
        "    if (alertStatus !== 'resolved') {\n"
        "      L.marker([myZone.lat, myZone.lon], { icon: li, zIndexOffset: 200 })\n"
        "        .bindPopup(`<div class=\"ptit2\" style=\"color:${col}\">⚠ ${a.title}</div>\n"
        "          <div class=\"prow\"><span class=\"pkey\">CLPS</span><span class=\"pval\">${clps.toFixed(3)}</span></div>\n"
        "          <div class=\"prow\"><span class=\"pkey\">Signal</span><span class=\"pval\">${a.dominant_signal}</span></div>`)\n"
        "        .addTo(lLG);\n"
        "    }"
    )

    if OLD not in src:
        print("[ward_dashboard.html] WARN: exact string not found — trying relaxed match...")
        # Relaxed: find marker by popup content
        import re
        # Find and replace the marker block
        pattern = (
            r'(    // Map marker \(only for non-baseline scenarios[^\n]*\n)'
            r'(    L\.marker\(\[myZone\.lat, myZone\.lon\][^;]+;)'
        )
        replacement = (
            "    // Map marker — skip resolved alerts so they disappear from map\n"
            "    if (alertStatus !== 'resolved') {\n"
            "      L.marker([myZone.lat, myZone.lon], { icon: li, zIndexOffset: 200 })\n"
            "        .bindPopup(`<div class=\"ptit2\" style=\"color:${col}\">⚠ ${a.title}</div>\n"
            "          <div class=\"prow\"><span class=\"pkey\">CLPS</span><span class=\"pval\">${clps.toFixed(3)}</span></div>\n"
            "          <div class=\"prow\"><span class=\"pkey\">Signal</span><span class=\"pval\">${a.dominant_signal}</span></div>`)\n"
            "        .addTo(lLG);\n"
            "    }"
        )
        new_src, n = re.subn(pattern, replacement, src, count=1, flags=re.DOTALL)
        if n == 0:
            print("[ward_dashboard.html] ERROR: Could not patch. Apply manually (see instructions).")
            return
        src = new_src

    else:
        src = src.replace(OLD, NEW, 1)

    shutil.copy(WARD, WARD + '.bak')
    with open(WARD, 'w', encoding='utf-8') as f:
        f.write(src)
    print("[ward_dashboard.html] Patched OK. Backup: ward_dashboard.html.bak")


if __name__ == '__main__':
    errors = []
    for path, label in [(ENG, 'engineer_dashboard.html'), (WARD, 'ward_dashboard.html')]:
        if not os.path.exists(path):
            print(f"ERROR: {label} not found at {path}")
            errors.append(label)

    if errors:
        print("\nMake sure you run this from the solapur_water_project root.")
        sys.exit(1)

    patch_engineer()
    patch_ward()
    print("\nDone. Refresh your browser — resolved alerts will no longer show map markers.")