"""
Hydro-Equity Engine — Phase 4b + N3
backend/routers/reports.py

N3 CHANGES (Person A):
  - generate_pdf() fully rewritten to use data_provider functions.
  - Zero hardcoded zone names, HEI numbers, or alert counts.
  - NRW read from outputs/v4_equity_minimal.json if the key exists,
    else shows "18% (baseline estimate)".
  - PDF generates cleanly even when output files are missing —
    tables show "Run V4/V5/V6 first to populate" instead of crashing.
  - reportlab must be installed: pip install reportlab

GET /reports/weekly     → commissioner or engineer role required
GET /reports/alert-log  → commissioner or engineer role required (CSV)
"""

import os
import io
import csv
import json
import logging
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

from backend.auth import get_current_user
from backend.database import engine
from backend import data_provider
from sqlalchemy import text

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/reports", tags=["Reports"])

OUTPUTS = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..', 'outputs')
)


# ── Shared helpers ────────────────────────────────────────────────

def _hei_label(hei: float) -> str:
    """Convert numeric HEI to text status label."""
    if hei < 0.70:  return 'SEVERE'
    if hei < 0.85:  return 'MODERATE'
    if hei <= 1.30: return 'EQUITABLE'
    return 'OVER-PRESSURE'


def _read_nrw() -> str:
    """
    N3: Read NRW from v4_equity_minimal.json if the key exists.
    Falls back to '18% (baseline estimate)' if file or key is missing.
    """
    path = os.path.join(OUTPUTS, 'v4_equity_minimal.json')
    if not os.path.exists(path):
        return '18% (baseline estimate)'
    try:
        with open(path, encoding='utf-8') as f:
            data = json.load(f)
        # Check common key names
        nrw = (
            data.get('nrw_pct') or
            data.get('nrw') or
            data.get('estimated_nrw')
        )
        if nrw is not None:
            # Convert 0.18 → "18%" or "18%" → "18%"
            if isinstance(nrw, (int, float)):
                val = float(nrw)
                if val <= 1.0:
                    val *= 100  # it was a fraction
                return f"{val:.1f}%"
            return str(nrw)
    except Exception:
        pass
    return '18% (baseline estimate)'


# ══════════════════════════════════════════════════════════════════
#  generate_pdf()
#
#  N3 contract (from Bible):
#  ┌─────────────────────────┬──────────────────────────────────┐
#  │ PDF Section             │ Data Source                      │
#  ├─────────────────────────┼──────────────────────────────────┤
#  │ CWEI                    │ data_provider.get_zone_status()  │
#  │ Severe/moderate counts  │ data_provider.get_zone_status()  │
#  │ Zone HEI table          │ data_provider.get_zone_status()  │
#  │ Alert table             │ data_provider.get_alerts("base") │
#  │ Burst risk table        │ data_provider.get_burst_top10()  │
#  │ NRW                     │ v4_equity_minimal.json or "18%"  │
#  │ Report date/time        │ datetime.now()                   │
#  └─────────────────────────┴──────────────────────────────────┘
#  ZERO hardcoded zone names, HEI values, or counts.
#  Graceful: uses empty table with note if output files are missing.
# ══════════════════════════════════════════════════════════════════

def generate_pdf() -> bytes:
    """
    Generate the weekly equity PDF report.

    All data sourced from data_provider functions (N3 requirement).
    Never crashes on missing output files — shows empty table + note.
    Returns bytes ready for StreamingResponse.
    """
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import cm
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.enums import TA_CENTER, TA_LEFT
    from reportlab.platypus import (
        SimpleDocTemplate, Table, TableStyle,
        Paragraph, Spacer, HRFlowable
    )

    # ── N3: Load all data via data_provider ───────────────────────
    # data_provider functions return [] / {} on missing files — safe defaults.
    zones_data      = data_provider.get_zone_status()          # list of zone dicts
    baseline_alerts = data_provider.get_alerts('baseline')     # list of alert dicts
    burst_data      = data_provider.get_burst_top10()          # list of segment dicts
    nrw_display     = _read_nrw()                              # "X%" or "18% (baseline estimate)"

    # ── N3: Compute all summary values from live data (not hardcoded) ─
    heis = [float(z.get('hei', 0) or 0) for z in zones_data]
    cwei = (sum(heis) / len(heis)) if heis else 0.0

    severe_count    = sum(1 for z in zones_data if z.get('status') == 'severe')
    moderate_count  = sum(1 for z in zones_data if z.get('status') == 'moderate')
    equitable_count = sum(1 for z in zones_data if z.get('status') == 'equitable')
    over_count      = sum(1 for z in zones_data if z.get('status') == 'over')

    # Sort alerts by clps descending; take top 8
    baseline_alerts_sorted = sorted(
        baseline_alerts,
        key=lambda a: float(a.get('clps', 0) or 0),
        reverse=True
    )[:8]

    # Sort burst segments by pss descending; take top 10
    burst_data_sorted = sorted(
        burst_data,
        key=lambda s: float(s.get('pss', 0) or 0),
        reverse=True
    )[:10]

    # ── Colours ───────────────────────────────────────────────────
    C_BLUE   = colors.HexColor('#0D5FA8')
    C_RED    = colors.HexColor('#D32F2F')
    C_ORANGE = colors.HexColor('#E65100')
    C_GREEN  = colors.HexColor('#2E7D32')
    C_GREY   = colors.HexColor('#8A96A4')
    C_LIGHT  = colors.HexColor('#F0F4F8')
    C_BORDER = colors.HexColor('#DDE3EA')

    # ── ReportLab setup ───────────────────────────────────────────
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm,
        topMargin=2*cm, bottomMargin=2*cm
    )
    styles = getSampleStyleSheet()

    def _style(name='Normal', **kw):
        s = styles[name].clone(name + str(id(kw)))
        for k, v in kw.items():
            setattr(s, k, v)
        return s

    H1   = _style('Heading1', fontSize=18, textColor=C_BLUE, spaceAfter=4)
    H2   = _style('Heading2', fontSize=13, textColor=C_BLUE, spaceBefore=14, spaceAfter=6)
    BODY = _style('Normal',   fontSize=10, textColor=colors.black, spaceAfter=4, leading=14)
    SMALL= _style('Normal',   fontSize=8,  textColor=C_GREY,  spaceAfter=2)

    # ── Common table style factory ────────────────────────────────
    def _tbl_style():
        return TableStyle([
            ('BACKGROUND',     (0, 0), (-1, 0),  C_BLUE),
            ('TEXTCOLOR',      (0, 0), (-1, 0),  colors.white),
            ('FONTNAME',       (0, 0), (-1, 0),  'Helvetica-Bold'),
            ('FONTSIZE',       (0, 0), (-1, 0),  9),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [C_LIGHT, colors.white]),
            ('GRID',           (0, 0), (-1, -1), 0.5, C_BORDER),
            ('FONTSIZE',       (0, 1), (-1, -1), 9),
            ('TOPPADDING',     (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING',  (0, 0), (-1, -1), 5),
        ])

    # ── Story build ───────────────────────────────────────────────
    story = []

    # Header
    story.append(Paragraph("HYDRO-EQUITY ENGINE",
                            _style('Heading1', fontSize=22, textColor=C_BLUE, spaceAfter=2)))
    story.append(Paragraph(
        "Solapur Municipal Corporation — Weekly Hydraulic Equity Report",
        _style('Normal', fontSize=12, textColor=C_GREY, spaceAfter=2)
    ))
    story.append(Paragraph(
        f"Generated: {datetime.now().strftime('%B %d, %Y at %H:%M')}  |  "
        f"Team Devsters  |  SAMVED-2026",
        SMALL
    ))
    story.append(HRFlowable(width="100%", thickness=2, color=C_BLUE, spaceAfter=16))

    # ── Section 1: Executive Summary ─────────────────────────────
    story.append(Paragraph("EXECUTIVE SUMMARY", H2))

    # N3: All values computed from live data
    cwei_label = _hei_label(cwei)

    summary_rows = [
        ['Metric', 'Value', 'Status'],
        # N3: cwei from data_provider.get_zone_status() mean
        ['City-Wide Equity Index (CWEI)',
         f'{cwei:.3f}  ({int(cwei * 100)}%)',
         cwei_label],
        # N3: zone count from live data
        ['Zones Monitored',              str(len(zones_data)),     '—'],
        # N3: severe count from live data
        ['Severe Inequity Zones',        str(severe_count),
         'URGENT' if severe_count > 0 else 'OK'],
        # N3: moderate count from live data
        ['Moderate Imbalance Zones',     str(moderate_count),
         'REVIEW'  if moderate_count > 0 else 'OK'],
        # N3: equitable count from live data
        ['Equitable Zones',              str(equitable_count),     'GOOD'],
        # N3: alert count from data_provider.get_alerts()
        ['Active Alerts (Baseline)',     str(len(baseline_alerts)), '—'],
        # N3: burst count from data_provider.get_burst_top10()
        ['High Burst Risk Segments',
         str(sum(1 for s in burst_data if s.get('risk_level') == 'HIGH')),
         '—'],
        # N3: NRW from v4_equity_minimal.json or "18% (baseline estimate)"
        ['Estimated NRW',                nrw_display,              'Estimate'],
    ]

    if not zones_data:
        summary_rows.append(['⚠ No zone data', 'Run: python scripts/v4_equity_minimal.py', '—'])

    t = Table(summary_rows, colWidths=[8*cm, 5*cm, 4.5*cm])
    t.setStyle(_tbl_style())
    story.append(t)
    story.append(Spacer(1, 0.4*cm))

    # ── Section 2: Zone HEI Table ─────────────────────────────────
    story.append(Paragraph("ZONE HYDRAULIC EQUITY INDEX (HEI)", H2))
    story.append(Paragraph(
        "HEI = Tail-End Pressure / Zone Average Pressure. "
        "Target: 0.85–1.30 (Equitable). Below 0.70 = Severe inequity.",
        SMALL
    ))
    story.append(Spacer(1, 0.2*cm))

    if zones_data:
        # N3: zones sorted by hei ascending (worst first) from live data
        zones_sorted = sorted(zones_data, key=lambda z: float(z.get('hei', 0) or 0))

        zone_rows = [['Zone ID', 'HEI Score', 'Status', 'Action Required']]
        for z in zones_sorted:
            hei   = float(z.get('hei', 0) or 0)
            lbl   = _hei_label(hei)
            # N3: zone_id from live data, never hardcoded
            zid   = str(z.get('zone_id', z.get('id', '—')))
            zname = zid.replace('_', ' ').title()  # e.g. "zone_3" → "Zone 3"

            action = (
                'URGENT — dispatch team'   if lbl == 'SEVERE'        else
                'Review valve settings'    if lbl == 'MODERATE'      else
                'Reduce upstream pressure' if lbl == 'OVER-PRESSURE'  else
                'Monitor only'
            )
            zone_rows.append([zname, f'{hei:.3f}', lbl, action])

        zt = Table(zone_rows, colWidths=[4*cm, 3.5*cm, 4*cm, 6*cm])
        zt.setStyle(_tbl_style())
        story.append(zt)
    else:
        story.append(Paragraph(
            "⚠ No zone data available. Run: python scripts/v4_equity_minimal.py",
            _style('Normal', fontSize=10, textColor=C_RED, spaceAfter=4)
        ))

    story.append(Spacer(1, 0.4*cm))

    # ── Section 3: Alert Table ────────────────────────────────────
    story.append(Paragraph("ACTIVE ALERTS — BASELINE SCENARIO (V5 CLPS)", H2))

    if baseline_alerts_sorted:
        alert_rows = [['Zone', 'CLPS Score', 'Severity', 'Dominant Signal']]
        for a in baseline_alerts_sorted:
            # N3: all values from live data
            zid      = str(a.get('zone_id', a.get('zone', '—')))
            zname    = zid.replace('_', ' ').title()
            clps_val = float(a.get('clps', 0) or 0)
            severity = str(a.get('severity', a.get('level', '—'))).upper()
            signal   = str(a.get('dominant_signal', a.get('signal', '—')))
            alert_rows.append([zname, f'{clps_val:.3f}', severity, signal])

        at = Table(alert_rows, colWidths=[5*cm, 4*cm, 4*cm, 4.5*cm])
        at.setStyle(_tbl_style())
        story.append(at)
    else:
        story.append(Paragraph(
            "⚠ No alert data. Run: python scripts/v5_clps.py",
            _style('Normal', fontSize=10, textColor=C_ORANGE, spaceAfter=4)
        ))

    story.append(Spacer(1, 0.4*cm))

    # ── Section 4: Burst Risk Table ───────────────────────────────
    story.append(Paragraph("TOP BURST-RISK PIPE SEGMENTS (V6 PSS)", H2))

    if burst_data_sorted:
        burst_rows = [['Segment ID', 'PSS Score', 'Risk Level',
                       'Material', 'Age (yr)', 'Dominant Factor']]
        for s in burst_data_sorted:
            # N3: all values from live data
            seg_id = str(s.get('segment_id', s.get('id', '—')))
            pss    = float(s.get('pss', 0) or 0)
            risk   = str(s.get('risk_level', '—'))
            mat    = str(s.get('material', '—'))
            age    = str(s.get('age', s.get('assumed_age', s.get('assumed_age_years', '—'))))
            dom    = str(s.get('dominant_factor', '—'))
            burst_rows.append([seg_id, f'{pss:.3f}', risk, mat, age, dom])

        bt = Table(burst_rows, colWidths=[3*cm, 3*cm, 3.5*cm, 3*cm, 2.5*cm, 3.5*cm])
        bt.setStyle(_tbl_style())
        story.append(bt)
    else:
        story.append(Paragraph(
            "⚠ No burst risk data. Run: python scripts/v6_pss.py",
            _style('Normal', fontSize=10, textColor=C_ORANGE, spaceAfter=4)
        ))

    # ── Footer ────────────────────────────────────────────────────
    story.append(Spacer(1, 0.6*cm))
    story.append(HRFlowable(width="100%", thickness=1, color=C_BORDER, spaceAfter=8))
    story.append(Paragraph(
        "This report is generated automatically by the Hydro-Equity Engine analytics pipeline. "
        "Data sources: V4 HEI Engine, V5 CLPS Leak Detection, V6 PSS Burst Prediction. "
        "All values are live-computed — nothing is hardcoded.",
        SMALL
    ))
    story.append(Paragraph(
        f"Hydro-Equity Engine v4.2  |  SAMVED-2026  |  Team Devsters  |  "
        f"RV College of Engineering, Bengaluru",
        _style('Normal', fontSize=7, textColor=C_GREY,
               alignment=TA_CENTER, spaceAfter=0)
    ))

    doc.build(story)
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════════
#  GET /reports/weekly
# ══════════════════════════════════════════════════════════════════

@router.get(
    "/weekly",
    summary="Download weekly equity report as PDF (N3 — real data)",
    description=(
        "Generates and downloads a PDF equity report. "
        "All values sourced from data_provider (V4/V5/V6 outputs). "
        "reportlab must be installed: pip install reportlab. "
        "Protected: commissioner or engineer role required."
    )
)
def get_weekly_report(current_user: dict = Depends(get_current_user)):
    role = current_user.get('role', '')
    if role not in ('commissioner', 'engineer'):
        raise HTTPException(
            status_code=403,
            detail="Access denied. commissioner or engineer role required."
        )

    try:
        pdf_bytes = generate_pdf()
    except ImportError:
        raise HTTPException(
            status_code=500,
            detail=(
                "ReportLab not installed. Run in your terminal: "
                "pip install reportlab --break-system-packages"
            )
        )
    except Exception as e:
        logger.error("[reports/weekly] PDF generation failed: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"PDF generation failed: {e}"
        )

    filename = f"hydro_equity_report_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


# ══════════════════════════════════════════════════════════════════
#  GET /reports/alert-log  (unchanged from M4 — fully real data)
# ══════════════════════════════════════════════════════════════════

@router.get(
    "/alert-log",
    summary="Download full alert history as CSV",
    description=(
        "Returns all alerts from the PostgreSQL alerts table as a downloadable CSV. "
        "Columns: alert_id, zone_id, clps, severity, dominant_signal, status, "
        "created_at, acknowledged_at, acknowledged_by, resolution_report, resolved_at. "
        "Protected: commissioner or engineer role required."
    )
)
def get_alert_log(current_user: dict = Depends(get_current_user)):
    role = current_user.get('role', '')
    if role not in ('commissioner', 'engineer'):
        raise HTTPException(
            status_code=403,
            detail="Access denied. commissioner or engineer role required."
        )

    CSV_COLUMNS = [
        'alert_id', 'zone_id', 'clps', 'severity', 'dominant_signal',
        'status', 'created_at', 'acknowledged_at', 'acknowledged_by',
        'resolution_report', 'resolved_at',
    ]

    output = io.StringIO()
    writer = csv.DictWriter(
        output, fieldnames=CSV_COLUMNS,
        extrasaction='ignore', lineterminator='\n'
    )
    writer.writeheader()

    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT alert_id, zone_id, clps, severity, dominant_signal,
                       status, created_at, acknowledged_at, acknowledged_by,
                       resolution_report, resolved_at
                FROM   alerts
                ORDER  BY created_at DESC
            """)).fetchall()

        for row in rows:
            writer.writerow({
                'alert_id':          row[0],
                'zone_id':           row[1]  or '',
                'clps':              round(float(row[2] or 0), 4),
                'severity':          row[3]  or '',
                'dominant_signal':   row[4]  or '',
                'status':            row[5]  or '',
                'created_at':        row[6].isoformat()  if row[6]  else '',
                'acknowledged_at':   row[7].isoformat()  if row[7]  else '',
                'acknowledged_by':   row[8]  or '',
                'resolution_report': row[9]  or '',
                'resolved_at':       row[10].isoformat() if row[10] else '',
            })

    except Exception as exc:
        logger.error("[reports/alert-log] DB error: %s", exc)
        writer.writerow({
            'alert_id':          'ERROR',
            'zone_id':           str(exc)[:120],
            'clps':              '',
            'severity':          '',
            'dominant_signal':   '',
            'status':            '',
            'created_at':        '',
            'acknowledged_at':   '',
            'acknowledged_by':   'Run db_setup.py and db_migrate.py first',
            'resolution_report': '',
            'resolved_at':       '',
        })

    csv_bytes = output.getvalue().encode('utf-8')
    today     = datetime.now().strftime('%Y-%m-%d')
    filename  = f"alert_log_{today}.csv"

    return StreamingResponse(
        io.BytesIO(csv_bytes),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Type":        "text/csv; charset=utf-8",
        }
    )