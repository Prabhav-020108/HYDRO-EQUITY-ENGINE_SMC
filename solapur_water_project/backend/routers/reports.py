"""
Hydro-Equity Engine — Phase 4b + M4
backend/routers/reports.py

GET /reports/weekly     → generates and downloads a PDF equity report
                          Protected: commissioner or engineer role required

GET /reports/alert-log  → downloads full alert history as CSV  (NEW M4)
                          Protected: commissioner or engineer role required
                          Columns: alert_id, zone_id, clps, severity,
                                   dominant_signal, status, created_at,
                                   acknowledged_at, acknowledged_by,
                                   resolution_report, resolved_at
                          Filename: alert_log_YYYY-MM-DD.csv
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
from sqlalchemy import text

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/reports", tags=["Reports"])

OUTPUTS = os.path.join(os.path.dirname(__file__), '..', '..', 'outputs')


# ── Shared helpers ────────────────────────────────────────────────────

def _load_file(name):
    """Load JSON output file. Returns data or None."""
    path = os.path.join(OUTPUTS, name)
    if not os.path.exists(path):
        return None
    with open(path, encoding='utf-8') as f:
        return json.load(f)


def _hei_label(hei):
    if hei < 0.70:  return 'SEVERE'
    if hei < 0.85:  return 'MODERATE'
    if hei <= 1.30: return 'EQUITABLE'
    return 'OVER-PRESSURE'


# ══════════════════════════════════════════════════════════════════════
#  GET /reports/weekly  (existing — completely unchanged)
# ══════════════════════════════════════════════════════════════════════

def generate_pdf() -> bytes:
    """Generate the weekly equity PDF report. Returns bytes."""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import cm
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.enums import TA_CENTER
    from reportlab.platypus import (
        SimpleDocTemplate, Table, TableStyle,
        Paragraph, Spacer, HRFlowable
    )

    zones_data   = _load_file('v4_zone_status.json') or []
    alerts_data  = _load_file('v5_alerts.json') or {}
    burst_data   = _load_file('v6_burst_top10.json') or []

    if isinstance(alerts_data, dict):
        baseline_alerts = alerts_data.get('baseline', [])
    else:
        baseline_alerts = alerts_data if isinstance(alerts_data, list) else []

    heis            = [float(z.get('hei', 0) or 0) for z in zones_data]
    cwei            = sum(heis) / len(heis) if heis else 0
    severe_count    = sum(1 for z in zones_data if z.get('status') == 'severe')
    moderate_count  = sum(1 for z in zones_data if z.get('status') == 'moderate')
    equitable_count = sum(1 for z in zones_data if z.get('status') == 'equitable')

    C_BLUE   = colors.HexColor('#0D5FA8')
    C_RED    = colors.HexColor('#D32F2F')
    C_ORANGE = colors.HexColor('#E65100')
    C_GREEN  = colors.HexColor('#2E7D32')
    C_GREY   = colors.HexColor('#8A96A4')
    C_LIGHT  = colors.HexColor('#F0F4F8')
    C_BORDER = colors.HexColor('#DDE3EA')

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm,
        topMargin=2*cm, bottomMargin=2*cm
    )
    styles = getSampleStyleSheet()

    def style(name='Normal', **kw):
        s = styles[name].clone(name + str(id(kw)))
        for k, v in kw.items():
            setattr(s, k, v)
        return s

    H1    = style('Heading1', fontSize=18, textColor=C_BLUE, spaceAfter=4)
    H2    = style('Heading2', fontSize=13, textColor=C_BLUE, spaceBefore=14, spaceAfter=6)
    BODY  = style('Normal',   fontSize=10, textColor=colors.black, spaceAfter=4, leading=14)
    SMALL = style('Normal',   fontSize=8,  textColor=C_GREY, spaceAfter=2)

    story = []
    story.append(Paragraph(
        "HYDRO-EQUITY ENGINE",
        style('Heading1', fontSize=22, textColor=C_BLUE, spaceAfter=2)
    ))
    story.append(Paragraph(
        "Solapur Municipal Corporation — Weekly Hydraulic Equity Report",
        style('Normal', fontSize=12, textColor=C_GREY, spaceAfter=2)
    ))
    story.append(Paragraph(
        f"Generated: {datetime.now().strftime('%B %d, %Y at %H:%M')}  |  "
        f"Team Devsters  |  SAMVED-2026",
        SMALL
    ))
    story.append(HRFlowable(width="100%", thickness=2, color=C_BLUE, spaceAfter=16))

    story.append(Paragraph("EXECUTIVE SUMMARY", H2))
    summary_data = [
        ['Metric', 'Value', 'Status'],
        ['City-Wide Equity Index (CWEI)', f'{cwei:.3f}  ({int(cwei*100)}%)', _hei_label(cwei)],
        ['Zones Monitored',             str(len(zones_data)),    '—'],
        ['Severe Inequity Zones',       str(severe_count),       'URGENT' if severe_count > 0 else 'OK'],
        ['Moderate Imbalance Zones',    str(moderate_count),     'REVIEW' if moderate_count > 0 else 'OK'],
        ['Equitable Zones',             str(equitable_count),    'GOOD'],
        ['Active Alerts (Baseline)',    str(len(baseline_alerts)), '—'],
        ['High Burst Risk Segments',
            str(sum(1 for s in burst_data if s.get('risk_level') == 'HIGH')), '—'],
        ['Estimated NRW', '18%', 'Baseline estimate'],
    ]
    t = Table(summary_data, colWidths=[8*cm, 5*cm, 4.5*cm])
    t.setStyle(TableStyle([
        ('BACKGROUND',    (0,0),(-1,0),  C_BLUE),
        ('TEXTCOLOR',     (0,0),(-1,0),  colors.white),
        ('FONTNAME',      (0,0),(-1,0),  'Helvetica-Bold'),
        ('FONTSIZE',      (0,0),(-1,0),  10),
        ('ROWBACKGROUNDS',(0,1),(-1,-1), [C_LIGHT, colors.white]),
        ('GRID',          (0,0),(-1,-1), 0.5, C_BORDER),
        ('FONTSIZE',      (0,1),(-1,-1), 9),
        ('TOPPADDING',    (0,0),(-1,-1), 5),
        ('BOTTOMPADDING', (0,0),(-1,-1), 5),
    ]))
    story.append(t)
    story.append(Spacer(1, 0.4*cm))

    story.append(Paragraph("ZONE HYDRAULIC EQUITY INDEX (HEI)", H2))
    story.append(Paragraph(
        "HEI = Tail-End Pressure / Zone Average Pressure. "
        "Target: 0.85–1.30 (Equitable). Below 0.70 = Severe inequity.", SMALL
    ))
    story.append(Spacer(1, 0.2*cm))

    zone_rows = [['Zone ID', 'HEI Score', 'Status', 'Action Required']]
    for z in sorted(zones_data, key=lambda z: float(z.get('hei', 0) or 0)):
        hei    = float(z.get('hei', 0) or 0)
        lbl    = _hei_label(hei)
        action = (
            'URGENT — dispatch team'   if lbl == 'SEVERE'        else
            'Review valve settings'    if lbl == 'MODERATE'      else
            'Reduce upstream pressure' if lbl == 'OVER-PRESSURE'  else
            'Monitor only'
        )
        zone_rows.append([
            z.get('zone_id', '—').replace('_', ' ').title(),
            f'{hei:.3f}', lbl, action,
        ])
    zt = Table(zone_rows, colWidths=[4*cm, 3.5*cm, 4*cm, 6*cm])
    zt.setStyle(TableStyle([
        ('BACKGROUND',    (0,0),(-1,0),  C_BLUE),
        ('TEXTCOLOR',     (0,0),(-1,0),  colors.white),
        ('FONTNAME',      (0,0),(-1,0),  'Helvetica-Bold'),
        ('FONTSIZE',      (0,0),(-1,0),  9),
        ('ROWBACKGROUNDS',(0,1),(-1,-1), [C_LIGHT, colors.white]),
        ('GRID',          (0,0),(-1,-1), 0.5, C_BORDER),
        ('FONTSIZE',      (0,1),(-1,-1), 9),
        ('TOPPADDING',    (0,0),(-1,-1), 5),
        ('BOTTOMPADDING', (0,0),(-1,-1), 5),
    ]))
    story.append(zt)
    story.append(Spacer(1, 0.4*cm))

    story.append(Paragraph("ACTIVE ALERTS — BASELINE SCENARIO (V5 CLPS)", H2))
    if baseline_alerts:
        alert_rows = [['Zone', 'CLPS Score', 'Severity', 'Dominant Signal']]
        for a in baseline_alerts[:8]:
            alert_rows.append([
                a.get('zone_id', '—').replace('_', ' ').title(),
                f"{float(a.get('clps', 0)):.3f}",
                str(a.get('severity', '—')).upper(),
                a.get('dominant_signal', '—'),
            ])
        at = Table(alert_rows, colWidths=[5*cm, 4*cm, 4*cm, 4.5*cm])
        at.setStyle(TableStyle([
            ('BACKGROUND',    (0,0),(-1,0),  C_BLUE),
            ('TEXTCOLOR',     (0,0),(-1,0),  colors.white),
            ('FONTNAME',      (0,0),(-1,0),  'Helvetica-Bold'),
            ('FONTSIZE',      (0,0),(-1,0),  9),
            ('ROWBACKGROUNDS',(0,1),(-1,-1), [C_LIGHT, colors.white]),
            ('GRID',          (0,0),(-1,-1), 0.5, C_BORDER),
            ('FONTSIZE',      (0,1),(-1,-1), 9),
            ('TOPPADDING',    (0,0),(-1,-1), 5),
            ('BOTTOMPADDING', (0,0),(-1,-1), 5),
        ]))
        story.append(at)
    else:
        story.append(Paragraph(
            "No active alerts. Run V5 (v5_clps.py) to generate alert data.", BODY
        ))
    story.append(Spacer(1, 0.4*cm))

    story.append(Paragraph("TOP BURST-RISK PIPE SEGMENTS (V6 PSS)", H2))
    if burst_data:
        burst_rows = [['Segment ID','PSS Score','Risk Level','Material','Age (yr)','Dominant Factor']]
        for s in burst_data[:10]:
            burst_rows.append([
                str(s.get('segment_id', '—')),
                f"{float(s.get('pss', 0)):.3f}",
                str(s.get('risk_level', '—')),
                str(s.get('material', '—')),
                str(s.get('age', s.get('assumed_age', '?'))),
                str(s.get('dominant_factor', '—')),
            ])
        bt = Table(burst_rows, colWidths=[3*cm, 3*cm, 3.5*cm, 3*cm, 2.5*cm, 3.5*cm])
        bt.setStyle(TableStyle([
            ('BACKGROUND',    (0,0),(-1,0),  C_BLUE),
            ('TEXTCOLOR',     (0,0),(-1,0),  colors.white),
            ('FONTNAME',      (0,0),(-1,0),  'Helvetica-Bold'),
            ('FONTSIZE',      (0,0),(-1,0),  8),
            ('ROWBACKGROUNDS',(0,1),(-1,-1), [C_LIGHT, colors.white]),
            ('GRID',          (0,0),(-1,-1), 0.5, C_BORDER),
            ('FONTSIZE',      (0,1),(-1,-1), 8),
            ('TOPPADDING',    (0,0),(-1,-1), 4),
            ('BOTTOMPADDING', (0,0),(-1,-1), 4),
        ]))
        story.append(bt)
    else:
        story.append(Paragraph(
            "No burst risk data. Run V6 (v6_pss.py) to generate data.", BODY
        ))

    story.append(Spacer(1, 0.6*cm))
    story.append(HRFlowable(width="100%", thickness=1, color=C_BORDER, spaceAfter=8))
    story.append(Paragraph(
        "This report is generated automatically by the Hydro-Equity Engine analytics pipeline. "
        "Data sources: V4 HEI Engine, V5 CLPS Leak Detection, V6 PSS Burst Prediction. "
        "For questions contact the engineering team.",
        SMALL
    ))
    story.append(Paragraph(
        "Hydro-Equity Engine v4.0 | SAMVED-2026 | Team Devsters | "
        "RV College of Engineering, Bengaluru",
        style('Normal', fontSize=7, textColor=C_GREY, alignment=TA_CENTER, spaceAfter=0)
    ))
    doc.build(story)
    return buf.getvalue()


@router.get(
    "/weekly",
    summary="Download weekly equity report as PDF",
    description=(
        "Generates and downloads a PDF equity report containing: "
        "CWEI, zone HEI table, alert summary, burst risk segments. "
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
            detail="ReportLab not installed. Run: pip install reportlab"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"PDF generation failed: {e}")

    filename = f"hydro_equity_report_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


# ══════════════════════════════════════════════════════════════════════
#  GET /reports/alert-log  (NEW — M4)
# ══════════════════════════════════════════════════════════════════════

@router.get(
    "/alert-log",
    summary="Download full alert history as CSV (M4)",
    description=(
        "Returns all alerts from the PostgreSQL alerts table as a downloadable CSV. "
        "Columns: alert_id, zone_id, clps, severity, dominant_signal, status, "
        "created_at, acknowledged_at, acknowledged_by, resolution_report, resolved_at. "
        "Filename: alert_log_YYYY-MM-DD.csv. "
        "Protected: commissioner or engineer role required."
    )
)
def get_alert_log(current_user: dict = Depends(get_current_user)):
    """
    Streams the full alerts table as a CSV download.
    Columns match the M4 spec exactly.
    If PostgreSQL is unavailable, returns a valid CSV with a header and one error row.
    """
    role = current_user.get('role', '')
    if role not in ('commissioner', 'engineer'):
        raise HTTPException(
            status_code=403,
            detail="Access denied. commissioner or engineer role required."
        )

    # ── M4 spec column order — do not change ─────────────────────────
    CSV_COLUMNS = [
        'alert_id',
        'zone_id',
        'clps',
        'severity',
        'dominant_signal',
        'status',
        'created_at',
        'acknowledged_at',
        'acknowledged_by',
        'resolution_report',
        'resolved_at',
    ]

    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=CSV_COLUMNS,
        extrasaction='ignore',
        lineterminator='\n'
    )
    writer.writeheader()

    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT
                    alert_id,
                    zone_id,
                    clps,
                    severity,
                    dominant_signal,
                    status,
                    created_at,
                    acknowledged_at,
                    acknowledged_by,
                    resolution_report,
                    resolved_at
                FROM alerts
                ORDER BY created_at DESC
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
        # Write one error row so the file is still a valid CSV
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