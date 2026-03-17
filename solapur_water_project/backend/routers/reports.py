"""
Hydro-Equity Engine — Phase 4b
backend/routers/reports.py

GET /reports/weekly → generates and downloads a PDF equity report.
Protected: requires Bearer token (commissioner or engineer).
Generates PDF using ReportLab from live V4/V5/V6 data.

Install: pip install reportlab
"""

import os, json, io
from datetime import datetime
from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse

from backend.auth import get_current_user
from backend.database import engine
from sqlalchemy import text

router = APIRouter(prefix="/reports", tags=["Reports"])

OUTPUTS = os.path.join(os.path.dirname(__file__), '..', '..', 'outputs')


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


def generate_pdf() -> bytes:
    """Generate the weekly equity PDF report. Returns bytes."""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import cm
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    from reportlab.platypus import (
        SimpleDocTemplate, Table, TableStyle,
        Paragraph, Spacer, HRFlowable
    )

    # ── Load data ─────────────────────────────────────────────────
    zones_data  = _load_file('v4_zone_status.json') or []
    alerts_data = _load_file('v5_alerts.json') or {}
    burst_data  = _load_file('v6_burst_top10.json') or []

    if isinstance(alerts_data, dict):
        baseline_alerts = alerts_data.get('baseline', [])
    else:
        baseline_alerts = alerts_data if isinstance(alerts_data, list) else []

    # ── Compute CWEI ──────────────────────────────────────────────
    heis = [float(z.get('hei', 0) or 0) for z in zones_data]
    cwei = sum(heis) / len(heis) if heis else 0
    severe_count   = sum(1 for z in zones_data if z.get('status') == 'severe')
    moderate_count = sum(1 for z in zones_data if z.get('status') == 'moderate')
    equitable_count= sum(1 for z in zones_data if z.get('status') == 'equitable')

    # ── Colors ────────────────────────────────────────────────────
    C_BLUE   = colors.HexColor('#0D5FA8')
    C_RED    = colors.HexColor('#D32F2F')
    C_ORANGE = colors.HexColor('#E65100')
    C_GREEN  = colors.HexColor('#2E7D32')
    C_GREY   = colors.HexColor('#8A96A4')
    C_LIGHT  = colors.HexColor('#F0F4F8')
    C_BORDER = colors.HexColor('#DDE3EA')

    # ── Doc setup ─────────────────────────────────────────────────
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

    H1   = style('Heading1', fontSize=18, textColor=C_BLUE,    spaceAfter=4)
    H2   = style('Heading2', fontSize=13, textColor=C_BLUE,    spaceBefore=14, spaceAfter=6)
    BODY = style('Normal',   fontSize=10, textColor=colors.black, spaceAfter=4, leading=14)
    SMALL= style('Normal',   fontSize=8,  textColor=C_GREY,    spaceAfter=2)
    CTR  = style('Normal',   fontSize=10, alignment=TA_CENTER, spaceAfter=2)
    BOLD = style('Normal',   fontSize=10, fontName='Helvetica-Bold', spaceAfter=4)

    story = []

    # ── Header ────────────────────────────────────────────────────
    story.append(Paragraph("HYDRO-EQUITY ENGINE", style('Heading1', fontSize=22, textColor=C_BLUE, spaceAfter=2)))
    story.append(Paragraph("Solapur Municipal Corporation — Weekly Hydraulic Equity Report", style('Normal', fontSize=12, textColor=C_GREY, spaceAfter=2)))
    story.append(Paragraph(f"Generated: {datetime.now().strftime('%B %d, %Y at %H:%M')}  |  Team Devsters  |  SAMVED-2026", SMALL))
    story.append(HRFlowable(width="100%", thickness=2, color=C_BLUE, spaceAfter=16))

    # ── Executive Summary ─────────────────────────────────────────
    story.append(Paragraph("EXECUTIVE SUMMARY", H2))

    cwei_color = C_RED if cwei < 0.70 else (C_ORANGE if cwei < 0.85 else C_GREEN)
    cwei_label = _hei_label(cwei)

    summary_data = [
        ['Metric', 'Value', 'Status'],
        ['City-Wide Equity Index (CWEI)', f'{cwei:.3f}  ({int(cwei*100)}%)', cwei_label],
        ['Zones Monitored', str(len(zones_data)), '—'],
        ['Severe Inequity Zones', str(severe_count), 'URGENT' if severe_count > 0 else 'OK'],
        ['Moderate Imbalance Zones', str(moderate_count), 'REVIEW' if moderate_count > 0 else 'OK'],
        ['Equitable Zones', str(equitable_count), 'GOOD'],
        ['Active Alerts (Baseline)', str(len(baseline_alerts)), '—'],
        ['High Burst Risk Segments', str(sum(1 for s in burst_data if s.get('risk_level') == 'HIGH')), '—'],
        ['Estimated NRW', '18%', 'Baseline estimate'],
    ]

    t = Table(summary_data, colWidths=[8*cm, 5*cm, 4.5*cm])
    t.setStyle(TableStyle([
        ('BACKGROUND',    (0, 0), (-1, 0),  C_BLUE),
        ('TEXTCOLOR',     (0, 0), (-1, 0),  colors.white),
        ('FONTNAME',      (0, 0), (-1, 0),  'Helvetica-Bold'),
        ('FONTSIZE',      (0, 0), (-1, 0),  10),
        ('ROWBACKGROUNDS',(0, 1), (-1, -1), [C_LIGHT, colors.white]),
        ('GRID',          (0, 0), (-1, -1), 0.5, C_BORDER),
        ('FONTSIZE',      (0, 1), (-1, -1), 9),
        ('TOPPADDING',    (0, 0), (-1, -1), 5),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
    ]))
    story.append(t)
    story.append(Spacer(1, 0.4*cm))

    # ── Zone HEI Table ────────────────────────────────────────────
    story.append(Paragraph("ZONE HYDRAULIC EQUITY INDEX (HEI)", H2))
    story.append(Paragraph(
        "HEI = Tail-End Pressure / Zone Average Pressure. "
        "Target: 0.85–1.30 (Equitable). Below 0.70 = Severe inequity.", SMALL
    ))
    story.append(Spacer(1, 0.2*cm))

    zone_rows = [['Zone ID', 'HEI Score', 'Status', 'Action Required']]
    sorted_zones = sorted(zones_data, key=lambda z: float(z.get('hei', 0) or 0))
    for z in sorted_zones:
        hei    = float(z.get('hei', 0) or 0)
        lbl    = _hei_label(hei)
        action = (
            'URGENT — dispatch team' if lbl == 'SEVERE' else
            'Review valve settings' if lbl == 'MODERATE' else
            'Reduce upstream pressure' if lbl == 'OVER-PRESSURE' else
            'Monitor only'
        )
        zone_rows.append([
            z.get('zone_id', '—').replace('_', ' ').title(),
            f'{hei:.3f}',
            lbl,
            action,
        ])

    zt = Table(zone_rows, colWidths=[4*cm, 3.5*cm, 4*cm, 6*cm])
    zt.setStyle(TableStyle([
        ('BACKGROUND',    (0, 0), (-1, 0),  C_BLUE),
        ('TEXTCOLOR',     (0, 0), (-1, 0),  colors.white),
        ('FONTNAME',      (0, 0), (-1, 0),  'Helvetica-Bold'),
        ('FONTSIZE',      (0, 0), (-1, 0),  9),
        ('ROWBACKGROUNDS',(0, 1), (-1, -1), [C_LIGHT, colors.white]),
        ('GRID',          (0, 0), (-1, -1), 0.5, C_BORDER),
        ('FONTSIZE',      (0, 1), (-1, -1), 9),
        ('TOPPADDING',    (0, 0), (-1, -1), 5),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
    ]))
    story.append(zt)
    story.append(Spacer(1, 0.4*cm))

    # ── Alerts Summary ────────────────────────────────────────────
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
            ('BACKGROUND',    (0, 0), (-1, 0),  C_BLUE),
            ('TEXTCOLOR',     (0, 0), (-1, 0),  colors.white),
            ('FONTNAME',      (0, 0), (-1, 0),  'Helvetica-Bold'),
            ('FONTSIZE',      (0, 0), (-1, 0),  9),
            ('ROWBACKGROUNDS',(0, 1), (-1, -1), [C_LIGHT, colors.white]),
            ('GRID',          (0, 0), (-1, -1), 0.5, C_BORDER),
            ('FONTSIZE',      (0, 1), (-1, -1), 9),
            ('TOPPADDING',    (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ]))
        story.append(at)
    else:
        story.append(Paragraph("No active alerts. Run V5 (v5_clps.py) to generate alert data.", BODY))

    story.append(Spacer(1, 0.4*cm))

    # ── Burst Risk ────────────────────────────────────────────────
    story.append(Paragraph("TOP BURST-RISK PIPE SEGMENTS (V6 PSS)", H2))

    if burst_data:
        burst_rows = [['Segment ID', 'PSS Score', 'Risk Level', 'Material', 'Age (yr)', 'Dominant Factor']]
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
            ('BACKGROUND',    (0, 0), (-1, 0),  C_BLUE),
            ('TEXTCOLOR',     (0, 0), (-1, 0),  colors.white),
            ('FONTNAME',      (0, 0), (-1, 0),  'Helvetica-Bold'),
            ('FONTSIZE',      (0, 0), (-1, 0),  8),
            ('ROWBACKGROUNDS',(0, 1), (-1, -1), [C_LIGHT, colors.white]),
            ('GRID',          (0, 0), (-1, -1), 0.5, C_BORDER),
            ('FONTSIZE',      (0, 1), (-1, -1), 8),
            ('TOPPADDING',    (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        story.append(bt)
    else:
        story.append(Paragraph("No burst risk data. Run V6 (v6_pss.py) to generate data.", BODY))

    story.append(Spacer(1, 0.6*cm))

    # ── Footer ────────────────────────────────────────────────────
    story.append(HRFlowable(width="100%", thickness=1, color=C_BORDER, spaceAfter=8))
    story.append(Paragraph(
        "This report is generated automatically by the Hydro-Equity Engine analytics pipeline. "
        "Data sources: V4 HEI Engine, V5 CLPS Leak Detection, V6 PSS Burst Prediction. "
        "For questions contact the engineering team.",
        SMALL
    ))
    story.append(Paragraph(
        "Hydro-Equity Engine v4.0 | SAMVED-2026 | Team Devsters | RV College of Engineering, Bengaluru",
        style('Normal', fontSize=7, textColor=C_GREY, alignment=TA_CENTER, spaceAfter=0)
    ))

    doc.build(story)
    return buf.getvalue()


# ── GET /reports/weekly ────────────────────────────────────────────
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
        from fastapi import HTTPException
        raise HTTPException(
            status_code=403,
            detail="Access denied. commissioner or engineer role required."
        )

    try:
        pdf_bytes = generate_pdf()
    except ImportError:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=500,
            detail="ReportLab not installed. Run: pip install reportlab"
        )
    except Exception as e:
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=f"PDF generation failed: {e}")

    filename = f"hydro_equity_report_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )