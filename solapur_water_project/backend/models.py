"""
Hydro-Equity Engine — Phase 4a
backend/models.py
SQLAlchemy ORM model for the User table.
"""

import uuid
from sqlalchemy import Column, String, Boolean, DateTime, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class User(Base):
    """
    Represents a system user.
    Roles:
        engineer        — full dashboard, all zones, all alerts, all recommendations
        ward_officer    — zone-scoped view (only their zone_id), complaint queue
        commissioner    — city-wide summary, governance reports, theft summary
        field_operator  — mobile alert feed, QR valve verification, field reports
    """
    __tablename__ = "users"

    user_id         = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    username        = Column(String(100), unique=True, nullable=False, index=True)
    hashed_password = Column(Text, nullable=False)
    role            = Column(String(50), nullable=False)   # see docstring above
    zone_id         = Column(String(50), nullable=True)    # only relevant for ward_officer & field_operator
    full_name       = Column(String(200), nullable=True)
    is_active       = Column(Boolean, default=True, nullable=False)
    created_at      = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self):
        return f"<User username={self.username} role={self.role} zone={self.zone_id}>"