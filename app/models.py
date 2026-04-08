from __future__ import annotations

import enum
from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class ShowStatus(str, enum.Enum):
    waiting = "waiting"
    queued = "queued"
    scraping = "scraping"
    ready_for_review = "ready_for_review"
    approved = "approved"
    live = "live"
    failed = "failed"


class RunStatus(str, enum.Enum):
    queued = "queued"
    running = "running"
    success = "success"
    failed = "failed"


class ProviderStatus(str, enum.Enum):
    pending = "pending"
    skipped = "skipped"
    success = "success"
    failed = "failed"


class Show(Base):
    __tablename__ = "shows"
    __table_args__ = (
        UniqueConstraint("source_url", "event_date", name="uq_show_source_date"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(255))
    event_date: Mapped[date] = mapped_column(Date())
    place: Mapped[str] = mapped_column(String(255))
    source_url: Mapped[str] = mapped_column(Text())
    run_offset_days: Mapped[int] = mapped_column(Integer(), default=14)
    run_at: Mapped[datetime] = mapped_column(DateTime())
    status: Mapped[str] = mapped_column(String(32), default=ShowStatus.waiting.value)
    approval_required: Mapped[bool] = mapped_column(Boolean(), default=True)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime(), nullable=True)
    latest_export_path: Mapped[str] = mapped_column(Text(), default="")
    company_count: Mapped[int] = mapped_column(Integer(), default=0)
    failure_count: Mapped[int] = mapped_column(Integer(), default=0)
    last_error: Mapped[str] = mapped_column(Text(), default="")
    notification_status: Mapped[str] = mapped_column(String(32), default=ProviderStatus.pending.value)
    clay_status: Mapped[str] = mapped_column(String(32), default=ProviderStatus.pending.value)
    heyreach_status: Mapped[str] = mapped_column(String(32), default=ProviderStatus.pending.value)
    smartlead_status: Mapped[str] = mapped_column(String(32), default=ProviderStatus.pending.value)
    created_at: Mapped[datetime] = mapped_column(DateTime(), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(), server_default=func.now(), onupdate=func.now())

    runs: Mapped[list["CampaignRun"]] = relationship(
        back_populates="show",
        cascade="all, delete-orphan",
        order_by="CampaignRun.created_at.desc()",
    )


class CampaignRun(Base):
    __tablename__ = "campaign_runs"

    id: Mapped[int] = mapped_column(primary_key=True)
    show_id: Mapped[int] = mapped_column(ForeignKey("shows.id", ondelete="CASCADE"))
    status: Mapped[str] = mapped_column(String(32), default=RunStatus.queued.value)
    output_path: Mapped[str] = mapped_column(Text(), default="")
    company_count: Mapped[int] = mapped_column(Integer(), default=0)
    failure_count: Mapped[int] = mapped_column(Integer(), default=0)
    error_message: Mapped[str] = mapped_column(Text(), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(), server_default=func.now())
    started_at: Mapped[datetime | None] = mapped_column(DateTime(), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(), nullable=True)

    show: Mapped[Show] = relationship(back_populates="runs")
