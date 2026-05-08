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
    enriched_export_path: Mapped[str] = mapped_column(Text(), default="")
    smartlead_ready_export_path: Mapped[str] = mapped_column(Text(), default="")
    company_count: Mapped[int] = mapped_column(Integer(), default=0)
    failure_count: Mapped[int] = mapped_column(Integer(), default=0)
    clay_table_id: Mapped[str] = mapped_column(Text(), default="")
    clay_table_name: Mapped[str] = mapped_column(Text(), default="")
    clay_table_url: Mapped[str] = mapped_column(Text(), default="")
    clay_last_polled_at: Mapped[datetime | None] = mapped_column(DateTime(), nullable=True)
    clay_last_imported_at: Mapped[datetime | None] = mapped_column(DateTime(), nullable=True)
    clay_total_rows: Mapped[int] = mapped_column(Integer(), default=0)
    clay_ready_rows: Mapped[int] = mapped_column(Integer(), default=0)
    clay_failed_rows: Mapped[int] = mapped_column(Integer(), default=0)
    clay_skipped_rows: Mapped[int] = mapped_column(Integer(), default=0)
    last_error: Mapped[str] = mapped_column(Text(), default="")
    notification_status: Mapped[str] = mapped_column(String(32), default=ProviderStatus.pending.value)
    clay_status: Mapped[str] = mapped_column(String(32), default=ProviderStatus.pending.value)
    heyreach_status: Mapped[str] = mapped_column(String(32), default=ProviderStatus.pending.value)
    smartlead_status: Mapped[str] = mapped_column(String(32), default=ProviderStatus.pending.value)
    smartlead_campaign_id: Mapped[int | None] = mapped_column(Integer(), nullable=True)
    smartlead_campaign_name: Mapped[str] = mapped_column(Text(), default="")
    smartlead_imported_at: Mapped[datetime | None] = mapped_column(DateTime(), nullable=True)
    smartlead_imported_rows: Mapped[int] = mapped_column(Integer(), default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(), server_default=func.now(), onupdate=func.now())

    runs: Mapped[list["CampaignRun"]] = relationship(
        back_populates="show",
        cascade="all, delete-orphan",
        order_by="CampaignRun.created_at.desc()",
    )
    clay_rows: Mapped[list["ClaySyncRow"]] = relationship(
        back_populates="show",
        cascade="all, delete-orphan",
        order_by="ClaySyncRow.id.asc()",
    )
    guide_rows: Mapped[list["ShowGuideRow"]] = relationship(
        back_populates="show",
        cascade="all, delete-orphan",
        order_by="ShowGuideRow.sheet_key.asc(), ShowGuideRow.position.asc(), ShowGuideRow.id.asc()",
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


class ClaySyncRow(Base):
    __tablename__ = "clay_sync_rows"
    __table_args__ = (
        UniqueConstraint("show_id", "clay_row_id", name="uq_clay_sync_rows_show_row"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    show_id: Mapped[int] = mapped_column(ForeignKey("shows.id", ondelete="CASCADE"))
    clay_row_id: Mapped[str] = mapped_column(String(255))
    row_status: Mapped[str] = mapped_column(String(64), default="")
    email: Mapped[str] = mapped_column(String(255), default="")
    row_hash: Mapped[str] = mapped_column(String(64), default="")
    imported_to_smartlead: Mapped[bool] = mapped_column(Boolean(), default=False)
    imported_at: Mapped[datetime | None] = mapped_column(DateTime(), nullable=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(), server_default=func.now(), onupdate=func.now())

    show: Mapped[Show] = relationship(back_populates="clay_rows")


class ShowGuideRow(Base):
    __tablename__ = "show_guide_rows"

    id: Mapped[int] = mapped_column(primary_key=True)
    show_id: Mapped[int] = mapped_column(ForeignKey("shows.id", ondelete="CASCADE"))
    sheet_key: Mapped[str] = mapped_column(String(64))
    source: Mapped[str] = mapped_column(String(32), default="workbook")
    position: Mapped[int] = mapped_column(Integer(), default=0)
    values_json: Mapped[str] = mapped_column(Text(), default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(), server_default=func.now(), onupdate=func.now())

    show: Mapped[Show] = relationship(back_populates="guide_rows")
