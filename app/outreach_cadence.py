"""Canonical Conduit pre-show outreach cadence.

This module only describes and schedules outreach.  It deliberately does not
call Smartlead or Pipedrive, create activities, or send messages.  Integrations
can consume the plan and persist their own identifiers and delivery state.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import Enum
from typing import TypeAlias


class CadenceSystem(str, Enum):
    SMARTLEAD = "smartlead"
    PIPEDRIVE = "pipedrive"


class CadenceAction(str, Enum):
    EMAIL = "email"
    LINKEDIN_MESSAGE = "linkedin_message"
    CALL = "call"


class CadenceState(str, Enum):
    ACTIVE = "active"
    STOPPED = "stopped"


@dataclass(frozen=True)
class CadenceStopMetadata:
    """Stop rules and externally recorded stop state.

    Smartlead and Pipedrive share an enrollment date, not a live event sync.
    ``synchronize_across_systems`` therefore defaults to false: a reply or
    other stop signal remains local to the system that receives it unless an
    integration explicitly opts into cross-system handling later.
    """

    stop_on_reply: bool = True
    stop_on_unsubscribe: bool = True
    stop_on_bounce: bool = True
    synchronize_across_systems: bool = False
    state: CadenceState = CadenceState.ACTIVE
    reason: str | None = None
    recorded_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.state is CadenceState.ACTIVE and (self.reason is not None or self.recorded_at is not None):
            raise ValueError("Active cadence stop metadata cannot contain a stop reason or timestamp.")
        if self.state is CadenceState.STOPPED and not (self.reason or "").strip():
            raise ValueError("Stopped cadence metadata requires a reason.")


@dataclass(frozen=True)
class SmartleadEmailStep:
    key: str
    email_number: int
    day_offset: int
    delay_from_previous_email_days: int

    @property
    def system(self) -> CadenceSystem:
        return CadenceSystem.SMARTLEAD

    @property
    def action(self) -> CadenceAction:
        return CadenceAction.EMAIL

    @property
    def label(self) -> str:
        return f"Smartlead Email {self.email_number}"


@dataclass(frozen=True)
class PipedriveActivityStep:
    key: str
    action: CadenceAction
    activity_number: int
    day_offset: int
    activity_type: str

    @property
    def system(self) -> CadenceSystem:
        return CadenceSystem.PIPEDRIVE

    @property
    def label(self) -> str:
        if self.action is CadenceAction.LINKEDIN_MESSAGE:
            return "Pipedrive LinkedIn Message"
        return f"Pipedrive Call {self.activity_number}"


CadenceStep: TypeAlias = SmartleadEmailStep | PipedriveActivityStep


SMARTLEAD_STEPS = (
    SmartleadEmailStep(
        key="smartlead_email_1",
        email_number=1,
        day_offset=0,
        delay_from_previous_email_days=0,
    ),
    SmartleadEmailStep(
        key="smartlead_email_2",
        email_number=2,
        day_offset=2,
        delay_from_previous_email_days=2,
    ),
    SmartleadEmailStep(
        key="smartlead_email_3",
        email_number=3,
        day_offset=6,
        delay_from_previous_email_days=4,
    ),
)

PIPEDRIVE_ACTIVITY_SCHEDULE = (
    PipedriveActivityStep(
        key="pipedrive_linkedin_message",
        action=CadenceAction.LINKEDIN_MESSAGE,
        activity_number=1,
        day_offset=4,
        activity_type="task",
    ),
    PipedriveActivityStep(
        key="pipedrive_call_1",
        action=CadenceAction.CALL,
        activity_number=1,
        day_offset=5,
        activity_type="call",
    ),
    PipedriveActivityStep(
        key="pipedrive_call_2",
        action=CadenceAction.CALL,
        activity_number=2,
        day_offset=7,
        activity_type="call",
    ),
)

COMBINED_CADENCE: tuple[CadenceStep, ...] = tuple(
    sorted((*SMARTLEAD_STEPS, *PIPEDRIVE_ACTIVITY_SCHEDULE), key=lambda step: step.day_offset)
)


@dataclass(frozen=True)
class ScheduledCadenceStep:
    definition: CadenceStep
    due_on: date

    @property
    def key(self) -> str:
        return self.definition.key

    @property
    def system(self) -> CadenceSystem:
        return self.definition.system

    @property
    def action(self) -> CadenceAction:
        return self.definition.action

    @property
    def label(self) -> str:
        return self.definition.label

    @property
    def day_offset(self) -> int:
        return self.definition.day_offset


@dataclass(frozen=True)
class OutreachCadencePlan:
    requested_start_on: date
    enrollment_day: date
    started_late: bool
    steps: tuple[ScheduledCadenceStep, ...]
    stop_metadata: CadenceStopMetadata

    @property
    def smartlead_steps(self) -> tuple[ScheduledCadenceStep, ...]:
        return tuple(step for step in self.steps if step.system is CadenceSystem.SMARTLEAD)

    @property
    def pipedrive_activities(self) -> tuple[ScheduledCadenceStep, ...]:
        return tuple(step for step in self.steps if step.system is CadenceSystem.PIPEDRIVE)


def build_outreach_plan(
    enrollment_day: date,
    *,
    requested_start_on: date | None = None,
    stop_metadata: CadenceStopMetadata | None = None,
) -> OutreachCadencePlan:
    """Build the cadence relative to the day the lead is actually enrolled."""

    requested_start_on = requested_start_on or enrollment_day
    steps = tuple(
        ScheduledCadenceStep(definition=definition, due_on=enrollment_day + timedelta(days=definition.day_offset))
        for definition in COMBINED_CADENCE
    )
    return OutreachCadencePlan(
        requested_start_on=requested_start_on,
        enrollment_day=enrollment_day,
        started_late=enrollment_day > requested_start_on,
        steps=steps,
        stop_metadata=stop_metadata or CadenceStopMetadata(),
    )


def build_outreach_plan_for_requested_start(
    requested_start_on: date,
    *,
    today: date | None = None,
    stop_metadata: CadenceStopMetadata | None = None,
) -> OutreachCadencePlan:
    """Start on the requested date, or restart the full cadence today if late.

    Past steps are never backfilled or compressed.  If the intended start date
    has passed, today becomes Day 0 and every later step keeps its canonical
    offset from that actual enrollment day.
    """

    today = today or date.today()
    enrollment_day = max(requested_start_on, today)
    return build_outreach_plan(
        enrollment_day,
        requested_start_on=requested_start_on,
        stop_metadata=stop_metadata,
    )
