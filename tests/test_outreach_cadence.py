from __future__ import annotations

from datetime import date, datetime, timezone
import unittest

from app.outreach_cadence import (
    COMBINED_CADENCE,
    PIPEDRIVE_ACTIVITY_SCHEDULE,
    SMARTLEAD_STEPS,
    CadenceAction,
    CadenceState,
    CadenceStopMetadata,
    CadenceSystem,
    build_outreach_plan,
    build_outreach_plan_for_requested_start,
)


class OutreachCadenceTests(unittest.TestCase):
    def test_combined_cadence_has_canonical_order_and_offsets(self) -> None:
        self.assertEqual(
            [(step.key, step.day_offset) for step in COMBINED_CADENCE],
            [
                ("smartlead_email_1", 0),
                ("smartlead_email_2", 2),
                ("pipedrive_linkedin_message", 4),
                ("pipedrive_call_1", 5),
                ("smartlead_email_3", 6),
                ("pipedrive_call_2", 7),
            ],
        )

    def test_smartlead_steps_model_email_sequence_delays(self) -> None:
        self.assertEqual([step.email_number for step in SMARTLEAD_STEPS], [1, 2, 3])
        self.assertEqual([step.day_offset for step in SMARTLEAD_STEPS], [0, 2, 6])
        self.assertEqual([step.delay_from_previous_email_days for step in SMARTLEAD_STEPS], [0, 2, 4])
        self.assertTrue(all(step.system is CadenceSystem.SMARTLEAD for step in SMARTLEAD_STEPS))

    def test_pipedrive_schedule_models_linkedin_then_two_calls(self) -> None:
        self.assertEqual(
            [(step.action, step.day_offset, step.activity_type) for step in PIPEDRIVE_ACTIVITY_SCHEDULE],
            [
                (CadenceAction.LINKEDIN_MESSAGE, 4, "task"),
                (CadenceAction.CALL, 5, "call"),
                (CadenceAction.CALL, 7, "call"),
            ],
        )

    def test_plan_resolves_every_step_from_actual_enrollment_day(self) -> None:
        plan = build_outreach_plan(date(2026, 7, 14))

        self.assertEqual(
            [(step.key, step.due_on) for step in plan.steps],
            [
                ("smartlead_email_1", date(2026, 7, 14)),
                ("smartlead_email_2", date(2026, 7, 16)),
                ("pipedrive_linkedin_message", date(2026, 7, 18)),
                ("pipedrive_call_1", date(2026, 7, 19)),
                ("smartlead_email_3", date(2026, 7, 20)),
                ("pipedrive_call_2", date(2026, 7, 21)),
            ],
        )
        self.assertEqual([step.key for step in plan.smartlead_steps], [step.key for step in SMARTLEAD_STEPS])
        self.assertEqual(
            [step.key for step in plan.pipedrive_activities],
            [step.key for step in PIPEDRIVE_ACTIVITY_SCHEDULE],
        )
        self.assertFalse(plan.started_late)

    def test_late_start_treats_today_as_day_zero_without_catch_up(self) -> None:
        plan = build_outreach_plan_for_requested_start(
            date(2026, 7, 1),
            today=date(2026, 7, 12),
        )

        self.assertEqual(plan.requested_start_on, date(2026, 7, 1))
        self.assertEqual(plan.enrollment_day, date(2026, 7, 12))
        self.assertTrue(plan.started_late)
        self.assertEqual(plan.steps[0].due_on, date(2026, 7, 12))
        self.assertEqual(plan.steps[-1].due_on, date(2026, 7, 19))

    def test_future_requested_start_is_not_pulled_forward(self) -> None:
        plan = build_outreach_plan_for_requested_start(
            date(2026, 7, 20),
            today=date(2026, 7, 12),
        )

        self.assertEqual(plan.enrollment_day, date(2026, 7, 20))
        self.assertFalse(plan.started_late)
        self.assertEqual(plan.steps[0].due_on, date(2026, 7, 20))

    def test_stop_metadata_is_policy_and_state_only(self) -> None:
        stopped_at = datetime(2026, 7, 13, 15, 30, tzinfo=timezone.utc)
        metadata = CadenceStopMetadata(
            state=CadenceState.STOPPED,
            reason="lead replied",
            recorded_at=stopped_at,
        )

        plan = build_outreach_plan(date(2026, 7, 12), stop_metadata=metadata)

        self.assertIs(plan.stop_metadata, metadata)
        self.assertTrue(plan.stop_metadata.stop_on_reply)
        self.assertTrue(plan.stop_metadata.stop_on_unsubscribe)
        self.assertTrue(plan.stop_metadata.stop_on_bounce)
        self.assertFalse(plan.stop_metadata.synchronize_across_systems)
        self.assertEqual(plan.stop_metadata.recorded_at, stopped_at)
        self.assertEqual(len(plan.steps), 6)

    def test_active_stop_metadata_rejects_stop_details(self) -> None:
        with self.assertRaisesRegex(ValueError, "Active cadence"):
            CadenceStopMetadata(reason="lead replied")

    def test_stopped_metadata_requires_reason(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires a reason"):
            CadenceStopMetadata(state=CadenceState.STOPPED)


if __name__ == "__main__":
    unittest.main()
