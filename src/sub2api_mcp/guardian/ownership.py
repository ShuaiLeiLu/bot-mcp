"""Guardian field-ownership rules that protect out-of-band human changes."""

from __future__ import annotations

from .contracts import GuardianFieldName, GuardianFieldOwner, GuardianFieldOwnership


def observe_field_value(
    ownership: GuardianFieldOwnership | None,
    *,
    current_value: int | float | bool | str,
    channel_id: str,
    field_name: GuardianFieldName,
) -> GuardianFieldOwnership:
    if ownership is None:
        return GuardianFieldOwnership(
            channel_id=channel_id,
            field_name=field_name,
            owner=GuardianFieldOwner.UPSTREAM,
            baseline_value=current_value,
        )
    if ownership.owner is GuardianFieldOwner.HUMAN:
        return ownership
    expected = (
        ownership.last_guardian_value
        if ownership.owner is GuardianFieldOwner.GUARDIAN
        else ownership.baseline_value
    )
    if current_value != expected:
        return ownership.model_copy(update={"owner": GuardianFieldOwner.HUMAN})
    return ownership
