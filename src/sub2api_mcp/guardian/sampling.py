"""Deterministic Guardian traffic filtering, de-duplication, and time buckets."""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import UTC, datetime

from .contracts import (
    GuardianEvidenceBucket,
    GuardianSampleSource,
    GuardianTrafficObservation,
    TrafficBucketBuildResult,
)


def _fingerprint(value: GuardianTrafficObservation) -> tuple[object, ...]:
    return (
        value.channel_id,
        value.occurred_at,
        value.event_type,
        value.score,
        value.ttfb_ms,
        value.status_code,
        value.is_monitor_request,
    )


def _bucket_at(value: datetime, bucket_seconds: int) -> datetime:
    timestamp = int(value.astimezone(UTC).timestamp())
    return datetime.fromtimestamp(
        timestamp - timestamp % bucket_seconds,
        tz=UTC,
    )


def _nearest_rank_p95(values: list[int]) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[math.ceil(len(ordered) * 0.95) - 1]


def build_traffic_buckets(
    observations: list[GuardianTrafficObservation],
    *,
    bucket_seconds: int = 60,
) -> TrafficBucketBuildResult:
    if not 30 <= bucket_seconds <= 300:
        raise ValueError("bucket_seconds must be between 30 and 300")

    seen: dict[str, tuple[object, ...]] = {}
    grouped: dict[tuple[str, datetime], list[GuardianTrafficObservation]] = defaultdict(list)
    duplicate_count = 0
    excluded_monitor_count = 0
    unattributed_count = 0

    for observation in observations:
        fingerprint = _fingerprint(observation)
        previous = seen.get(observation.request_id_hash)
        if previous is not None:
            if previous != fingerprint:
                raise ValueError("traffic observation changed for duplicate request hash")
            duplicate_count += 1
            continue
        seen[observation.request_id_hash] = fingerprint
        if observation.is_monitor_request:
            excluded_monitor_count += 1
            continue
        if observation.channel_id is None:
            unattributed_count += 1
            continue
        grouped[
            (observation.channel_id, _bucket_at(observation.occurred_at, bucket_seconds))
        ].append(observation)

    buckets: list[GuardianEvidenceBucket] = []
    for (channel_id, bucket_at), values in sorted(grouped.items()):
        event_count = len(values)
        buckets.append(
            GuardianEvidenceBucket(
                channel_id=channel_id,
                bucket_at=bucket_at,
                score=sum(value.score for value in values) / event_count,
                quality=min(1.0, event_count / 5),
                sources=frozenset({GuardianSampleSource.TRAFFIC}),
                event_count=event_count,
                ttfb_p95_ms=_nearest_rank_p95(
                    [value.ttfb_ms for value in values if value.ttfb_ms is not None]
                ),
            )
        )

    return TrafficBucketBuildResult(
        buckets=tuple(buckets),
        duplicate_count=duplicate_count,
        excluded_monitor_count=excluded_monitor_count,
        unattributed_count=unattributed_count,
    )
