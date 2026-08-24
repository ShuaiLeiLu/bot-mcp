from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

from sub2api_mcp.adapters.langbot import LangBotClient, LangBotRequestError
from sub2api_mcp.contracts import (
    DeliveryPurpose,
    DeliveryTargetCreate,
    DeliveryTargetRecord,
    MediaPolicy,
    NotificationPayload,
    OutboxEventType,
    TargetType,
)
from sub2api_mcp.delivery import DeliveryService, OutboxWorker
from sub2api_mcp.metrics import Metrics
from sub2api_mcp.repository import SqliteRepository


def _target(policy: MediaPolicy = MediaPolicy.AUTO) -> DeliveryTargetRecord:
    now = datetime(2026, 8, 23, tzinfo=UTC)
    return DeliveryTargetRecord(
        delivery_target_id="target-1",
        name="target",
        bot_uuid="bot-1",
        target_type=TargetType.GROUP,
        target_id="opaque-group-id",
        purposes=frozenset({DeliveryPurpose.STATUS}),
        media_policy=policy,
        required=True,
        enabled=True,
        created_at=now,
        updated_at=now,
    )


@pytest.mark.asyncio
async def test_bot_discovery_accepts_arbitrary_adapter_names() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/platform/bots"
        return httpx.Response(
            200,
            json={
                "code": 0,
                "data": {
                    "bots": [
                        {"uuid": "bot-1", "name": "future", "adapter": "future-adapter-v9"},
                        {"uuid": "bot-2", "name": "telegram", "adapter": "telegram"},
                    ]
                },
            },
        )

    client = LangBotClient(
        "https://langbot.example",
        "api-key",
        transport=httpx.MockTransport(handler),
    )

    bots = await client.list_bots()

    assert [bot.adapter for bot in bots] == ["future-adapter-v9", "telegram"]
    await client.close()


@pytest.mark.asyncio
async def test_delivery_uses_common_person_group_message_chain_contract() -> None:
    requests: list[dict[str, object]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={"code": 0, "data": {"sent": True}})

    client = LangBotClient(
        "https://langbot.example",
        "api-key",
        transport=httpx.MockTransport(handler),
    )
    service = DeliveryService(client)

    await service.deliver(_target(MediaPolicy.TEXT_ONLY), NotificationPayload(text="hello"))

    assert requests == [
        {
            "target_type": "group",
            "target_id": "opaque-group-id",
            "message_chain": [{"type": "Plain", "text": "hello"}],
        }
    ]
    await client.close()


@pytest.mark.asyncio
async def test_inline_png_is_sent_as_a_data_uri() -> None:
    requests: list[dict[str, object]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={"code": 0, "data": {"sent": True}})

    client = LangBotClient(
        "https://langbot.example",
        "api-key",
        transport=httpx.MockTransport(handler),
    )
    service = DeliveryService(client)

    await service.deliver(
        _target(MediaPolicy.IMAGE),
        NotificationPayload(text="status", image_base64="aGVsbG8="),
    )

    assert requests[0]["message_chain"] == [  # type: ignore[index]
        {
            "type": "Image",
            "base64": "data:image/png;base64,aGVsbG8=",
        }
    ]
    await client.close()


@pytest.mark.asyncio
async def test_auto_media_policy_falls_back_only_for_explicit_unsupported_media() -> None:
    requests: list[dict[str, object]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        requests.append(body)
        if len(requests) == 1:
            return httpx.Response(422, json={"code": -1, "msg": "file message is unsupported"})
        return httpx.Response(200, json={"code": 0, "data": {"sent": True}})

    client = LangBotClient(
        "https://langbot.example",
        "api-key",
        transport=httpx.MockTransport(handler),
    )
    service = DeliveryService(client)
    payload = NotificationPayload(
        text="video ready",
        file_url="https://video.example/outputs/result.mp4",
        file_name="result.mp4",
    )

    result = await service.deliver(_target(), payload)

    assert result.used_fallback is True
    assert requests[0]["message_chain"] == [  # type: ignore[index]
        {
            "type": "File",
            "url": "https://video.example/outputs/result.mp4",
            "name": "result.mp4",
        }
    ]
    assert requests[1]["message_chain"] == [  # type: ignore[index]
        {
            "type": "Plain",
            "text": "video ready\nhttps://video.example/outputs/result.mp4",
        }
    ]
    await client.close()


@pytest.mark.asyncio
async def test_auto_media_policy_falls_back_after_internal_media_failure() -> None:
    requests: list[dict[str, object]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        requests.append(body)
        if len(requests) == 1:
            return httpx.Response(500, json={"code": -1, "msg": "Internal server error"})
        return httpx.Response(200, json={"code": 0, "data": {"sent": True}})

    client = LangBotClient(
        "https://langbot.example",
        "api-key",
        transport=httpx.MockTransport(handler),
    )
    service = DeliveryService(client)
    payload = NotificationPayload(
        text="status with trigger time",
        image_base64="aGVsbG8=",
    )

    result = await service.deliver(_target(), payload)

    assert result.used_fallback is True
    assert requests[0]["message_chain"] == [  # type: ignore[index]
        {
            "type": "Image",
            "base64": "data:image/png;base64,aGVsbG8=",
        }
    ]
    assert requests[1]["message_chain"] == [  # type: ignore[index]
        {"type": "Plain", "text": "status with trigger time"}
    ]
    await client.close()


@pytest.mark.asyncio
async def test_transient_delivery_failure_does_not_downgrade_media() -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503, json={"code": -1, "msg": "temporarily unavailable"})

    client = LangBotClient(
        "https://langbot.example",
        "api-key",
        transport=httpx.MockTransport(handler),
    )
    service = DeliveryService(client)

    with pytest.raises(LangBotRequestError) as captured:
        await service.deliver(
            _target(),
            NotificationPayload(
                text="video ready",
                file_url="https://video.example/outputs/result.mp4",
                file_name="result.mp4",
            ),
        )

    assert captured.value.retryable is True
    assert calls == 1
    await client.close()


@pytest.mark.asyncio
async def test_outbox_worker_advances_snapshot_after_required_delivery(
    tmp_path: Path,
) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"code": 0, "data": {"sent": True}})

    repository = SqliteRepository(tmp_path / "state.db")
    await repository.initialize()
    target = await repository.upsert_delivery_target(
        DeliveryTargetCreate.model_validate(
            _target(MediaPolicy.TEXT_ONLY).model_dump(
                exclude={"delivery_target_id", "created_at", "updated_at"}
            )
        )
    )
    await repository.set_snapshot("pending", {"version": 1})
    await repository.enqueue_outbox(
        OutboxEventType.STATUS_CHANGED,
        {
            "notification": {"text": "status changed"},
            "deliveredSnapshot": {"version": 1},
        },
        [target.delivery_target_id],
    )
    client = LangBotClient(
        "https://langbot.example",
        "api-key",
        transport=httpx.MockTransport(handler),
    )
    worker = OutboxWorker(repository, DeliveryService(client), Metrics.create())

    assert await worker.run_once("delivery-worker") is True
    assert await repository.outbox_backlog() == 0
    assert await repository.get_snapshot("delivered") == {"version": 1}
    await client.close()


@pytest.mark.asyncio
async def test_malformed_outbox_payload_is_failed_without_crashing_worker(
    tmp_path: Path,
) -> None:
    repository = SqliteRepository(tmp_path / "state.db")
    await repository.initialize()
    target = await repository.upsert_delivery_target(
        DeliveryTargetCreate.model_validate(
            _target(MediaPolicy.TEXT_ONLY).model_dump(
                exclude={"delivery_target_id", "created_at", "updated_at"}
            )
        )
    )
    await repository.enqueue_outbox(
        OutboxEventType.STATUS_CHANGED,
        {"unexpected": "payload"},
        [target.delivery_target_id],
    )
    client = LangBotClient(
        "https://langbot.example",
        "api-key",
        transport=httpx.MockTransport(lambda request: httpx.Response(500)),
    )
    worker = OutboxWorker(repository, DeliveryService(client), Metrics.create())

    assert await worker.run_once("delivery-worker") is True
    assert await repository.outbox_backlog() == 1
    await client.close()
