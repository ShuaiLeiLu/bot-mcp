# PRD: Guardian 异常状态与渠道错误驱动的账号恢复

Status: Approved; implementation in progress on main
Date: 2026-08-25
Owner: Guardian
Migration type: Strangler cutover followed by legacy removal

## 1. Objective

将现有 Scheduler 的普通错误账号恢复，以及账号隔离生命周期中的复测/回池，
统一迁移到 Guardian，并取消对正常账号的无条件主动探测。迁移完成后：

- Scheduler 只负责周期性渠道采集、共享快照发布和必要的账号隔离检测；
- Guardian 是唯一账号恢复调度者；
- 定时账号清单发现 error/disabled/inactive 时，只探测这些异常账号；
- 渠道明确进入 failed/error 时，创建一次渠道全账号检查事件；
- Guardian 对该渠道唯一映射分组内的全部非人工暂停账号检查一次；
- 明确成功的账号恢复 active + schedulable，明确失败的账号关闭调度；
- 超时、网络错误和不完整响应保持原状；
- 正常且可调度账号平时零测试；
- 每轮扫描都复测仍处于异常状态的账号；异常测试不通不产生模型消耗；
- 同一持续渠道错误周期不重复执行全量检查；
- 普通 error 账号、渠道失败隔离账号和慢首字隔离账号保留各自来源记录；
- 人工暂停、人工关闭、过期、限流、过载、临时不可调度和归属不明确账号永不自动恢复；
- 现有 MCP 工具名称保持兼容，内部改为提交 Guardian 恢复任务。

成功标准是生产中不再存在“无条件遍历测试账号”的周期恢复任务。每一次账号测试
必须由异常账号状态或新的渠道 failed/error episode 触发，并能追溯到 Guardian
run、触发来源、策略 revision 和账号结果账本。

## 2. Current State

当前存在三条相关路径：

1. SchedulerService.handle_probe() 每 60 秒发布共享快照后，根据旧环境配置创建
   JobType.RECOVERY。
2. SchedulerService.handle_recovery() 调用 LegacySub2APIAdapter.recover()；
   适配器内部拥有时间窗、候选筛选、内存轮转和每轮上限。
3. 账号隔离生命周期由 MAINTENANCE 任务复测系统隔离账号并执行回池。

Guardian 当前拥有渠道级评分、置信度、熔断恢复选择和恢复探测账本，但尚未真正执行
Sub2API 账号恢复。继续保留这些并行路径会导致：

- 恢复所有权和配置分散；
- 普通错误账号与隔离账号使用不同调度锁和预算；
- Scheduler 每轮恢复可能与 Guardian 恢复候选重复；
- 内存轮转在重启后丢失；
- 管理页面无法给出统一的候选、预算、结果和阻塞原因。

## 3. Assumptions

1. “全部集成到 Guardian”指自动恢复的编排、预算、状态、通知、审计和可见性全部归
   Guardian；已验证的 Sub2API HTTP 测试/恢复底层方法继续复用，不复制协议实现。
2. 渠道周期采集仍由 Scheduler 每 60 秒执行，避免 Guardian 再次访问同一监控接口；
   这不是账号主动探测。
3. 账号隔离的产生仍属于维护/保护逻辑；隔离账号只在其所属渠道进入错误时参与本轮
   全账号检查，不再独立定时复测。
4. JobType.RECOVERY 暂时保留作为耐久任务类型和 MCP 兼容合同，但其处理器改为
   Guardian；在后续大版本才考虑重命名。
5. 旧恢复时间窗和每轮 5 个账号的内存轮转废弃；定时清单读取保留，每轮只对异常
   状态账号触发测试。
6. Guardian 不再提供 observe_only。系统只有调度启动/停止；启动后渠道调度写回和
   条件账号处置都是真实执行。
7. “人工暂停”在当前 Sub2API 合同中定义为 status=active 且 schedulable=false；
   它与 status=disabled/inactive 的停用账号不同。

如以上假设不符合预期，应在实施前修订本文档。

## 4. Target Architecture

~~~text
Scheduler (60s)
  |
  +-- one Sub2API channel/account snapshot
  +-- publish canonical shared snapshot
  +-- enqueue maintenance protection when required
  '-- never enqueue automatic RECOVERY

Guardian loop (15s)
  |
  +-- consume shared snapshot exactly once
  +-- score channels / confidence / freshness
  +-- classify account inventory without tests
  +-- enqueue error/disabled/inactive account tests for this snapshot
  +-- detect operational/degraded -> failed/error transition
  +-- create or resume one durable channel-error episode
  '-- enqueue Guardian-owned conditional remediation jobs
          |
          v
GuardianAccountRecoveryService
  |
  +-- acquire one durable recovery lease
  +-- STATE trigger: select error/disabled/inactive accounts only
  +-- CHANNEL_ERROR trigger: resolve channel group and load every account
  +-- skip active+schedulable=false manual pauses
  +-- enforce per-snapshot and channel-episode idempotency
  +-- execute only conditionally selected account tests
  +-- enable explicit successes and disable explicit failures
  +-- persist ledger + Guardian events + metrics
  '-- enqueue durable RECOVERY_RESULT notification
~~~

### 4.1 Ownership

Only one persisted owner may execute channel-error remediation:

- SCHEDULER: migration compatibility mode only;
- GUARDIAN: Guardian is the sole executor.

The owner is stored durably. Both Scheduler and Guardian read the same value before creating a
recovery job. An unknown value fails closed. Cutover directly changes SCHEDULER to GUARDIAN;
there is no production shadow/observe owner. After production verification, Scheduler ownership
code is deleted.

### 4.2 Trigger classes

| Trigger | When | Accounts tested |
|---|---|---|
| BAD_ACCOUNT_STATE | Periodic inventory observes status error, disabled or inactive | Every abnormal account in this inventory snapshot |
| CHANNEL_ERROR | Channel transitions into failed/error | Every eligible account in the uniquely mapped group |

Inventory collection is a read-only list operation and does not consume a model test. The
account test endpoint is called only after one of these conditions is satisfied.

### 4.3 Account classes

| Source | Eligibility | Result action |
|---|---|---|
| AVAILABLE | active+schedulable | No periodic test; tested only by CHANNEL_ERROR |
| MANUAL_PAUSE | active+not schedulable | Never test or write |
| UPSTREAM_ERROR | status=error | Test every inventory scan or CHANNEL_ERROR |
| DISABLED | status=disabled/inactive and not expired/temporary | Test every inventory scan or CHANNEL_ERROR |
| CHANNEL_TEST_FAILED | Durable system quarantine owned by failed-channel protection | Test every inventory scan or CHANNEL_ERROR |
| SLOW_FIRST_TOKEN | Durable system latency quarantine | Test every inventory scan or CHANNEL_ERROR |

Candidate sources never collapse into a generic “paused” state.

### 4.4 Manual pause classification

The status field takes precedence over the dispatch switch:

- active + schedulable=false: manual pause, never touch;
- error: abnormal candidate even when schedulable=false;
- disabled/inactive: stopped candidate, unless expired or temporarily protected;
- active + schedulable=true: normal account.

Guardian persists the last classification for audit and transition detection, but it does not
need to probe an account merely to classify it.

### 4.5 Hard exclusion precedence

Before every account test and again immediately before every write:

1. explicit human pause/disable;
2. pending quarantine intent with uncertain mutation state;
3. expired account or auto_pause_on_expired;
4. active rate-limit, overload or temporary-unschedulable deadline;
5. missing/ambiguous account identity or group mapping;
6. account already tested for the same snapshot or in the same channel-error episode.

Any exclusion produces a safe ledger outcome and zero upstream mutation。Network errors,
timeouts, malformed SSE and missing first-token data never count as recovery.

### 4.6 Result application

Guardian finishes testing/classifying the group before applying the final result:

- explicit success: set status active, set schedulable=true, then read back both fields;
- definitive failure: set schedulable=false and persist system ownership, then read it back;
- indeterminate result: preserve the current state;
- human pause or ambiguous ownership: preserve the current state without executing a test.

If no account succeeds, emit NO_HEALTHY_ACCOUNT and never invent an available account. Accounts
with definitive failures may still be disabled; minimum-pool protection continues to apply to
proactive latency/maintenance isolation, but it does not keep a definitively failed account
enabled during an explicit channel-error remediation episode.

### 4.7 Snapshot, error episode and idempotency

BAD_ACCOUNT_STATE tests each abnormal account at most once per canonical inventory snapshot.
The next 60-second snapshot may test it again while it remains abnormal. A successful recovery
removes it from later abnormal-state selection.

An episode starts only when:

- a channel transitions from operational/degraded to failed/error; or
- a new channel is first observed in failed/error state after enough baseline history exists.

The episode remains open while the channel stays failed/error. Every account ID is recorded after
one attempted classification/test, so a 60-second duplicate snapshot cannot trigger another test.
If group membership changes during an open episode, only newly added eligible accounts are tested.
The episode closes when the channel leaves failed/error. A later transition creates a new episode
and may test the group again.

CHANNEL_ERROR expands selection to all non-paused accounts in the mapped group, but an account
already tested from the same snapshot is not tested twice. Afterward, accounts that remain
error/disabled/inactive continue through BAD_ACCOUNT_STATE on later snapshots.

## 5. Policy Contract

Add an additive Guardian policy section:

~~~json
{
  "account_recovery": {
    "enabled": false,
    "owner": "SCHEDULER",
    "trigger": "CONDITIONAL",
    "max_concurrency": 1,
    "max_accounts_per_episode": 1000
  }
}
~~~

Rules:

- defaults remain disabled for a new installation;
- trigger is fixed to CONDITIONAL: abnormal account state or channel error;
- there is no recovery window and no normal-account periodic test loop;
- every new inventory snapshot may test accounts that remain error/disabled/inactive;
- all eligible accounts in the uniquely mapped group are processed, one at a time by default;
- max_accounts_per_episode is a validation ceiling, not a sampling cap. If a group exceeds it,
  the entire episode fails closed rather than testing a partial subset;
- manual MCP runs may process abnormal accounts from the latest snapshot or resume a pending
  channel-error episode.
  They cannot test an active+schedulable account outside a channel-error episode;
- policy updates use existing revision, admin scope, idempotency and audit requirements.

The existing channel-level recovery thresholds remain separate. The old periodic
recovery_budget UI is retired for account recovery:

- channel recovery: Guardian channel fuse/re-entry;
- account remediation: one full account-group check caused by a channel error episode.

### 5.1 Direct scheduling mode

GuardianPolicy.observe_only and the rollout stages OBSERVE/LOAD_FACTOR/PRIORITY/SCHEDULABLE are
deprecated and removed from operator controls. The runtime contract becomes:

- enabled=false: Guardian does not write channel fields or run conditional account remediation;
- enabled=true: Guardian evaluates and directly applies every approved scheduling decision;
- emergency stop atomically sets enabled=false and cancels/blocks unstarted writes.

When enabled, Guardian owns these channel fields unless a persisted human takeover exists:

- load_factor;
- priority;
- schedulable.

The real Sub2API writer must:

1. fetch the current channel field immediately before mutation;
2. re-evaluate confidence, freshness, manual ownership, cooldown and per-run cap;
3. write one bounded field change;
4. read back and verify the exact value;
5. persist before/after, result, reason, policy revision and idempotency key;
6. stop the remaining run and alert on failed verification.

No production observation phase is used. Local tests, replay, container smoke tests and database
backup still run before deployment, but the deployed policy starts in direct scheduling mode as
requested.

## 6. Persistence

Use an additive Guardian schema migration. No existing table or column is renamed or dropped.

### 6.1 Account recovery runs

guardian_account_recovery_runs:

- run_id, trigger, policy_revision, snapshot_id, episode_id, channel_id, group_id;
- status, candidate_count, selected_count;
- tested_count, recovered_count, blocked_count, failed_count;
- started_at, finished_at, error_code.

### 6.2 Account recovery ledger

guardian_account_recovery_ledger:

- opaque ledger_id;
- validated account_id;
- source and quarantine reason;
- group_ids_json when known;
- result: RECOVERED, STILL_UNHEALTHY, STILL_SLOW, BLOCKED,
  INDETERMINATE, RESTORE_FAILED;
- bounded reason;
- optional measured latency;
- whether a test was executed or skipped before I/O;
- occurred_at and run_id.

Indexes support episode idempotency and per-account one-test-per-episode enforcement.
Credentials, prompts, raw SSE and complete upstream bodies are never persisted.

### 6.3 Dispatch baseline

guardian_account_dispatch_baselines stores:

- account_id and canonical group membership;
- last status and schedulable state obtained without an account test;
- ownership: HUMAN, SYSTEM, ERROR or UNKNOWN;
- source snapshot ID and observed timestamp.

The baseline never causes a test. It exists only to distinguish user pause from an automatic
error transition.

### 6.4 Migration marker

guardian_metadata.account_recovery_migration_v1 records:

- source configuration was imported;
- current owner;
- cutover timestamp;
- last legacy recovery job ID seen before cutover.

Migration is idempotent and increments policy revision only when the new section was absent.

## 7. Legacy Configuration Migration

Existing environment fields are deprecated inputs:

- SUB2API_MCP_RECOVERY_ENABLED;
- SUB2API_MCP_RECOVERY_WINDOW_START;
- SUB2API_MCP_RECOVERY_WINDOW_END;
- SUB2API_MCP_RECOVERY_MAX_ACCOUNTS_PER_RUN;

Only the old enabled state may seed account_recovery.enabled. The old time window and five-account
rotation are intentionally not migrated. Inventory cadence remains, while active tests are
controlled by abnormal account state or channel-error episodes.

Migration phases:

1. **Expand and verify locally:** add Guardian account-recovery policy/tables, Guardian job
   handler and real channel writer; production remains on the previous release.
2. **Baseline:** build dispatch baselines from ordinary inventory snapshots without testing any
   account using backup/replay fixtures, not a production observe mode.
3. **Cut over:** deploy with scheduling enabled and atomically set owner to GUARDIAN. Scheduler
   stops creating automatic recovery jobs; MCP manual recovery delegates to Guardian.
4. **Bake:** verify zero Scheduler-owned periodic recovery jobs and one execution per real error
   episode.
5. **Contract:** remove SchedulerService.handle_recovery, Scheduler recovery formatting,
   LegacySub2APIAdapter.recover() orchestration and runtime JobManager registration to the
   Scheduler. Remove old recovery window/cadence environment parsing after one compatibility
   release.

Rollback first invokes emergency stop, then restores the previous container and database backup.
There is no fallback that allows Scheduler and Guardian to write concurrently.

## 8. MCP, REST and UI Compatibility

### 8.1 Existing MCP tool

sub2api_submit_recovery remains available and returns a durable JobType.RECOVERY job. Its
handler is Guardian after cutover. This avoids breaking LangBot prompts or external clients.

### 8.2 New Guardian interfaces

- guardian_get_account_recovery_status
- guardian_run_account_recovery(confirm, episode_id)
- guardian_start_scheduling(confirm)
- guardian_stop_scheduling(confirm)
- GET /api/guardian/v1/account-recovery/status
- POST /api/guardian/v1/account-recovery/runs
- POST /api/guardian/v1/scheduling/start
- POST /api/guardian/v1/scheduling/stop

Status includes owner, open error episodes, channel/group mapping, candidate counts by
classification, tested/untested counts, last run summary and blocked reasons. Account identifiers
are visible only to admin-authorized interfaces and never used as metric labels.

### 8.3 Guardian UI

The existing light Guardian page adds:

- account recovery owner and direct scheduling state;
- enabled/owner/trigger and safe concurrency;
- ordinary error vs channel-failure quarantine vs latency-quarantine counts;
- open/closed channel error episodes and last run outcome;
- explicit “resume pending episode” confirmation;
- direct scheduling start and emergency stop controls.

The page removes observe mode and rollout-stage controls. Existing rollout endpoints return a
stable deprecation error and never silently switch execution behavior.

## 9. Notifications

Account recovery uses durable, non-coalesced RECOVERY_RESULT events. Each message includes:

- Beijing trigger time, channel and error episode;
- Guardian recovery run ID suffix;
- total accounts, manual pauses skipped, tested, enabled, disabled, indeterminate counts;
- per-account safe result for administrator person targets;
- source/reason and measured latency when applicable;
- confirmation that recovery was Guardian-owned;
- explicit notice when no account tested successfully.

No target is required merely to execute a safe automatic recovery. Missing/invalid delivery
context must not block Guardian state progress; notification remains in the durable outbox.

## 10. Observability

On-call questions:

1. Which component owns recovery now, and did both components ever execute one error episode?
2. Which accounts were manually paused, eligible, tested, enabled, disabled or indeterminate?
3. Did one persistent channel error accidentally trigger more than one account test episode?
4. Did an upstream test succeed but verified restore fail?

Signals:

- guardian_account_recovery_runs_total{trigger,status};
- guardian_account_recovery_accounts_total{classification,result};
- guardian_account_recovery_tests_total{result};
- guardian_account_recovery_duration_seconds{result};
- guardian_channel_error_episodes{state};
- structured events with runId, episodeId, channelId, groupId, bounded result/reason, policy
  revision and snapshot ID.

Metric labels use fixed enums only; account IDs belong only in redacted admin logs/events.

## 11. Testing Strategy

### Unit

- policy validation and legacy unconditional-rotation deprecation;
- error transition/episode identity and duplicate snapshot suppression;
- complete group enumeration and all-account result classification;
- source-specific enable/disable gates;
- manual pause and pending-intent hard exclusions;
- no-data/network/SSE failure fail-closed behavior.

### Repository

- empty/current/interrupted migration and restart;
- unique run/ledger idempotency;
- dispatch baseline ownership and one-test-per-account-per-episode uniqueness;
- owner cutover compare-and-set and rollback.

### Integration

- Scheduler publishes snapshot but creates no recovery job when owner is Guardian;
- enabling Guardian directly invokes the verified Sub2API writer for approved field proposals;
- manual field ownership, stale/low-confidence data and cooldown block writes;
- a failed write verification stops the remainder of the run and emits an alert;
- scheduling stop blocks queued/unstarted writes without entering an observe state;
- exactly one Guardian remediation job per channel error episode;
- manual MCP compatibility routes to Guardian;
- explicit successes enable, definitive failures disable and manual pauses remain untouched;
- Scheduler and Guardian cannot execute the same episode;
- notification failure does not roll back a completed recovery;
- service restart does not replay any tested account in an open episode;
- a continuously failed channel produces no repeated full-group sweep; remaining abnormal
  accounts are retested from later inventory snapshots.

### Production verification

- database/environment backup and checksum;
- baseline ownership audit before cutover;
- verified current-field read, one bounded write and exact read-back for each channel field;
- one controlled channel error with successful, failed, paused and indeterminate accounts;
- zero legacy-created recovery jobs after cutover;
- zero test requests for active+schedulable accounts during a healthy-channel observation window;
- container health, full tests, lint, types, audit and local-only Docker build.

## 12. Commands

~~~text
Install:     uv sync --frozen --all-extras
Focused:     uv run pytest tests/unit/guardian tests/unit/test_scheduler.py -q
Integration: uv run pytest tests/integration/guardian tests/contract/test_mcp_tools.py -q
Full:        uv run pytest -q
Lint:        uv run ruff check .
Types:       uv run pyright
Audit:       uv run pip-audit
Build:       docker build --tag bot-mcp-ci:guardian-account-recovery .
~~~

## 13. Project Structure

- guardian/account_recovery.py: sole recovery orchestration, selection and execution.
- guardian/contracts.py: policy/run/candidate/result contracts.
- guardian/repository.py: owner, episodes, dispatch baselines, runs and result ledger.
- guardian/service.py: cadence, manual run, status, notification and metrics.
- adapters/sub2api.py: thin candidate-fetch/test/verified-restore operations.
- scheduler.py: snapshot collection only; legacy recovery removed after bake.
- tools.py, guardian/api.py, guardian/static/*: compatibility and operator controls.
- tests/unit/guardian, tests/integration/guardian, tests/contract: behavior proof.

## 14. Code Style

Public contracts are strict Pydantic models; pure selection is deterministic and side-effect
free. Orchestration receives typed ports and never parses raw Sub2API responses.

~~~python
selection = select_account_recovery_candidates(
    candidates,
    policy=policy.account_recovery,
    now=now,
    episode=episode,
)
for candidate in selection.selected:
    outcome = await operations.recover_account(candidate, deadline=selection.deadline)
    await repository.record_account_recovery(run_id, candidate, outcome)
~~~

## 15. Boundaries

### Always

- preserve current quarantine work and merge it before overlapping implementation;
- use a durable single owner and lease;
- re-check eligibility immediately before writes;
- retain MCP compatibility and durable job IDs;
- make migrations additive, idempotent and rollback-safe;
- write a failing behavior test before every behavior change.

### Ask first

- change the channel statuses that trigger an episode;
- allow partial processing when a group exceeds the safety ceiling;
- remove the compatible MCP tool;
- deploy or switch owner to GUARDIAN.

### Never

- run Scheduler and Guardian recovery concurrently;
- periodically test an active+schedulable account while channels are operational/degraded;
- test the same account twice in one continuous channel error episode;
- claim scheduling is enabled while the real writer adapter is absent;
- continue a run after a write cannot be verified;
- auto-enable an account without explicit test success and read-back verification;
- auto-enable a human-paused or ineligible account;
- infer system ownership solely from upstream schedulable=false;
- persist or log credentials, raw SSE, prompts, full upstream bodies or unredacted PII.

## 16. Acceptance Criteria

1. Scheduler never creates periodic recovery jobs after Guardian cutover.
2. Active+schedulable accounts cause zero periodic account test requests.
3. Error/disabled/inactive accounts are tested once per new inventory snapshot.
4. A new failed/error episode tests every eligible account in the uniquely mapped group once.
5. Explicit successes become active+schedulable after read-back verification.
6. Definitive failures become non-schedulable after read-back verification.
7. Human pause and every hard exclusion produce zero tests/writes.
8. Duplicate snapshots and process restarts cannot replay a tested account in the same episode.
9. Manual sub2api_submit_recovery processes only abnormal accounts in the latest snapshot or a
   pending error episode.
10. Notification failure cannot block or duplicate account state changes.
11. After bake, legacy Scheduler recovery orchestration, window and unconditional rotation are
    removed.
12. Full quality, migration, container and production rollback checks pass.
13. Guardian has no observe or rollout mode: enabled applies verified scheduling writes and
    disabled applies none.
14. Load factor, priority and schedulable writes respect human ownership, confidence, freshness,
    cooldown, idempotency and read-back verification.

## 17. Resolved Decisions

1. No recovery window or unconditional all-account test loop is retained.
2. A periodic inventory scan may test only error/disabled/inactive accounts.
3. A newly detected failed/error channel episode may test the mapped group once.
4. Every eligible account in the mapped group is classified; there is no five-account sampling
   cap.
5. Manual pauses are untouched.
6. Explicit success enables, definitive failure disables, and indeterminate results preserve the
   current state.

7. 同一个异常账号可以在下一份 60 秒清单快照中再次测试；同一快照内不重复测试。
