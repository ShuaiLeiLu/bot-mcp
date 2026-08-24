# PRD：Sub2API Guardian 共享采样与受控自动调度 V2

## 0. 文档信息

- 状态：**Draft — 待产品评审**
- 版本：0.2-draft.1
- 日期：2026-08-24
- 实现仓库：`ShuaiLeiLu/bot-mcp`
- 上一版：[Guardian 调度系统 PRD 0.1](./prd-guardian-scheduler.md)
- 方案摘要：[Guardian 共享采样与受控自动调度](./ideas/guardian-shared-sampling.md)
- 变更原因：常规 Guardian 独立探测会重复消耗 Token；改用既有探测与历史流量后，原有按样本条数评分和写回规则不再成立。

本 PRD 是 V2 增量规格。与 0.1 冲突时以本文件为准；未覆盖的认证、安全、页面基础架构和恢复原始配置要求继续继承 0.1。

## 1. 已采用假设

本草案按产品负责人“先按当前建议出一个版本”的确认采用以下默认值：

1. 可靠性优先级高于错误隔离、成本和延迟优化。
2. Guardian 以渠道为评分与调度单位；账号级关闭、自愈继续由现有维护模块负责。
3. 无新数据时不扣健康分，只降低置信度并冻结自动写回。
4. 正常渠道不进行 Guardian 独立主动探测。
5. 只有 Guardian 自己熔断的渠道允许每 300 秒进行一次低频恢复探测。
6. 人工暂停、人工排除、人工熔断和上游人工关闭均不得自动恢复。
7. 首先观察运行至少 24 小时且消费不少于 100 个唯一共享快照。
8. 写回按 `load_factor`、`priority`、`schedulable` 三阶段分别审批。
9. 首版不自动修改渠道并发上限。
10. 所有生产写回继续要求产品负责人显式批准；仅批准 PRD 不等于批准写回。

## 2. 问题定义

V1 的评分模型隐含三个前提：样本按固定频率产生、每条样本独立、样本数量可以代表时间。共享历史记录不满足这些前提：

- Guardian 每 15 秒扫描会重复消费同一份 60 秒探测结果；
- 高流量渠道的请求数量会淹没低流量渠道；
- 无流量不代表异常，旧数据也不代表当前健康；
- 渠道监控请求可能再次出现在真实流量日志中，造成双重计分；
- 一个聚合渠道快照不能准确定位渠道内的具体异常账号；
- 直接用历史分数写权重，会在低置信度下形成错误调度。

因此 V2 必须同时重做采样、评分、置信度、状态机和写回策略，不能只替换数据读取接口。

### 2.1 当前实现基线

截至本草案日期：

- 生产 Guardian 仍为关闭和观察模式，不执行生产写回；
- 若开启，当前引擎会按 15 秒扫描周期重新读取一份上游快照；
- `probe.enabled`、探测间隔和“新鲜流量时跳过探测”等策略字段尚未真正控制引擎取样；
- 当前每轮会把读取到的快照记录为新的 `PROBE` 样本，无法识别重复快照；
- 生产写回适配器尚未启用。

V2 上线前必须以测试证明这些旧行为已被共享快照消费模型替代，而不是仅修改页面文案或默认配置。

## 3. 产品目标

### 3.1 核心目标

- 常规健康渠道的 Guardian 额外付费主动探测次数为零。
- 复用现有渠道监控和真实流量，形成去重、可追溯的渠道健康历史。
- 健康分与证据置信度分离，低置信度时不做自动写回。
- 在不违反分组保底和人工控制权的前提下，给出稳定的权重、优先级与上下池建议。
- 分阶段开放实际写回，并可逐字段、逐分组撤销控制权。
- 将实际恢复探测次数、估算 Token 和成本完整记账。

### 3.2 成功指标

观察期内：

- 共享快照重复消费率为 0；
- 正常渠道 Guardian 独立探测次数为 0；
- 评分轮次的上游读取次数相对 V1 设计下降至少 80%；
- 数据陈旧时的自动写回次数为 0；
- 人工控制状态被自动覆盖次数为 0；
- 同一字段在冷却时间内往返写回次数为 0；
- 历史回放中错误熔断率低于 0.5%，已标注致命故障测试集召回率达到 100%；
- 微信通知包含触发时间、证据来源、健康分、置信度和动作原因。

## 4. 用户与核心任务

### 4.1 调度管理员

- 了解某个评分由哪些共享快照、真实请求和恢复探测构成。
- 查看健康分、置信度、数据年龄和建议动作。
- 选择价格、速度或均衡策略，并配置分组预算。
- 分别开启 `load_factor`、`priority`、`schedulable` 写回。
- 暂停、排除、人工熔断或收回某个字段的 Guardian 控制权。

### 4.2 值班人员

- 在数据陈旧、熔断、回池、预算耗尽或写回失败时收到可读通知。
- 从事件日志定位触发动作的原始证据和规则版本。
- 一键停止全部写回并恢复 Guardian 接管前配置。

## 5. 设计原则

1. **单一采集，多方消费**：现有调度器是常规渠道快照的唯一采集者。
2. **没有证据不是坏证据**：数据缺失只降低置信度，不降低健康分。
3. **固定时间桶而非请求条数**：高流量渠道不能仅因请求多而支配长期评分。
4. **分数与置信度分离**：分数描述已观察到的健康，置信度决定是否允许动作。
5. **人工控制优先**：自动系统不得猜测人工操作意图。
6. **连续量与离散量分工**：`load_factor` 负责连续分流，`priority` 负责粗粒度健康层级。
7. **写回有所有权**：每个被 Guardian 管理的字段都有接管基线、当前所有者和审计链。
8. **恢复探测有硬预算**：预算耗尽时保持熔断并告警，不透支 Token。

## 6. V2 总体架构

```text
现有 Scheduler（每 60 秒）
  → 拉取 channel-monitors、分组和账号汇总
  → 生成 canonical shared snapshot
  → 持久化 snapshot_id / captured_at / payload_hash
  → 原有状态变化通知

Sub2API ops 增量采集（每 60 秒）
  → 拉取真实请求
  → 去除监控自身请求
  → request_id_hash 去重
  → 聚合为每渠道每分钟 traffic bucket

Guardian Scanner（每 15 秒，仅本地）
  → 查找未消费 shared snapshot / traffic bucket
  → 每份证据只消费一次
  → 更新时间桶、健康分和置信度
  → 执行状态机与权重计算
  → 观察或受控写回
  → 审计与通知

Recovery Worker（默认每 300 秒）
  → 仅选择 Guardian 自动熔断渠道
  → 检查全局/渠道预算
  → 执行最小请求恢复探测
  → 写入 RECOVERY_PROBE 样本
```

Guardian Scanner 不再直接调用 `guardian_snapshot()` 获取新上游数据；手动“同步”仍可触发一次只读快照采集，但不会执行账号测试。

## 7. 样本来源与信任模型

新增四类来源：

| 来源 | 枚举 | 默认可靠度 | 是否消耗额外 Token | 用途 |
|---|---|---:|---|---|
| 既有渠道监控 | `SHARED_MONITOR` | 0.85 | 否 | 所有渠道基础状态、延迟、分组账号数。 |
| 真实业务流量 | `TRAFFIC` | 最高 1.0 | 否 | 实际成功率、错误类型和延迟。 |
| 自动恢复探测 | `RECOVERY_PROBE` | 1.0 | 是 | 自动熔断渠道回池判定。 |
| 人工立即探测 | `MANUAL_PROBE` | 1.0 | 是 | 管理员诊断；默认不在同一轮直接写回。 |

### 7.1 明确不作为实时样本的数据

- `availability_7d` 只展示，不直接进入实时健康分；其时间尺度与分母不透明，直接加入会与真实请求重复计分。
- 分组账号数量用于保底与容量判断，不作为单个渠道的健康分。
- 普通 API 请求数量不直接增加权重，只用于当前时间桶的证据覆盖度。

### 7.2 真实流量过滤

必须排除以下请求，防止双重计分：

- 当前渠道监控自身 API Key 或已识别 User-Agent 发出的请求；
- Guardian `RECOVERY_PROBE` 和 `MANUAL_PROBE` 产生的请求；
- 重复 `request_id_hash`；
- 无法可靠归属到渠道的记录。无法归属的记录进入“未归属”统计，不参与动作。

## 8. 快照、去重与时间桶

### 8.1 共享快照标识

```text
snapshot_id = SHA-256(
  schema_version
  + captured_at_source
  + canonical(entries sorted by monitor_id)
)
```

每个 `snapshot_id` 只能被 Guardian 消费一次。相同 payload 但新的上游 `last_checked_at` 视为新观察；仅本地重复拉取不产生新样本。

### 8.2 真实请求标识

优先使用上游请求 ID 的 HMAC 哈希；无请求 ID 时使用稳定字段生成有时限的去重指纹。禁止持久化原始 API Key、Authorization 或完整请求正文。

### 8.3 固定时间桶

- 默认桶宽：60 秒；
- 每个渠道、来源、时间桶最多一个聚合样本；
- 单桶使用全部已校验请求的事件计数和分数和计算均值，但最多保留 20 条脱敏示例；请求量不会跨时间桶增加统计权重；
- 最近 10 分钟为短期窗口；
- 最近 120 分钟为长期窗口；
- 重启后从数据库继续消费，不重复生成已存在的桶。

## 9. 事件分类

事件基础分沿用 V1，保证语义兼容：

| 事件 | 分数 |
|---|---:|
| `PERFECT` | 100 |
| `SLOW_TTFB` | 65 |
| `UPSTREAM_UNKNOWN` | 40 |
| `GATEWAY_ERROR` | 25 |
| `QUOTA_EXHAUSTED` | 15 |
| `PROBE_FAIL` | 10 |
| `FATAL` | 0 |

映射规则：

- `SHARED_MONITOR operational` 且延迟不超过 5000ms → `PERFECT`；
- `SHARED_MONITOR operational/degraded` 且延迟超过 5000ms → `SLOW_TTFB`；
- `SHARED_MONITOR failed/error` → `PROBE_FAIL`，不得仅凭聚合状态判为 `FATAL`；
- 真实流量根据 HTTP 状态、错误分类和延迟分类；
- 单次真实 401/402/403 先记 `FATAL` 候选，自动熔断需满足第 12.3 节的确认规则；
- 带明确重置时间的 429 记为限流，不做永久关闭。

## 10. V2 健康评分

### 10.1 来源内聚合

对时间桶 `b` 和来源 `s`：

```text
sourceScore(b,s) = accepted event scores 的算术均值
```

来源可靠度：

```text
r_shared_monitor = 0.85
r_recovery_probe = 1.00
r_manual_probe   = 1.00
r_traffic        = min(1, acceptedRequestCount / 5)
```

### 10.2 时间桶融合

同一分钟存在多个来源时：

```text
bucketScore(b) = Σ(sourceScore(b,s) × r_s) / Σr_s
bucketQuality(b) = 1 - Π(1 - r_s)
```

`bucketQuality` 只影响置信度和时间权重，不直接改变事件分数。

### 10.3 时间衰减

```text
shortWeight(b) = bucketQuality(b) × 2 ^ (-ageMinutes / 3)
longWeight(b)  = bucketQuality(b) × 2 ^ (-ageMinutes / 30)

shortScore = weightedMean(last 10 minutes, shortWeight)
longScore  = weightedMean(last 120 minutes, longWeight)
healthScore = shortScore × 0.7 + longScore × 0.3
```

若长期窗口为空，则 `longScore = shortScore`；全部窗口为空时保留最后一次健康分，不生成 0 分。

### 10.4 置信度

```text
latestAgeSeconds = 当前时间 - 最近证据时间
freshness = 2 ^ (-latestAgeSeconds / 180)
coverage = min(1, 最近 10 分钟有证据的唯一时间桶数 / 5)
quality = Σ(bucketQuality × 2 ^ (-ageMinutes / 3))
          / Σ(2 ^ (-ageMinutes / 3))

confidence = clamp(
  freshness × (coverage × 0.6 + quality × 0.4),
  0,
  1
)
```

健康分与置信度必须分别持久化、展示和审计，禁止将低置信度简单折算成低健康分。

### 10.5 冷启动

- 少于 5 个唯一新鲜时间桶时为 `WARMING_UP`；
- 冷启动可以展示建议，不允许任何自动写回；
- 可从最近 120 分钟真实流量回填历史桶，但不得复制同一请求；
- 7 日可用率不作为冷启动分数先验。

### 10.6 黄金算例

最近三个时间桶的 `(score, quality, ageMinutes)` 分别为：

```text
(100, 0.85, 0)
(65,  1.00, 1)
(25,  0.85, 2)
```

按本节公式计算：

```text
shortScore = 68.82317750686934
longScore = 63.971259697525845
healthScore = 67.36760216406628
confidence = 0.7196488001243996
```

实现和 API 黄金测试必须在双精度范围内与这些值一致。

## 11. 数据新鲜度

| 等级 | 最近证据年龄 | 行为 |
|---|---:|---|
| `FRESH` | `<=180s` | 正常评分，可按置信度门槛决策。 |
| `STALE` | `>180s 且 <=600s` | 保留分数，冻结所有自动写回并告警一次。 |
| `EXPIRED` | `>600s` | 状态显示数据过期，继续冻结；不把渠道判为失败。 |

数据恢复后重新进入至少 3 个唯一桶的稳定观察，才允许解除 `STALE/EXPIRED` 写回冻结。

## 12. V2 状态机

### 12.1 优先级顺序

```text
人工排除
  > 人工暂停
  > 人工熔断
  > 上游人工关闭/暂停调度
  > 数据过期
  > 分组/渠道守护范围
  > 自动熔断与降级
  > 健康状态
```

### 12.2 降级

满足以下条件才允许自动降级：

- `confidence >= 0.60`；
- `healthScore < 75`，或最近 10 个桶中至少 5 个桶超过 15000ms；
- 当前不处于人工状态、数据陈旧或写回冷却。

降级渠道保持可调度，降低 `load_factor`，必要时将基线 `priority` 增加一级。

### 12.3 自动熔断

普通错误熔断需同时满足：

- `confidence >= 0.85`；
- `healthScore < 60`；
- 最近 5 个唯一时间桶中至少 3 个失败桶；
- 熔断后不违反分组最小可用池；
- 本轮未超过“最多熔断 1 个渠道”。

致命候选只有在以下任一条件满足时可跳过普通分数门槛：

- `RECOVERY_PROBE` 或 `MANUAL_PROBE` 明确返回 401/402/403；
- 5 分钟内至少两个不同真实请求返回同类凭据致命错误。

仅有 `SHARED_MONITOR failed/error` 不允许触发致命快速熔断。

### 12.4 保底强留

若熔断会使分组可用渠道数低于 `min_pool_size=1`：

- 状态进入 `FORCED_KEEP`；
- `schedulable` 保持开启；
- `priority=5`；
- `load_factor` 使用最小值；
- 立即发送高优先级告警。

### 12.5 人工与上游状态保护

- 人工暂停：不接流量、不主动探测、不自动恢复；允许读取共享状态用于展示，但不据此产生恢复动作；
- 人工排除：不评分、不探测、不写回；
- 人工熔断：只能人工解除；
- 上游已关闭且无 Guardian 写回审计：视为人工操作，不得重新启用；
- 只有写回所有者为 Guardian 的自动熔断状态可以自动回池。

## 13. 恢复探测与 Token 预算

### 13.1 选择条件

恢复探测仅选择：

- 当前状态为 Guardian 自动 `FUSED`；
- 不属于人工暂停、排除、人工熔断或上游人工关闭；
- 能唯一映射到一个允许测试的账号；无法唯一映射时保持熔断并要求人工处理；
- 冷却已结束；
- 距离上次恢复探测至少 300 秒；
- 全局和渠道预算均未耗尽。

### 13.2 默认预算

| 参数 | 默认值 |
|---|---:|
| 恢复探测间隔 | 300 秒 |
| 最大并发 | 1 |
| 单渠道每小时上限 | 12 次 |
| 全局每日请求上限 | 50 次 |
| 全局每日估算 Token 上限 | 10000 |
| 超预算行为 | 保持熔断并通知，不再探测 |

Token 与费用口径来自实际探测响应或保守估算；共享监控和真实流量不计入 Guardian 探测费用。

### 13.3 回池条件

- 最近 3 次 `RECOVERY_PROBE` 全部成功；
- `healthScore >= 80`；
- `confidence >= 0.85`；
- 健康持续至少 60 秒；
- 分组写回未被暂停。

回池只恢复 Guardian 自己接管过的字段，并以接管前基线为上限，不覆盖期间发生的人工修改。

## 14. 调度策略与权重

### 14.1 候选资格

参与权重计算的渠道必须：

- 当前允许调度；
- 不处于 `FUSED`、`EXCLUDED`、`MANUALLY_PAUSED`、`UPSTREAM_DISABLED` 或数据过期；
- `confidence >= 0.75`；
- `healthScore >= 40`；
- 属于当前受管分组。

低置信度渠道保留当前真实权重并预留对应预算，不参与本轮重新归一化，避免其他渠道的份额被动放大：

```text
reservedBudget = Σ低置信度渠道当前 load_factor
allocatableBudget = max(0, groupBudget - reservedBudget)
```

若 `reservedBudget > groupBudget`，整个分组冻结权重写回并告警，不自动压缩人工或低置信度渠道权重。

### 14.2 无量纲信号

对同一分组候选渠道：

```text
priceSignal_i = (minValidRate / effectiveRate_i) ^ priceExp
speedSignal_i = (minValidP95 / max(ttfbP95_i, 100)) ^ speedExp

PRICE:    strategySignal_i = priceSignal_i
SPEED:    strategySignal_i = speedSignal_i
BALANCED: strategySignal_i =
  priceSignal_i ^ balancedPriceRatio
  × speedSignal_i ^ (1 - balancedPriceRatio)

healthSignal_i = clamp(
  (healthScore_i - gateFloor) / (100 - gateFloor),
  0,
  1
)
confidenceSignal_i = confidence_i ^ confidenceExp

rawWeight_i = strategySignal_i
              × healthSignal_i
              × confidenceSignal_i
              × scheduleMultiplier_i
```

缺失价格或延迟时使用组内中位数，并将对应信号乘以 0.8 的缺失惩罚；不得因数据缺失获得优势。

`confidenceExp` 默认 1，可配置范围 0.1～10。

### 14.3 预算归一化

```text
targetLoadFactor_i = allocatableBudget × rawWeight_i / ΣrawWeight
```

- 默认 `groupBudget=400`；
- 使用“迭代上下限裁剪 + 最大余数法”转为整数；满足上下限时总和严格等于可分配预算；
- 单渠道最小值 1、最大值 100；
- 分组内只有一个合格渠道时获得全部允许预算，但仍受最大值限制。
- 若上下限使预算无法完全分配，明确记录 `unallocatedBudget`，禁止静默突破上下限。

### 14.4 防抖与步长

| 参数 | 默认值 |
|---|---:|
| 最小相对变化 | 15% |
| 最小绝对变化 | 2 |
| 单次最大相对步长 | 20% |
| `load_factor` 写回冷却 | 600 秒 |
| `priority` 写回冷却 | 900 秒 |
| 每轮最大写回渠道数 | 1 |

只有同时超过相对或绝对变化门槛时才写回；超过最大步长时逐轮逼近目标。

## 15. 优先级策略

`priority` 不再直接表达精细权重，只表达相对于“接管前基线”的健康层级：

| 条件 | 建议 priority |
|---|---|
| `score >= 75` 且 `confidence >= 0.75` | 基线 priority |
| `60 <= score < 75` | 基线 + 1 |
| `40 <= score < 60` | 基线 + 2 |
| `FORCED_KEEP` | 5 |
| `FUSED` | 不依赖 priority，直接不可调度 |

结果限制在 1～5。策略模式的价格/速度差异只影响 `load_factor`，避免 priority 与权重互相打架。

## 16. 写回所有权与自动应用

### 16.1 字段级开关

继续保留并独立审批：

- `autoApply.loadFactor`
- `autoApply.priority`
- `autoApply.schedulable`

`concurrency` 首版只能人工修改。

### 16.2 所有权

每个渠道字段记录：

- `baseline_value`：Guardian 接管前值；
- `last_guardian_value`：最近一次 Guardian 写入值；
- `owner`：`UPSTREAM`、`HUMAN` 或 `GUARDIAN`；
- `last_write_at`、`reason`、`policy_revision`。

若当前上游值既不等于基线，也不等于 Guardian 最近写入值，则认定发生人工修改：

- 将字段所有权切换为 `HUMAN`；
- 停止覆盖该字段；
- 页面显示“人工接管”；
- 只有管理员显式“重新交给 Guardian”后恢复写回。

### 16.3 安全门

任何写回必须同时通过：

- Guardian 全局启用；
- 非观察模式；
- 对应字段 `autoApply` 已单独批准；
- 数据 `FRESH`；
- 置信度达到该动作门槛；
- 字段所有权允许；
- 分组保底、冷却、步长、每轮上限、revision 和幂等检查；
- 写回适配器已启用且健康。

## 17. 分阶段放权

### 阶段 A：历史回放与算法对拍

- 使用脱敏历史流量和共享快照回放；
- 不调用生产写接口；
- 验证评分、置信度、熔断、恢复和权重稳定性。

### 阶段 B：生产观察模式

- 至少 24 小时且不少于 100 个唯一共享快照；
- 所有 `autoApply=false`；
- 输出建议值、真实值、差异和如果写回会发生什么。

### 阶段 C：单分组 `load_factor` 灰度

- 选择一个非核心分组；
- 只开放 `load_factor`；
- 每轮最多写一个渠道；
- 至少运行一个完整业务高峰周期。

### 阶段 D：优先级灰度

- `load_factor` 灰度无误后单独审批；
- priority 只随健康层级改变；
- 保留 900 秒冷却。

### 阶段 E：上下池灰度

- 最后开放 `schedulable`；
- 仅系统自动熔断/恢复；
- 人工状态保护和一键恢复演练必须先通过。

每个阶段都需要产品负责人单独批准，不能因上一阶段通过而自动升级。

## 18. 配置模型

### 18.1 共享采样

| 字段 | 默认值 | 范围 |
|---|---:|---:|
| `sampling.mode` | `SHARED` | `SHARED` / `ACTIVE`（兼容） |
| `sampling.scanIntervalSeconds` | 15 | 5～3600 |
| `sampling.sharedSnapshotIntervalSeconds` | 60 | 30～3600 |
| `sampling.bucketSeconds` | 60 | 30～300 |
| `sampling.freshSeconds` | 180 | 30～3600 |
| `sampling.expireSeconds` | 600 | 60～86400 |
| `sampling.minWarmupBuckets` | 5 | 1～60 |

`ACTIVE` 只作为显式兼容模式保留，默认关闭，页面需展示 Token 风险警告。

### 18.2 评分与置信度

| 字段 | 默认值 |
|---|---:|
| `scoring.shortWindowMinutes` | 10 |
| `scoring.longWindowMinutes` | 120 |
| `scoring.shortHalfLifeMinutes` | 3 |
| `scoring.longHalfLifeMinutes` | 30 |
| `scoring.shortRatio` | 0.7 |
| `confidence.degradeMin` | 0.60 |
| `confidence.weightMin` | 0.75 |
| `confidence.fuseMin` | 0.85 |
| `confidence.recoverMin` | 0.85 |
| `weights.confidenceExp` | 1.0 |

### 18.3 写回

| 字段 | 默认值 |
|---|---:|
| `autoApply.loadFactor` | `false` |
| `autoApply.priority` | `false` |
| `autoApply.schedulable` | `false` |
| `writes.maxChannelsPerRun` | 1 |
| `writes.loadCooldownSeconds` | 600 |
| `writes.priorityCooldownSeconds` | 900 |
| `writes.maxRelativeStep` | 0.20 |

分组与渠道覆盖继续遵循“未设置则继承全局”；任何覆盖必须在页面展示来源。

## 19. 管理页面变更

### 19.1 总览

新增：

- 当前采样模式；
- 最近共享快照时间与年龄；
- 待消费快照/流量桶数量；
- 新鲜、陈旧、过期渠道数量；
- 本日恢复探测请求、Token 和预算余量；
- 当前放权阶段和实际开启的写回字段。

### 19.2 渠道池

每个渠道展示：

- 健康分与置信度；
- 数据新鲜度；
- 最近样本来源组合；
- 当前/建议 `load_factor`；
- 当前/建议 priority；
- 字段所有权；
- 写回冻结原因；
- 评分解释抽屉。

### 19.3 策略配置

将原“主动探测”区替换为：

1. 共享采样；
2. 真实流量；
3. 恢复探测与预算；
4. 评分与置信度；
5. 权重与防抖；
6. 自动应用与灰度阶段。

危险开关必须展示影响范围、前置条件和二次确认。

## 20. 数据模型

### 20.1 新增/变更表

| 表 | 用途 |
|---|---|
| `guardian_input_snapshots` | 共享快照、payload hash、采集与消费时间。 |
| `guardian_traffic_buckets` | 按渠道/分钟聚合的真实流量证据。 |
| `guardian_samples` | 增加 `source_event_id`、`bucket_at`、`reliability`、`ingested_at`。 |
| `guardian_channels` | 增加 `confidence`、`freshness_state`、`last_evidence_at`、`warmup_buckets`。 |
| `guardian_field_ownership` | 字段基线、当前所有者和最近 Guardian 写入值。 |
| `guardian_probe_ledger` | 增加预算周期、实际/估算 Token、请求来源和阻止原因。 |

唯一约束：

- `guardian_input_snapshots(snapshot_id)`；
- `guardian_traffic_buckets(channel_id, bucket_at)`；
- `guardian_samples(channel_id, source, source_event_id)`；
- `guardian_field_ownership(channel_id, field_name)`。

### 20.2 迁移

- 现有 `PROBE` 样本保留但标记 `legacy=true`；
- V2 启用后不将 legacy 样本用于自动写回，只用于页面历史参考；
- 现有策略 JSON 迁移到 V2 defaults，revision 增加；
- 迁移失败时保持 Guardian 关闭，不影响现有渠道监控和通知。

### 20.3 保留周期

- 请求去重哈希：7 天；
- 分钟流量桶：30 天；
- Guardian 评分样本：90 天；
- 共享快照正文：7 天，摘要与审计保留 90 天；
- 写回审计和原始配置：不自动清理；
- 清理任务按批次执行，不阻塞调度租约。

## 21. REST 与 MCP 接口

保留现有 `/api/guardian/v1/*` 与 MCP 工具，采用向后兼容的可选字段扩展。

新增只读端点：

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/api/guardian/v1/sampling/status` | 共享快照、桶、陈旧度和消费状态。 |
| GET | `/api/guardian/v1/channels/{id}/explanation` | 评分、置信度和动作解释。 |
| GET | `/api/guardian/v1/write-ownership` | 字段控制权总览。 |
| GET | `/api/guardian/v1/probe-budget` | 恢复探测预算与消耗。 |

新增管理操作：

| 方法 | 路径 | 用途 |
|---|---|---|
| POST | `/api/guardian/v1/channels/{id}/ownership` | 显式交还或收回字段控制权。 |
| POST | `/api/guardian/v1/rollout/advance` | 二次确认后进入下一放权阶段。 |
| POST | `/api/guardian/v1/rollout/stop` | 立即停止写回但保留观察。 |

所有新增 mutation 使用 `sub2api:admin`、`Idempotency-Key`、revision、审计和安全错误结构。

MCP 增加：

- `guardian_get_sampling_status`
- `guardian_explain_channel_score`
- `guardian_get_write_ownership`
- `guardian_get_probe_budget`
- `guardian_advance_rollout`
- `guardian_stop_writeback`

## 22. 通知

自动通知只在以下事件产生：

- 首次进入 `STALE` 或 `EXPIRED`；
- 降级、自动熔断、保底强留和回池；
- 恢复探测预算达到 80% 或耗尽；
- 字段检测到人工接管；
- 写回失败、回滚或放权阶段变化。

通知必须包含：

- 北京时间触发时间；
- 渠道/分组；
- 健康分和置信度；
- 最近证据来源与年龄；
- 当前状态 → 目标状态；
- 动作或冻结原因；
- 是否实际写回。

相同渠道、相同事件和相同目标的未送达通知只保留最新状态；维护与恢复结果不合并。

## 23. 可观测性与成本

新增指标：

- `guardian_shared_snapshots_total{status}`
- `guardian_snapshot_age_seconds`
- `guardian_duplicate_observations_total{source}`
- `guardian_traffic_buckets_total{status}`
- `guardian_channel_confidence{channel}`
- `guardian_channels_by_freshness{state}`
- `guardian_write_frozen_total{reason}`
- `guardian_recovery_probe_requests_total{result}`
- `guardian_recovery_probe_tokens_total{priced}`
- `guardian_field_ownership_changes_total{from,to}`

日志必须包含 `runId/snapshotId/channelId/groupId/source/score/confidence/freshness/action/reason/policyRevision`，禁止记录密钥、原始请求正文和完整用户标识。

## 24. 测试策略

### 24.1 单元测试

- 快照 canonical 化和去重；
- 真实流量过滤与时间桶上限；
- 来源融合、时间衰减、置信度公式；
- 冷启动、陈旧和过期；
- 致命确认、普通熔断、保底和回池；
- 三种策略、缺失信号惩罚、最大余数归一化；
- 步长、阈值、冷却和所有权检测。

### 24.2 属性/不变量测试

- 重复输入不改变分数或样本数；
- 增加健康证据不能降低健康分；
- 降低置信度不能触发新的写回；
- 分组预算守恒；
- 任意动作不得突破最小可用池；
- 人工状态在任意输入序列下都不会自动恢复。

### 24.3 集成测试

- Scheduler 写共享快照，Guardian 只消费一次；
- 重启后不重放旧快照；
- 正常渠道不会调用账号测试接口；
- 自动熔断渠道按 300 秒和预算执行恢复探测；
- 写回前后值、所有权和审计一致；
- 停止写回不影响观察和微信通知。

### 24.4 历史回放

至少覆盖：

- 24 小时正常业务；
- 间歇 429/5xx；
- 持续凭据错误；
- 高延迟但成功；
- 无流量和数据中断；
- 单渠道分组保底；
- 人工暂停期间状态恢复；
- 价格、延迟和健康分同时变化。

## 25. 验收标准

### 25.1 采样与成本

- 同一个 `snapshot_id` 重放 100 次只产生一个证据桶；
- 24 小时正常运行中，Guardian 对健康渠道账号测试调用次数为 0；
- 真实请求重复记录不重复计分；
- 恢复探测达到预算后不再发起请求且产生一次告警。

### 25.2 评分

- 文档公式黄金样本误差 `<1e-9`；
- 不规则采样、重复采样和高流量场景的分数符合固定桶预期；
- 无新数据时健康分保持、置信度下降；
- 数据陈旧或置信度不足时写回次数为 0。

### 25.3 调度

- 权重整数和严格满足预算或显式最大值约束；
- 相同输入重复运行不产生写回；
- 小于 15% 且绝对差小于 2 的变化不写回；
- 单次变化不超过 20%；
- priority 不因价格或速度微小变化抖动；
- 自动熔断、回池和人工状态保护 100% 命中规则矩阵。

### 25.4 放权与安全

- 默认 Guardian 关闭、观察模式开启、所有 `autoApply=false`；
- 未经单独审批不能推进放权阶段；
- 检测到人工改值后对应字段停止写回；
- 一键停止与恢复接管前配置演练通过；
- 所有 mutation 鉴权、幂等、revision 和审计测试通过。

### 25.5 UI

- 页面能解释任一渠道最近一次分数和冻结/动作原因；
- 所有新增参数有单位、默认值、范围和继承来源；
- 390px 与 1440px 无横向溢出；
- 键盘可完成查看、编辑、确认和停止写回；
- Chrome 控制台无错误。

## 26. 工程命令

```powershell
uv sync --frozen --all-extras
uv run pytest -q
uv run ruff check .
uv run pyright
uv run pip-audit
docker build --tag bot-mcp-ci:guardian-v2 .
```

## 27. 项目结构

```text
src/sub2api_mcp/
  scheduler.py                    # 唯一常规共享快照采集入口
  repository.py                   # 共享快照持久化
  guardian/
    contracts.py                  # V2 policy、score、confidence DTO
    sampling.py                   # 去重、过滤、时间桶和融合
    scoring.py                    # 时间衰减健康分与置信度
    state_machine.py              # 新鲜度、人工状态、熔断与回池
    weights.py                    # 无量纲策略信号和预算归一化
    ownership.py                  # 字段所有权与人工接管检测
    writeback.py                  # Sub2API 写回与回滚适配器
    engine.py                     # 本地消费与单轮编排
    repository.py                 # Guardian V2 持久化
    api.py                        # REST API
    service.py                    # 应用服务与放权状态
    static/                       # 管理页面

tests/unit/guardian/
tests/integration/guardian/
tests/contract/guardian/
tests/browser/guardian/
docs/
```

## 28. 代码风格

- Python 3.12、严格类型、Pydantic `extra="forbid"`；
- 枚举使用大写稳定值；外部 API 字段保持 camelCase；
- 算法函数纯函数优先，时间和随机性必须注入；
- 金额、Token、计数和时间单位在字段名中明确；
- 对外原因使用稳定机器码，中文文案在展示层映射。

示例：

```python
decision = evaluate_channel(
    score=score.health_score,
    confidence=score.confidence,
    freshness=score.freshness,
    manual_control=channel.manual_control,
    policy=effective_policy,
)
```

## 29. 边界

### 始终执行

- 先更新规格和测试，再实现行为；
- 每个观察必须有稳定去重 ID；
- 低置信度和陈旧数据必须冻结写回；
- 写回前检查人工状态、字段所有权、保底、冷却、步长和 revision；
- 每次发布运行完整测试、类型检查、安全审计和容器构建。

### 需要产品确认

- V2 PRD 从 Draft 变为 Accepted；
- 数据库 schema 迁移进入生产；
- 启用任何恢复主动探测预算；
- 推进每一个写回阶段；
- 修改默认事件分、置信度门槛或恢复预算；
- 自动管理并发上限。

### 禁止

- 把没有新数据视为失败；
- 把同一快照或监控请求重复计分；
- 正常渠道由 Guardian 独立主动探测；
- 用低置信度历史数据写回；
- 自动恢复任何人工或上游人为暂停状态；
- 预算耗尽后继续恢复探测；
- 将密钥、完整请求或敏感用户标识写入样本、日志或通知。

## 30. 风险与缓解

| 风险 | 影响 | 缓解 |
|---|---|---|
| 渠道归属不准确 | 错误评分 | 无法唯一归属的流量不计分，只展示未归属统计。 |
| 共享快照本身陈旧 | 错误动作 | 独立新鲜度状态和写回冻结。 |
| 高流量渠道支配评分 | 权重偏差 | 固定一分钟桶、单桶样本上限。 |
| 监控流量双重计分 | 过度自信 | 请求 ID、User-Agent 和探测台账三重过滤。 |
| 恢复探测消耗失控 | Token 超支 | 请求数与 Token 双预算、并发 1、超限熔断。 |
| load_factor/priority 互相打架 | 调度抖动 | 连续分流只由 load_factor 控制，priority 只做健康层级。 |
| 外部人工改值被覆盖 | 失去控制权 | 字段所有权检测，发现改值立即停止覆盖。 |

## 31. 未决问题

以下问题不阻塞 Draft 评审，但必须在实现前确认：

1. 恢复探测每日硬预算最终按请求、Token 还是金额执行；当前草案同时限制请求和 Token。
2. 单分组灰度的业务高峰观察周期长度。
3. `priority` 灰度是否必须再次人工批准；当前草案要求单独批准。
4. Sub2API 是否能提供稳定渠道 ID 的 ops 归属字段；否则需要延续 API Key/分组映射。
5. 上游是否提供明确的“人工暂停”来源标记；没有时采用“无 Guardian 审计的关闭均视为人工”的保守规则。

## 32. 评审结论

当前状态：**等待产品负责人评审**。

评审通过只授权进入技术计划与任务拆分，不授权生产写回，也不自动启用 Guardian。
