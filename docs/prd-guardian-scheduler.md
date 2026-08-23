# PRD：Sub2API Guardian 调度系统

## 0. 文档信息

- 状态：**Accepted — 已批准进入实现**
- 版本：0.1
- 日期：2026-08-23
- 产品负责人：已于 2026-08-23 在任务会话中批准
- 实现仓库：`ShuaiLeiLu/bot-mcp`
- 调研依据：[WogHub Guardian 调度规则调研](./woghub-guardian-scheduling-rules.md)
- 目标参照：<https://schedule.woghub.com/policy>（页面版本 v0.0.31）

## 1. 假设与待确认事项

在产品确认前，本 PRD 使用以下默认假设：

1. 新调度系统继续作为现有 `bot-mcp` 的子系统运行，不拆成第二个服务。
2. 管理页面和 API 与 MCP 服务同源，默认路径为 `/guardian/` 和 `/api/guardian/v1/*`。
3. 不复制目标系统源码或品牌素材；按观察到的功能、规则和信息架构独立实现等价产品。
4. 首版单实例运行，继续使用 SQLite；通过租约保证同一时刻只有一个调度轮次。
5. 初次上线强制 `observeOnly=true`：计算期望状态但不写回 Sub2API。
6. API 继续复用现有 MCP API Key 权限体系；浏览器端密钥只保存在页面内存，不写 `localStorage/sessionStorage`。
7. 消息通知复用现有 LangBot 全渠道投递，不新增 Telegram 专用耦合。
8. 当前已明确的健康评分、熔断、回池和范围规则要求行为一致；未公开的权重归一化公式必须先完成黄金样本对拍。
9. 正式启用写回前，需要产品负责人明确批准从观察模式切换到执行模式。

待确认：

- 是否接受同源内存密钥登录，还是必须增加 HttpOnly 管理会话。
- 是否要求页面视觉像素级一致，还是信息结构和交互行为一致即可。
- 是否需要多上游/多工作区；本 PRD 首版保留数据结构，但只启用一个上游。
- 观察期要求：建议至少 24 小时且不少于 100 轮。

## 2. 产品目标

构建一套自主可控的 Sub2API 渠道守护与动态调度系统，提供：

- 主动探测与真实流量联合采样；
- 可解释、可复现的健康评分；
- 分组保底、软降级、自动熔断和健康回池；
- 价格优先、速度优先、均衡三种调度策略；
- 优先级、负载因子、并发和调度状态的安全写回；
- 全局、分组、渠道三级配置；
- 与参照系统同结构的监控和设置页面；
- 全量事件、前后值和写回原因审计；
- 观察模式、回滚和恢复原始配置能力；
- 通过 LangBot 向微信及其他适配器发送状态、异常和恢复通知。

## 3. 用户与核心场景

### 3.1 调度管理员

- 查看所有渠道与账号健康度。
- 调整全局/分组/渠道参数。
- 暂停、排除、手动熔断、手动恢复或立即探测。
- 查看当前流量和写回记录。
- 在放权前比较观察模式和实际 Sub2API 状态。

### 3.2 值班人员

- 接收状态汇总、异常、熔断和回池通知。
- 通过事件日志定位触发原因。
- 一键恢复 Guardian 接管前配置。

### 3.3 系统集成方

- 通过稳定 REST/MCP API 读取策略、状态和事件。
- 使用幂等操作触发同步、单轮评估和渠道动作。

## 4. 非目标

- 不替换 Sub2API 自身请求路由器。
- 不处理支付、充值或转账。
- 不自动恢复人工暂停渠道。
- 不在无法分类错误时执行破坏性写回。
- 不在首版支持多实例并行写回。
- 不复制第三方项目的源码、账号数据或品牌标识。

## 5. 产品信息架构

侧栏与参照系统保持同结构：

1. 总览
2. 分组调度
3. 渠道池
4. 实时路由
5. 探测费用
6. 调度说明
7. 事件日志
8. 策略配置
9. 连接设置
10. 信息与通知

策略配置包含：

- 运营配置
- 系统级规则
- 守护范围

## 6. 核心调度模型

### 6.1 调度时序

```text
每 15 秒扫描
  → 找出普通探测/恢复探测到期渠道
  → 拉取真实流量增量
  → 并发探测
  → 样本分类与赋分
  → 计算短期/长期/最终健康分
  → 应用范围、熔断、保底、降级和回池规则
  → 计算权重、priority、load_factor、schedulable
  → 防抖、冷却、每轮上限和乐观锁检查
  → 观察或写回
  → 审计与通知
```

### 6.2 样本来源

- `probe`：主动账号测试。
- `traffic`：Sub2API ops 真实请求日志。

每条样本必须包含：

- `channelId`
- `source`
- `eventType`
- `score`
- `occurredAt`
- `ttfbMs`
- `statusCode`
- 安全截断后的 `message`
- 可选 `requestIdHash`

禁止存储原始 API Key、Authorization 头或完整上游响应。

### 6.3 事件分值

默认值：

| 事件 | 分数 |
|---|---:|
| `PERFECT` | 100 |
| `SLOW_TTFB` | 65 |
| `UPSTREAM_UNKNOWN` | 40 |
| `GATEWAY_ERROR` | 25 |
| `QUOTA_EXHAUSTED` | 15 |
| `PROBE_FAIL` | 10 |
| `FATAL` | 0 |

评分首字慢阈值默认 5000ms。

### 6.4 精确健康分公式

短期窗口默认 10，长期窗口默认 60。

对于短期样本 `s[0..n-1]`，`s[0]` 为最新样本：

```text
latestWeight = 0.5
decay = 0.5

shortScore = s[0] × latestWeight
           + weightedGeometricMean(s[1..n-1], decay) × (1-latestWeight)

finalScore = shortScore × 0.7 + longMean × 0.3
```

其中其余短期样本的权重为 `decay^i / Σdecay^i`。该公式已使用参照系统实时样本反算到浮点值完全一致。

### 6.5 状态

内部状态使用显式枚举：

- `PENDING`
- `HEALTHY`
- `DEGRADED`
- `RATE_LIMITED`
- `FUSED`
- `FORCED_KEEP`
- `MANUALLY_PAUSED`
- `EXCLUDED`
- `UPSTREAM_DISABLED`

人工状态与自动状态分开持久化，自动评分不得覆盖人工暂停或排除。

## 7. 熔断和保底

默认规则：

- 错误窗口 5，失败至少 3，且健康分低于 60。
- 延迟窗口 10，慢响应至少 5，慢阈值 15000ms。
- 每轮最多熔断 1 个。
- 熔断冷却 180 秒。
- 分组至少保留 1 个可用渠道。
- 可用池最低分 3。
- 401/402/403 和致命关键字属于一票否决候选。
- 致命错误仍受分组保底约束。
- 短时 429/5xx 只降级；持续窗口失败可临时熔断。
- 有 `rate_limit_reset_at` 的限流渠道不得永久关闭。
- 延迟超标默认只降级。

## 8. 降级与回池

### 8.1 降级

- 阈值 75。
- 优先级每层增加 1，最大 5。
- `load_factor` 乘 0.5，最低 1。
- 渠道仍可接流量。

### 8.2 回池

- 恢复探测间隔 180 秒。
- 目标分 75。
- 连续成功 3 次。
- 健康保持 60 秒。
- 达标后恢复正常优先级、负载和调度状态。
- 恢复探测独立于普通主动探测开关。

## 9. 权重与策略

### 9.1 策略

- `PRICE`
- `SPEED`
- `BALANCED`

默认 `PRICE`。

### 9.2 参数

- 分组权重预算：400
- 健康闸门：40
- 均衡价格占比：0.5
- 价格指数：1
- 速度指数：1
- `load_factor`：1～100
- 变化阈值：10%
- 写回冷却：60 秒

### 9.3 权重公式验收要求

参照系统未公开后端归一化实现。实现前必须：

1. 从只读 `/api/groups` 保存不少于 20 轮脱敏黄金快照；
2. 覆盖单组、多组共享渠道、无价格、低健康分、降级和权重闸门场景；
3. 对候选公式执行拟合和交叉验证；
4. 在独立样本上权重误差达到：
   - `priority` 完全一致；
   - `load_factor` 误差不超过 1；
   - 单渠道权重相对误差不超过 1%；
5. 未达标时只能以“自有算法”命名，不能宣称完全一致。

初始候选公式（待对拍，不是已确认规则）：

```text
priceSignal = 1 / max(effectiveRate, epsilon)^priceExp
speedSignal = 1 / max(ttfbP95, floorMs)^speedExp
strategySignal = price / speed / weightedBlend(price, speed)
healthSignal = gate(score) × degradation(score)
rawWeight = strategySignal × healthSignal
weight = normalize(rawWeight, groupBudget)
```

## 10. 三级配置

### 10.1 全局

包含调度、自动写回、熔断、保底、降级、回池、权重、采样、评分、分类和守护范围。

### 10.2 分组覆盖

- 是否参与守护
- 策略
- 保底数
- 权重预算
- 均衡价格占比
- 熔断开关
- 回池开关
- 调权开关
- 定时探测开关
- 探测间隔（最小 30 秒，步进 30）
- 探测模型

未设置字段继承全局；清除 override 恢复全局。

### 10.3 渠道覆盖

- 优先级 1～5
- `load_factor >= 1`
- 并发上限 `>=1`
- Guardian 内部调度倍率 `>=0`，步进 0.01
- 探测模型
- 人工暂停
- 排除
- 手动熔断/恢复
- 立即探测

### 10.4 火箭

- 快捷 10/30/60/180 分钟
- 自定义 1～10080 分钟
- 生效时 `priority=1`，原 `load_factor+1000`
- 到期或取消恢复原值

## 11. 守护范围优先级

```text
排除分组
  > 排除渠道
  > 人工暂停渠道
  > managedGroupMode / managedGroupIds
  > managedAccountTypes / managedPlatforms
  > 分组 enabled
```

- 排除：完全不监控并恢复原配置。
- 暂停：不接流量但继续监控，必须人工恢复。
- 自动熔断：允许满足回池条件后自动恢复。

## 12. 数据模型

SQLite 新增表：

| 表 | 用途 |
|---|---|
| `guardian_policy` | 全局策略 JSON、revision、更新时间。 |
| `guardian_group_overrides` | 分组覆盖。 |
| `guardian_channel_overrides` | 渠道优先级、负载、并发、倍率、探测模型与火箭覆盖。 |
| `guardian_channels` | 当前状态、期望值、原始值、冷却、streak 和人工状态。 |
| `guardian_samples` | 探测与真实流量样本。 |
| `guardian_runs` | 每轮开始/结束、统计与结果。 |
| `guardian_events` | 可过滤事件日志。 |
| `guardian_write_audits` | 写回前后值、原因、幂等键和结果。 |
| `guardian_probe_ledger` | 自动探测 token/成本估算。 |
| `guardian_leases` | 单实例调度租约。 |
| `guardian_original_config` | 接管前配置，用于恢复控制权。 |

所有写操作使用参数化 SQL 和事务；事件/样本列表必须分页。

## 13. REST API v1

统一响应：

```json
{
  "ok": true,
  "requestId": "uuid",
  "data": {}
}
```

错误：

```json
{
  "ok": false,
  "requestId": "uuid",
  "error": {
    "code": "POLICY_REVISION_CONFLICT",
    "message": "策略已被其他会话修改",
    "retryable": false
  }
}
```

主要端点：

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/api/guardian/v1/overview` | 总览。 |
| GET | `/api/guardian/v1/policy` | 策略与 defaults。 |
| PATCH | `/api/guardian/v1/policy` | 带 `If-Match` revision 更新策略。 |
| GET | `/api/guardian/v1/status` | 引擎与上一轮状态。 |
| POST | `/api/guardian/v1/runs` | 幂等触发单轮；默认 dry-run。 |
| POST | `/api/guardian/v1/runs/{id}/cancel` | 取消运行中轮次。 |
| POST | `/api/guardian/v1/syncs` | 同步 Sub2API 只读状态。 |
| GET | `/api/guardian/v1/groups` | 分组列表。 |
| PATCH | `/api/guardian/v1/groups/{id}/policy` | 分组覆盖。 |
| DELETE | `/api/guardian/v1/groups/{id}/policy` | 回落全局。 |
| GET | `/api/guardian/v1/channels` | 渠道分页/筛选。 |
| PATCH | `/api/guardian/v1/channels/{id}` | 渠道设置。 |
| POST | `/api/guardian/v1/channels/{id}/actions` | probe/pause/exclude/fuse/recover/boost。 |
| GET | `/api/guardian/v1/live-routing` | 实时路由。 |
| GET | `/api/guardian/v1/probe-spend` | 探测成本。 |
| GET | `/api/guardian/v1/events` | 事件分页。 |
| POST | `/api/guardian/v1/restores/preview` | 恢复原始配置预览。 |
| POST | `/api/guardian/v1/restores` | 二次确认后执行恢复。 |

所有 mutation：

- 需要 `sub2api:admin`；
- 接受 `Idempotency-Key`；
- 写审计；
- 不回显密钥；
- 恢复/批量写回需显式确认标志。

## 14. MCP 工具扩展

新增：

- `guardian_get_policy`
- `guardian_update_policy`
- `guardian_get_overview`
- `guardian_list_groups`
- `guardian_list_channels`
- `guardian_get_channel`
- `guardian_run_once`
- `guardian_cancel_run`
- `guardian_channel_action`
- `guardian_list_events`
- `guardian_get_probe_spend`
- `guardian_preview_restore`
- `guardian_execute_restore`

破坏性工具必须要求明确参数，例如 `confirm=true`，并遵守 API scope。

## 15. 管理页面

### 15.1 技术方案

- 静态 HTML/CSS/ES Modules，由现有 Starlette 服务提供。
- 不增加 Node 生产运行时和第三方前端依赖。
- API Key 仅保存在当前页面内存。
- 不使用 `innerHTML` 渲染外部数据；统一使用 `textContent`。
- 响应式桌面/移动布局、键盘导航、可见焦点、WCAG AA 对比度。

### 15.2 视觉和交互验收

- 左侧固定导航、顶部运行状态和动作条。
- 卡片、表格、标签、状态色、页签和模态框与参照系统信息层级一致。
- 策略页面字段、说明、范围和开关完整。
- 表单存在未保存提示、放弃修改、revision 冲突提示。
- 暂停/排除/熔断/恢复有不同文案和状态色。
- 禁止直接复制第三方 Logo、代码或独有品牌素材。

## 16. 安全威胁模型

### 资产

- Sub2API Admin Key
- MCP 管理令牌
- 账号调度状态
- 写回权限
- 原始配置与审计记录

### 边界与控制

| 威胁 | 控制 |
|---|---|
| 未授权读取/写回 | API Key scope；所有数据 API 鉴权。 |
| 密钥泄漏 | 仅环境变量；API/UI/日志永不回显。 |
| SSRF | 上游 URL 仅来自受信任环境配置；拒绝重定向。 |
| 错误分类误伤 | 未知/超时默认不可熔断；致命分类有明确 allowlist。 |
| 批量雪崩 | 保底池、每轮上限、租约、冷却。 |
| 并发覆盖策略 | revision + `If-Match`。 |
| 重复写回 | 幂等键和审计唯一约束。 |
| XSS | `textContent`、CSP、无第三方脚本。 |
| 恢复错误值 | 持久化接管前快照；执行前预览。 |

## 17. 可观测性

指标：

- 调度轮次耗时/结果
- 到期任务和排队数
- 每类样本数与得分
- 熔断、保底、回池、写回计数
- dry-run 与实际动作差异
- 权重/优先级/负载变化
- 探测费用和未计价数
- API 延迟与错误率

日志必须带：`requestId/runId/channelId/groupId/action/reason`，禁止带密钥或完整上游正文。

## 18. 发布策略

### 阶段 A：算法离线对拍

- 使用参照系统脱敏黄金快照。
- 验证评分、分类、状态机和权重。
- 不访问生产写接口。

### 阶段 B：生产观察模式

- 同步真实状态和流量。
- 运行全部计算。
- `autoApply.* = false`。
- 输出“本系统期望动作 vs 当前真实状态”。

### 阶段 C：单分组灰度

- 选择一个非核心分组。
- 每轮最多写 1 个渠道。
- 随时可恢复接管前配置。

### 阶段 D：扩大范围

- 达到验收门槛后逐组开启。
- 保留全局熔断开关和一键恢复。

## 19. 验收标准

### 算法

- 健康分与黄金样本绝对误差 `<1e-9`。
- 事件分类 100% 命中测试矩阵。
- 熔断、保底、降级、回池决策 100% 命中规则测试。
- 人工暂停/排除绝不自动恢复。
- 权重对拍达到第 9.3 节门槛。

### API

- 所有输入 Pydantic 严格校验。
- 错误结构统一。
- 所有列表分页。
- mutation 幂等、鉴权、审计。

### UI

- 10 个菜单和 3 个策略页签均可用。
- 设置字段、范围、默认值和说明与 PRD 一致。
- Chrome 实机无控制台错误。
- 390px 与 1440px 视口无横向溢出或不可操作控件。
- 键盘可完成导航和策略编辑。

### 安全与运维

- `pip-audit` 无未处置高危漏洞。
- 密钥扫描干净。
- 容器保持非 root、只读文件系统和最小权限。
- 一键恢复演练通过。
- 观察期无生产写回。

## 20. 工程命令

```powershell
uv sync --frozen --all-extras
uv run pytest -q
uv run ruff check .
uv run pyright
uv run pip-audit
docker build --tag bot-mcp-ci:guardian .
```

## 21. 项目结构

```text
src/sub2api_mcp/guardian/
  contracts.py       # 策略、样本、状态和 API DTO
  scoring.py         # 精确评分
  classifier.py      # 错误分类
  state_machine.py   # 熔断/降级/回池
  weights.py         # 价格/速度/均衡权重
  engine.py          # 单轮编排
  repository.py      # Guardian 持久化
  api.py             # REST API
  tools.py           # MCP 工具
  web.py             # 静态页面路由
  static/            # HTML/CSS/JS

tests/unit/guardian/
tests/integration/guardian/
tests/contract/guardian/
tests/browser/guardian/
```

## 22. 边界

### 始终执行

- 先写测试，再实现算法。
- 任何外部响应先校验。
- 默认观察模式。
- 写回前检查租约、范围、保底、revision 和幂等键。
- 每次提交运行完整质量门。

### 需要产品确认

- 启用任何生产写回。
- 增加持久管理会话或新认证方式。
- 多实例写回。
- 权重算法无法达到参照系统对拍门槛时改用自有算法。

### 禁止

- 提交或记录密钥。
- 自动恢复人工暂停。
- 绕过分组保底。
- 用未知错误触发永久关闭。
- 在未通过观察期时切换生产控制权。
