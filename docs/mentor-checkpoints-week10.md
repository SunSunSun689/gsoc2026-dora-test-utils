# Mentor Checkpoints — Week 10

> 向 mentor 确认的问题汇总。Discussion #20 (Week 3) 和 #28 (Week 7) 中的意见已全部落实。

---

## ✅ 已完成变更（mentor 已知，供 review）

### 1. 移除 vendored DORA patch，切换到 baked events（Option 1）

**mentor 原话 (Discussion #20)**：
> "Go with Option 1 for the MVP. Please don't ship the vendored-Channel version."

**做了**：
- 删除 `dora-patches/testing-input-channel.patch`
- `Cargo.toml`: path dep → git dep (`dora-node-api = { git = "...", rev = "45436aad" }`)
- `NodeHarness` 重写为 deferred init：`send_data/send_stop` 缓冲事件到 Vec，`tick/run/send_output` 时懒创建 DoraNode，用 `TestingInput::Input` 一次性注入
- CI 精简：check/test/clippy 不再 clone dora（0 秒开销）

**效果**：

| | Before | After |
|---|---|---|
| e2e 串行 | 4.23s (2/10 hang) | 0.01s (0 hang) |
| e2e 并行 | 死锁 | 0.00s 正常 |
| Clean build | ❌ (需 clone 12G dora) | ✅ (git dep 自动拉取) |
| Vendored patch | 存在 | 已删除 |

### 2. `--test-threads=1` 移除

**mentor 原话 (Discussion #28)**：
> "The CI workaround is not acceptable as a steady state. Replace it with honesty."

**做了**：死锁根因消除后，e2e/smoke 不再需要 `--test-threads=1`。仅 `integration-test` 保留（dora daemon 绑 port 6013，是真实约束不是 workaround）。

### 3. API Stability 说明

**mentor 原话 (Discussion #28)**：
> "write a short note explaining which APIs are intended to be public/stable"

**做了**：`src/lib.rs` 新增 API Stability 表格，NodeHarness/Mock/Source/Sink 标记为 Stable，RecordSession/ReplaySession 标记为 Experimental。

---

## ✅ Week 10 完成：ReplaySession

```rust
// API
let result = ReplaySession::load("baseline.json")?
    .replay_sink("test-sink", "output.json")
    .dataflow("dataflow.yml")?    // 覆盖原 YAML 路径（可选）
    .with_timeout(Duration::from_secs(10))  // 覆盖超时（可选）
    .run()?;

result.is_clean();               // bool
result.diff();                   // &DiffReport (Display + Serialize)
result.assert_no_regression();   // panic with diff
```

- **Two-layer comparison**: Layer 1 JSON 快速比 → Layer 2 Arrow 语义比（容忍类型差异）
- **DiffReport**: SinkDiff/FieldDiff/DiffStatus(Match/Mismatch/Missing/Extra)，支持 Display + Serialize
- **测试**: 7 unit + 11 e2e = 71 total, all pass

---

## ❓ 需要 mentor 确认

### Q1: Upstream PR 时机

`docs/upstream-pr-plan.md` 规划了两个 upstream PR：

**(a) `TestingOutput::ToChannel` flume→tokio-mpsc**
- 纯内部迁移，补全 #1603 未完成的部分
- 最容易合入：不新增 API，只改实现
- 已准备好随时提交

**(b) `TestingInput::Channel` 新 API**
- 新增 `Channel(tokio::sync::mpsc::Receiver<...>)` 变体
- 需要先开 Discussion 讨论 API 设计
- 如果被拒绝：当前 baked events 方案已满足所有需求（escape hatch）

**问题**：现在开始 (a)？还是等 Week 11？

### Q2: DORA version bump

当前 **已升级到** `1fba721`（2026-08-09，Week 11）。`TestingOutput::ToChannel` 的
flume→tokio mpsc 迁移已由上游完成，我们直接消费。详见 `docs/PROGRESS.md` Week 11。
arrow 已从 58 升级到 59（匹配新的 DORA rev）。

**原始内容**（已过时）：
> 当前 pin 在 `45436aad`（2026-06-01 确认）。我们评估过升级到 `v1.0.0-rc.4`：
> - ✅ 代码兼容（patch clean apply）
> - ⚠️ arrow 58→59 需改 Cargo.toml
> - ⚠️ rc.4 仍有 daemon thread 死锁问题（不是升级能解决的）
> 
> **问题**：是否需要升级？还是保持 `45436aad` 直到 final submission？

### Q3: RecordSession/ReplaySession API 反馈

当前 API 是否符合 mentor 期望？有没有需要调整的地方？

---

## 文件清单

| 文件 | 内容 |
|------|------|
| `docs/upstream-pr-plan.md` | 两个 upstream PR 的详细设计 |
| `docs/superpowers/specs/2026-07-27-replaysession-design.md` | ReplaySession 设计 spec |
| `docs/superpowers/plans/2026-07-27-replaysession.md` | 实现计划 |
| `docs/PROGRESS.md` | 整体进度 + Week 10 详情 |
