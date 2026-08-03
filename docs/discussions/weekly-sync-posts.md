# Weekly Sync Discussion Posts（待发布）

> 以下 4 篇帖子需要发布到 GitHub Discussions，Category: `Weekly Sync`。
> 发布时间：本周内一次性补上（Week 8/9/10/12）。

---

---

## Week 8 Sync — 多输出 TestSource + 三管道 Demo + CI 死锁诊断

**日期**: 2026-07-14 ~ 2026-07-20
**Category**: Weekly Sync
**Labels**: `week-8`, `progress`

### 本周完成

#### 1. 多输出 TestSource（Issue #3 修复）

原来的 `run_test_source` 为每个 output 创建独立的 `DoraNode`，导致 N 个 output 时 daemon 收到 N 次 `Register` 消息，后续消息被拒绝。重构为**单个 DoraNode 共享所有 output**：

```rust
pub fn run_test_source(config: SourceConfig) -> Result<()> {
    validate_all_specs(&config.outputs)?;
    let (mut node, _events) = DoraNode::init_from_env()?;
    for spec in &config.outputs {
        emit_output(&mut node, spec)?;
    }
    Ok(())
}
```

#### 2. 三管道 Demo（`scripts/demo-week8.sh`）

| Pipeline | 节点 |
|----------|------|
| echo | test-source → echo-node → test-sink |
| multi-echo | test-source → multi-output-echo → test-sink (×3 outputs) |
| classifier | test-source → classifier-node → test-sink |

#### 3. Code Review 修复（Issues #3–#5 全部关闭）

| Issue | 修复 |
|-------|------|
| #3 Multi-DoraNode per output | 单个 DoraNode 共享 output |
| #4 二进制命名不一致 | `test_source` → `test-source` 统一 |
| #5 `--inline-data` 缺失 | 恢复为单输出模式下的备选方案 |

#### 4. CI 死锁诊断

**症状**: GitHub Actions 2-vCPU runner 上 `cargo test` 永久死锁（6h timeout）。

**根因**: `TestingInput::Channel` 使用 `flume::Receiver`（flume 0.10 内部 spinlock）。在抢占式内核上，spinlock holder（daemon 线程）可能被抢占，主线程 `Sender::drop()` 永等锁 → 死锁。对应上游 issue：[dora-rs/dora#1603](https://github.com/dora-rs/dora/issues/1603)。

**临时 Workaround（三层）**:
1. 代码层：`send_input()` 加 `yield_now()` + Drop sleep 500ms
2. CI 层：retry ×5 + `timeout 120s`
3. CI 层：harness 测试 `continue-on-error: true`，核心测试 `--skip harness`

详见 `docs/CI-DEADLOCK-FIX.md`。

### 关键提交

| Commit | Description |
|--------|-------------|
| `9341f18` | feat(week8): multi-output test-source + classifier-node + integration tests |
| `7615723` | fix(code-review): single DoraNode, binary names, inline-data, error handling |
| `f60e51c` | chore: regenerate patch + update README for tokio-mpsc migration |
| `355a829` | docs: document CI deadlock root cause and fix |

### 验证

- `cargo check` ✅
- `cargo test --lib` ✅
- `cargo test --test integration -- --test-threads=1` ✅
- `cargo fmt --check` ✅
- `cargo clippy` ✅
- `bash scripts/demo-week8.sh` ✅

### 待讨论

1. **CI 死锁永久解决方案**：是否向上游提 PR 将 `TestingInput::Channel` 从 flume 迁移到 tokio mpsc？
2. 当前 workaround 可用但脆弱——harness/e2e 测试失败在 CI 上被静默容忍。

### 下周计划

- Week 9：在本地完成 flume→tokio mpsc 迁移，移除所有 CI workaround

---

---

## Week 9 Sync — flume→tokio mpsc 迁移 + RecordSession 实现

**日期**: 2026-07-21 ~ 2026-07-27
**Category**: Weekly Sync
**Labels**: `week-9`, `progress`

### 本周完成

#### 1. flume→tokio mpsc 迁移（根治死锁）

将本 crate 中所有 `flume` channel 替换为 `tokio::sync::mpsc`：

| 变更 | 影响 |
|------|------|
| 移除 6 个 CI workaround | `#[ignore]`、`continue-on-error`、retry×5、timeout、`--skip harness`、`yield_now()` |
| 62 tests 并行安全 | 不再需要 `--test-threads=1`（仅 integration-test 保留，因为 dora daemon 绑 port 6013） |
| Drop 简化 | 移除 NodeHarness Drop 中的 500ms sleep |

#### 2. 移除 vendored DORA patch

按 mentor 建议（Discussion #20, #28），从 baked events + deferred init 模型彻底切换到干净的 git dependency：

```toml
# Before: vendored path + patch
dora-node-api = { path = "dora/apis/rust/node" }

# After: clean git dep
dora-node-api = { git = "https://github.com/dora-rs/dora.git", rev = "45436aad" }
```

`dora-patches/` 目录已删除。`cargo fetch` 即可构建，无需 clone 12GB dora 仓库。

#### 3. PR #35 合并冲突解决

Merge upstream/main → week9，5 个冲突全部手动解决：
- `.github/workflows/ci.yml` — 保留 HEAD（flume→tokio 简化）
- `src/harness.rs` — 保留 HEAD（tokio 注释，Drop 无 sleep）

#### 4. RecordSession 实现（Week 9 后半）

**新文件 `src/record.rs`**：

```rust
// 录制 baseline
let recording = RecordSession::attach("dataflow.yml")?
    .record_sink("test-sink", "output.json")
    .with_timeout(Duration::from_secs(10))
    .run()?;
recording.save("baseline.json")?;
```

| 类型 | 说明 |
|------|------|
| `RecordSession` | builder-pattern API：`attach` → `record_sink` → `with_timeout` → `run` |
| `Recording` | `save(path)` / `load(path)` JSON 持久化 + metadata |
| `RecordingMetadata` | dataflow_yaml, recorded_at_unix, timeout_secs, dora_version |
| `RecordError` | 8-variant error enum (Display + Error + From) |

#### 5. SinkConfig record_mode

`SinkConfig` 新增 `record_mode: bool` 字段。启用时 TestSink 写出原始接收数据（`{"data": [...], "data_type": "...", "count": N}`）而非比对结果。

### 测试

| 类型 | 数量 | 文件 |
|------|------|------|
| 库单元测试 | 46 | `src/` |
| e2e (harness) | 5 | `tests/e2e.rs` |
| e2e_record | 4 | `tests/e2e_record.rs` |
| smoke | 3 | `tests/smoke.rs` |

### 关键提交

| Commit | Description |
|--------|-------------|
| `9292907` | fix: migrate NodeHarness from flume to tokio::sync::mpsc |
| `a62ff7d` | refactor: remove vendored DORA patch — deferred init + baked events |
| `866c117` | Merge upstream/main into week9 — resolve 5 conflicts |
| `35d40f1` | feat(sink): add record_mode to SinkConfig for raw data capture |
| `0c0d4fb` | feat(record): add RecordSession and Recording types |
| `5c8dc44` | test(record): add e2e tests for RecordSession |

### 验证

- `cargo check` ✅
- `cargo fmt --check` ✅
- `cargo clippy --lib` ✅ (zero warnings)
- `cargo test --lib` ✅ (46/46)
- `cargo test --test e2e -- --test-threads=1` ✅
- `cargo test --test e2e_record -- --test-threads=1` ✅ (4/4)

### 待讨论

1. RecordSession API 是否符合 mentor 预期？
2. Upstream PR 策略确认（见 `docs/upstream-pr-plan.md`）

### 下周计划

- Week 10：ReplaySession 实现 + 回归检测

---

---

## Week 10 Sync — ReplaySession 实现 + 两轮 Code Review

**日期**: 2026-07-28 ~ 2026-08-03
**Category**: Weekly Sync
**Labels**: `week-10`, `progress`

### 本周完成

#### 1. ReplaySession 实现

**核心 API**：

```rust
// 重放 + 比对
let result = ReplaySession::load("baseline.json")?
    .replay_sink("test-sink", "output.json")
    .dataflow("dataflow.yml")?           // override YAML path (optional)
    .with_timeout(Duration::from_secs(10)) // override timeout (optional)
    .run()?;

// 结果
result.is_clean();               // bool — no regressions?
result.diff();                   // &DiffReport — structured diff
result.assert_no_regression();   // panic! with formatted diff
```

**二层比对架构**：

| Layer | 方法 | 容错 |
|-------|------|------|
| 1: JSON structural | 递归 `json_diff`（字段路径 + 值） | 严格匹配 |
| 2: Arrow semantic | `sink::compare_semantic` | 容忍类型差异（Int32↔Int64 等） |

**DiffReport 结构**：

```
DiffReport
├── SinkDiff[]
│   ├── sink_id
│   ├── status: Match | Mismatch | Missing | Extra
│   └── FieldDiff[] — field-level path + expected vs actual values
```

`DiffReport` 实现了 `Display` + `Serialize`，可直接打印或序列化为 JSON。

#### 2. 测试

| 类型 | 数量 | 内容 |
|------|------|------|
| 单元测试（comparison logic） | 7 | json_diff, compare_data_semantic, DiffReport |
| e2e（ReplaySession） | 11 | clean replay, mismatch detection, missing/extra sinks, timeout override, dataflow override |

#### 3. 第一轮 Code Review 修复（13/15）

| 类别 | 关键修复 |
|------|---------|
| **正确性** | `.data` 前缀匹配过宽（也匹配 `.data_type`）→ 改为 exact match；`as_f64` 死代码（Int64 永远不可达）→ `as_i64` 优先 |
| **事件丢失** | `run_to_completion` 已 init 状态下 Stop 注入失败；`ensure_init` 静默丢弃 pending events |
| **CI** | `e2e_replay` 11 tests 在 CI 上零覆盖 → 补入 `integration-test` job |
| **类型保真** | `write_record_output` 用 Debug 格式写 `data_type`；`number_to_arrow_array` u64→Float64 截断 |

#### 4. 第二轮 Code Review 修复（13/15）

| 类别 | 关键修复 |
|------|---------|
| **假 Match** | semantic comparison 返回空时回退到 json diffs |
| **NaN panic** | `Duration::from_secs_f64` 加 `clamp(0.1, 3600)` + `is_finite()` 守卫 |
| **副作用时序** | 过期文件删除移到验证之后（不在错误路径破坏用户数据） |
| **跨类型 Number** | `3` vs `3.0` 假 Mismatch：数值类型通过 `as_f64()` 比较 |
| **HashMap 稳定性** | `compare_recordings` 按 sink_id 排序后输出 |
| **Metadata 一致性** | `ReplayResult.metadata` 反映实际的 timeout/YAML 覆盖值 |

**延期 4 个**:
- `traits.rs` data_type 丢失 → 需 ArrowFile 升级路径
- `sink.rs` receive timeout → EventStream 无 timeout API

### 关键提交

| Commit | Description |
|--------|-------------|
| `c81efec` → `2922e03` | feat(replay): ReplayError → DiffReport → ReplaySession::run() |
| `6075132` | fix(replay): `.data` prefix → exact match |
| `3234bf2` | test(replay): 8 e2e tests |
| `5462bc8` | test(replay): timeout override, Missing, Extra e2e tests |
| `dfa071a` | fix: 13 code-review findings (Round 1) |
| `4d36ac3` | fix: 11 code-review findings (Round 2) |

### 验证

- `cargo check` ✅
- `cargo fmt --check` ✅
- `cargo clippy --lib` ✅ (zero warnings)
- `cargo test --lib` ✅ (52/52 → 80/80 after Week 12 work)
- `cargo test --test e2e_replay -- --test-threads=1` ✅ (11/11)
- 全部 CI 6 个 job 绿色

### 待讨论

1. **Upstream PR 时机**：代码已准备就绪（`SunSunSun689/dora:testing-output-tokio-mpsc`），现在提交还是等 final submission 后？
2. **DORA version bump**：当前 pin `45436aad`，是否需要升级？
3. **RecordSession/ReplaySession API 反馈**：是否符合 mentor 预期？
4. Weekly Sync Discussion 帖积压：本周补上 Week 8/9/10/12

---

### 🔴 Final Submission 准备（需 mentor 确认）

Coding Phase 2 截止日期：**2026-08-24**（Standard）或 **2026-11-02**（Extension）。

#### Q1: Standard vs Extension？

| 路径 | Deadline | 剩余时间 | 策略 |
|------|----------|---------|------|
| Standard | 8/24 | 3 周 | Week 11–13 高度聚焦，只做必须项 |
| Extension | 11/2 | 13 周 | 上游 PR、Python binding、CI 模板都可以从容完成 |

Proposal 里写了 350h Extended 路径。**请 mentor 确认走哪条路。**

#### Q2: Final submission 交付物优先级

以下清单请 mentor 确认每项的优先级（必须 / 建议 / 延后）：

| # | 交付物 | 状态 | 建议优先级 |
|---|--------|------|-----------|
| 1 | 本 crate 代码 + 109 tests | ✅ done | 必须 |
| 2 | README + API docs + rustdoc | ✅ done | 必须 |
| 3 | CI (6 jobs 全绿) | ✅ done | 必须 |
| 4 | Final demo 脚本 | ⚠️ 需整合（现有 3 个分散脚本） | 必须？ |
| 5 | Upstream PR (a): flume→tokio mpsc | 🔔 代码就绪，未提交 PR | 必须？ |
| 6 | Final report (GSoC 格式) | ❌ 未开始 | 必须 |
| 7 | 4 个 deferred code review issues | ⚠️ 已文档化 | 建议？ |
| 8 | Upstream PR (b): Channel API | ❌ 未开始 | 延后 |
| 9 | Python bindings | ❌ 未开始 | 延后 |
| 10 | CI 模板 (GitHub Actions) | ❌ 未开始 | 延后 |

#### Q3: Upstream PR (a) 是否纳入 final submission？

- 代码 + tracking issue ([dora-rs/dora#2956](https://github.com/dora-rs/dora/issues/2956)) 已完成
- 如果属于 submission → 本周立刻提交 PR
- 如果属于 post-submission → 记录到 deferred 列表

#### Q4: Final report 格式要求？

GSoC 要求提交 final report。需要确认：
- Mentor 有没有模板或示例？
- 沿用 `docs/MIDTERM-REPORT.md` 的格式即可？
- 需要包含 demo 视频/GIF 吗？

#### Q5: Demo 的期望形式？

- 一个综合脚本（`scripts/demo-final.sh`）全跑完？
- 还是需要 live demo session？
- 需要录制视频吗？
- 是否需要性能 bench 数据（Record/Replay 耗时对比等）？

### 下周计划

- Week 11：Upstream PR (a) 提交（等待 mentor 绿灯）
- Week 12（提前）：demo 脚本、README 更新、边缘测试

---

---

## Week 12 Sync — Demo 脚本 + README 更新 + 边缘测试

**日期**: 2026-08-02（提前完成，原计划 8/11–8/17）
**Category**: Weekly Sync
**Labels**: `week-12`, `progress`

### 本周完成

#### 1. Demo 程序

- **`examples/demo_replay.rs`**：完整的 Record→Replay→Regression Detection Rust 示例
- **`scripts/demo-week12.sh`**：一键构建 + demo + 全量测试套件（build → lib tests → e2e → e2e_replay → smoke → demo example）

#### 2. README 更新

- 新增 RecordSession/ReplaySession API 文档（含完整代码示例）
- 新增 API Stability 表格：

| API | Status |
|-----|--------|
| `NodeHarness` | ✅ Stable |
| `MockEventStream` / `MockOutputSender` / `OutputCollector` | ✅ Stable |
| `TestSource` / `TestSink` (`src/source.rs`, `src/sink.rs`) | ✅ Stable |
| `RecordSession` / `Recording` | ⚠️ Experimental |
| `ReplaySession` / `ReplayResult` / `DiffReport` | ⚠️ Experimental |
| `ReplayError` | ⚠️ Experimental |

- 更新测试数量（52→80 lib, 109 total）
- 更新项目结构、CI job 说明

#### 3. 边缘测试（+28 单元测试，52→80）

| 模块 | 新增 | 覆盖场景 |
|------|------|---------|
| `compare_recordings` | 4 | Match/Missing/Extra/Mixed 状态分配 |
| `compare_sink_outputs` | 2 | `.data_type` 差异保留（前缀修复验证） |
| `json_diff` | 4 | 嵌套对象、null 值、额外/缺失 key |
| `compare_data_semantic` | 3 | 空数组、长度不匹配、string fallback |
| `DiffReport` | 2 | 混合状态展示、全部 Match 计数 |
| `NodeHarness` | 4 | 初始化后事件清空警告、非法 ID panic、recv_output None、send_output 错误 |
| `TestSink` | 3 | Null 转换错误、int overflow cast、Boolean 数组 |
| `TestSource` | 6 | UInt16/UInt64 hint、对象缺少 hint 报错、异构数组拒绝、分数→Int64 报错、UInt16 overflow |

#### 4. Week 10–12 Progress Report

编写了 `docs/WEEKLY_REPORT-week10-12.md`（158 行），为 mentor sync 准备，覆盖：
- Week 10: ReplaySession + 二层比对
- Round 1 + Round 2 code review（28 个 issue）
- Week 12: demo、README、边界测试
- Upstream PR (a) 状态
- 测试统计：64 → 109
- 2 个待讨论问题

### 关键提交

| Commit | Description |
|--------|-------------|
| `98d1f24` | docs: Week 12 — demo script, README update, edge-case tests |
| `5abca01` | docs: annotate upstream flume→tokio-mpsc migration plan |
| `bfb93d3` | docs: Week 10-12 progress report for mentor sync |
| `8ae9811` | docs: remove unnecessary DORA version bump question from report |

### 验证

- `cargo check` ✅
- `cargo fmt --check` ✅
- `cargo clippy --lib` ✅
- `cargo test --lib` ✅ (80/80 pass, 0.04s)
- `cargo test --test e2e` ✅ (5/5)
- `cargo test --test e2e_record -- --test-threads=1` ✅ (4/4)
- `cargo test --test e2e_replay -- --test-threads=1` ✅ (11/11)
- `cargo test --test smoke -- --test-threads=1` ✅ (3/3)
- `bash scripts/demo-week12.sh` ✅

### 待讨论

1. 是否需要更细粒度的边界测试？
2. Week 8/9/10/12 讨论帖积压——本周一次性补上
3. **Final Submission 准备**：见上方 Week 10 帖的 Q1–Q5，本周 sync 中确认

### 下周计划

- Week 11（剩余）：提交 Upstream PR (a)，等待 review
- Week 13：Demo 准备 + Final Submission

---

---

## 发布检查清单

- [ ] Week 8 帖 → 复制到 GitHub Discussions
- [ ] Week 9 帖 → 复制到 GitHub Discussions
- [ ] Week 10 帖 → 复制到 GitHub Discussions（含 **Final Submission Q1–Q5**）
- [ ] Week 12 帖 → 复制到 GitHub Discussions
- [ ] 每帖创建后，在相应 Discussion 下回复一个简短摘要
- [ ] 将 Final Submission Q1–Q5 的 mentor 回复记录回 `docs/discussions/weekly-sync-posts.md`

---

*Generated 2026-08-03 | Week 10 final day*
