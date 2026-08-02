# Week 10–12 进度报告

> 准备向 mentor (bobdingAI / ZhangHanDong) 汇报，覆盖 2026-07-27 至今的工作。

## 概述

三周内完成了 **ReplaySession 实现**、**两轮 code review 修复（28 个 issue）**、**Week 12 文档/测试/边界打磨**，以及 **upstream PR (a) 代码准备**。库单元测试从 52 增长到 80，全部 CI 检查绿色。

---

## 1. Week 10：ReplaySession 实现（7/27）

### 新增 API

```rust
// 录制基线
let recording = RecordSession::attach("dataflow.yml")?
    .record_sink("test-sink", "output.json")
    .with_timeout(Duration::from_secs(10))
    .run()?;
recording.save("baseline.json")?;

// 重放比对
let result = ReplaySession::load("baseline.json")?
    .replay_sink("test-sink", "output.json")
    .run()?;

result.is_clean();              // bool
result.diff();                  // &DiffReport (Display + Serialize)
result.assert_no_regression();  // panic with formatted diff
```

### 二层比对架构

- **Layer 1**：快速 JSON 结构 diff（字段级路径 + 值）
- **Layer 2**：Arrow 语义比对（容忍 Int32→Int64 等类型差异）
- **DiffReport**：结构化报告，支持 Match/Mismatch/Missing/Extra 四种状态

### 测试

| 类型 | 数量 |
|------|------|
| 单元测试（comparison logic） | 7 |
| e2e 测试（ReplaySession） | 11 |

---

## 2. 两轮 Code Review 修复（28 个 issue）

### 第一轮（15 个 issue，修复 13）

| 类别 | 关键修复 |
|------|---------|
| **正确性** | `.data` 前缀匹配过宽（也匹配 `.data_type`）、`as_f64` 死代码路径（Int64 分支永远不可达）、baseline/current `.data` 解包不对称、`--stop-after` 小数格式与 dora CLI 不兼容 |
| **事件丢失** | `run_to_completion` 已初始化状态下 Stop 注入失败、`ensure_init` 静默丢弃 pending events |
| **CI** | `e2e_replay` 11 个测试在 CI 上零覆盖 |
| **类型保真** | `write_record_output` 用 Debug 格式写 data_type、`number_to_arrow_array` 无提示时 u64→Float64 静默截断、过时文件误报、相对路径破坏可移植性 |

### 第二轮（15 个 issue，修复 13）

| 类别 | 关键修复 |
|------|---------|
| **假 Match** | semantic comparison 返回空时回退到 json diffs（而非静默丢弃） |
| **NaN panic** | `Duration::from_secs_f64` 加 `.clamp(0.1, 3600)` + `is_finite()` 守卫 |
| **副作用时序** | 过时文件删除移到验证之后（不再在错误路径上破坏用户数据） |
| **data_type 序列化** | 存储原始 `serde_json::Value`（不再 `.to_string()` 破坏 Struct/Timestamp 复杂类型） |
| **跨类型 Number** | `3` vs `3.0` 假 Mismatch：数值类型通过 `as_f64()` 比较 |
| **HashMap 稳定性** | `compare_recordings` 按 sink_id 排序后输出 |
| **Metadata 一致性** | `ReplayResult.metadata` 反映实际 timeout/YAML 覆盖值 |
| **Demo 脚本** | 移除 `grep \|\| true` 假绿模式、回归检测失败时 `exit 1` |
| **SinkNotInBaseline** | 恢复 fast-fail（拼写错误的 sink ID 不再耗时全量 dora run） |

---

## 3. Week 12：文档、Demo、边界测试

### Demo

- `examples/demo_replay.rs`：Record→Replay→regression detection 完整 Rust 示例
- `scripts/demo-week12.sh`：一键构建 + demo + 全量测试套件

### README 更新

- 新增 RecordSession/ReplaySession API 文档（含代码示例）
- 新增 API 稳定性表格
- 更新项目结构、测试数量（52→80）、周进度表
- 修正 CI job 数量和说明

### 边界测试（+28 单元测试）

| 模块 | 新增用例 |
|------|---------|
| `compare_recordings` | Match/Missing/Extra/Mixed 状态分配（4） |
| `compare_sink_outputs` | `.data_type` 差异保留（前缀修复验证）（2） |
| `json_diff` | 嵌套对象、null 值、额外/缺失 key（4） |
| `compare_data_semantic` | 空数组、长度不匹配、string fallback（3） |
| `DiffReport` | 混合状态展示、全部 Match 计数（2） |
| `NodeHarness` | 初始化后事件清空警告、非法 ID panic、recv_output None、send_output 错误（4） |
| `TestSink` | Null 转换错误、int overflow cast、Boolean 数组（3） |
| `TestSource` | UInt16/UInt64 hint、对象缺少 hint 报错、异构数组拒绝、分数→Int64 报错、UInt16 overflow（6） |

---

## 4. Upstream PR (a)：flume→tokio mpsc

### 状态

代码已完成，已推送到 `SunSunSun689/dora:testing-output-tokio-mpsc`。

### 改动（dora-rs/dora）

| 文件 | 改动 |
|------|------|
| `apis/rust/node/src/integration_testing.rs` | `TestingOutput::ToChannel(flume::Sender)` → `tokio::sync::mpsc::Sender`；新增 `TestingInput::Channel` 变体 |
| `apis/rust/node/src/daemon_connection/node_integration_testing.rs` | 新增 `EventSource` 枚举（Vec + Channel）；`OutputWriter::Channel` 切到 tokio mpsc + `blocking_send`；`check_poisoned` 提取为独立方法 |
| `apis/rust/node/src/node/mod.rs` | 内部测试代码适配 |

### 相关 Issue

已创建 tracking issue：**[dora-rs/dora#2956](https://github.com/dora-rs/dora/issues/2956)** — TestingOutput::ToChannel still uses flume after EventStream migration (#1603)

### 待办

PR 尚未正式提交（代码已就绪），等待 mentor 确认时机。

---

## 5. 测试统计

| 类别 | Week 9 | 现在 |
|------|--------|------|
| 库单元测试 | 46 | **80** |
| e2e (harness) | 5 | 5 |
| e2e_record | 4 | 4 |
| e2e_replay | 0 | **11** |
| 集成测试 | 6 | 6 |
| 冒烟测试 | 3 | 3 |
| **总计** | **64** | **109** |

---

## 6. CI 状态

6 个 CI job，全绿：

- `check` — `cargo check`
- `test` — `cargo test --lib` + `cargo test --test e2e` + `cargo test --test smoke`
- `clippy` — `cargo clippy -- -D warnings`
- `fmt` — `cargo fmt --check`
- `integration-test` — 构建 dora CLI + 集成测试 + e2e_record + e2e_replay

---

## 7. 待讨论问题

1. **Upstream PR (a) 时机**：代码已就绪，现在提交还是等 Week 13 final submission 后？
2. **DORA version bump**：当前 pin 在 `45436aad`（6月初确认），是否需要升级？
3. **Weekly Sync Discussions**：Week 8/9/10/12 的讨论帖尚未发布，本周补上
