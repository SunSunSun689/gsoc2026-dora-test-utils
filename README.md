# GSoC 2026 Project: dora-test-utils

为 [DORA](https://dora-rs.ai/) 数据流框架提供单元测试和集成测试支持的 Rust 工具库。

## 三层测试支持

```
┌──────────────────────────────────────────────────┐
│  Layer 1: NodeHarness — 单元测试                  │
│  不放 daemon，直接用内存 channel 驱动单个节点     │
├──────────────────────────────────────────────────┤
│  Layer 2: TestSource / TestSink — 集成测试       │
│  扔进真实 YAML dataflow，端到端验证                │
├──────────────────────────────────────────────────┤
│  Layer 3: Record / Replay — 回归测试              │
│  录制一次真实运行 → 之后每次重放比对               │
└──────────────────────────────────────────────────┘
```

### Layer 1: NodeHarness（单元测试）

不需要启动 dora daemon，在 `#[test]` 里直接驱动节点：

```rust
use dora_test_utils::NodeHarness;

#[test]
fn test_my_node() {
    let mut harness = NodeHarness::new().expect("failed to create harness");

    // Buffer input data (deferred init — node created on first tick)
    harness.send_data("image", serde_json::json!([1, 2, 3]));

    // Run to completion, collect all events
    let events = harness.run_to_completion();
    assert!(!events.is_empty());

    // Collect node outputs
    let outputs = harness.recv_output("result");
    assert!(outputs.is_some());
}
```

### Layer 2: TestSource + TestSink（集成测试）

五个现成的二进制节点，直接写进 dataflow YAML 就能用：

| 二进制 | 作用 |
|--------|------|
| `test-source` | 从 JSON 文件读数据，发到 DORA 输出（支持多输出） |
| `test-sink` | 接收 DORA 输入，跟预期文件比对，输出匹配结果 |
| `echo-node` | 透传：收到啥发啥，用于验证链路通不通 |
| `classifier-node` | 按阈值分流：Int64 数值 > 阈值发到 high，否则发到 low |
| `distance-guard` | 具身智能示例：距离读数 < 0.5m 时发急停信号（末端碰撞防护） |

```yaml
nodes:
  - id: test-source
    path: ./target/debug/test-source
    args: "--output data:source.json"
    outputs: [data]
  - id: my-node
    path: ./target/debug/my-node
    inputs:
      data: test-source/data
    outputs: [result]
  - id: test-sink
    path: ./target/debug/test-sink
    inputs:
      result: my-node/result
    args: "--expected-file expected.json --output-file result.json"
```

```bash
dora run my-dataflow.yml --stop-after 10s
cat result.json  # {"match": true} 或 {"match": false, "differences": [...]}
```

### Layer 3: RecordSession / ReplaySession（回归测试）

录制一次真实 dataflow 运行的输出，之后每次重放自动比对，检测回归：

```rust
use dora_test_utils::record::RecordSession;
use std::time::Duration;

// ── 录制基线 ──
let recording = RecordSession::attach("dataflow.yml")?
    .record_sink("test-sink", "sink_output.json")
    .with_timeout(Duration::from_secs(10))
    .run()?;
recording.save("baseline.json")?;

// ── 重放比对 ──
let result = ReplaySession::load("baseline.json")?
    .replay_sink("test-sink", "sink_output.json")
    .with_timeout(Duration::from_secs(10))
    .run()?;

if result.is_clean() {
    println!("No regressions detected");
} else {
    println!("{}", result.diff());  // structured diff report
    result.assert_no_regression();  // panics with formatted diff
}
```

**二层比对**：
- Layer 1: 快速 JSON 结构 diff
- Layer 2: Arrow 语义比对（容忍 Int32→Int64 等类型差异）

**DiffReport** 支持 `Display` + `Serialize`，区分四种状态：
- `Match` — 完全一致
- `Mismatch` — 数据差异（含字段级路径和值）
- `Missing` — 基线中有但重放中没有的 sink
- `Extra` — 重放中有但基线中没有的 sink

## 快速上手

### 前置条件

- Rust 工具链
- dora CLI（从 PATH 获取，或 `cargo install dora-cli --git https://github.com/dora-rs/dora.git`）

### 编译所有二进制

```bash
cargo build --bin test-source --bin test-sink --bin echo-node --bin classifier-node --bin distance-guard
```

### 跑测试

```bash
# 库单元测试（85 个）
cargo test --lib

# 端到端测试（5 个）
cargo test --test e2e

# Record e2e 测试（4 个，需要 dora CLI）
cargo test --test e2e_record -- --test-threads=1

# Replay e2e 测试（13 个，需要 dora CLI）
cargo test --test e2e_replay -- --test-threads=1

# 集成测试（6 个，需要 dora CLI）
cargo test --test integration -- --test-threads=1

# 冒烟测试（3 个）
cargo test --test smoke

# 全部
cargo test
```

### 演示脚本

```bash
bash scripts/demo-final.sh
```

一键展示全部三层测试能力：

1. **Layer 1** — `examples/harness_demo.rs`：NodeHarness 单元测试，不起 daemon。场景：Realman GEN72 机械臂关节限位安全监测
2. **Layer 2** — 三条机械臂主题的真实 dataflow 流水线：关节位置回传（echo）/ 关节位置 + 末端速度双路回传（multi-echo）/ 末端碰撞防护急停（distance-guard，0.15m 读数触发 stop）
3. **Layer 3** — `examples/demo_replay.rs`：DORA 官方 rust-dataflow example（上游节点零修改），RecordSession 录制基线 → ReplaySession 检测回归

脚本自动 clone dora（pin 到 `1fba721`）、构建全部二进制、跑三个 demo、再跑完整 116 测试套件。

## 项目结构

```
src/
├── lib.rs          # crate 入口，模块声明 + API 稳定性表格
├── harness.rs      # NodeHarness — 单元测试驱动（deferred-init 模型）
├── source.rs       # TestSource — 数据注入库（JSON → Arrow 转换）
├── sink.rs         # TestSink — 数据比对库（语义比对 + 严格比对）
├── record.rs       # RecordSession + ReplaySession + DiffReport
├── traits.rs       # IntoInputData trait
├── mock/           # MockEventStream、MockOutputSender
└── bin/
    ├── test_source.rs    # test-source CLI
    ├── test-sink.rs      # test-sink CLI
    ├── classifier_node.rs # classifier-node CLI
    └── distance_guard.rs # distance-guard CLI（末端碰撞防护示例）
tests/
├── fixtures/       # YAML dataflow、测试数据文件（静态可直接 dora run）
├── echo-node.rs    # echo-node 二进制（透传）
├── e2e.rs          # NodeHarness 端到端测试 (5)
├── e2e_record.rs   # RecordSession e2e 测试 (4)
├── e2e_replay.rs   # ReplaySession e2e 测试 (13)
├── integration.rs  # 集成测试 (6)
└── smoke.rs        # 冒烟测试 (3)
docs/               # 设计文档、进度记录、upstream PR 计划
scripts/            # Demo 脚本
```

## API 稳定性

| API | 状态 | 说明 |
|-----|------|------|
| `NodeHarness` | **Stable** | 单元测试驱动，deferred-init 模型 |
| `TestSource` / `TestSink` | **Stable** | JSON/Arrow 数据注入和比对 |
| `MockEventStream` / `MockOutputSender` | **Stable** | 无 daemon mock 测试 |
| `IntoInputData` trait | **Stable** | 数据注入 trait |
| `RecordSession` / `Recording` | **Experimental** | 录制 dataflow 输出为基线 |
| `ReplaySession` / `ReplayResult` | **Experimental** | 重放比对，检测回归 |
| `DiffReport` / `SinkDiff` / `FieldDiff` | **Experimental** | 结构化差异报告 |

## 测试统计（Week 12）

| 类别 | 数量 | 位置 |
|------|------|------|
| 库单元测试 | 85 | `src/*.rs` |
| 端到端测试 (e2e) | 5 | `tests/e2e.rs` |
| Record e2e (e2e_record) | 4 | `tests/e2e_record.rs` |
| Replay e2e (e2e_replay) | 13 | `tests/e2e_replay.rs` |
| 集成测试 | 6 | `tests/integration.rs` |
| 冒烟测试 | 3 | `tests/smoke.rs` |
| **总计** | **116** | |

## CI

5 个 CI jobs：

- **check** — `cargo check`
- **test** — `cargo test --lib` + e2e + smoke + integration + e2e_record + e2e_replay
- **clippy** — `cargo clippy -- -D warnings`
- **fmt** — `cargo fmt --check`
- **integration-test** — 编译 dora CLI + test 二进制 + 集成测试 + record/replay e2e

GitHub Actions 配置在 `.github/workflows/ci.yml`。

## 进度

| Week | 内容 | 状态 |
|------|------|------|
| 1-2 | API 设计 + 脚手架 | ✅ |
| 3-4 | NodeHarness 核心实现 | ✅ |
| 5 | TestSource + TestSink 库 + CLI | ✅ |
| 6 | Echo 流水线 + 集成测试 | ✅ |
| 7 | 边界测试 + CI 集成 | ✅ |
| 8 | 多输出 + classifier + 3 条流水线 | ✅ |
| 9 | flume→tokio mpsc + RecordSession | ✅ |
| 10 | ReplaySession + code review 修复 | ✅ |
| 11 | DORA upgrade (45436aad→1fba721) + flume removed + integration test fix | ✅ |
| 12 | Docs polish + demo refinement | 🚧 |
| 13 | Final submission | ⏳ |

详见 [`docs/PROGRESS.md`](docs/PROGRESS.md)。

## 许可

本项目为 GSoC 2026 项目，最终将合入 [dora-rs/dora](https://github.com/dora-rs/dora) 主仓库。
