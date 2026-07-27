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
│  Layer 3: Record / Replay — 回归测试 (开发中)     │
│  录制一次真实运行 → 之后每次重放比对               │
└──────────────────────────────────────────────────┘
```

### Layer 1: NodeHarness（单元测试）

不需要启动 dora daemon，在 `#[test]` 里直接驱动节点：

```rust
use dora_test_utils::NodeHarness;

#[test]
fn test_my_node() {
    let mut harness = NodeHarness::new().expect("创建 harness 失败");

    // 往节点注入数据
    harness.send_data("image", serde_json::json!([1, 2, 3]));

    // 跑到结束，收集所有事件
    let events = harness.run_to_completion();
    assert!(!events.is_empty());

    // 节点产生的输出也能拿到
    let outputs = harness.recv_output("result");
    assert!(outputs.is_some());
}
```

### Layer 2: TestSource + TestSink（集成测试）

四个现成的二进制节点，直接写进 dataflow YAML 就能用：

| 二进制 | 作用 |
|--------|------|
| `test-source` | 从 JSON 文件读数据，发到 DORA 输出（支持多输出） |
| `test-sink` | 接收 DORA 输入，跟预期文件比对，输出匹配结果 |
| `echo-node` | 透传：收到啥发啥，用于验证链路通不通 |
| `classifier-node` | 按阈值分流：Int64 数值 > 阈值发到 high，否则发到 low |

**用法示例** — 写一个 YAML dataflow：

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

然后一行命令跑起来：

```bash
dora run my-dataflow.yml --stop-after 10s
cat result.json  # {"match": true} 或 {"match": false, "differences": [...]}
```

**多输出模式**（Week 8 新增）：

```bash
test-source --output data_a:a.json --output data_b:b.json
```

一条命令往两个输出通道发不同的数据。

## 快速上手

### 前置条件

- Rust 工具链
- 本仓库 clone 到本地
- dora CLI（可通过 `cargo install dora-cli --git https://github.com/dora-rs/dora.git` 安装，或从 PATH 获取）
  - 集成测试和 record/replay 测试需要 `dora run` 命令

### 编译所有二进制

```bash
cargo build --bin test-source --bin test-sink --bin echo-node --bin classifier-node
```

### 跑测试

```bash
# 库单元测试（42 个）
cargo test --lib

# 端到端测试（5 个）
cargo test --test e2e

# 集成测试（6 个，需要 dora CLI）
cargo test --test integration -- --test-threads=1

# 全部
cargo test
```

### 跑演示脚本

```bash
bash scripts/demo-week8.sh
```

一键跑通 3 个流水线（echo、multi-echo、classifier）+ 全部测试。

## 项目结构

```
src/
├── lib.rs          # crate 入口，模块声明
├── harness.rs      # NodeHarness — 单元测试驱动
├── source.rs       # TestSource — 数据注入库
├── sink.rs         # TestSink — 数据比对库
├── traits.rs       # IntoInputData trait
├── mock/           # MockEventStream、MockOutputSender
└── bin/
    ├── test_source.rs    # test-source CLI
    ├── test-sink.rs      # test-sink CLI
    └── classifier_node.rs # classifier-node CLI
tests/
├── fixtures/       # YAML dataflow、测试数据文件
├── echo-node.rs    # echo-node 二进制（透传）
├── e2e.rs          # 端到端测试 (5)
├── integration.rs  # 集成测试 (6)
└── smoke.rs        # 冒烟测试 (3)
docs/               # 设计文档、进度记录、upstream PR 计划
```

## 测试统计（Week 10）

| 类别 | 数量 |
|------|------|
| 库单元测试 | 45 |
| 端到端测试 (e2e) | 5 |
| Record/Replay 测试 (e2e_record) | 4 |
| 集成测试 | 6 |
| 冒烟测试 | 3 |
| Mock 测试 | 6 |
| **总计** | **62** |
| CI jobs | 5（check / test / clippy / fmt / integration） |

## CI

5 个 CI jobs，全绿：

- **check** — `cargo check`
- **test** — `cargo test --lib`
- **clippy** — `cargo clippy -- -D warnings`
- **fmt** — `cargo fmt --check`
- **integration-test** — 编译 dora CLI + 跑集成测试

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
| 9 | flume→tokio mpsc + Record/Replay 设计 | ⏳ |
| 10+ | Record/Replay 实现 | ⏳ |

详见 [`docs/PROGRESS.md`](docs/PROGRESS.md)。

## 许可

本项目为 GSoC 2026 项目，最终将合入 [dora-rs/dora](https://github.com/dora-rs/dora) 主仓库。
