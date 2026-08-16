# DORA Upstream 贡献计划

> 基于 mentor ZhangHanDong 在 Discussion #20 (Week 3) 和 Discussion #28 (Week 7) 的指导。

**Status (2026-08-09)**: PR (a) 上游已自行完成（commit `1fba721`，2026-08-04）。
我们已于 Week 11 升级 DORA dep → `1fba721`，移除 `flume` 依赖，切换 `harness.rs`
到 `tokio::sync::mpsc::unbounded_channel()`。详见 `docs/PROGRESS.md` Week 11。
PR (b) 是 `TestingInput::Channel` API 提案，延后至 post-submission。

---

## PR (a)：将 `TestingOutput::ToChannel` 从 flume 迁移到 tokio::sync::mpsc

### 背景

DORA 项目在 PR [#1603](https://github.com/dora-rs/dora/issues/1603) 中已经将 `EventStream` 内部通道从 flume 迁移到了 tokio::sync::mpsc，原因是 flume 0.10.14 内部的 `Spinlock<Waker>` 在低 CPU 核数环境（如 GitHub Actions 2-vCPU runner）下会导致死锁/活锁。

但是 `#1603` **没有**修改 integration testing 模块中的通道类型。当前 DORA upstream（commit `45436aad`）的 `TestingOutput::ToChannel` 仍然使用 `flume::Sender`：

```rust
// apis/rust/node/src/integration_testing.rs (当前 upstream)
pub enum TestingOutput {
    ToFile(std::path::PathBuf),
    ToWriter(Box<dyn std::io::Write + Send>),
    ToChannel(flume::Sender<serde_json::Map<String, serde_json::Value>>),  // ← 仍然是 flume
}
```

### 为什么要改

1. **一致性问题**：EventStream 已经迁移到 tokio mpsc，但 integration testing 模块还在用 flume。DORA 项目对 flume 的依赖是不完整的迁移，这个 PR 补全它。

2. **死锁风险**：`TestingOutput::ToChannel` 中的 `flume::Sender` 和 #1603 修复的 `EventStream` 是**同样的 flume 0.10.14 Spinlock 问题**。虽然输出路径的死锁概率远低于输入路径（daemon thread 不会在输出通道上阻塞等待），但使用 tokio mpsc 可以从根本上消除这个风险。

3. **上游一致性**：既然 DORA 已经决定从 flume 迁移到 tokio mpsc（#1603 已合入），那么所有 testing 相关的通道都应该完成这个迁移。这是一个"补全迁移"的 PR，不是"引入新依赖"的 PR。

4. **独立可合入**：这个改动**只修改 DORA 已有的代码**，不引入任何新 API。它是最容易合入的 PR 类型——纯粹的内部实现改进。

### 需要修改的文件

#### 1. `apis/rust/node/src/integration_testing.rs`

**第 269-272 行**：`TestingOutput::ToChannel` 的 flume → tokio 迁移：

```rust
// 当前 (flume):
ToChannel(flume::Sender<serde_json::Map<String, serde_json::Value>>),

// 改为 (tokio):
ToChannel(tokio::sync::mpsc::Sender<serde_json::Map<String, serde_json::Value>>),
```

同时更新文档注释（第 267-268 行），将 `[flume::Receiver]` 改为 `[tokio::sync::mpsc::Sender]`。

#### 2. `apis/rust/node/src/daemon_connection/node_integration_testing.rs`

**第 237 行**：`OutputWriter` 枚举的 Channel 变体：

```rust
// 当前 (flume):
Channel(flume::Sender<serde_json::Map<String, serde_json::Value>>),

// 改为 (tokio):
Channel(tokio::sync::mpsc::Sender<serde_json::Map<String, serde_json::Value>>),
```

**第 180-184 行**：`handle_output()` 中的发送调用：

```rust
// 当前 (flume):
OutputWriter::Channel(sender) => {
    sender
        .send(output)               // flume::Sender::send() — 不阻塞
        .context("failed to send output to channel")?;
}

// 改为 (tokio — 在非 async 上下文中需要用 blocking_send):
OutputWriter::Channel(sender) => {
    sender
        .blocking_send(output)       // tokio::Sender::blocking_send()
        .context("failed to send output to channel")?;
}
```

> **关键区别**：`flume::Sender::send()` 是非阻塞的（unbounded channel 直接 push），而 `tokio::sync::mpsc::Sender::send()` 是 async 的。在 `handle_output()` 这个函数中（运行在 daemon simulation thread 中，不是 tokio runtime），必须使用 `blocking_send()`。

#### 3. `apis/rust/node/src/node/mod.rs`

**第 1782-1817 行**：DoraNode 内部测试代码中的 `flume` 引用：

```rust
// 测试辅助函数 test_node():
fn test_node() -> (
    DoraNode,
    crate::EventStream,
    tokio::sync::mpsc::Receiver<serde_json::Map<String, serde_json::Value>>,  // 改类型
) {
    // ...
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();  // flume::unbounded() → tokio
    let outputs = TestingOutput::ToChannel(tx);
    // ...
}

// 测试中收集输出:
let outputs: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
// 替代原来的 flume::Receiver::try_iter()
```

**第 1852-1984 行**：另外两个内部测试函数同样的改动。

#### 4. 不需要改动

- `Cargo.toml` 不需要动——DORA workspace 已经依赖了 `tokio`（含 `sync` feature），不需要额外添加依赖。
- `TestingInput` 相关代码不变——这个 PR 只改输出路径。
- `EventStream` 相关代码不变——已经在 #1603 中迁移过了。

### 提交策略

1. **分支**：从 DORA main 创建分支 `testing-output-tokio-mpsc`
2. **Commit message**：
   ```
   refactor(testing): migrate TestingOutput::ToChannel from flume to tokio mpsc

   Completes the flume→tokio migration started in #1603, which migrated
   EventStream but left the integration-testing output channel on flume.

   TestingOutput::ToChannel now uses tokio::sync::mpsc::Sender instead of
   flume::Sender.  In the daemon simulation thread (non-async context),
   blocking_send() is used instead of send().
   ```
3. **PR 描述**：引用 #1603 作为 precedent，说明这是补全已有迁移，无新增 API
4. **验证**：`cargo test -p dora-node-api` 全部通过

### 影响范围

- **破坏性变更**：是。任何使用了 `TestingOutput::ToChannel` 的代码需要从 `flume::Sender` 改为 `tokio::sync::mpsc::Sender`。
- **影响面**：极小。`TestingOutput::ToChannel` 是测试专用 API，只在单元测试中使用，不涉及生产数据流路径。
- **下游适配**：`dora-test-utils` 的 harness.rs 需要将 `flume::Sender` 改为 `tokio::sync::mpsc::Sender`，`try_recv()` 不变（两者 API 兼容）。

---

## PR (b)：新增 `TestingInput::Channel` 变体——运行时事件注入

### 背景

当前 DORA 的 `TestingInput` 只支持两种输入方式：

```rust
pub enum TestingInput {
    FromJsonFile(PathBuf),         // 从 JSON 文件预加载所有事件
    Input(IntegrationTestInput),   // 在代码中预定义所有事件
}
```

两种方式的共同点是：**所有事件必须在 DoraNode 创建时就确定**。这对于文件回放和声明式测试足够，但对于交互式测试框架（如 `dora-test-utils` 的 `NodeHarness`）则不够——

`NodeHarness` 的原始设计使用 `send_data()` / `send_stop()` 在运行时动态注入事件，允许：
- 先发输入，再 tick，再发输出，再断言
- `send_output()` 在 `run_to_completion()` **之后**调用（此时所有预定义事件已处理完，如果没有 live channel，无法再与 daemon 通信）

虽然后来我们采用了 **Option 1（baked events + deferred init）** 作为当前实现，但 `TestingInput::Channel` 作为一个通用的运行时注入机制，对整个 DORA 生态仍然有价值。

### 使用场景

```rust
// 场景 1: 单元测试框架
let (tx, rx) = tokio::sync::mpsc::channel(1024);
let harness = DoraNode::init_testing(
    TestingInput::Channel(rx),     // ← 运行时注入
    TestingOutput::ToChannel(out_tx),
    options,
)?;

// 在测试中动态发送事件:
tx.blocking_send(Input { id: "data", ... });
tx.blocking_send(Stop);

// 场景 2: 外部工具集成（如 fuzzer、property-based test）
// 可以在任意时刻注入任意事件，不需要预定义
```

### 需要修改的文件

#### 1. `apis/rust/node/src/integration_testing.rs`

添加 `Channel` 变体到 `TestingInput` 枚举（第 228 行之后）：

```rust
pub enum TestingInput {
    FromJsonFile(std::path::PathBuf),
    Input(IntegrationTestInput),
    /// Live channel for runtime event injection.
    ///
    /// Events are read from the channel on demand — the node blocks
    /// until the test harness pushes a `TimedIncomingEvent` via
    /// `tokio::sync::mpsc::Sender::blocking_send`.  This enables
    /// interactive `harness.send_input(…)`-style APIs without
    /// pre-declaring all events at construction time.
    ///
    /// Uses `tokio::sync::mpsc` instead of `flume` to avoid the
    /// spinlock deadlock documented in #1603 and #2855.
    Channel(tokio::sync::mpsc::Receiver<integration_testing_format::TimedIncomingEvent>),
}
```

#### 2. `apis/rust/node/src/daemon_connection/node_integration_testing.rs`

**新增 `EventSource` 枚举**（第 27 行之后）：

```rust
/// Where IntegrationTestingEvents draws its input events from.
enum EventSource {
    /// Pre-loaded event list (file-backed or in-memory).
    Vec(std::vec::IntoIter<TimedIncomingEvent>),
    /// Live channel for runtime injection (test harness pushes events).
    Channel(tokio::sync::mpsc::Receiver<TimedIncomingEvent>),
}
```

**修改 `IntegrationTestingEvents` 结构体**（第 30 行）：

```rust
pub struct IntegrationTestingEvents {
    event_source: EventSource,  // 原来是 events: IntoIter<TimedIncomingEvent>
    output_writer: OutputWriter,
    start_timestamp: uhlc::Timestamp,
    start_time: Instant,
    options: TestingOptions,
}
```

**修改构造函数 `new()`**（第 41-96 行），将 `TestingInput::Channel(rx)` 映射为 `EventSource::Channel(rx)`：

```rust
let event_source = match input {
    TestingInput::FromJsonFile(path) => {
        let input: IntegrationTestInput = /* deserialize */;
        Self::check_poisoned(&input)?;
        let mut events = input.events;
        events.sort_by(|a, b| a.time_offset_secs.total_cmp(&b.time_offset_secs));
        EventSource::Vec(events.into_iter())
    }
    TestingInput::Input(input) => {
        Self::check_poisoned(&input)?;
        let mut events = input.events;
        events.sort_by(|a, b| a.time_offset_secs.total_cmp(&b.time_offset_secs));
        EventSource::Vec(events.into_iter())
    }
    TestingInput::Channel(rx) => EventSource::Channel(rx),
};
```

**修改 `next_event()` 方法**（第 188-204 行），添加 Channel 读取逻辑：

```rust
fn next_event(&mut self) -> eyre::Result<Option<Timestamped<NodeEvent>>> {
    let event = match &mut self.event_source {
        EventSource::Vec(iter) => match iter.next() {
            Some(e) => e,
            None => return Ok(None),
        },
        // tokio::sync::mpsc uses std::sync::Mutex internally instead of
        // flume's spinlock, so parallel harness instances don't deadlock.
        // The channel disconnects when the harness drops its sender,
        // causing blocking_recv() to return None.
        // (See dora-rs/dora#1603 and dora-rs/dora#2855.)
        EventSource::Channel(rx) => match rx.blocking_recv() {
            Some(e) => e,
            None => return Ok(None),
        },
    };
    // ... rest of event processing unchanged
}
```

将 `check_poisoned` 提取为独立方法（代码重构，不改变逻辑）。

#### 3. `apis/rust/node/src/node/mod.rs`

内部测试代码适配（与 PR (a) 中的改动类似，更新 `flume` → `tokio` 引用）。

### 需要讨论的问题（提交 PR 前与 maintainer 确认）

1. **是否是 DORA 想要的方向？** `TestingInput::Channel` 是为交互式测试设计的，DORA team 可能更倾向于文件回放和声明式测试。需要先开 Discussion 或 Issue 讨论。

2. **Channel 的 bounded vs unbounded**：当前设计使用调用者提供的 `Receiver`（任意 capacity），保持灵活。是否应该强制 unbounded？

3. **与 Record/Replay 的关系**：Channel 模式下没有预定义的事件列表，`RecordingStatus` 检查不适用。当前实现中 Channel 变体跳过 `check_poisoned()`——这合理吗？

4. **超时机制**：如果 harness 忘记发送事件，`blocking_recv()` 会永久阻塞。是否应该在 `next_event()` 中添加可选的超时？

### 提交策略

1. **先讨论后实现**：在 dora-rs/dora 开 Discussion，说明这个 API 的用途、设计、和 `dora-test-utils` 的关系
2. **等 PR (a) 合入后再提交**：因为 (b) 的代码基于 (a) 修改后的 `ToChannel`（tokio mpsc）
3. **分支**：从 (a) 合入后的 main 创建 `testing-input-channel`
4. **Commit message**：
   ```
   feat(testing): add TestingInput::Channel variant for runtime event injection

   Adds a new TestingInput::Channel(tokio::sync::mpsc::Receiver<...>) variant
   that enables interactive test harnesses to inject events at runtime
   rather than pre-declaring all events at construction time.

   Motivated by dora-test-utils (dora-rs/gsoc2026-dora-test-utils), which
   provides a NodeHarness for ergonomic unit testing of DORA nodes.
   ```

### 如果 (b) 被拒绝

mentor 已给出 escape hatch：回退到 baked events（当前 week10 的实现）。`TestingInput::Input` + deferred init 已经能满足 `NodeHarness` 的所有需求，不需要 live channel。

(b) 的价值在于：
- 对 DORA 生态的通用贡献（不只是 dora-test-utils 需要）
- 提前与 maintainer 建立关系（对 GSoC 评价有利）
- 展示理解 DORA 架构并能贡献回去的能力

---

## 时间线建议

| 步骤 | 时间 |
|------|------|
| PR (a) 提交到 dora-rs/dora | Week 10–11 (现在) |
| PR (a) 合入 | 等待 review |
| PR (b) Discussion 在 dora-rs/dora 开 | 与 (a) 并行 |
| PR (b) 提交 | (a) 合入后 |
| dora-test-utils 适配 (a) | (a) 合入后（去掉 flume dep，用回 tokio mpsc） |

> PR (a) 是最容易合入的——它纯粹是补全已有迁移，不引入新 API。建议优先集中精力在这个上。PR (b) 需要更多讨论和耐心。
