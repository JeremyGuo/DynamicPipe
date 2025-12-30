## ModularDP

一个轻量的 C++17 **模块化流水线（pipeline）并发框架**：用统一的 `Source / Pipeline / Sink` 抽象把数据处理拆成可组合的模组，并用 `ThreadScheduler` 在全局范围内对 worker 线程总量做“额度”管理。

适用场景：把“读文件 / 解码 / 解析 / 特征提取 / 写出”这类可串联的数据处理拆成多个独立模组，在 **不复制数据** 的前提下并发运行，并且能用 **统一的内存预算** + **全局线程上限** 做资源兜底。

### 亮点

- **所有权转移，数据只保留一份**：模块间 `std::move` 传递，支持 move-only 类型，减少拷贝与内存峰值。
- **统一内存管理**：只在最后一个 `SinkPipeline::Start(total_memory_bytes)` 传入总预算，框架按链路比例分配到各模块的 `OnMemorySet()`。
- **线程动态分配**：模块内自适应扩缩容 worker；全局用 `ThreadScheduler` 做额度限制，避免多模块叠加导致线程爆炸。
- **可复用模组目录**：内置 `file_source / file_manager / json_decoder`，也支持快速新增自定义模组。

### 目录（TOC）

- [快速开始](#快速开始)
- [架构概览](#架构概览)
- [核心抽象](#核心抽象)
- [设计理念：数据只保留一份（所有权转移）](#设计理念数据只保留一份所有权转移)
- [设计理念：统一内存管理（全链路预算--模块自管理）](#设计理念统一内存管理全链路预算--模块自管理)
- [设计理念：线程动态分配（自适应-worker--全局额度）](#设计理念线程动态分配自适应-worker--全局额度)
- [目录结构](#目录结构)
- [内置模组（modules）](#内置模组modules)
- [如何新增一个模组（推荐流程）](#如何新增一个模组推荐流程)
- [Cursor 最小 Prompt 示例：快速新增一个模组](#cursor-最小-prompt-示例快速新增一个模组)
- [构建与运行](#构建与运行)
- [运行单元测试（含随机压力测试）](#运行单元测试含随机压力测试)
- [TSAN（ThreadSanitizer）并发检查](#tsanthreadsanitizer并发检查)

### 快速开始

构建并运行内置示例（见 `examples/basic_pipeline.cpp`）：

```bash
cd /home/guojunyi/Projs/ModularDP
cmake -S . -B build
cmake --build build -j
./build/example_basic_pipeline
```

### 架构概览

典型链路（最常见形态）：

```text
SourcePipeline<T0>
    │  Get()/Release()  （所有权转移：std::move）
    ▼
Pipeline<T0, T1>
    │  Emit()/CloseOutput()
    ▼
Pipeline<T1, T2>
    ▼
SinkPipeline<T2>  ── Start(total_memory_bytes) 触发“全链路内存预算分配 + 上游→下游启动”
```

并发模型：

- **模块内自适应**：每个 `Sink/Pipeline` 自己决定何时扩缩容 worker（默认实现基于积压与超时）。
- **全局可控**：所有模块共享同一个 `ThreadScheduler`，通过“额度”限制总 worker 数，避免多级 pipeline / 多链路并行时线程爆炸。

### 核心抽象

- **`SourcePipeline<T>`**：只有输出（无上游）。默认提供线程安全队列 + `Get()` 拉取；下游使用完后可显式 `Release()`（用于资源回收语义，若不需要可为空实现）。
- **`Pipeline<Tin, Tout>`**：中间模组，既是 `SinkPipeline<Tin>` 又是 `SourcePipeline<Tout>`；同样使用 `ThreadScheduler` 自适应线程；所有 worker 结束后自动关闭输出通道。
- **`SinkPipeline<T>`**：只有输入（无输出）。构造时注入 `ThreadScheduler`；调用 `Start(total_memory_bytes, options)` 会触发全链路内存分配与启动，并在运行时自适应扩缩容 worker。
- **`ThreadScheduler`**：跨多个模块统一管理线程总量（全局额度），仅提供“额度”申请/归还（线程创建/销毁由各模块自行完成）；并支持按调用者独立的最小创建间隔 `min_spawn_interval_us`。

### 设计理念：数据只保留一份（所有权转移）

- **核心约束**：数据在模块间传递时，统一使用 `std::move` 做“所有权转移”，保证“被处理的数据只有一份”，以便支持 **move-only 类型**（如 `std::unique_ptr`）以及减少拷贝。
- **接口约定**：
  - 上游 `SourcePipeline<T>::Get()` 返回 `std::optional<T>`，调用方拿到的是“拥有所有权的 T”。
  - 处理函数 `SinkPipeline<T>::Process(T&& input)` **接管 input 所有权**；如果需要把输入归还/回收给上游，调用 `GetUpstream()->Release(std::move(input))`。
  - `Release(T&&)` 语义是“归还此前 Get 得到的数据”，用于资源回收/对象池/句柄释放；若不需要可空实现。
- **非阻塞获取**：`SourcePipeline<T>::TryGet()` 提供非阻塞尝试（队列空则返回 `nullopt`）。

### 设计理念：统一内存管理（全链路预算 + 模块自管理）

这套框架把“内存管理”拆成两层：

- **全链路统一“预算”**：只在最后一个 `SinkPipeline::Start(total_memory_bytes)` 传入一次“整条 pipeline 的总内存预算（字节）”，由框架负责回溯链条并计算各模块配额。
- **模块内部自主“落地”**：每个模块只需要实现 `OnMemorySet(memory_bytes)`，把拿到的预算用于初始化/调整自己的 arena、pool、缓存、队列容量等；**模块内部如何分配、是否 mmap、是否对象池**完全由模块决定。

关键机制（与代码一致）：

- **链路回溯**：框架会从最后一个 sink 通过 `GetUpstreamModuleBase()` 回溯得到链条 `M0(Source) -> M1 -> ... -> Mn(Sink)`。
- **比例链分配（factor chain）**：
  - 对于参与分配的模块：要求每个 `Sink/Pipeline` 实现 `GetFactor()`，其语义是 \( \text{memory}_i / \text{memory}_{i-1} \)。
  - 框架会把 `total_memory_bytes` 按该“倍率链”分配到参与者的 `memory_i`，并对每个模块调用一次 `OnMemorySet(memory_i)`。
- **可选跳过与“分段起算”**：模块可通过 `HasFactor()==false` 表示“几乎不依赖预分配预算”（例如不使用 arena、或可退化到按需分配）。
  - 对于 `HasFactor()==false` 的模块以及更上游的模块：会收到 `OnMemorySet(0)`；
  - 内存比例链会从“最后一个 `HasFactor()==false` 的模块之后”重新开始计算，并将 `total_memory_bytes` 仅分配给后续模块。
- **只设置一次 + 先分配后启动**：框架会先对每个模块调用一次内存回调（`ApplyMemoryOnce`），再按“上游→下游”顺序启动（`StartAfterMemory`），保证模块启动前其内存配置已就绪。

在现有内置模组里，你能看到典型用法：

- `FileSourceModule`：`HasFactor()==false`，表示“几乎不需要额外预算”，不参与比例链。
- `FileManagerModule`：`consume_only=true` 时 `HasFactor()==false`（arena 不够就独立分配，不阻塞等待池子）；否则参与分配并在 `OnMemorySet` 初始化 arena。
- `JsonDecoderModule`：`GetFactor()==4.0`，表示解码/解析阶段通常比上游需要更大的内存工作集（rapidjson allocator/pool）。

### 设计理念：线程动态分配（自适应 worker + 全局额度）

框架的线程模型目标是：**每个模块可自适应扩缩容**，但全局线程总量始终可控，避免多条 pipeline/多级模块叠加时“线程爆炸”。

- **全局线程额度**：`ThreadScheduler` 只做“额度”申请/归还，统一限制跨所有模块的活跃 worker 总数（`max_total_threads`）。
- **按模块独立节奏扩容**：每个模块持有一个 `ThreadScheduler::Caller`，并用 `min_spawn_interval_us` 做 per-caller 限流，抑制短时间爆发式创建线程。
- **模块内自适应扩缩容**（默认实现）：
  - worker 在 `GetFor(timeout)` 上带超时地拉取数据：超时意味着“空闲可观测”，用于触发缩容；
  - scaler 周期性观察上游积压（`GetQueuedCount()`）与超时情况：积压大且近期不超时则扩容，连续空闲则缩容；
  - worker 退出/异常退出都会在清理逻辑中**归还额度**，确保全局记账不泄漏。

### 目录结构

- `include/`
  - `module.h`：核心框架（Source/Pipeline/Sink + 内存分配链 + 自适应线程）
  - `thread_scheduler.h`：全局线程额度调度器
- `modules/`：可复用模组（每个模组一个子目录）
- `examples/`：最小示例程序
- `tests/`：单元测试与压力测试

### 内置模组（modules）

- `modules/file_source/`：从目录/文件列表收集并输出文件路径（producer 线程在 `StartAfterMemory` 中启动）
- `modules/file_manager/`：读取文件内容并输出 `FileChunk`（支持 arena/mmap；`consume_only` 可退化为按需分配）
- `modules/json_decoder/`：把 `FileChunk` 解码为结构化 `DecodedJson`（基于 rapidjson，可用外部内存池降低碎片）

### 如何新增一个模组（推荐流程）

1. **新建目录**：`modules/my_module/`
2. **实现模块类**：继承 `SourcePipeline<T>` / `Pipeline<Tin, Tout>` / `SinkPipeline<T>` 之一
   - 必须实现 `OnMemorySet(std::size_t)`（即使不使用也可空实现）
   - 若是 `Pipeline/Sink`：必须实现 `GetFactor()`；如不参与链式分配则覆写 `HasFactor()==false`
   - 遵循“所有权转移”约定：在 `Process(T&&)` 里用完输入后按需调用 `GetUpstream()->Release(std::move(input))`
3. **加 CMake**：在 `modules/my_module/CMakeLists.txt` 添加静态库（或 header-only INTERFACE）
4. **注册子目录**：在 `modules/CMakeLists.txt` 里 `add_subdirectory(my_module)`
5. **添加验证**：建议最少加一个 `tests/test_*.cpp` 或 `examples/*.cpp`，确保 `ctest` 可覆盖

### Cursor 最小 Prompt 示例：快速新增一个模组

把下面这段直接丢给 Cursor（尽量保持“最小约束 + 明确产出文件”），它就能在当前框架下很快生成一个可编译、可测试、可复用的模组：

```text
新增一个中间模组 `UppercaseModule`，把上游的 `std::string` 转成大写再输出（std::string -> std::string）。
```

### 构建与运行

（提示：上面的“快速开始”已包含一次构建与运行；这里保留为完整命令区，方便复制。）

```bash
cd /home/guojunyi/Projs/ModularDP
cmake -S . -B build
cmake --build build -j
./build/example_basic_pipeline
```

### 运行单元测试（含随机压力测试）

项目内置 CTest：

```bash
cd /home/guojunyi/Projs/ModularDP
cmake -S . -B build
cmake --build build -j
ctest --test-dir build --output-on-failure
```

- `module`：端到端正确性 + `Start()` 只能调用一次 + 线程额度归零
- `stress`：随机参数多轮压力测试（多级 pipeline 串联），验证不死锁/结果一致/线程额度归零

### TSAN（ThreadSanitizer）并发检查

通过 CMake 选项启用：

```bash
cmake -S . -B build-tsan -DMODULARDP_ENABLE_TSAN=ON -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build-tsan -j
ctest --test-dir build-tsan --output-on-failure
```

说明：
- CI 中会使用 **clang + TSAN** 跑测试（见 `.github/workflows/ci.yml`）。
- 在部分环境里 **GCC TSAN** 可能出现 `ThreadSanitizer: unexpected memory mapping`，项目已在 GCC 下默认加了 `-no-pie` 以提高兼容性；若仍遇到问题，建议改用 clang。


