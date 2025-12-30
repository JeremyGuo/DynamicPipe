## ModularDP

一个轻量的 C++17 **模块化流水线（pipeline）并发框架**：用统一的 `Source / Pipeline / Sink` 抽象把数据处理拆成可组合的模块，并用 `ThreadScheduler` 在全局范围内对 worker 线程总量做“额度”管理。

核心抽象包括：

- **`SourcePipeline<T>`**：只有输出（无上游）。默认提供线程安全队列 + `Get()` 拉取；下游使用完后可显式 `Release()`（用于资源回收语义，若不需要可为空实现）。
- **`ThreadScheduler`**：跨多个模块统一管理线程总量（全局额度），仅提供“额度”申请/归还（线程创建/销毁由各模块自行完成）；并支持按调用者独立的最小创建间隔 `min_spawn_interval_us`。
- **`SinkPipeline<T>`**：只有输入（无输出）。构造时注入 `ThreadScheduler`；`Start(options)` 启动自适应 worker：能持续 `Get` 则按间隔扩容，长时间空闲则缩容。
- **`Pipeline<Tin, Tout>`**：中间模块，既是 `SinkPipeline<Tin>` 又是 `SourcePipeline<Tout>`；同样使用 `ThreadScheduler` 自适应线程；所有 worker 结束后自动关闭输出通道。

### 设计理念：数据只保留一份（所有权转移）

- **核心约束**：数据在模块间传递时，统一使用 `std::move` 做“所有权转移”，保证“被处理的数据只有一份”，以便支持 **move-only 类型**（如 `std::unique_ptr`）以及减少拷贝。
- **接口约定**：
  - 上游 `SourcePipeline<T>::Get()` 返回 `std::optional<T>`，调用方拿到的是“拥有所有权的 T”。
  - 处理函数 `SinkPipeline<T>::Process(T&& input)` **接管 input 所有权**；如果需要把输入归还/回收给上游，调用 `GetUpstream()->Release(std::move(input))`。
  - `Release(T&&)` 语义是“归还此前 Get 得到的数据”，用于资源回收/对象池/句柄释放；若不需要可空实现。
- **非阻塞获取**：`SourcePipeline<T>::TryGet()` 提供非阻塞尝试（队列空则返回 `nullopt`）。

### 构建与运行

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


