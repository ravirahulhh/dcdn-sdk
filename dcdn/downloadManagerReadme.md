重构摘要:

	•	以 FSM 为中心：所有状态变更与副作用都经过 applyFsm_，HTTP/P2P 的回调只负责“把外部事件转译成内部事件”。

	•	公共数据路径复用：drainAndWriteBuffers_ 与 finishOneSubTask_ 让 HTTP/P2P 共享写入、进度、短读回填、续排、完成判定的逻辑，后续扩展不会再复制粘贴。

	•	对现有下载器零侵入：仍然直接使用已有的 util::HttpDownloader / download::P2PDownloader（继承 BaseDownloader），回调是底层 DownloaderTaskOption::Notify 的同一风格。

	•	结构清晰、便于扩展：HYBRID、带宽配比、优先级、追帧策略、动态分片大小等都可通过在 FSM 内新增事件与副作用函数实现。

	•	保留领域事件：ETaskStatusChanged / ETaskProgress / EBufferReady / EChunkScheduled / ETaskError 统一从 publish_ 发出，上层订阅者结构稳定。

重构理由：

	•	一致性：不论成功还是失败，任务的生命周期都由 applyFsm_ 驱动，避免逻辑分散。

	•	扩展性：未来失败可以细分为 网络错误 / 取消 / P2P无源 / 超时 等，不用到处改逻辑。

	•	统一的对外事件：失败后上层（UI / 日志 / 调度器）只要订阅 TaskEvent::Failed，就能感知。