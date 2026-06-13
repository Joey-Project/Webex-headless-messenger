# Project TODO

- [deferred] Adaptive Card 暂保留 raw JSON attachment payload，不做复杂 builder/DSL；有实际卡片需求后再评估。
- [deferred] Sidecar 暂不做 durable local queue；默认用 supervisor restart + REST catch-up + message ID 去重恢复，只有具体部署无法接受重启窗口时再加队列。
