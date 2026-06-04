---
id: 20260603-webex-rust-bootstrap
title: Webex Rust Crate Bootstrap
status: completed
created: 2026-06-03
updated: 2026-06-04
branch: wip/webex-messaging-crate-pr
pr: https://github.com/Joey-Project/Webex-headless-messenger/pull/1
supersedes: []
superseded_by:
---

# Webex Rust Crate Bootstrap

## Summary
- 初始化 `webex-headless-messenger` Rust crate，用于 Generic Account 通过 Webex OAuth Integration 访问 Messaging REST API。
- 第一阶段聚焦稳定公开 surface：OAuth、REST typed client、pagination、webhook 类型与签名验证、无公网 ingress 场景下的 polling receiver。
- WebSocket/Mercury 不在第一阶段直接复刻；官方资料显示 realtime listen 主要通过 Webex JS SDK 暴露，Rust crate 先保留 experimental marker。

## Current State
- Rust toolchain 已安装到用户级 `~/.cargo`，并通过 `~/.zshenv` 加入 PATH。
- `cargo test --all-features` 已通过，覆盖分页 Link、Retry-After、OAuth authorization URL、path encoding、请求序列化、Debug 脱敏、webhook 签名。
- `cargo clippy --all-features --all-targets -- -D warnings`、`cargo doc --no-deps --all-features`、project journal validator 已通过。
- README 已替换模板内容，写明 scope、使用示例和 WebSocket 边界。
- 内部 reviewer findings 已处理：普通依赖启用 `tokio/rt`、poller 使用 backlog continuation 追分页、token/secret Debug 脱敏、Device Token pending/slow_down 处理、默认与扩展 scope 分离、pagination host 限制。最终 reviewer 复查为 no findings。
- PR review fix-loop 已处理：默认 OAuth scope 补 `spark:kms`，Authorization Code helper 补 PKCE API，Device Token 错误解析覆盖 Webex `errors[0].description`/`message` 形态，poller 去重缓存加容量上限，smoke token cache 在 Unix 上以 owner-only 权限写入。
- 第二轮 PR review fix-loop 已处理：smoke 示例在 cache hit 路径也会收紧既有 token cache 权限，`slow_down` 使用独立 `DeviceTokenStatus::SlowDown` 变体并在示例中递增后续 polling interval。
- 第三轮 GitHub Codex review fix-loop 已处理：OAuth 自定义 base URL join 前统一按目录 URL 规范化，membership update 字段改为 partial serialization，poller 在 backlog 跨 tick 时先缓冲较新的消息，直到较旧 backlog drain 完成后再按时间顺序返回。
- 第四轮 review fix-loop 已处理：OAuth refresh 缺 client secret 时本地早失败，poller 只在完整 catch-up batch 可返回时提交 seen IDs 且错误时保留 pending backlog，smoke token cache hit 改为 no-follow/owner/regular-file 校验后再读取。
- 第五轮 review fix-loop 已处理：默认首轮 skip-existing baseline 建立期间如果后续分页失败，poller 保持未初始化并从头重试 baseline，避免下一轮把历史消息当 fresh 发出。
- 第六轮 review fix-loop 已处理：REST pagination `next` URL 校验扩展到 configured base path 前缀，避免 path-based proxy 场景把 bearer token 发到同 host 的非 Webex 路径。

## Next Steps
- 如果后续要接真实账号，新增不含 secret 的 examples 或 smoke docs，避免提交 token。
- 若需要 realtime 低延迟，优先设计 JS SDK sidecar 事件转发协议，而不是直接实现 Mercury 私有连接。

## Evidence
- Official Webex OpenAPI specs: `https://github.com/webex/webex-openapi-specs`
- Official Webex OAuth / Device Grant docs: `https://developer.webex.com/create/docs/login-with-webex`
- Official Webex Webhooks guide: `https://developer.webex.com/docs/api/guides/webhooks`
- Local validation: `cargo fmt --check`; `cargo test --all-features`; `cargo clippy --all-features --all-targets -- -D warnings`; `cargo doc --no-deps --all-features`; `project_journal.py validate --repo /home/codex/Joey-Project/Webex-headless-messenger`
