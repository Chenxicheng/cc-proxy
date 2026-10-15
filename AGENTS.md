# Repository Guidelines

## 工作目录边界

`/Users/jingdi/IdeaProjects/cc-proxy` 是本项目的主工作目录，所有项目修改和产出都在此目录完成。`/Users/jingdi/workspace/claude-code-proxy` 是只读参考项目；可读取其文件以了解背景或对照实现，不得在其中创建、修改或删除文件。

## 项目结构与模块组织

`src/cli.ts` 是 npm CLI 入口，只负责配置交互、选择并启动内置 Rust 二进制、进程信号与错误反馈。`native/` 是完整代理服务，负责 HTTP、协议转换、SSE、usage/error JSONL 和运行时日志。Rust 生产代码只放在 `native/src/`：`server/` 负责启动、路由、handlers 和 upstream HTTP，`protocol/` 负责请求、响应与 SSE 转换，根级 `config.rs`、`error.rs`、`storage.rs` 保存明确的跨域能力；禁止新增语义模糊的 `common`、`shared` 或 `utils` 模块。Rust 白盒单元测试按生产模块路径镜像放在 `native/tests/unit/`，由对应模块通过 `#[cfg(test)]` 和 `#[path]` 引入；进程外集成测试直接放在 `native/tests/*.rs`。发布时将 macOS Intel x64、macOS Apple Silicon arm64、Linux x64 与 Windows x64 二进制暂存到 `bin/<target>/`，并与 CLI 一起打入唯一的离线 npm 包；`bin/` 是生成目录，不提交。Node 不代理请求，也不提供 Rust 服务的 fallback。根级 `tests/` 保存 CLI、离线分发测试和手动运行的 `live-e2e.mjs`；后者在 `tests/.live-e2e-*` 临时目录中链接仓库外 Config、写入运行日志，正常结束即清理，异常中断遗留目录应在确认进程退出后清理。`bench/` 保存确定性 mock upstream、fixtures 和 benchmark harness。`docs/` 保存 API、迁移规格与验收说明。

启动后的 fullscreen 展示由 `src/terminal.ts` 负责，只消费 Rust 运行日志。修改终端交互时查阅 `docs/terminal.md`；历史必须有界，退出必须恢复终端、光标与输入模式。

## 构建、测试与开发命令

- `npm install`：安装依赖；要求 Node.js `>=20.0.0`。
- `npm run dev`：通过 `ts-node` 本地运行 CLI。
- `npm run build`：编译 npm CLI 到 `dist/`。
- `npm test`：运行 Jest 测试套件。
- `npm test -- --runInBand`：串行运行完整测试，提交前优先使用。
- `npm run lint`：运行 npm CLI 与测试的 ESLint 检查。
- `npm run format`：用 Prettier 格式化 npm CLI 与测试文件。
- `cargo test --manifest-path native/Cargo.toml`：运行 Rust 测试。
- `cargo fmt --manifest-path native/Cargo.toml --check`：检查 Rust 格式。
- `cargo clippy --manifest-path native/Cargo.toml --all-targets -- -D warnings`：将 Rust lint warning 视为失败。
- `cargo build --manifest-path native/Cargo.toml --release`：构建本机 release 二进制。

Cargo 的所有构建产物统一写入仓库同级的 `.cc-proxy-target/`，由根目录 `.cargo/config.toml` 配置；本地命令、脚本和 CI 都从仓库根目录运行 Cargo，并通过该目录读取 release 二进制。

## 编码风格与命名约定

TypeScript 使用 strict mode、两空格缩进、单引号和分号；它不得包含协议转换或请求代理逻辑。Rust 使用 `rustfmt` 默认格式；协议转换与 HTTP handlers 分离，共享状态只保存线程安全的 client、配置和 bounded writer。配置和内部状态使用明确类型；协议 wire payload 使用 `serde_json::Value` 保留供应商扩展字段。不新增兼容层、无界队列或每请求 client。

## 测试规范

Node 测试使用 Jest + `ts-jest`，Rust 测试使用内置 test harness 与 `tokio::test`。协议变更必须更新共享 fixtures 和 Rust 测试；CLI、分发或配置变更必须更新 Jest 测试。离线包验收必须确认同一 tarball 同时包含四个平台的二进制，并在 Linux x64 clean install 后完成启动与优雅退出。默认集成测试使用本地 mock upstream，不依赖真实 API key、真实网络请求或外部服务状态。真实 E2E 仅手动运行：直接启动 Rust REST 服务，读取仓库外 JSON Config，验证非流式文本、流式文本和工具结果闭环；不提交凭据、prompt、完整响应或运行日志。benchmark 必须固定 payload、upstream 延迟、日志配置和构建模式。

## Commit 与 Pull Request 规范

提交信息遵循历史中的 Conventional Commits，例如 `fix(proxy): ...`、`feat(config): ...`、`refactor(storage): ...`。每个 commit 只包含一个逻辑变更。提交前运行 npm 全套检查、Rust 全套检查、release build 和 `git diff --check`。协议或分发变化必须同步测试、README、API 文档和 CHANGELOG。不要提交密钥、token、`.env`、`dist/`、`target/` 或生成的 release artifacts。
