# BlazeNet 项目级约束

## 语言与文本

- 所有非功能性文本（注释、错误信息、打印、日志、界面文案）一律使用简体中文；技术术语保持原名。

## Rust 编码规范

- 遵守全局 `/root/.codex/rust-coding-standards.md`；本项目专属约束以本文件为准。

## 编译交付规范

- 所有编译交付物统一放置到 `bin/` 目录，按平台分 `bin/linux/` 与 `bin/windows/`。
- 制作机、网吧服务器程序：仅 Windows，交付到 `bin/windows/`。
- 原始节点、IDC 节点、调度中心、relay：仅 Linux，交付到 `bin/linux/`。
- 编译时严格遵守，不做跨平台冗余交付。

## 配置规范（仅限本项目）

- Linux 可执行文件：有且仅有一个参数 `--config <路径>` 指定配置文件，其余配置项均从配置文件读取。
- Windows 可执行文件：自动加载"程序目录上一级目录下的 `config/` 目录"中的配置文件，文件名固定并写死在代码中（制作机 `producer.toml`、网吧 agent `cafe-agent.toml`）。
- 配置文件统一使用 TOML 格式。

## 网络约束

- 打洞端口必须使用 10001-65535（NAT 网关限制）。
- relay 只做打洞协助，不承载块数据（应用层路径门控）。

## 测试与 CI/Release 约束

- 单元测试覆盖率必须 100% 且全部通过（全绿）；覆盖率统计使用 `cargo-llvm-cov`。
- 配置 GitHub Actions 工作流：
  - CI：`cargo fmt --check`、`cargo clippy -D warnings`、`cargo test --all-targets`、覆盖率 100% 门禁；
  - Release：打 tag 触发，构建 `bin/linux/` 与 `bin/windows/` 交付物并上传 artifacts；
  - CI 与 Release 未全绿不得合并、不得发布。
- CI/Release 状态确认：每 30 秒轮询一次 `gh run list`/`gh run view`，不使用 `gh run watch` 长时间阻塞；未完成先汇报中间状态继续其他工作。
