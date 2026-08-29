# CI 安全扫描

主 CI workflow（`.github/workflows/ci.yml`）在每个 Pull Request 以及推送到 `main` 时运行三项必需的安全门禁。仓库管理员需要在 `main` 的 branch protection 或 ruleset 中要求下表中的完整 status check 名称：

| Status check | 扫描内容 | 阻断阈值 |
| --- | --- | --- |
| `CI / Dependency Vulnerability Scan` | Go module 直接与传递依赖（govulncheck v1.1.3） | 任意可达漏洞 |
| `CI / Container Image Scan` | CI 构建出的镜像归档（Trivy v0.74.0） | HIGH、CRITICAL（包含尚未修复的条目） |
| `CI / Commit Secret Scan` | Pull Request 引入的提交；`main` push 扫描完整 Git 历史（Gitleaks v8.24.3） | 任意高置信度密钥 |

每个 job 只读取仓库内容；上传 SARIF 额外使用 `security-events: write`，原始报告会作为短期 artifact 保存，便于定位修复。上传到 GitHub Code Scanning 的报告不改变扫描门禁结果，因 fork Pull Request 可能没有写入 `security-events` 的权限。

## 本地运行

以下命令应在仓库根目录执行：

```bash
# Go 依赖漏洞
go run golang.org/x/vuln/cmd/govulncheck@v1.1.3 ./...

# 构建并扫描同一个镜像归档（需要 Docker 与 Trivy）
docker build --tag trpc-agent-service:security .
docker save trpc-agent-service:security --output trpc-service.tar
trivy image --input trpc-service.tar --scanners vuln \
  --severity HIGH,CRITICAL --ignore-unfixed=false --exit-code 1
rm -f trpc-service.tar

# 当前工作树/历史密钥扫描（需要 Gitleaks 8.24.3）
gitleaks git --redact --config gitleaks.toml --log-opts="--all"
```

Pull Request 只检查引入的提交时，可将 `--log-opts` 替换为 `"$(git merge-base origin/main HEAD)..HEAD"`。`scripts/security-secret-fixture.sh` 会在临时 Git 仓库中验证一次通过和一次阻断，且不会把模拟 token 写入本仓库：

```bash
GITLEAKS_BIN=/path/to/gitleaks ./scripts/security-secret-fixture.sh
```

## 发现问题后的处理

依赖漏洞应升级到扫描器报告的修复版本并重新运行完整测试。镜像漏洞应升级基础镜像或受影响模块；不要只依赖可变 tag。密钥扫描命中后，先吊销/轮换凭据，再从 Git 历史中清理泄漏内容。

误报或暂时无法修复的镜像条目必须在 `.trivyignore` 中以最小漏洞 ID 范围登记；密钥条目必须在 `gitleaks.toml` 中以最小路径或提交范围登记。每个例外都要在变更说明中写明 owner、理由、跟踪 issue 和到期日期，并通过 `scripts/validate-security-allowlist.sh` 校验。到期后必须删除例外；不得用 allowlist 隐藏真实凭据，也不得把 secret 值写入日志、SARIF 或 issue。

## 失败与报告策略

漏洞数据库不可用、报告格式错误或扫描器异常都会使 job 失败（fail closed），同时保留清理步骤。表格输出用于 CI 日志，SARIF 用于 Code Scanning，artifact 保留 14 天。任何报告上传失败都不会把已完成的扫描误报为成功。
