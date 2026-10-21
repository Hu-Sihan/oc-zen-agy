# oc-zen-agy

OpenCode Zen 的 OpenAI Chat Completions 兼容转发服务。使用 Node.js 原生模块，零第三方运行依赖，要求 **Node.js ≥ 24.5.0**。

## 快速启动

克隆仓库或解压源码包后，进入包含 `package.json` 的项目根目录：

```bash
git clone https://github.com/Hu-Sihan/oc-zen-agy.git
cd oc-zen-agy
cp config.example.json config.json
mkdir -p state
```

Windows PowerShell 初始化方式：

```powershell
Copy-Item .\config.example.json .\config.json
New-Item -ItemType Directory -Path .\state -Force | Out-Null
```

将已有的有效会话文件放到 `state/sessions.txt`，每行一个会话 ID。源码与压缩包不分发真实会话或本机配置，本版本也不提供会话采集命令。

无需 `npm install`，直接启动：

```bash
npm start
```

保持服务运行，另开终端并进入同一目录验证：

```bash
npm run probe
```

`probe` 默认调用 `big-pickle`，返回 HTTP 200 且正文包含 `OK` 表示实际推理成功。健康检查成功仅说明本地进程正常。按 `Ctrl+C` 停止前台服务。

客户端使用 Chat Completions 协议，Base URL 填 `http://127.0.0.1:9999/v1`。未设置本地密钥时，可按客户端要求填写占位 API Key。

## 配置

`config.example.json` 是配置模板。以下为仅监听本机的示例：

```json
{
  "host": "127.0.0.1",
  "port": 9999,
  "concurrency": 4,
  "stateDir": "./state",
  "requestTimeoutMs": 120000,
  "maxBodyBytes": 8388608
}
```

| 参数 | 含义 |
|---|---|
| `host` | IP、`localhost` 或地址数组；`"0.0.0.0"` 监听所有 IPv4 地址 |
| `port` | 所有监听地址共用的端口，默认 9999 |
| `concurrency` | 整个进程同时处理的上游请求数，默认 4；设为 1 时串行转发 |
| `stateDir` | 会话文件所在目录，相对于配置文件目录解析 |
| `requestTimeoutMs` | 请求准备、排队、上游处理和响应的总超时，默认 120000 毫秒 |
| `maxBodyBytes` | 请求体及 SSE 转 JSON 时的上游字节上限，默认 8388608，即 8 MiB；SSE 透传不整体缓存 |
| `baseURL` | 可选，上游 Chat Completions 基地址，默认 `https://opencode.ai/inference/openai/v1` |
| `modelsURL` | 可选，模型列表地址，默认 `https://opencode.ai/inference/v1/models` |

例如，多地址监听使用 `"host": ["127.0.0.1", "192.168.1.10", "::1"]`，其中地址必须属于本机。`0.0.0.0` 不应与其他 IPv4 地址重复配置；同时监听所有 IPv4/IPv6 地址可使用 `["0.0.0.0", "::"]`。IPv6 监听使用 `ipv6Only: true`。任一监听失败会关闭已启动的监听并退出。

配置与 `sessions.txt` 只在启动时读取，修改后需要重启。会话文件支持 UTF-8 BOM、空行和以 `#` 开始的注释，按首次出现顺序去重。会话文件缺失或为空时聊天返回 503，模型列表仍可调用；包含格式无效的 ID 时启动失败。

| 环境变量 | 默认值 | 含义 |
|---|---|---|
| `ZEN_API_KEY` | `public` | 发给上游的密钥 |
| `ZEN_LOCAL_API_KEY` | 空 | 本地访问密钥，设置后所有接口均需 `Authorization: Bearer <密钥>` |
| `ZEN_PROBE_MODEL` | `big-pickle` | `npm run probe` 使用的模型 |

本地密钥不会作为上游密钥转发。程序不自动加载 `.env`，也不读取 `HTTP_PROXY` / `HTTPS_PROXY`。环境变量和 `x-zen-*` 响应头沿用现有名称。

## HTTP 接口

| 方法 | 路径 | 行为 |
|---|---|---|
| GET | `/healthz` | 返回 `ok`、`mode`、`sessions`、`active`、`queued`、`concurrency` |
| GET | `/v1/model` | 实时转发 Zen 模型列表 |
| GET | `/v1/models` | 与 `/v1/model` 相同的 OpenAI 兼容路径 |
| POST | `/v1/chat/completions` | 聊天接口，支持流式和非流式响应 |

模型列表不缓存、不筛选，查询参数也会转发。列表中的模型不一定支持 Chat Completions，实际权限与协议由上游决定。

流式请求示例在 `examples/chat.json`。Linux 调用：

```bash
curl -N http://127.0.0.1:9999/v1/chat/completions \
  -H 'Content-Type: application/json' \
  --data-binary @examples/chat.json
```

Windows PowerShell 调用：

```powershell
curl.exe -N http://127.0.0.1:9999/v1/chat/completions -H "Content-Type: application/json" --data-binary "@examples/chat.json"
```

设置了本地访问密钥时，调用需增加对应的 Authorization 请求头。

`stream: true` 直接返回 SSE；`stream: false` 或省略时，将上游 SSE 合并为完整 `chat.completion` JSON，保留正文、推理字段、工具调用、usage 和结束原因。不完整流返回错误。`max_tokens` 包含模型推理预算，过低时可能没有最终正文。

聊天响应包含 `x-zen-request-id`、`x-zen-model`、`x-zen-attempts`（固定为 1）、`x-zen-compatibility` 和 `x-zen-upstream-stream`。

## 请求流程

1. 启动时读取一次 `sessions.txt`，去重并建立只读会话池。
2. 入口回调校验请求；有效聊天请求依次轮换会话、组装固定请求头和工具定义，再加入单向 FIFO 队列。
3. 单线程调度循环根据空闲并发名额出队，直接调用上游。
4. 上游响应回调适配并回复客户端，结束或中断后释放名额并触发后续出队。

所有监听地址共用队列、会话游标与并发上限；模型列表也进入同一队列，健康检查不入队。会话按 `A → B → C → A` 轮换，校验失败和模型列表请求不消耗会话；已经入队的聊天即使随后取消，也已完成本次轮换。程序不锁定或写回会话文件，不更新池内容，也不限制会话池容量。

并发满载时请求在内存中等待，等待时间计入总超时。排队超时返回 504，排队期间断开则不再请求上游。停机时拒绝排队任务，已经出队的任务最多获得 2 秒收尾时间。重启不恢复未完成任务。

`src/profile.mjs` 固化 OpenCode 1.19.3 的客户端特征和 88 个工具定义。当 `tools` 与 `tool_choice` 都未提供时，加入固定工具及 `tool_choice: "none"`；客户端显式提供的工具策略和其他模型参数会保留。代理不执行工具。上游始终使用流式请求，默认请求 usage。

没有自动重试、备用模型或多线路切换。上游非成功状态和正文透传；客户端断开或超时会取消上游请求并释放名额。真实上游能否继续接受当前固定请求组合，需要使用 `npm run probe` 验证。

## Ubuntu systemd

先进入**实际项目根目录**，确认 Node.js 版本、`config.json` 和 `state/sessions.txt` 已准备好；停止占用同一端口的前台服务。以下命令自动使用当前目录与 Node 路径：

```bash
OC_ZEN_APP_DIR="$(pwd -P)"
OC_ZEN_NODE_BIN="$(command -v node)"
OC_ZEN_RUN_USER="$(id -un)"

sudo tee /etc/systemd/system/oc-zen-agy.service >/dev/null <<EOF
[Unit]
Description=oc-zen-agy API Proxy
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=$OC_ZEN_RUN_USER
WorkingDirectory=$OC_ZEN_APP_DIR
ExecStart="$OC_ZEN_NODE_BIN" "$OC_ZEN_APP_DIR/src/server.mjs" serve --config "$OC_ZEN_APP_DIR/config.json"
Environment=NODE_ENV=production
EnvironmentFile=-/etc/oc-zen-agy.env
Restart=on-failure
RestartSec=5
TimeoutStopSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

sudo systemd-analyze verify /etc/systemd/system/oc-zen-agy.service
```

校验通过后启用并启动：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now oc-zen-agy.service
sudo systemctl status oc-zen-agy.service --no-pager -l
sudo journalctl -u oc-zen-agy.service -n 50 --no-pager
```

环境变量可按需写入 `/etc/oc-zen-agy.env`，文件权限设置为 `600`。从旧服务迁移时，先停止并禁用 `zen-deepseek.service`，保留原配置与会话；如果使用了旧环境变量文件，将所需配置迁入新文件。更名不会自动修改已安装的旧 systemd 服务。

日常重启：`sudo systemctl restart oc-zen-agy.service`。

## 开发与验证

```bash
npm test
npm run test:network
```

`npm test` 使用内存双工流及真实 Node HTTP 解析器，验证请求兼容、FIFO 调度、并发、超时、断开、停机、会话轮换、TLS 校验、模型透传和监听回滚。`test:network` 使用本机真实 HTTP/TLS 网络及模拟上游，不消耗真实模型额度；真实多网卡绑定和 Zen 推理需要在部署环境另行验证。

| 路径 | 用途 |
|---|---|
| `src/server.mjs` | 启动、停止及 probe 入口 |
| `src/forwarder.mjs` | 请求处理、会话池、FIFO、并发、多地址监听 |
| `src/profile.mjs` | 固定客户端特征及工具 Schema |
| `src/sse.mjs` | SSE 转完整 JSON |
| `examples/chat.json` | 流式请求示例 |
| `test/`、`support/` | 测试及内存传输工具；TLS 私钥仅为本机测试夹具 |
| `state/sessions.txt` | 用户提供的会话文件，不提交到 Git |

本机 `config.json`、`state/`、环境变量文件、日志、缓存与 `dist/` 已加入 Git 忽略规则。
