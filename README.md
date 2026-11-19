# onebot-extproxy

OneBot v11 / NapCat 的「按功能 × 群」反向 WS 中继。

NapCat 以「反向 WS 客户端」方式并行外连多个消费者；OneBot v11 与 NapCat 都没有「按群订阅」
能力，所以每个外部消费者都会收到**全部**群消息。本项目在 NapCat 与远端消费者之间加一层
过滤：NapCat 的外接 client 改连中继，中继用与 NapCat 相同的握手外连真实远端，并对
`NapCat → 远端` 的事件帧按 `feature_switch` 的「功能 × 群」矩阵过滤；`远端 → NapCat` 的
API 调用/响应原样透传。

> 代码从个人部署中抽取并清洗；示例里的群号、QQ 号、域名、URL、token 全部为占位值。

```
                            ┌──────────────────────────────────────────┐
 QQ ⇄ NapCat                │              onebot-extproxy             │
   │  反向 WS client         │                                          │
   ├── ws://…/nonebot  ──────────────▶ （不经过中继，保持直连）          │
   ├── ws://relay:8090/consumer-a ─┐                                    │
   ├── ws://relay:8090/consumer-b ──┼─ WS server（NapCat 接入）         │
   └── ws://relay:8090/consumer-c ──┘        │                           │
                                            │ 按 feature_switch/        │
                                            │ state.json 过滤           │
                                            ▼                           │
                               WS client（与 NapCat 同款握手头）        │
                               ├─ wss://example.invalid:13999/… ──▶ A   │
                               ├─ wss://example.invalid:13888/… ──▶ B   │
                               └─ wss://example.invalid:5000/…  ──▶ C   │
                            └──────────────────────────────────────────┘
```

## 过滤语义

- 只过滤 `post_type ∈ {message, notice, request}` **且带 `group_id`** 的事件帧；
  `meta_event`（lifecycle/heartbeat）、无 `group_id` 的帧（私聊等）、非 JSON 帧一律原样转发。
- 功能 key 形如 `ws_<consumer>`（在 `consumers.json` 的 `feature` 字段自定义，缺省为
  `ws_<name>`），可与 `feature_switch` 的「功能 × 群」开关共用同一个 `state.json`。
- 取值顺序（与 `feature_switch` 完全一致的四层回落）：

  ```
  matrix[feature][group] → feature_defaults[feature] → group_defaults[group] → group_default
  ```

- **未纳管默认放行**：外部 feature key 在 state 中**完全缺失**（既不在 matrix 也不在
  feature_defaults）时视为「未纳管」，默认 `True`（全转发）。这样升级初期的行为与现状一致，
  只有显式配置过的群/功能才会被关闭。
- 一旦 feature 出现在 state 里（哪怕只有一行 matrix），就进入受管模式，严格按四层回落。
  建议用法「先设功能级默认，再按群关闭」：

  ```jsonc
  {
    "version": 2,
    "group_default": false,
    "group_defaults": {},
    "feature_defaults": {"ws_consumer_a": true},
    "matrix": {"ws_consumer_a": {"10000": false}}
  }
  ```

  只写 matrix 而不写 feature_defaults 时，未列出的群会回落到 `group_default`（示例中为
  `false`），即「默认关」——这与 `feature_switch` 内部功能的语义一致，不是 bug。
- state 文件热更新（mtime+size 缓存，无需重启中继）；文件删除 = 回落默认（外部 feature 默认开）。
- 坏 state：已成功加载过 → 保留 last-good；首读即坏/文件不存在 → fail-open（全转发），只记日志。

## 安装 / Quickstart

需要 Python 3.10+（推荐 3.13）与 `websockets>=17,<18`。

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -e .

cp consumers.example.json consumers.json
chmod 600 consumers.json          # 含远端 token，务必限制权限

# 只校验配置并打印脱敏摘要（不建连接）
onebot-extproxy --check-config --config consumers.json

# 前台运行
onebot-extproxy --config consumers.json
```

Docker / compose：

```bash
cp consumers.example.json consumers.json     # 填入远端 URL/token
docker compose -f compose.example.yml up -d --build
docker logs -f onebot-extproxy
```

`Dockerfile` 基于 `python:3.13-slim`，安装入口 `onebot-extproxy`，默认
`CMD ["onebot-extproxy", "--config", "/config/consumers.json"]`；`/config` 与 `/state`
以只读方式挂载。

## 配置（consumers.json）

```jsonc
{
  "version": 1,
  "listen": {"host": "0.0.0.0", "port": 8090},   // 只监听容器网络；不要发布到公网
  "state_path": "/state/state.json",             // feature_switch 的 state.json（只读挂载）
  "buffer_size": 50,                             // 远端断线期间缓冲最近 N 帧
  "queue_max": 1000,                             // 远端在线时的发送队列上限（超出丢最旧）
  "reconnect_min": 1.0,                          // 远端重连指数退避下限（秒）
  "reconnect_max": 30.0,                         // 上限（秒）
  "stats_interval": 60,                          // 统计日志间隔（秒）
  "ping_interval": null,                         // 默认禁用 WS ping；需要探活可改数字
  "proxy": null,                                 // WS 出站代理；null=直连（不读系统代理环境变量）
  "consumers": [
    {"name": "consumer-a", "feature": "ws_consumer_a", "path": "/consumer-a",
     "url": "wss://example.invalid:13999/onebot/v11/ws", "token": "<REMOTE_TOKEN_A>"}
  ]
}
```

| 字段 | 必填 | 说明 |
|---|---|---|
| `name` | 是 | 唯一名字，用于日志与统计 |
| `url` | 是 | `ws://` 或 `wss://`；远端 OneBot v11 反向 WS 地址 |
| `token` | 否 | 远端 token，可为空字符串（远端不校验时） |
| `feature` | 否 | 过滤用的功能 key，缺省 `ws_<name>` |
| `path` | 否 | NapCat 连接的中继路径，缺省 `/<name>`（尾部 `/` 会被规整） |

- `consumers.json` 请 `chmod 600`（含明文 token）；仓库里只提交 `consumers.example.json`。
- `listen`、`state_path`、`buffer_size` 等顶层项均可用 CLI / 环境变量覆盖。

### CLI 覆盖

| 参数 | 说明 |
|---|---|
| `--config/-c` | `consumers.json` 路径（默认脚本同目录或 `./consumers.json`） |
| `--state` | 覆盖 `state_path` |
| `--host` / `--port` | 覆盖监听地址/端口 |
| `--buffer` | 覆盖 `buffer_size` |
| `--log-level` | `CRITICAL/ERROR/WARNING/INFO/DEBUG` |
| `--ping-interval` / `--stats-interval` | 覆盖对应秒数 |
| `--check-config` | 只校验配置并打印脱敏摘要 + state 探针，不建连接 |

### 环境变量覆盖

`EXTPROXY_CONFIG` / `EXTPROXY_HOST` / `EXTPROXY_PORT` / `EXTPROXY_STATE_PATH` /
`EXTPROXY_BUFFER` / `EXTPROXY_LOG_LEVEL` / `EXTPROXY_PING_INTERVAL` /
`EXTPROXY_STATS_INTERVAL` / `EXTPROXY_PROXY`

优先级：**CLI > 环境变量 > 配置文件 > 默认值**。

## 部署与安全边界（务必阅读）

- **不校验 NapCat 侧 token**：WS server 不鉴权连接方，任何能访问监听端口的进程都会被当作
  NapCat。只应部署在内网 / 容器内部网络，不要映射到公网；需要加固请自行加网络隔离/反代。
- **不是隐私边界**：过滤只决定「转发哪些事件给某个远端」，中继进程本身仍能看到全部群消息。
  真正的隐私隔离要靠独立 QQ 账号 / 独立 NapCat。
- **断线不补发**：远端断线期间只在内存缓冲最近 `buffer_size` 帧，重连后先重放最近一次
  lifecycle/connect，再补发缓冲帧；更早事件与进程重启期间的事件永久丢弃，不保证重放。
- **远端兼容需实测**：中继以与 NapCat 同款握手头外连（`X-Self-ID`、
  `Authorization: Bearer <token>`、`x-client-role: Universal`、`User-Agent: OneBot/11`,
  `perMessageDeflate=false`, maxPayload 50MB）。远端若按来源 IP/UA/额外 header 做限制，
  需自行确认。
- **token 安全**：`consumers.json` 含明文 token；日志与 `--check-config` 只打印脱敏 URL 与
  `token=configured`，不会打印 token 本身。
- **SIGTERM** 优雅退出（停止监听 → 关闭各远端连接 → 退出）；同一 consumer 出现重复 NapCat
  连接时新连接替换旧连接（旧连接以 1012 关闭）。每个 consumer 路径同一时刻只服务一个
  NapCat 连接；多 bot 请为每个 bot 单独部署中继。
- 统计日志（每 `stats_interval` 秒）：
  `[name] napcat=up/down remote=up/down buffered=N forwarded=N dropped_policy=N dropped_overflow=N reconnects=N connect_failures=N`。

## 测试

```bash
# 单元测试（policy + relay 配置，不建网络）
pytest -q

# 追加本机回环集成测试（127.0.0.1 临时端口，不连外网）
pytest -q --run-integration
```

集成测试覆盖：事件按群过滤、私聊/meta 放行、远端→NapCat API 透传、远端断线缓冲最近 N 帧、
重连后 lifecycle 重放、NapCat 断线关闭远端、未知路径 1008 拒绝。

## License

MIT，见 [LICENSE](LICENSE)。安全说明见 [SECURITY.md](SECURITY.md)。
