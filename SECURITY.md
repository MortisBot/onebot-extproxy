# 安全说明

`onebot-extproxy` 是一层「按功能 × 群」的事件过滤中继，**不是**鉴权网关，**不是**隐私边界。

## 威胁模型（请先读）

1. **不校验 NapCat 侧身份**：WS server 不校验连接方提供的 `Authorization`/`X-Self-ID`，
   任何能连到监听端口的进程都会被当作 NapCat。因此：
   - 只应部署在**内网 / 容器内部网络**，不要发布到公网；
   - 若必须跨主机部署，请自行在前面加 TLS + 来源限制（防火墙/反向代理），
     或绑定容器网络地址。
2. **过滤不是隐私边界**：过滤只决定「哪些事件转发给某个远端消费者」。
   中继进程本身仍然能看到全部群消息；NapCat、宿主与容器网络上的其它进程亦然。
   需要真正的隐私隔离，请在不同 bot 之间使用独立的 QQ 账号/独立 NapCat。
3. **远端 token 存在 `consumers.json` 中（明文）**：请 `chmod 600`，不要提交/外传；
   日志与 `--check-config` 输出只打印脱敏 URL 与 `token=configured`。
4. **不校验远端证书之外的可信度**：`ws://` 明文传输 token 与消息内容；
   跨网络部署请使用 `wss://`。`proxy` 配置同样会看到明文。
5. **不做持久化/补发**：远端断线期间只在内存中缓冲最近 N 帧，连上后补发；
   进程重启、缓冲溢出的事件**永久丢弃**，不提供审计或重放保证。
6. **远端兼容性未做来源认证**：中继以与 NapCat 相同的握手头（`X-Self-ID`、
   `Authorization: Bearer <token>`、`x-client-role: Universal`、`User-Agent: OneBot/11`）
   外连远端；远端若按来源 IP/UA/额外 header 做限制，需要自行确认。

## 推荐部署

- 容器之间用专用 bridge 网络互通，中继端口不映射到宿主/公网；
- `consumers.json`、`state.json` 均以只读方式挂载；
- 定期轮换远端 token，并同步更新 `consumers.json`；
- 用 `--check-config` 做上线前校验（不会建连接）。

## 报告漏洞

请通过仓库的私有安全渠道（Security Advisory）报告，不要在公开 issue 中贴出
真实 URL、token、群号或消息内容。
