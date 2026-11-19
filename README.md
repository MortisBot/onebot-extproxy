# onebot-extproxy

OneBot v11 / NapCat 的「按群订阅」反向 WS 中继：NapCat 外连的多个消费者（NoneBot、外部 bot）默认会收到全部群消息；本中继按「功能 × 群」矩阵过滤 `message/notice/request` 事件帧，`meta`/私聊/非 JSON 帧原样转发，远端 API 调用透传。

> 状态：**骨架，待清洗**。源码来源：`/home/enamht/wakaba-bot/extproxy`（relay.py / policy.py / self_check.py / README.md）。清洗规格见 [`docs/08-opensource-plan.md`](../docs/08-opensource-plan.md) §4.1。

## 待办

- [ ] 拷贝 relay.py / policy.py / self_check.py 与 README
- [ ] README 中真实远端 IP/宿主路径 → 占位符；`consumers.example.json` 保留占位
- [ ] 补 `requirements.txt`（websockets 17.x）/ `Dockerfile` / `compose.example.yml`
- [ ] 补 `LICENSE`(MIT) / `SECURITY.md`：说明"不校验 NapCat 侧 token、仅限内网部署、不是隐私边界"
- [ ] 自检：`python3 self_check.py`（离线部分）+ 容器内集成测试

## 不包含

- 真实 `consumers.json`（远端 URL/token 全占位）
- NapCat/Koishi 镜像或插件、任何登录态/群号/群消息数据
