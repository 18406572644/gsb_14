# 可靠消息聊天室（Node + ws + SQLite）

基于 WebSocket 的可靠消息投递聊天室。不引入 MQ，以 SQLite 为唯一持久化设施，实现：

- **消息不丢失**：先落库、再 ACK、后广播；服务重启后消息完整可补发
- **ACK 确认**：双向确认 —— 发送方收服务端 ACK（含分配的 seq）；接收方对推送做累积 ACK
- **断线补发**：重连后按 `lastSeq` 增量回放缺口，分批拉取
- **幂等去重**：`clientMsgId` 唯一约束防发送重试产生重复；客户端按 `seq` 过滤重复投递
- **消息时序可控**：每房间单调递增 `seq`，由计数器在写事务内分配，保证房间内全序
- **连接管理**：心跳保活、全局/单用户连接数上限、背压断开、优雅退出
- **房间权限**：管理员 / 成员 / 禁言三种状态，管理员可禁言、解禁
- **房间邀请**：管理员定向生成带有效期的邀请，被邀请人确认后加入；过期 / 已撤销 /
  已使用的邀请不可用；服务端校验邀请人权限、目标用户身份、成员关系与房间人数上限；
  接受后成员关系持久落库（非仅当前连接临时入房），多端同步入房
- **发送限流**：按用户令牌桶

## 快速开始

```bash
npm install
npm start          # http://localhost:8080
npm test           # 23 个集成测试
```

浏览器打开 `http://localhost:8080`，用不同昵称开三个标签页即可体验：建房、发消息、
禁言管理；管理员在右侧成员面板点「＋ 邀请」按昵称定向邀请用户，被邀请人在左侧
「收到的房间邀请」中点「接受邀请」加入，成功/失败均有提示。断网/刷新页面后自动重连
并补发离线期间的消息。

要求 Node.js ≥ 22.13（使用内置 `node:sqlite`，唯一第三方依赖是 `ws`）。

## 架构

```
src/
├── config.js   配置（端口、连接上限、心跳、重发、限流，均可环境变量覆盖）
├── db.js       SQLite 持久层：schema、幂等写入、seq 分配、游标
├── hub.js      连接注册中心：房间索引、广播、未 ACK 追踪、心跳/重发扫描
├── server.js   HTTP + WS 服务：认证、消息路由、权限检查、限流、生命周期
└── util.js     token 签名、帧解析等工具
public/index.html   演示客户端（实现完整可靠投递协议）
test/chat.test.js   集成测试（node:test）
```

### 数据模型

| 表 | 说明 |
|---|---|
| `users` | 用户（演示级 token 认证） |
| `rooms` | 房间，`last_seq` 为房间消息序号计数器；`max_occupancy` 为人数上限（`<=0` 不限） |
| `members` | 成员关系：`role`（admin/member）+ `muted_until`（禁言截止时间）。邀请接受即在该表持久落库 |
| `invites` | 房间邀请：`inviter_id`/`invitee_id`、`status`（active/used/revoked）、`expires_at`、`used_at`/`revoked_at`；`(room_id, invitee_id)` 上有仅针对 active 的部分唯一索引 |
| `messages` | 消息。主键 `(room_id, seq)`；唯一键 `(room_id, sender_id, client_msg_id)` 为幂等键 |
| `cursors` | 每用户每房间已确认游标 `last_ack_seq`，断线补发的服务端兜底依据 |

## 可靠性设计

### 1. 不丢失：持久化先于广播

发送路径在一个 SQLite 事务内完成「递增 `rooms.last_seq` 分配 seq + 写入 messages」，
**提交后**才向发送方回 ACK、向房间广播。因此：凡是客户端收到 ACK 的消息，必然已落库，
进程崩溃/重启后不丢（WAL + `synchronous=FULL`）。广播失败的连接由补发机制兜底。

### 2. 发送幂等：clientMsgId 唯一约束

客户端为每条消息生成唯一 `clientMsgId`，未收到 ACK 时以**同一 ID** 重发。
服务端命中 `(room_id, sender_id, client_msg_id)` 唯一约束时直接返回原消息的 ACK
（含原 seq），不重复写入、不重复广播。网络重试、双击、超时重发都不会产生重复消息。

### 3. 至少一次投递 + 幂等消费 = 效果上的恰好一次

- 服务端向在线连接推送消息后登记「未 ACK 队列」，超时未收到该连接的累积 ACK 则重发；
  超过最大重发次数判定连接不可用并断开，等客户端重连走补发。
- 客户端按房间维护 `lastSeenSeq`，凡是 `seq <= lastSeenSeq` 的投递一律丢弃 ——
  重发、补发重叠都不会重复上屏。

### 4. 断线补发：sync 协议

客户端持久化每个房间的 `lastSeenSeq`。重连后：

```
client → {type:'join', room, lastSeq: 41}
server → {type:'joined', ...}
server → {type:'msg', seq: 42} ... {type:'msg', seq: 57}   （缺口回放，按序）
server → {type:'sync_done', roomId, lastSeq: 57, hasMore: false}
```

`hasMore=true` 时客户端用新的 `lastSeq` 继续 `sync` 拉取下一批（单批上限
`SYNC_BATCH_SIZE`，默认 500）。`lastSeq` 缺省时使用服务端保存的确认游标
（新设备场景）；历史消息可用 `history` 向前翻页。

### 5. 时序可控

`seq` 由 `rooms.last_seq` 在写事务内递增分配（单写者 + 事务 = 无空洞、无并发交错），
房间内消息严格全序。客户端凭 seq 即可检测空洞并触发补发，无需依赖时钟。

### 6. 房间邀请与加入确认

邀请是**管理员定向发给某个已注册用户**的入房凭证，带有效期与使用状态：

```
admin   → invite_create {roomId, targetName, ttlMinutes}
server  → invite_created（回管理员）
server  → invite_received（推给被邀请人全部在线设备）

invitee → invite_accept {inviteId, lastSeq}
server  → joined + 缺口回放            （被邀请人，成员关系此时持久落库）
server  → joined                       （其其他在线设备，多端同步入房）
server  → member_joined                （房间广播，成员列表据此刷新）
server  → invite_closed reason=used    （被邀请人收起邀请条目）
```

- **状态机**：`active → used`（接受）/ `revoked`（管理员撤销或过期 sweep 收口）。
  `(room_id, invitee_id)` 上有仅针对 `active` 的**部分唯一索引**，同一房间对同一用户
  同时只存在一张有效邀请；旧邀请过期后管理员可直接重新发起（旧的自动收口）。
- **服务端校验**：发起时校验邀请人是本房间管理员、目标用户存在且尚非成员、房间未满；
  接受时校验邀请归属（仅被邀请人）、状态为 active、未过期。
- **原子接受 + 人数上限竞态**：接受在单个 `IMMEDIATE` 事务内完成「active 条件置 used +
  成员计数上限校验 + members upsert」。即使发起后、接受前房间被他人占满，也会在事务内
  以 `ROOM_FULL` 拒绝且不消费邀请。
- **持久生效而非临时入房**：接受写入的是 `members` 表，断线重连、服务重启、换设备后
  成员身份依旧（`rooms` 查询可见）；当前 WebSocket 连接的房间索引只是运行时投递通道。
- **过期处理**：后台定时 sweep 把过期 active 邀请收口为 revoked 并实时通知被邀请人；
  接受请求也会即时判定过期，过期/撤销/已使用一律拒绝。

## 协议（JSON 文本帧）

### 客户端 → 服务端

| 类型 | 字段 | 说明 |
|---|---|---|
| `ping` | `t` | 应用层心跳，回 `pong` |
| `create_room` | `name, maxOccupancy?` | 建房（可指定人数上限，非负整数，0 表示不限），创建者为管理员，回 `joined` |
| `join` | `room, lastSeq?` | 加入房间（room 可为 id 或名称）；带进度则立即补发；同样受人数上限约束 |
| `leave` | `roomId` | 离开房间 |
| `msg` | `roomId, clientMsgId, content` | 发消息，回 `ack` |
| `ack` | `roomId, seq` | 累积确认：seq 及之前均已收到 |
| `sync` | `roomId, lastSeq?` | 请求补发 |
| `history` | `roomId, beforeSeq?, limit?` | 历史翻页（升序返回） |
| `rooms` | — | 我加入的房间列表 |
| `members` | `roomId` | 成员列表（含在线状态） |
| `mute` | `roomId, userId, minutes` | 禁言（仅管理员，1..1440 分钟） |
| `unmute` | `roomId, userId` | 解除禁言（仅管理员） |
| `invite_create` | `roomId, targetName` 或 `targetUserId, ttlMinutes?` | 管理员发起定向邀请（`ttlMinutes` 1..10080，默认 1440） |
| `invite_list` | `roomId?` | 不带 `roomId`：我收到的在途邀请；带 `roomId`：管理员查该房间全部邀请 |
| `invite_revoke` | `inviteId` | 管理员撤销在途邀请 |
| `invite_accept` | `inviteId, lastSeq?` | 接受邀请加入房间（仅被邀请人），成功回 `joined` 并广播 `member_joined` |

### 服务端 → 客户端

| 类型 | 说明 |
|---|---|
| `welcome` | 连接建立：`{userId, name, serverTime}` |
| `joined` | 入房成功：`{roomId, name, role, mutedUntil, lastSeq, maxOccupancy}` |
| `msg` | 房间消息：`{roomId, seq, clientMsgId, from, fromName, content, ts}` |
| `ack` | 发送确认：`{roomId, clientMsgId, seq, ts}` |
| `sync_done` | 一批补发结束：`{roomId, lastSeq, hasMore}` |
| `history` / `rooms` / `members` | 对应查询的响应 |
| `notice` | 房间事件（`muted` / `unmuted`） |
| `member_joined` | 新成员经邀请加入：`{roomId, member:{userId,name,role,mutedUntil,joinedAt}}` |
| `invite_created` | 管理员发起成功：`{invite}` |
| `invite_received` | 推送给被邀请人：`{invite}`（其所有在线设备） |
| `invites` | 邀请列表：`{scope:'mine', invites}` 或 `{scope:'room', roomId, invites}` |
| `invite_revoked` | 管理员撤销成功：`{inviteId, roomId}` |
| `invite_closed` | 邀请被收口（推送被邀请人）：`{inviteId, roomId, reason: 'used'|'revoked'|'expired'}` |
| `error` | `{code, message, ref?}`，code 见下 |
| `server_shutdown` | 服务即将关闭，请准备重连 |

邀请对象 `invite`：`{id, roomId, roomName, inviterId, inviterName, inviteeId, inviteeName,
status, createdAt, expiresAt, usedAt?, revokedAt?}`。

错误码：`BAD_FRAME` `BAD_REQUEST` `UNKNOWN_TYPE` `NOT_MEMBER` `NO_SUCH_ROOM`
`ROOM_EXISTS` `ROOM_FULL` `FORBIDDEN` `MUTED` `RATE_LIMITED` `INTERNAL`
`NO_SUCH_USER` `ALREADY_MEMBER` `INVITE_EXISTS` `NO_SUCH_INVITE` `INVITE_EXPIRED`
`INVITE_REVOKED` `INVITE_USED` `INVITE_UNAVAILABLE`；
升级阶段拒绝：`401`（认证失败）、`503 SERVER_FULL` / `503 TOO_MANY_DEVICES`。

### 连接建立

```
POST /api/login {"name":"alice"}  →  {userId, name, token}
GET  /ws?token=<token>            →  WebSocket 升级
```

## 关键配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `PORT` / `HOST` | `8080` / `0.0.0.0` | 监听地址 |
| `CHAT_DB_PATH` | `chat.db` | SQLite 路径（`:memory:` 用于测试） |
| `MAX_CONNECTIONS` | `1000` | 全局并发连接上限 |
| `MAX_CONNECTIONS_PER_USER` | `3` | 单用户连接上限（多端） |
| `HEARTBEAT_INTERVAL_MS` / `HEARTBEAT_TIMEOUT_MS` | `30000` / `75000` | 心跳周期 / 判死超时 |
| `ACK_RESEND_AFTER_MS` / `ACK_MAX_RESEND` | `3000` / `5` | 未 ACK 重发阈值 / 最大次数 |
| `MAX_UNACKED_PER_CONN` | `1000` | 单连接未确认积压上限（背压） |
| `RATE_LIMIT_PER_SEC` / `RATE_LIMIT_BURST` | `10` / `20` | 发送限流令牌桶 |
| `SYNC_BATCH_SIZE` | `500` | 补发单批条数 |
| `DEFAULT_MAX_OCCUPANCY` | `50` | 新建房间默认人数上限（0 不限） |
| `MAX_OCCUPANCY_LIMIT` | `100000` | `create_room` 可设置的人数上限边界（0 不限制） |
| `INVITE_DEFAULT_TTL_MINUTES` | `1440` | 邀请默认有效期（分钟，24 小时） |
| `INVITE_MAX_TTL_MINUTES` | `10080` | 邀请最长有效期（分钟，7 天） |
| `INVITE_SWEEP_INTERVAL_MS` | `30000` | 过期邀请收口扫描周期 |
| `AUTH_SECRET` | — | token HMAC 密钥，**生产必须设置** |

## 已知边界（演示级取舍）

- 认证为演示级（用户名即账号、HMAC token），生产应替换为正式账号体系；
- 单进程架构，多实例部署需引入外部 Pub/Sub 做跨节点广播（DB 层无需改动）；
- 消息无保留期清理，长期使用需自行加定时清理任务。
