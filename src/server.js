'use strict';

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { WebSocketServer } = require('ws');

const { ChatDB } = require('./db');
const { Hub, Connection } = require('./hub');
const defaultConfig = require('./config');
const {
  randomId,
  randomSecret,
  signToken,
  verifyToken,
  isNonEmptyString,
  parseFrame,
  now,
} = require('./util');

/** 业务错误：handler 抛出，统一转成 error 帧回给客户端 */
class ChatError extends Error {
  constructor(code, message) {
    super(message);
    this.code = code;
  }
}
const fail = (code, message) => {
  throw new ChatError(code, message);
};

/** 令牌桶限流（按用户），防刷屏 */
class TokenBucket {
  constructor(ratePerSec, burst) {
    this.rate = ratePerSec;
    this.burst = burst;
    this.buckets = new Map();
  }
  take(key) {
    const t = now();
    let b = this.buckets.get(key);
    if (!b) {
      b = { tokens: this.burst, updated: t };
      this.buckets.set(key, b);
    }
    b.tokens = Math.min(this.burst, b.tokens + ((t - b.updated) / 1000) * this.rate);
    b.updated = t;
    if (b.tokens < 1) return false;
    b.tokens -= 1;
    return true;
  }
}

/** 数据库消息行 -> 下发帧 */
function msgFrame(m) {
  return {
    type: 'msg',
    roomId: m.roomId,
    seq: m.seq,
    clientMsgId: m.clientMsgId,
    from: m.from,
    fromName: m.fromName,
    content: m.content,
    ts: m.ts,
  };
}

/** 入房成功帧（create_room / join / invite_accept 共用） */
function joinedFrame(room, member) {
  return {
    type: 'joined',
    roomId: room.id,
    name: room.name,
    role: member.role,
    mutedUntil: member.muted_until,
    lastSeq: room.last_seq,
    maxOccupancy: room.max_occupancy,
  };
}

function createChatServer(overrides = {}) {
  const config = { ...defaultConfig, ...overrides };
  const db = new ChatDB(config.dbPath);
  const hub = new Hub(config);
  const limiter = new TokenBucket(config.rateLimitPerSec, config.rateLimitBurst);
  const publicDir = path.join(__dirname, '..', 'public');

  // ---------------------------------------------------------------- 消息处理

  /** 断线补发：把 roomId 中 seq > fromSeq 的消息按序推给连接，分批，客户端按 sync_done 续拉 */
  function replayRoom(conn, roomId, fromSeq) {
    const batch = db.getMessagesAfter(roomId, fromSeq, config.syncBatchSize + 1);
    const hasMore = batch.length > config.syncBatchSize;
    const slice = hasMore ? batch.slice(0, config.syncBatchSize) : batch;
    for (const m of slice) hub.send(conn, msgFrame(m), { track: true, roomId, seq: m.seq });
    const lastSeq = slice.length ? slice[slice.length - 1].seq : fromSeq;
    hub.send(conn, { type: 'sync_done', roomId, lastSeq, hasMore });
  }

  function requireMember(conn, roomId) {
    const member = db.getMember(roomId, conn.userId);
    if (!member) fail('NOT_MEMBER', 'not a member of this room');
    return member;
  }

  function requireAdmin(conn, roomId) {
    const member = requireMember(conn, roomId);
    if (member.role !== 'admin') fail('FORBIDDEN', 'admin role required');
    return member;
  }

  /** 原始 invite 行 -> 下发对象（含房间名/邀请人名/被邀请人名） */
  function inviteView(inv) {
    const room = db.getRoom(inv.room_id);
    const inviter = db.getUserById(inv.inviter_id);
    const invitee = db.getUserById(inv.invitee_id);
    return {
      id: inv.id,
      roomId: inv.room_id,
      roomName: room ? room.name : inv.room_id,
      inviterId: inv.inviter_id,
      inviterName: inviter ? inviter.name : '',
      inviteeId: inv.invitee_id,
      inviteeName: invitee ? invitee.name : '',
      status: inv.status,
      createdAt: inv.created_at,
      expiresAt: inv.expires_at,
      usedAt: inv.used_at ?? undefined,
      revokedAt: inv.revoked_at ?? undefined,
    };
  }

  const handlers = {
    ping(conn, msg) {
      hub.send(conn, { type: 'pong', t: msg.t });
    },

    create_room(conn, msg) {
      if (!isNonEmptyString(msg.name, 64)) fail('BAD_REQUEST', 'invalid room name');
      if (db.getRoomByName(msg.name)) fail('ROOM_EXISTS', 'room name already taken');
      let maxOccupancy = config.defaultMaxOccupancy;
      if (msg.maxOccupancy !== undefined && msg.maxOccupancy !== null) {
        if (!Number.isInteger(msg.maxOccupancy) || msg.maxOccupancy < 0) {
          fail('BAD_REQUEST', 'maxOccupancy must be a non-negative integer');
        }
        if (config.maxOccupancyLimit > 0 && msg.maxOccupancy > config.maxOccupancyLimit) {
          fail('BAD_REQUEST', `maxOccupancy must be <= ${config.maxOccupancyLimit}`);
        }
        maxOccupancy = msg.maxOccupancy;
      }
      const room = db.createRoom(randomId('r_'), msg.name, conn.userId, maxOccupancy);
      hub.joinRoom(conn, room.id);
      hub.send(conn, joinedFrame(room, db.getMember(room.id, conn.userId)));
    },

    join(conn, msg) {
      if (!isNonEmptyString(msg.room, 128)) fail('BAD_REQUEST', 'invalid room');
      const room = db.getRoom(msg.room) || db.getRoomByName(msg.room);
      if (!room) fail('NO_SUCH_ROOM', 'room not found');
      const wasMember = !!db.getMember(room.id, conn.userId);
      if (!wasMember) {
        // 直接加入同样受人数上限约束
        if (room.max_occupancy > 0 && db.countMembers(room.id) >= room.max_occupancy) {
          fail('ROOM_FULL', 'room has reached its member limit');
        }
      }
      db.joinRoom(room.id, conn.userId);
      hub.joinRoom(conn, room.id);
      const member = db.getMember(room.id, conn.userId);
      hub.send(conn, joinedFrame(room, member));
      // 补发：优先用客户端上报的进度，否则用服务端游标（新设备则从游标开始）
      const fromSeq = Number.isInteger(msg.lastSeq) ? msg.lastSeq : db.getCursor(room.id, conn.userId);
      if (fromSeq < room.last_seq) replayRoom(conn, room.id, fromSeq);
    },

    leave(conn, msg) {
      hub.leaveRoom(conn, msg.roomId);
      hub.send(conn, { type: 'left', roomId: msg.roomId });
    },

    msg(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      if (!isNonEmptyString(msg.clientMsgId, 64)) fail('BAD_REQUEST', 'invalid clientMsgId');
      if (!isNonEmptyString(msg.content, config.maxContentLength)) {
        fail('BAD_REQUEST', `content must be 1..${config.maxContentLength} chars`);
      }
      const member = requireMember(conn, msg.roomId);
      if (member.muted_until > now()) {
        fail('MUTED', `you are muted until ${new Date(member.muted_until).toISOString()}`);
      }
      if (!limiter.take(conn.userId)) fail('RATE_LIMITED', 'sending too fast, slow down');

      // 先落库（同事务分配 seq），再 ACK，再广播 —— 崩溃也不丢已确认消息
      const { message, duplicate } = db.insertMessage({
        roomId: msg.roomId,
        clientMsgId: msg.clientMsgId,
        senderId: conn.userId,
        content: msg.content,
      });
      hub.send(conn, {
        type: 'ack',
        roomId: msg.roomId,
        clientMsgId: msg.clientMsgId,
        seq: message.seq,
        ts: message.ts,
      });
      if (!duplicate) {
        // 重复提交（客户端重试）只回 ACK，不再广播 —— 发送幂等
        hub.broadcast(msg.roomId, msgFrame(message), { track: true, seq: message.seq });
      }
    },

    // 客户端累积 ACK：清除未确认队列 + 持久化游标（断线补发的兜底依据）
    ack(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128) || !Number.isInteger(msg.seq)) return;
      if (!conn.rooms.has(msg.roomId)) return; // 只处理本连接已加入的房间
      conn.ack(msg.roomId, msg.seq);
      db.saveCursor(msg.roomId, conn.userId, msg.seq);
    },

    sync(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      requireMember(conn, msg.roomId);
      const fromSeq = Number.isInteger(msg.lastSeq) ? msg.lastSeq : db.getCursor(msg.roomId, conn.userId);
      replayRoom(conn, msg.roomId, fromSeq);
    },

    history(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      requireMember(conn, msg.roomId);
      const limit = Math.min(Math.max(1, msg.limit || 50), config.historyMaxLimit);
      const before = Number.isInteger(msg.beforeSeq) ? msg.beforeSeq : Number.MAX_SAFE_INTEGER;
      const messages = db.getMessagesBefore(msg.roomId, before, limit);
      hub.send(conn, { type: 'history', roomId: msg.roomId, messages, hasMore: messages.length === limit });
    },

    rooms(conn) {
      hub.send(conn, { type: 'rooms', rooms: db.listRoomsForUser(conn.userId) });
    },

    members(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      requireMember(conn, msg.roomId);
      const online = new Set(hub.onlineUserIds(msg.roomId));
      const members = db.listMembers(msg.roomId).map((m) => ({ ...m, online: online.has(m.userId) }));
      hub.send(conn, { type: 'members', roomId: msg.roomId, members });
    },

    mute(conn, msg) {
      requireAdmin(conn, msg.roomId);
      const target = db.getMember(msg.roomId, msg.userId);
      if (!target) fail('NOT_MEMBER', 'target is not a member');
      if (target.role === 'admin') fail('FORBIDDEN', 'cannot mute an admin');
      const minutes = Number(msg.minutes);
      if (!Number.isFinite(minutes) || minutes < 1 || minutes > 1440) {
        fail('BAD_REQUEST', 'minutes must be 1..1440');
      }
      const until = now() + Math.round(minutes * 60_000);
      db.setMuted(msg.roomId, msg.userId, until);
      hub.broadcast(msg.roomId, {
        type: 'notice',
        roomId: msg.roomId,
        event: 'muted',
        userId: msg.userId,
        until,
        by: conn.userId,
      });
    },

    unmute(conn, msg) {
      requireAdmin(conn, msg.roomId);
      const target = db.getMember(msg.roomId, msg.userId);
      if (!target) fail('NOT_MEMBER', 'target is not a member');
      db.setMuted(msg.roomId, msg.userId, 0);
      hub.broadcast(msg.roomId, {
        type: 'notice',
        roomId: msg.roomId,
        event: 'unmuted',
        userId: msg.userId,
        by: conn.userId,
      });
    },

    // ------------------------------------------------ 房间邀请

    /**
     * 管理员生成定向邀请。
     * 入参：{roomId, targetName? / targetUserId?, ttlMinutes?}
     * 校验：邀请人须为本房间管理员；目标用户须存在且尚不是本房间成员；
     *      房间未达人数上限（接受时还会在事务内二次校验）；同房间对同用户无在途 active 邀请。
     */
    invite_create(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      requireAdmin(conn, msg.roomId); // 邀请人房间权限
      const room = db.getRoom(msg.roomId);
      if (!room) fail('NO_SUCH_ROOM', 'room not found');

      let target = null;
      if (isNonEmptyString(msg.targetUserId, 128)) {
        target = db.getUserById(msg.targetUserId);
      } else if (isNonEmptyString(msg.targetName, 64)) {
        target = db.getUserByName(msg.targetName);
      } else {
        fail('BAD_REQUEST', 'targetName or targetUserId required');
      }
      if (!target) fail('NO_SUCH_USER', 'target user not found');
      if (db.getMember(room.id, target.id)) fail('ALREADY_MEMBER', 'target is already a member');
      if (room.max_occupancy > 0 && db.countMembers(room.id) >= room.max_occupancy) {
        fail('ROOM_FULL', 'room has reached its member limit');
      }

      let ttl = config.inviteDefaultTtlMinutes;
      if (msg.ttlMinutes !== undefined && msg.ttlMinutes !== null) {
        ttl = Number(msg.ttlMinutes);
        if (!Number.isInteger(ttl) || ttl < 1 || ttl > config.inviteMaxTtlMinutes) {
          fail('BAD_REQUEST', `ttlMinutes must be 1..${config.inviteMaxTtlMinutes}`);
        }
      }
      // 同一房间对同一用户只允许一张在途 active 邀请；若旧邀请已过期（尚未被 sweep
      // 收口），先撤销旧的，再重新发起 —— 过期邀请不应阻塞管理员重发。
      const pending = db.getActiveInviteForPair(room.id, target.id);
      if (pending) {
        if (pending.expires_at > now()) {
          fail('INVITE_EXISTS', 'an active invite for this user already exists');
        }
        db.revokeInvite(pending.id, conn.userId);
        hub.sendToUser(target.id, {
          type: 'invite_closed', inviteId: pending.id, roomId: room.id, reason: 'expired',
        });
      }

      const expiresAt = now() + ttl * 60_000;
      let invite;
      try {
        invite = db.createInvite({
          id: randomId('inv_'),
          roomId: room.id,
          inviterId: conn.userId,
          inviteeId: target.id,
          expiresAt,
        });
      } catch (err) {
        // 并发下命中 partial unique index —— 同一用户已有 active 邀请
        if (String(err.message).includes('UNIQUE constraint failed: invites')) {
          fail('INVITE_EXISTS', 'an active invite for this user already exists');
        }
        throw err;
      }

      const view = inviteView(invite);
      hub.send(conn, { type: 'invite_created', invite: view });
      // 通知被邀请人的所有在线设备（多端一致）
      hub.sendToUser(target.id, { type: 'invite_received', invite: view });
    },

    /**
     * 列出邀请：
     *  - 不带 roomId：我收到的 active 邀请；
     *  - 带 roomId：该房间管理员查看本房间全部邀请（含 used/revoked）。
     */
    invite_list(conn, msg) {
      if (isNonEmptyString(msg.roomId, 128)) {
        requireAdmin(conn, msg.roomId);
        hub.send(conn, { type: 'invites', scope: 'room', roomId: msg.roomId,
          invites: db.listInvitesForRoom(msg.roomId) });
        return;
      }
      hub.send(conn, { type: 'invites', scope: 'mine', invites: db.listInvitesForInvitee(conn.userId) });
    },

    /** 管理员撤销邀请：active -> revoked，并通知被邀请人 */
    invite_revoke(conn, msg) {
      if (!isNonEmptyString(msg.inviteId, 128)) fail('BAD_REQUEST', 'invalid inviteId');
      const invite = db.getInvite(msg.inviteId);
      if (!invite) fail('NO_SUCH_INVITE', 'invite not found');
      requireAdmin(conn, invite.room_id);
      const revoked = db.revokeInvite(invite.id, conn.userId);
      if (!revoked) fail('INVITE_UNAVAILABLE', 'invite is no longer active');
      hub.send(conn, { type: 'invite_revoked', inviteId: invite.id, roomId: invite.room_id });
      hub.sendToUser(invite.invitee_id, {
        type: 'invite_closed', inviteId: invite.id, roomId: invite.room_id, reason: 'revoked',
      });
    },

    /**
     * 被邀请人接受邀请。DB 在单个 IMMEDIATE 事务内完成
     * 「active 条件置 used + 人数上限校验 + 成员关系 upsert」，
     * 保证成员身份持久落库（members 表）而非仅当前连接临时入房。
     */
    invite_accept(conn, msg) {
      if (!isNonEmptyString(msg.inviteId, 128)) fail('BAD_REQUEST', 'invalid inviteId');
      const invite = db.getInvite(msg.inviteId);
      if (!invite) fail('NO_SUCH_INVITE', 'invite not found');
      if (invite.invitee_id !== conn.userId) fail('FORBIDDEN', 'this invite is not for you');

      const room = db.getRoom(invite.room_id);
      if (!room) fail('NO_SUCH_ROOM', 'room not found');

      const result = db.acceptInvite(invite.id, { maxOccupancy: room.max_occupancy });
      if (!result.ok) {
        const map = {
          NO_SUCH_INVITE: ['NO_SUCH_INVITE', 'invite not found'],
          INVITE_EXPIRED: ['INVITE_EXPIRED', 'invite has expired'],
          INVITE_REVOKED: ['INVITE_REVOKED', 'invite has been revoked'],
          INVITE_USED: ['INVITE_USED', 'invite has already been used'],
          ROOM_FULL: ['ROOM_FULL', 'room has reached its member limit'],
          INVITE_UNAVAILABLE: ['INVITE_UNAVAILABLE', 'invite is no longer available'],
        };
        const [code, text] = map[result.reason] || ['INVITE_UNAVAILABLE', 'invite unavailable'];
        fail(code, text);
      }

      const { member, alreadyMember } = result;
      // 当前连接进入房间运行时索引（成员关系此前已持久落库）
      hub.joinRoom(conn, room.id);
      hub.send(conn, joinedFrame(room, member));
      // 断线补发：优先客户端进度，否则服务端游标
      const fromSeq = Number.isInteger(msg.lastSeq) ? msg.lastSeq : db.getCursor(room.id, conn.userId);
      if (fromSeq < room.last_seq) replayRoom(conn, room.id, fromSeq);

      // 同一用户的其他在线设备：被动同步入房并按服务端游标补发
      const others = hub.byUser.get(conn.userId);
      if (others) {
        for (const other of others) {
          if (other === conn || other.rooms.has(room.id)) continue;
          hub.joinRoom(other, room.id);
          hub.send(other, joinedFrame(room, member));
          const cur = db.getCursor(room.id, conn.userId);
          if (cur < room.last_seq) replayRoom(other, room.id, cur);
        }
      }

      // 通知被邀请人全部设备：该邀请已消费（用于收起邀请条目）
      hub.sendToUser(conn.userId, {
        type: 'invite_closed', inviteId: invite.id, roomId: room.id, reason: 'used',
      });

      if (!alreadyMember) {
        // 房间内广播新成员加入，在线成员据此刷新成员列表
        const user = db.getUserById(conn.userId);
        hub.broadcast(room.id, {
          type: 'member_joined',
          roomId: room.id,
          member: {
            userId: conn.userId, name: user.name, role: member.role,
            mutedUntil: member.muted_until, joinedAt: member.joined_at,
          },
        });
      }
    },
  };

  function onFrame(conn, raw) {
    const msg = parseFrame(raw);
    if (!msg) {
      hub.send(conn, { type: 'error', code: 'BAD_FRAME', message: 'invalid JSON frame' });
      return;
    }
    const handler = handlers[msg.type];
    if (!handler) {
      hub.send(conn, { type: 'error', code: 'UNKNOWN_TYPE', message: `unknown type: ${msg.type}` });
      return;
    }
    try {
      handler(conn, msg);
    } catch (err) {
      if (err instanceof ChatError) {
        hub.send(conn, {
          type: 'error',
          code: err.code,
          message: err.message,
          ref: msg.clientMsgId || msg.roomId || undefined,
        });
      } else {
        console.error('[handler error]', msg.type, err);
        hub.send(conn, { type: 'error', code: 'INTERNAL', message: 'internal error' });
      }
    }
  }

  // ---------------------------------------------------------------- HTTP 层

  const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css' };

  function readBody(req, limit = 4096) {
    return new Promise((resolve, reject) => {
      let size = 0;
      const chunks = [];
      req.on('data', (c) => {
        size += c.length;
        if (size > limit) {
          reject(new Error('body too large'));
          req.destroy();
          return;
        }
        chunks.push(c);
      });
      req.on('end', () => resolve(Buffer.concat(chunks).toString()));
      req.on('error', reject);
    });
  }

  const httpServer = http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://localhost');
    const json = (code, obj) => {
      res.writeHead(code, { 'content-type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(obj));
    };

    if (req.method === 'POST' && url.pathname === '/api/login') {
      // 演示级登录：按用户名创建/复用账号，返回签名 token
      try {
        const body = JSON.parse(await readBody(req));
        if (!isNonEmptyString(body.name, 32)) return json(400, { error: 'invalid name' });
        let user = db.getUserByName(body.name);
        if (!user) user = db.createUser(randomId('u_'), body.name, randomSecret());
        const token = signToken(user.id, user.token_random, config.authSecret);
        return json(200, { userId: user.id, name: user.name, token });
      } catch {
        return json(400, { error: 'bad request' });
      }
    }

    if (req.method === 'GET' && url.pathname === '/healthz') {
      return json(200, { ok: true, ...hub.stats() });
    }

    if (req.method === 'GET') {
      const rel = url.pathname === '/' ? 'index.html' : url.pathname.slice(1);
      const file = path.resolve(publicDir, rel);
      if (!file.startsWith(publicDir) || !MIME[path.extname(file)]) {
        res.writeHead(404).end('not found');
        return;
      }
      fs.readFile(file, (err, data) => {
        if (err) {
          res.writeHead(404).end('not found');
          return;
        }
        res.writeHead(200, { 'content-type': MIME[path.extname(file)] });
        res.end(data);
      });
      return;
    }

    res.writeHead(404).end('not found');
  });

  // ---------------------------------------------------------------- WS 层

  const wss = new WebSocketServer({ noServer: true });

  httpServer.on('upgrade', (req, socket, head) => {
    const reject = (code, text) => {
      socket.write(`HTTP/1.1 ${code} ${text}\r\nConnection: close\r\n\r\n`);
      socket.destroy();
    };
    const url = new URL(req.url, 'http://localhost');
    if (url.pathname !== '/ws') return reject(404, 'Not Found');

    const userId = verifyToken(url.searchParams.get('token'), config.authSecret);
    const user = userId && db.getUserById(userId);
    if (!user) return reject(401, 'Unauthorized');

    const denied = hub.checkAdmission(user.id);
    if (denied) return reject(503, denied);

    wss.handleUpgrade(req, socket, head, (ws) => {
      const conn = new Connection(ws, user);
      hub.add(conn);

      ws.on('pong', () => {
        conn.lastPong = now();
      });
      ws.on('message', (raw) => onFrame(conn, raw));
      ws.on('close', () => hub.remove(conn));
      ws.on('error', () => {}); // 错误后必随 close，统一在 close 清理

      hub.send(conn, { type: 'welcome', userId: user.id, name: user.name, serverTime: now() });
    });
  });

  // ---------------------------------------------------------------- 定时任务

  /** 过期邀请收口：active -> revoked，并实时通知被邀请人刷新/收起邀请 */
  function sweepInvites() {
    let expired;
    try {
      expired = db.sweepExpiredInvites();
    } catch (err) {
      console.error('[invite sweep error]', err);
      return;
    }
    for (const inv of expired) {
      hub.sendToUser(inv.invitee_id, {
        type: 'invite_closed', inviteId: inv.id, roomId: inv.room_id, reason: 'expired',
      });
    }
  }

  const timers = [
    setInterval(() => hub.heartbeatSweep(), config.heartbeatIntervalMs),
    setInterval(() => hub.resendSweep(), config.ackResendIntervalMs),
    setInterval(sweepInvites, config.inviteSweepIntervalMs),
  ];
  for (const t of timers) t.unref();

  // ---------------------------------------------------------------- 生命周期

  function start() {
    return new Promise((resolve) => {
      httpServer.listen(config.port, config.host, () => {
        const addr = httpServer.address();
        console.log(`[chat] listening on http://${addr.address}:${addr.port}  (db: ${config.dbPath})`);
        resolve(addr);
      });
    });
  }

  function stop() {
    for (const t of timers) clearInterval(t);
    for (const conn of [...hub.all]) {
      hub.send(conn, { type: 'server_shutdown' });
      conn.ws.terminate();
    }
    wss.close();
    httpServer.close();
    db.close();
  }

  return { config, db, hub, httpServer, wss, start, stop };
}

// 直接运行：node src/server.js
if (require.main === module) {
  const server = createChatServer();
  server.start();
  const shutdown = () => {
    console.log('\n[chat] shutting down...');
    server.stop();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

module.exports = { createChatServer };
