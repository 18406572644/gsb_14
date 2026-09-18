'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const WebSocket = require('ws');
const { createChatServer } = require('../src/server');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** 启动一个隔离的测试服务器（内存库、随机端口、默认关闭重发以免干扰计数） */
async function startServer(overrides = {}) {
  const server = createChatServer({
    port: 0,
    host: '127.0.0.1',
    dbPath: ':memory:',
    heartbeatIntervalMs: 60_000,
    ackResendAfterMs: 60_000, // 默认不在测试内重发；重发场景单独配置
    ...overrides,
  });
  const addr = await server.start();
  return { server, port: addr.port };
}

async function login(port, name) {
  const res = await fetch(`http://127.0.0.1:${port}/api/login`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', connection: 'close' },
    body: JSON.stringify({ name }),
  });
  assert.equal(res.status, 200);
  return res.json();
}

/** 测试客户端：手动 ACK（测试可控）。log 全量记录供断言；waitFor 消费式匹配（每帧至多满足一个等待者） */
class Client {
  static async connect(port, token) {
    const c = new Client();
    c.log = []; // 全部帧（断言用）
    c.pending = []; // 未被 waitFor 消费的帧
    c.waiters = [];
    c.closed = new Promise((res) => (c._onClosed = res));
    c.ws = new WebSocket(`ws://127.0.0.1:${port}/ws?token=${encodeURIComponent(token)}`);
    c.ws.on('message', (raw) => {
      const m = JSON.parse(raw.toString());
      c.log.push(m);
      for (const w of [...c.waiters]) {
        if (w.pred(m)) {
          c.waiters.splice(c.waiters.indexOf(w), 1);
          clearTimeout(w.timer);
          w.resolve(m);
          return;
        }
      }
      c.pending.push(m);
    });
    c.ws.on('close', () => c._onClosed());
    await new Promise((res, rej) => {
      c.ws.once('open', res);
      c.ws.once('error', rej);
    });
    await c.waitFor((m) => m.type === 'welcome');
    return c;
  }

  send(obj) {
    this.ws.send(JSON.stringify(obj));
  }

  waitFor(pred, timeout = 3000) {
    const idx = this.pending.findIndex(pred);
    if (idx >= 0) {
      const [m] = this.pending.splice(idx, 1);
      return Promise.resolve(m);
    }
    return new Promise((resolve, reject) => {
      const w = { pred, resolve };
      w.timer = setTimeout(() => {
        reject(new Error('waitFor: timed out'));
      }, timeout);
      this.waiters.push(w);
    });
  }

  /** 已收到的某房间消息帧（seq 列表） */
  roomSeqs(roomId) {
    return this.log.filter((m) => m.type === 'msg' && m.roomId === roomId).map((m) => m.seq);
  }

  close() {
    this.ws.close();
    return this.closed;
  }
}

async function createRoom(client, name) {
  client.send({ type: 'create_room', name });
  const joined = await client.waitFor((m) => m.type === 'joined' && m.name === name);
  return joined.roomId;
}

async function joinRoom(client, room, lastSeq = 0) {
  client.send({ type: 'join', room, lastSeq });
  return client.waitFor((m) => m.type === 'joined');
}

async function createRoomCap(client, name, maxOccupancy) {
  client.send({ type: 'create_room', name, maxOccupancy });
  const joined = await client.waitFor((m) => m.type === 'joined' && m.name === name);
  return joined.roomId;
}

/** 管理员发起邀请，返回 invite_created 帧中的 invite（被邀请人应另行等待 invite_received） */
async function createInvite(admin, roomId, targetName, ttlMinutes) {
  admin.send({ type: 'invite_create', roomId, targetName, ...(ttlMinutes ? { ttlMinutes } : {}) });
  const fr = await admin.waitFor((m) => m.type === 'invite_created' && m.invite.roomId === roomId);
  return fr.invite;
}

const acceptInvite = (client, inviteId, lastSeq = 0) =>
  client.send({ type: 'invite_accept', inviteId, lastSeq });

/** 直接把某邀请改成已过期（协议最短 TTL 为 1 分钟，测试用 DB 注入避免等待） */
function expireInviteInDb(server, inviteId) {
  server.db.db.prepare('UPDATE invites SET expires_at = ? WHERE id = ?').run(Date.now() - 1, inviteId);
}

// ---------------------------------------------------------------- 测试用例

test('登录、连接、建房后成为管理员', async () => {
  const { server, port } = await startServer();
  try {
    const u = await login(port, 'alice');
    assert.ok(u.userId && u.token);
    const a = await Client.connect(port, u.token);
    const roomId = await createRoom(a, 'general');
    const joined = a.log.find((m) => m.type === 'joined');
    assert.equal(joined.role, 'admin');
    assert.ok(roomId);
    await a.close();
  } finally {
    server.stop();
  }
});

test('发送收到 ACK，房间内广播按 seq 全序投递', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    for (let i = 1; i <= 3; i++) {
      a.send({ type: 'msg', roomId, clientMsgId: `m${i}`, content: `hello ${i}` });
    }
    // 发送者收到 3 个 ACK，seq 递增
    for (let i = 1; i <= 3; i++) {
      const ack = await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === `m${i}`);
      assert.equal(ack.seq, i);
    }
    // 接收者按序收到 1,2,3
    await b.waitFor((m) => m.type === 'msg' && m.roomId === roomId && m.seq === 3);
    assert.deepEqual(b.roomSeqs(roomId), [1, 2, 3]);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('重复 clientMsgId 幂等：返回同一 seq，不重复广播', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'dup-1', content: 'hello' });
    const ack1 = await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'dup-1');
    // 网络重试：同 clientMsgId 重发
    a.send({ type: 'msg', roomId, clientMsgId: 'dup-1', content: 'hello' });
    const ack2 = await a.waitFor(
      (m) => m.type === 'ack' && m.clientMsgId === 'dup-1' && m !== ack1
    );
    assert.equal(ack1.seq, ack2.seq);

    await b.waitFor((m) => m.type === 'msg' && m.roomId === roomId);
    await sleep(300);
    assert.deepEqual(b.roomSeqs(roomId), [1], '接收端只应收到一次广播');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('断线补发：重连后按序补齐离线期间的消息，且不重复', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    let b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'online' });
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1);
    await b.close(); // —— B 掉线 ——

    for (const [i, c] of [2, 3, 4].entries()) {
      a.send({ type: 'msg', roomId, clientMsgId: `m${i + 2}`, content: `offline ${c}` });
    }
    await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'm4');

    // —— B 重连，携带本地进度 lastSeq=1 ——
    b = await Client.connect(port, ub.token);
    await joinRoom(b, roomId, 1);
    await b.waitFor((m) => m.type === 'sync_done' && m.roomId === roomId);
    assert.deepEqual(b.roomSeqs(roomId), [2, 3, 4], '补发且仅补发缺口，按序到达');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('已追平的连接重连后不再收到旧消息', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    let b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'hi' });
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1);
    await b.close();

    b = await Client.connect(port, ub.token);
    await joinRoom(b, roomId, 1); // 已追平
    await sleep(300);
    assert.deepEqual(b.roomSeqs(roomId), [], '不应有任何补发');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('服务端对未 ACK 消息重发，ACK 后停止', async () => {
  const { server, port } = await startServer({
    ackResendIntervalMs: 50,
    ackResendAfterMs: 100,
    ackMaxResend: 10,
  });
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'hi' });
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1);
    // 不 ACK，等服务端重发
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1, 2000);
    assert.ok(b.roomSeqs(roomId).length >= 2, '应观察到至少一次重发');

    b.send({ type: 'ack', roomId, seq: 1 });
    await sleep(100);
    const countAfterAck = b.roomSeqs(roomId).length;
    await sleep(400);
    assert.equal(b.roomSeqs(roomId).length, countAfterAck, 'ACK 后不应再有重发');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('禁言：管理员可禁言/解禁，被禁言者发送被拒', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'mute', roomId, userId: ub.userId, minutes: 10 });
    await b.waitFor((m) => m.type === 'notice' && m.event === 'muted' && m.userId === ub.userId);

    b.send({ type: 'msg', roomId, clientMsgId: 'x1', content: 'am i muted?' });
    const err = await b.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'MUTED');

    a.send({ type: 'unmute', roomId, userId: ub.userId });
    await b.waitFor((m) => m.type === 'notice' && m.event === 'unmuted');

    b.send({ type: 'msg', roomId, clientMsgId: 'x2', content: 'free again' });
    const ack = await b.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'x2');
    assert.equal(ack.seq, 1);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('权限：普通成员不能禁言他人，管理员不可被禁言', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const uc = await login(port, 'carol');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const c = await Client.connect(port, uc.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);
    await joinRoom(c, roomId);

    b.send({ type: 'mute', roomId, userId: uc.userId, minutes: 5 });
    const err1 = await b.waitFor((m) => m.type === 'error');
    assert.equal(err1.code, 'FORBIDDEN');

    a.send({ type: 'mute', roomId, userId: ua.userId, minutes: 5 });
    const err2 = await a.waitFor((m) => m.type === 'error');
    assert.equal(err2.code, 'FORBIDDEN');
    await Promise.all([a.close(), b.close(), c.close()]);
  } finally {
    server.stop();
  }
});

test('连接数限制：单用户连接数超限被拒绝', async () => {
  const { server, port } = await startServer({ maxConnectionsPerUser: 2 });
  try {
    const u = await login(port, 'alice');
    const c1 = await Client.connect(port, u.token);
    const c2 = await Client.connect(port, u.token);
    await assert.rejects(
      Client.connect(port, u.token),
      /503|TOO_MANY_DEVICES|Unexpected server response/
    );
    await c1.close();
    await c2.close();
  } finally {
    server.stop();
  }
});

test('发送限流：突发超过令牌桶被拒绝', async () => {
  const { server, port } = await startServer({ rateLimitPerSec: 1, rateLimitBurst: 2 });
  try {
    const u = await login(port, 'alice');
    const a = await Client.connect(port, u.token);
    const roomId = await createRoom(a, 'general');

    for (let i = 0; i < 5; i++) {
      a.send({ type: 'msg', roomId, clientMsgId: `r${i}`, content: `spam ${i}` });
    }
    const err = await a.waitFor((m) => m.type === 'error' && m.code === 'RATE_LIMITED');
    assert.ok(err);
    await sleep(300);
    const ackCount = a.log.filter((m) => m.type === 'ack').length;
    assert.equal(ackCount, 2, '突发容量为 2，其余应被限流');
    await a.close();
  } finally {
    server.stop();
  }
});

test('历史消息分页拉取', async () => {
  const { server, port } = await startServer();
  try {
    const u = await login(port, 'alice');
    const a = await Client.connect(port, u.token);
    const roomId = await createRoom(a, 'general');
    for (let i = 1; i <= 5; i++) {
      a.send({ type: 'msg', roomId, clientMsgId: `h${i}`, content: `msg ${i}` });
    }
    await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'h5');

    a.send({ type: 'history', roomId, beforeSeq: 4, limit: 2 });
    const h = await a.waitFor((m) => m.type === 'history');
    assert.deepEqual(h.messages.map((m) => m.seq), [2, 3], '升序返回 beforeSeq 之前的一页');
    assert.equal(h.hasMore, true);
    await a.close();
  } finally {
    server.stop();
  }
});

test('服务端游标兜底：新设备不带 lastSeq 时从已确认进度继续', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    let b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'first' });
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1);
    b.send({ type: 'ack', roomId, seq: 1 }); // 上报确认进度
    await sleep(100);
    await b.close();

    a.send({ type: 'msg', roomId, clientMsgId: 'm2', content: 'second' });
    await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'm2');

    // 新设备重连，不带 lastSeq —— 应使用服务端游标，只补 seq 2
    b = await Client.connect(port, ub.token);
    b.send({ type: 'join', room: roomId });
    await b.waitFor((m) => m.type === 'sync_done' && m.roomId === roomId);
    assert.deepEqual(b.roomSeqs(roomId), [2]);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('持久化：服务重启后消息不丢失', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'chat-test-'));
  const dbPath = path.join(dir, 'test.db');
  try {
    let token, roomId;
    {
      const { server, port } = await startServer({ dbPath });
      const u = await login(port, 'alice');
      token = u.token;
      const a = await Client.connect(port, token);
      roomId = await createRoom(a, 'persist');
      for (let i = 1; i <= 3; i++) {
        a.send({ type: 'msg', roomId, clientMsgId: `p${i}`, content: `durable ${i}` });
      }
      await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'p3');
      await a.close();
      server.stop();
    }
    {
      const { server, port } = await startServer({ dbPath });
      const a = await Client.connect(port, token); // 同一 token 仍有效
      await joinRoom(a, roomId, 0);
      await a.waitFor((m) => m.type === 'sync_done' && m.roomId === roomId);
      assert.deepEqual(a.roomSeqs(roomId), [1, 2, 3], '重启后历史消息完整可补发');
      await a.close();
      server.stop();
    }
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------- 房间邀请

test('邀请全流程：管理员发起 → 被邀请人收到并接受 → 持久成为成员并广播', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'g1');

    const inv = await createInvite(a, roomId, 'bob', 60);
    assert.ok(inv.id && inv.roomId === roomId);
    assert.equal(inv.inviteeName, 'bob');
    assert.ok(inv.expiresAt > Date.now());

    // 被邀请人实时收到推送
    const recv = await b.waitFor((m) => m.type === 'invite_received' && m.invite.id === inv.id);
    assert.equal(recv.invite.roomName, 'g1');
    assert.equal(recv.invite.inviterName, 'alice');

    // invite_list 只返回我的 active 邀请
    b.send({ type: 'invite_list' });
    const list = await b.waitFor((m) => m.type === 'invites' && m.scope === 'mine');
    assert.equal(list.invites.length, 1);
    assert.equal(list.invites[0].id, inv.id);

    // 管理员视角：该房间全部邀请（含被邀请人名）
    a.send({ type: 'invite_list', roomId });
    const rlist = await a.waitFor((m) => m.type === 'invites' && m.scope === 'room');
    assert.equal(rlist.invites.length, 1);
    assert.equal(rlist.invites[0].inviteeName, 'bob');
    assert.equal(rlist.invites[0].status, 'active');

    acceptInvite(b, inv.id);
    const joined = await b.waitFor((m) => m.type === 'joined' && m.roomId === roomId);
    assert.equal(joined.role, 'member');

    // 房间内既有成员（管理员）收到 member_joined
    const mj = await a.waitFor((m) => m.type === 'member_joined' && m.roomId === roomId);
    assert.equal(mj.member.userId, ub.userId);
    assert.equal(mj.member.role, 'member');

    // 邀请被收口为 used，从我的在途邀请中消失
    await b.waitFor((m) => m.type === 'invite_closed' && m.inviteId === inv.id && m.reason === 'used');
    b.send({ type: 'invite_list' });
    const list2 = await b.waitFor((m) => m.type === 'invites' && m.scope === 'mine');
    assert.equal(list2.invites.length, 0);
    assert.equal(server.db.getInvite(inv.id).status, 'used');

    // 成员身份已持久落库（而非仅当前连接临时加入）
    assert.ok(server.db.getMember(roomId, ub.userId), 'members 表应有持久记录');
    // 成员列表同步：管理员视角可见 bob
    a.send({ type: 'members', roomId });
    const mem = await a.waitFor((m) => m.type === 'members' && m.roomId === roomId);
    assert.ok(mem.members.some((x) => x.userId === ub.userId));

    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('邀请成员身份持久生效：断线重连与服务重启后仍是成员', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'chat-inv-'));
  const dbPath = path.join(dir, 'inv.db');
  try {
    const { server: s1, port: p1 } = await startServer({ dbPath });
    const ua = await login(p1, 'alice');
    const ub = await login(p1, 'bob');
    const a = await Client.connect(p1, ua.token);
    const roomId = await createRoom(a, 'g2');
    const inv = await createInvite(a, roomId, 'bob', 60);
    await a.close();
    s1.stop();

    // —— 重启后接受邀请 ——
    const { server: s2, port: p2 } = await startServer({ dbPath });
    const b = await Client.connect(p2, ub.token);
    acceptInvite(b, inv.id);
    await b.waitFor((m) => m.type === 'joined' && m.roomId === roomId);
    assert.ok(s2.db.getMember(roomId, ub.userId), '接受后成员关系落库');
    await b.close();
    s2.stop();

    // —— 再次重启，新连接通过 rooms 查询确认成员身份仍在 ——
    const { server: s3, port: p3 } = await startServer({ dbPath });
    const b2 = await Client.connect(p3, ub.token);
    b2.send({ type: 'rooms' });
    const roomsFr = await b2.waitFor((m) => m.type === 'rooms');
    assert.ok(roomsFr.rooms.some((r) => r.id === roomId), '重启后仍为该房间成员');
    await b2.close();
    s3.stop();
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('权限：非管理员不能发起邀请，非目标用户不能接受邀请', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const uc = await login(port, 'carol');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const c = await Client.connect(port, uc.token);
    const roomId = await createRoom(a, 'g3');
    await joinRoom(b, roomId);

    // 普通成员发起邀请 → FORBIDDEN
    b.send({ type: 'invite_create', roomId, targetName: 'carol' });
    const e1 = await b.waitFor((m) => m.type === 'error' && m.code === 'FORBIDDEN');
    assert.ok(e1);

    const inv = await createInvite(a, roomId, 'carol', 60);
    await c.waitFor((m) => m.type === 'invite_received' && m.invite.id === inv.id);

    // bob 不是被邀请人，接受 → FORBIDDEN
    b.send({ type: 'invite_accept', inviteId: inv.id });
    const e2 = await b.waitFor((m) => m.type === 'error' && m.code === 'FORBIDDEN');
    assert.ok(e2);
    assert.equal(server.db.getInvite(inv.id).status, 'active', '冒用接受不应消费邀请');

    await Promise.all([a.close(), b.close(), c.close()]);
  } finally {
    server.stop();
  }
});

test('发起邀请校验：目标不存在、已是成员、重复在途邀请、TTL 非法', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    await login(port, 'carol'); // 仅注册账号、暂不连接
    await login(port, 'dave');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'g4');
    await joinRoom(b, roomId);

    a.send({ type: 'invite_create', roomId, targetName: 'ghost' });
    assert.equal((await a.waitFor((m) => m.type === 'error')).code, 'NO_SUCH_USER');

    a.send({ type: 'invite_create', roomId, targetName: 'bob' });
    assert.equal((await a.waitFor((m) => m.type === 'error')).code, 'ALREADY_MEMBER');

    const inv = await createInvite(a, roomId, 'carol', 60);
    // 再次对同一用户发起在途邀请 → INVITE_EXISTS
    a.send({ type: 'invite_create', roomId, targetName: 'carol' });
    assert.equal((await a.waitFor((m) => m.type === 'error')).code, 'INVITE_EXISTS');

    a.send({ type: 'invite_create', roomId, targetName: 'dave', ttlMinutes: 0 });
    assert.equal((await a.waitFor((m) => m.type === 'error')).code, 'BAD_REQUEST');
    a.send({ type: 'invite_create', roomId, targetName: 'dave', ttlMinutes: 999999 });
    assert.equal((await a.waitFor((m) => m.type === 'error')).code, 'BAD_REQUEST');

    assert.ok(server.db.getInvite(inv.id));
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('过期邀请无法接受，过期后可重新发起', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'g5');
    const inv = await createInvite(a, roomId, 'bob', 60);
    await b.waitFor((m) => m.type === 'invite_received');

    expireInviteInDb(server, inv.id); // 直接置为已过期
    acceptInvite(b, inv.id);
    const err = await b.waitFor((m) => m.type === 'error' && m.code === 'INVITE_EXPIRED');
    assert.ok(err);
    assert.ok(!server.db.getMember(roomId, ub.userId), '过期接受不得加入');
    assert.equal(server.db.getInvite(inv.id).status, 'active', '过期未接受仍为 active，待 sweep 收口');

    // 过期后管理员可对同一用户重新发起
    const inv2 = await createInvite(a, roomId, 'bob', 60);
    assert.ok(inv2.id && inv2.id !== inv.id);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('撤销邀请：被邀请人接受被拒并收到关闭通知；已撤销不可再撤销', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'g6');
    const inv = await createInvite(a, roomId, 'bob', 60);
    await b.waitFor((m) => m.type === 'invite_received');

    a.send({ type: 'invite_revoke', inviteId: inv.id });
    await a.waitFor((m) => m.type === 'invite_revoked' && m.inviteId === inv.id);
    await b.waitFor((m) => m.type === 'invite_closed' && m.inviteId === inv.id && m.reason === 'revoked');
    assert.equal(server.db.getInvite(inv.id).status, 'revoked');

    acceptInvite(b, inv.id);
    assert.equal((await b.waitFor((m) => m.type === 'error')).code, 'INVITE_REVOKED');

    a.send({ type: 'invite_revoke', inviteId: inv.id });
    assert.equal((await a.waitFor((m) => m.type === 'error')).code, 'INVITE_UNAVAILABLE');

    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('人数上限：满员房间不能发起邀请；发起后被占满则接受时拒绝', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const uc = await login(port, 'carol');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const c = await Client.connect(port, uc.token);

    // 上限 1：仅管理员即满员，发起邀请直接被拒
    const fullRoom = await createRoomCap(a, 'cap1', 1);
    a.send({ type: 'invite_create', roomId: fullRoom, targetName: 'bob' });
    assert.equal((await a.waitFor((m) => m.type === 'error')).code, 'ROOM_FULL');

    // 上限 2：发起时有空位（alice 1 人）；之后 carol 直接加入占满，bob 接受时被拒
    const roomId = await createRoomCap(a, 'cap2', 2);
    const inv = await createInvite(a, roomId, 'bob', 60);
    await joinRoom(c, roomId); // alice + carol = 2，满员
    acceptInvite(b, inv.id);
    const err = await b.waitFor((m) => m.type === 'error' && m.code === 'ROOM_FULL');
    assert.ok(err);
    assert.equal(server.db.getInvite(inv.id).status, 'active', '满员拒绝不应消费邀请');
    assert.ok(!server.db.getMember(roomId, ub.userId));

    // carol 离开房间运行时不影响持久成员计数；这里直接校验计数为 2
    assert.equal(server.db.countMembers(roomId), 2);

    await Promise.all([a.close(), b.close(), c.close()]);
  } finally {
    server.stop();
  }
});

test('多设备：一台设备接受邀请，同一用户其他在线设备同步入房', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b1 = await Client.connect(port, ub.token);
    const b2 = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'g7');
    const inv = await createInvite(a, roomId, 'bob', 60);
    await b1.waitFor((m) => m.type === 'invite_received');
    await b2.waitFor((m) => m.type === 'invite_received');

    acceptInvite(b1, inv.id);
    await b1.waitFor((m) => m.type === 'joined' && m.roomId === roomId);
    // 另一台设备被动收到 joined
    await b2.waitFor((m) => m.type === 'joined' && m.roomId === roomId);

    assert.ok(server.db.getMember(roomId, ub.userId));
    await Promise.all([a.close(), b1.close(), b2.close()]);
  } finally {
    server.stop();
  }
});

test('过期扫描：sweep 收口过期邀请并通知被邀请人', async () => {
  const { server, port } = await startServer({ inviteSweepIntervalMs: 40 });
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'g8');
    const inv = await createInvite(a, roomId, 'bob', 60);
    await b.waitFor((m) => m.type === 'invite_received');

    expireInviteInDb(server, inv.id);
    await b.waitFor(
      (m) => m.type === 'invite_closed' && m.inviteId === inv.id && m.reason === 'expired',
      2000
    );
    assert.equal(server.db.getInvite(inv.id).status, 'revoked', 'sweep 应置为 revoked');

    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('create_room 的 maxOccupancy 非法被拒，默认带人数上限', async () => {
  const { server, port } = await startServer();
  try {
    const u = await login(port, 'alice');
    const a = await Client.connect(port, u.token);
    a.send({ type: 'create_room', name: 'badcap', maxOccupancy: -3 });
    assert.equal((await a.waitFor((m) => m.type === 'error')).code, 'BAD_REQUEST');

    a.send({ type: 'create_room', name: 'okcap' });
    const j = await a.waitFor((m) => m.type === 'joined' && m.name === 'okcap');
    assert.equal(j.maxOccupancy, 50, '默认人数上限 50');
    await a.close();
  } finally {
    server.stop();
  }
});
