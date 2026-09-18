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

async function createInvite(admin, roomId, userName, minutes) {
  admin.send({ type: 'invite_create', roomId, userName, ...(minutes != null ? { minutes } : {}) });
  return (await admin.waitFor((m) => m.type === 'invite_created')).invite;
}

/** 强制把邀请改成已到期（测试用，绕过真实等待） */
function forceExpireInvite(server, inviteId) {
  server.db.db.prepare('UPDATE invites SET expires_at = 1 WHERE id = ?').run(inviteId);
}

// ---------------------------------------------------------------- 邀请测试用例

test('邀请全流程：管理员生成 → 被邀请人实时收到 → 接受后持久入组并同步列表', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const uc = await login(port, 'carol');
    const a = await Client.connect(port, ua.token);
    const c = await Client.connect(port, uc.token);
    const roomId = await createRoom(a, 'team');

    const inv = await createInvite(a, roomId, 'carol');
    assert.equal(inv.status, 'pending');
    assert.equal(inv.roomId, roomId);
    assert.equal(inv.inviteeName, 'carol');
    assert.ok(inv.expiresAt > Date.now(), '应带有效期');

    // 被邀请人实时收到推送
    const pushed = await c.waitFor((m) => m.type === 'invite');
    assert.equal(pushed.invite.id, inv.id);

    // 接受
    c.send({ type: 'invite_accept', inviteId: inv.id });
    const joined = await c.waitFor((m) => m.type === 'joined' && m.roomId === roomId);
    assert.equal(joined.role, 'member');
    const myResult = await c.waitFor(
      (m) => m.type === 'invite_result' && m.status === 'accepted' && m.inviteId === inv.id
    );
    assert.equal(myResult.roomId, roomId);

    // 邀请人收到接受回执，房间收到成员加入通知
    await a.waitFor((m) => m.type === 'invite_result' && m.status === 'accepted' && m.userId === uc.userId);
    await a.waitFor((m) => m.type === 'notice' && m.event === 'member_joined' && m.userId === uc.userId);

    // 房间成员列表同步
    a.send({ type: 'members', roomId });
    const membersResp = await a.waitFor((m) => m.type === 'members' && m.roomId === roomId);
    assert.ok(membersResp.members.some((m) => m.userId === uc.userId));

    // 成员关系落库（持久生效，非当前连接临时挂接）
    const row = server.db.getMember(roomId, uc.userId);
    assert.ok(row);
    assert.equal(row.role, 'member');
    assert.equal(server.db.getInvite(inv.id).status, 'accepted');

    // 断线重连后成员身份仍在
    await c.close();
    const c2 = await Client.connect(port, uc.token);
    c2.send({ type: 'join', room: roomId, lastSeq: 0 });
    await c2.waitFor((m) => m.type === 'joined' && m.roomId === roomId);
    await a.close();
    await c2.close();
  } finally {
    server.stop();
  }
});

test('权限：非管理员不能生成邀请，非被邀请人不能接受，非管理员不能撤销', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const uc = await login(port, 'carol');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const c = await Client.connect(port, uc.token);
    const roomId = await createRoom(a, 'team');
    await joinRoom(b, roomId);

    // 普通成员生成邀请 → FORBIDDEN
    b.send({ type: 'invite_create', roomId, userName: 'carol' });
    const err1 = await b.waitFor((m) => m.type === 'error' && m.action === 'invite_create');
    assert.equal(err1.code, 'FORBIDDEN');

    const inv = await createInvite(a, roomId, 'carol');
    await c.waitFor((m) => m.type === 'invite');

    // 其他人不能接受发给 carol 的邀请
    b.send({ type: 'invite_accept', inviteId: inv.id });
    const err2 = await b.waitFor((m) => m.type === 'error' && m.action === 'invite_accept');
    assert.equal(err2.code, 'FORBIDDEN');

    // 普通成员不能撤销
    b.send({ type: 'invite_revoke', inviteId: inv.id });
    const err3 = await b.waitFor((m) => m.type === 'error' && m.action === 'invite_revoke');
    assert.equal(err3.code, 'FORBIDDEN');

    // 真正的被邀请人此时仍可接受
    c.send({ type: 'invite_accept', inviteId: inv.id });
    await c.waitFor((m) => m.type === 'joined' && m.roomId === roomId);
    await Promise.all([a.close(), b.close(), c.close()]);
  } finally {
    server.stop();
  }
});

test('生成邀请校验：目标用户不存在、已是成员、邀请自己', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'team');
    await joinRoom(b, roomId);

    a.send({ type: 'invite_create', roomId, userName: 'ghost' });
    const e1 = await a.waitFor((m) => m.type === 'error');
    assert.equal(e1.code, 'NO_SUCH_USER');

    a.send({ type: 'invite_create', roomId, userName: 'bob' });
    const e2 = await a.waitFor((m) => m.type === 'error');
    assert.equal(e2.code, 'ALREADY_MEMBER');

    a.send({ type: 'invite_create', roomId, userName: 'alice' });
    const e3 = await a.waitFor((m) => m.type === 'error');
    assert.equal(e3.code, 'BAD_REQUEST');

    a.send({ type: 'invite_create', roomId, userName: 'carol', minutes: 0 });
    const e4 = await a.waitFor((m) => m.type === 'error');
    assert.equal(e4.code, 'BAD_REQUEST');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('过期的邀请无法接受', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const uc = await login(port, 'carol');
    const a = await Client.connect(port, ua.token);
    const c = await Client.connect(port, uc.token);
    const roomId = await createRoom(a, 'team');

    const inv = await createInvite(a, roomId, 'carol', 60);
    await c.waitFor((m) => m.type === 'invite');
    forceExpireInvite(server, inv.id);

    c.send({ type: 'invite_accept', inviteId: inv.id });
    const err = await c.waitFor((m) => m.type === 'error' && m.action === 'invite_accept');
    assert.equal(err.code, 'INVITE_EXPIRED');
    assert.equal(server.db.getInvite(inv.id).status, 'expired');

    // 被邀请人未入组
    assert.equal(server.db.getMember(roomId, uc.userId), undefined);
    await a.close();
    await c.close();
  } finally {
    server.stop();
  }
});

test('撤销的邀请无法接受，双方收到撤销回执', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const uc = await login(port, 'carol');
    const a = await Client.connect(port, ua.token);
    const c = await Client.connect(port, uc.token);
    const roomId = await createRoom(a, 'team');

    const inv = await createInvite(a, roomId, 'carol');
    await c.waitFor((m) => m.type === 'invite');

    a.send({ type: 'invite_revoke', inviteId: inv.id });
    await a.waitFor((m) => m.type === 'invite_result' && m.status === 'revoked');
    await c.waitFor((m) => m.type === 'invite_result' && m.status === 'revoked');

    c.send({ type: 'invite_accept', inviteId: inv.id });
    const err = await c.waitFor((m) => m.type === 'error' && m.action === 'invite_accept');
    assert.equal(err.code, 'INVITE_NOT_OPEN');
    assert.equal(server.db.getInvite(inv.id).status, 'revoked');
    await a.close();
    await c.close();
  } finally {
    server.stop();
  }
});

test('拒绝邀请后邀请关闭，不能再接受', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const uc = await login(port, 'carol');
    const a = await Client.connect(port, ua.token);
    const c = await Client.connect(port, uc.token);
    const roomId = await createRoom(a, 'team');

    const inv = await createInvite(a, roomId, 'carol');
    await c.waitFor((m) => m.type === 'invite');

    c.send({ type: 'invite_decline', inviteId: inv.id });
    await c.waitFor((m) => m.type === 'invite_result' && m.status === 'declined');
    await a.waitFor((m) => m.type === 'invite_result' && m.status === 'declined' && m.userId === uc.userId);

    c.send({ type: 'invite_accept', inviteId: inv.id });
    const err = await c.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'INVITE_NOT_OPEN');
    assert.equal(server.db.getInvite(inv.id).status, 'declined');

    a.send({ type: 'invites', roomId });
    const list = await a.waitFor((m) => m.type === 'invites' && m.roomId === roomId);
    assert.equal(list.invites[0].status, 'declined');
    await a.close();
    await c.close();
  } finally {
    server.stop();
  }
});

test('人数上限：满员时不能生成邀请；生成后满员则接受被拒', async () => {
  const { server, port } = await startServer({ maxRoomMembers: 3 });
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const uc = await login(port, 'carol');
    const ud = await login(port, 'dave');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const c = await Client.connect(port, uc.token);
    const d = await Client.connect(port, ud.token);
    const roomId = await createRoom(a, 'team');
    await joinRoom(b, roomId); // 2/3

    const inv = await createInvite(a, roomId, 'carol');
    await c.waitFor((m) => m.type === 'invite');

    // 第三人直接加入把房间填满到 3/3
    await joinRoom(d, roomId);

    // 此时接受邀请：服务端再次校验人数上限
    c.send({ type: 'invite_accept', inviteId: inv.id });
    const err = await c.waitFor((m) => m.type === 'error' && m.action === 'invite_accept');
    assert.equal(err.code, 'ROOM_FULL');
    assert.equal(server.db.getMember(roomId, uc.userId), undefined, '被拒后不应入组');
    // 邀请仍待处理，腾出名额后还能用
    assert.equal(server.db.getInvite(inv.id).status, 'pending');
    await Promise.all([a.close(), b.close(), c.close(), d.close()]);
  } finally {
    server.stop();
  }
});

test('人数上限：已满员时生成邀请直接被拒，普通加入同样被拒', async () => {
  const { server, port } = await startServer({ maxRoomMembers: 2 });
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const uc = await login(port, 'carol');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const c = await Client.connect(port, uc.token);
    const roomId = await createRoom(a, 'team');
    await joinRoom(b, roomId); // 2/2

    a.send({ type: 'invite_create', roomId, userName: 'carol' });
    const e1 = await a.waitFor((m) => m.type === 'error');
    assert.equal(e1.code, 'ROOM_FULL');

    c.send({ type: 'join', room: roomId, lastSeq: 0 });
    const e2 = await c.waitFor((m) => m.type === 'error');
    assert.equal(e2.code, 'ROOM_FULL');
    await Promise.all([a.close(), b.close(), c.close()]);
  } finally {
    server.stop();
  }
});

test('离线邀请不丢：被邀请人不在线，上线后通过 invite_pending 拉取并接受', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    await login(port, 'carol'); // 仅建号
    const a = await Client.connect(port, ua.token);
    const roomId = await createRoom(a, 'team');

    const inv = await createInvite(a, roomId, 'carol'); // carol 不在线

    const c = await Client.connect(port, (await login(port, 'carol')).token);
    c.send({ type: 'invite_pending' });
    const pending = await c.waitFor((m) => m.type === 'invite_pending');
    assert.equal(pending.invites.length, 1);
    assert.equal(pending.invites[0].id, inv.id);

    c.send({ type: 'invite_accept', inviteId: inv.id });
    await c.waitFor((m) => m.type === 'joined' && m.roomId === roomId);
    assert.ok(server.db.getMember(roomId, pending.invites[0].inviteeId));
    await a.close();
    await c.close();
  } finally {
    server.stop();
  }
});

test('重复生成同一待处理邀请幂等：刷新有效期而非新建', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    await login(port, 'carol');
    const a = await Client.connect(port, ua.token);
    const roomId = await createRoom(a, 'team');

    const inv1 = await createInvite(a, roomId, 'carol', 60);
    const inv2 = await createInvite(a, roomId, 'carol', 120);
    assert.equal(inv1.id, inv2.id, '同房间同用户的待处理邀请应复用');
    assert.ok(inv2.expiresAt >= inv1.expiresAt, '有效期应被刷新');
    assert.equal(server.db.listInvitesForRoom(roomId).length, 1);
    await a.close();
  } finally {
    server.stop();
  }
});

test('接受邀请幂等：已通过其他方式入组后再接受不报错、不重复', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const uc = await login(port, 'carol');
    const a = await Client.connect(port, ua.token);
    const c = await Client.connect(port, uc.token);
    const roomId = await createRoom(a, 'team');

    const inv = await createInvite(a, roomId, 'carol');
    await c.waitFor((m) => m.type === 'invite');

    // 先通过普通加入入组
    await joinRoom(c, roomId);
    // 再接受邀请：成功关闭邀请，成员不重复
    c.send({ type: 'invite_accept', inviteId: inv.id });
    await c.waitFor((m) => m.type === 'invite_result' && m.status === 'accepted');
    assert.equal(server.db.listMembers(roomId).filter((m) => m.userId === uc.userId).length, 1);
    assert.equal(server.db.getInvite(inv.id).status, 'accepted');
    await a.close();
    await c.close();
  } finally {
    server.stop();
  }
});

test('邀请持久化：服务重启后邀请仍可拉取、接受并持久入组', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'chat-inv-'));
  const dbPath = path.join(dir, 'test.db');
  try {
    let roomId, inviteId, aliceToken;
    {
      const { server, port } = await startServer({ dbPath });
      const alice = await login(port, 'alice');
      aliceToken = alice.token;
      await login(port, 'carol'); // carol 建号
      const a = await Client.connect(port, alice.token);
      roomId = await createRoom(a, 'team');
      inviteId = (await createInvite(a, roomId, 'carol', 1440)).id;
      await a.close();
      server.stop();
    }
    {
      const { server, port } = await startServer({ dbPath });
      const a = await Client.connect(port, aliceToken);
      const carolToken = (await login(port, 'carol')).token;
      const c = await Client.connect(port, carolToken);

      c.send({ type: 'invite_pending' });
      const pending = await c.waitFor((m) => m.type === 'invite_pending');
      assert.equal(pending.invites.length, 1);
      assert.equal(pending.invites[0].id, inviteId);

      c.send({ type: 'invite_accept', inviteId });
      await c.waitFor((m) => m.type === 'joined' && m.roomId === roomId);
      assert.ok(server.db.getMember(roomId, pending.invites[0].inviteeId));
      assert.equal(server.db.getInvite(inviteId).status, 'accepted');
      await a.close();
      await c.close();
      server.stop();
    }
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

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
