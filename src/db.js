'use strict';

const { DatabaseSync } = require('node:sqlite');
const { now } = require('./util');

/**
 * 持久层：SQLite（WAL 模式）。
 *
 * 可靠性设计要点：
 * 1. 消息与房间序号 seq 在同一事务中「先落库、后广播」——进程崩溃也不丢已确认消息。
 * 2. messages 上 (room_id, sender_id, client_msg_id) 唯一约束——客户端重试/网络重复
 *    提交同一条消息时不会产生重复记录，实现发送幂等。
 * 3. seq 为每房间单调递增序号，由 rooms.last_seq 计数器在事务内分配——保证房间内
 *    消息全序（时序可控），客户端可凭 seq 检测空洞并触发补发。
 */

const SCHEMA = `
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
PRAGMA synchronous = FULL;

CREATE TABLE IF NOT EXISTS users (
  id           TEXT PRIMARY KEY,
  name         TEXT NOT NULL UNIQUE,
  token_random TEXT NOT NULL,
  created_at   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS rooms (
  id         TEXT PRIMARY KEY,
  name       TEXT NOT NULL UNIQUE,
  created_by TEXT NOT NULL REFERENCES users(id),
  created_at INTEGER NOT NULL,
  last_seq   INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS members (
  room_id     TEXT NOT NULL REFERENCES rooms(id),
  user_id     TEXT NOT NULL REFERENCES users(id),
  role        TEXT NOT NULL DEFAULT 'member' CHECK (role IN ('admin','member')),
  muted_until INTEGER NOT NULL DEFAULT 0,
  joined_at   INTEGER NOT NULL,
  PRIMARY KEY (room_id, user_id)
);

CREATE TABLE IF NOT EXISTS messages (
  room_id       TEXT NOT NULL REFERENCES rooms(id),
  seq           INTEGER NOT NULL,
  client_msg_id TEXT NOT NULL,
  sender_id     TEXT NOT NULL REFERENCES users(id),
  content       TEXT NOT NULL,
  ts            INTEGER NOT NULL,
  PRIMARY KEY (room_id, seq),
  UNIQUE (room_id, sender_id, client_msg_id)  -- 幂等键
);

-- 服务端保存的每用户每房间已确认游标（断线补发的兜底依据）
CREATE TABLE IF NOT EXISTS cursors (
  room_id      TEXT NOT NULL REFERENCES rooms(id),
  user_id      TEXT NOT NULL REFERENCES users(id),
  last_ack_seq INTEGER NOT NULL DEFAULT 0,
  updated_at   INTEGER NOT NULL,
  PRIMARY KEY (room_id, user_id)
);

-- 房间邀请：定向发给指定用户，带有效期与使用状态（持久化，重启不丢）
CREATE TABLE IF NOT EXISTS invites (
  id          TEXT PRIMARY KEY,
  room_id     TEXT NOT NULL REFERENCES rooms(id),
  inviter_id  TEXT NOT NULL REFERENCES users(id),
  invitee_id  TEXT NOT NULL REFERENCES users(id),
  status      TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending','accepted','declined','revoked','expired')),
  expires_at  INTEGER NOT NULL,
  created_at  INTEGER NOT NULL,
  accepted_at INTEGER NOT NULL DEFAULT 0
);

-- 同一房间对同一用户至多存在一条待处理邀请
CREATE UNIQUE INDEX IF NOT EXISTS idx_invites_open
  ON invites (room_id, invitee_id) WHERE status = 'pending';
`;

const MSG_SELECT = `
  SELECT m.room_id AS roomId, m.seq, m.client_msg_id AS clientMsgId,
         m.sender_id AS "from", u.name AS fromName, m.content, m.ts
    FROM messages m JOIN users u ON u.id = m.sender_id
`;

const INVITE_SELECT = `
  SELECT i.id, i.room_id AS roomId, r.name AS roomName,
         i.inviter_id AS inviterId, iu.name AS inviterName,
         i.invitee_id AS inviteeId, ru.name AS inviteeName,
         i.status, i.expires_at AS expiresAt,
         i.created_at AS createdAt, i.accepted_at AS acceptedAt
    FROM invites i
    JOIN rooms r ON r.id = i.room_id
    JOIN users iu ON iu.id = i.inviter_id
    JOIN users ru ON ru.id = i.invitee_id
`;

class ChatDB {
  constructor(dbPath) {
    this.db = new DatabaseSync(dbPath);
    this.db.exec(SCHEMA);
    this._prepare();
  }

  _prepare() {
    const d = this.db;
    this.stmt = {
      insertUser: d.prepare('INSERT INTO users (id, name, token_random, created_at) VALUES (?, ?, ?, ?)'),
      userByName: d.prepare('SELECT * FROM users WHERE name = ?'),
      userById: d.prepare('SELECT * FROM users WHERE id = ?'),

      insertRoom: d.prepare('INSERT INTO rooms (id, name, created_by, created_at) VALUES (?, ?, ?, ?)'),
      roomById: d.prepare('SELECT * FROM rooms WHERE id = ?'),
      roomByName: d.prepare('SELECT * FROM rooms WHERE name = ?'),
      roomsForUser: d.prepare(
        `SELECT r.id, r.name, r.last_seq AS lastSeq, m.role, m.muted_until AS mutedUntil
           FROM rooms r JOIN members m ON m.room_id = r.id
          WHERE m.user_id = ? ORDER BY r.created_at`
      ),

      upsertMember: d.prepare(
        `INSERT INTO members (room_id, user_id, role, muted_until, joined_at)
         VALUES (?, ?, ?, 0, ?)
         ON CONFLICT (room_id, user_id) DO NOTHING`
      ),
      member: d.prepare('SELECT * FROM members WHERE room_id = ? AND user_id = ?'),
      setMuted: d.prepare('UPDATE members SET muted_until = ? WHERE room_id = ? AND user_id = ?'),
      membersOfRoom: d.prepare(
        `SELECT m.user_id AS userId, u.name, m.role, m.muted_until AS mutedUntil
           FROM members m JOIN users u ON u.id = m.user_id WHERE m.room_id = ?`
      ),

      // —— 消息写入（事务内使用）——
      msgByClientId: d.prepare(
        `${MSG_SELECT} WHERE m.room_id = ? AND m.sender_id = ? AND m.client_msg_id = ?`
      ),
      bumpSeq: d.prepare('UPDATE rooms SET last_seq = last_seq + 1 WHERE id = ? RETURNING last_seq'),
      insertMsg: d.prepare(
        'INSERT INTO messages (room_id, seq, client_msg_id, sender_id, content, ts) VALUES (?, ?, ?, ?, ?, ?)'
      ),

      // —— 消息读取 ——
      msgsAfter: d.prepare(`${MSG_SELECT} WHERE m.room_id = ? AND m.seq > ? ORDER BY m.seq LIMIT ?`),
      msgsBefore: d.prepare(
        `${MSG_SELECT} WHERE m.room_id = ? AND m.seq < ? ORDER BY m.seq DESC LIMIT ?`
      ),

      // —— 游标 ——
      upsertCursor: d.prepare(
        `INSERT INTO cursors (room_id, user_id, last_ack_seq, updated_at) VALUES (?, ?, ?, ?)
         ON CONFLICT (room_id, user_id)
         DO UPDATE SET last_ack_seq = MAX(last_ack_seq, excluded.last_ack_seq), updated_at = excluded.updated_at`
      ),
      cursor: d.prepare('SELECT last_ack_seq AS lastAckSeq FROM cursors WHERE room_id = ? AND user_id = ?'),

      // —— 邀请 ——
      insertInvite: d.prepare(
        `INSERT INTO invites (id, room_id, inviter_id, invitee_id, status, expires_at, created_at)
         VALUES (?, ?, ?, ?, 'pending', ?, ?)`
      ),
      refreshInvite: d.prepare('UPDATE invites SET expires_at = ?, inviter_id = ? WHERE id = ?'),
      inviteById: d.prepare(`${INVITE_SELECT} WHERE i.id = ?`),
      openInviteForUser: d.prepare(
        `${INVITE_SELECT} WHERE i.room_id = ? AND i.invitee_id = ? AND i.status = 'pending'`
      ),
      invitesForRoom: d.prepare(`${INVITE_SELECT} WHERE i.room_id = ? ORDER BY i.created_at DESC`),
      pendingInvitesForUser: d.prepare(
        `${INVITE_SELECT} WHERE i.invitee_id = ? AND i.status = 'pending' ORDER BY i.created_at DESC`
      ),
      // 仅当邀请仍处于 pending 时才能流转状态（撤销/拒绝/接受），过期/已撤销的邀请无法使用
      bumpInviteIfPending: d.prepare(
        `UPDATE invites SET status = ?, accepted_at = ? WHERE id = ? AND status = 'pending' RETURNING id`
      ),
      expireInvites: d.prepare(
        `UPDATE invites SET status = 'expired' WHERE status = 'pending' AND expires_at <= ?`
      ),
      countMembers: d.prepare('SELECT COUNT(*) AS n FROM members WHERE room_id = ?'),
    };
  }

  /** 在 IMMEDIATE 事务中执行 fn，失败回滚。node:sqlite 为同步驱动，单进程内无并发交错。 */
  _tx(fn) {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const r = fn();
      this.db.exec('COMMIT');
      return r;
    } catch (err) {
      try { this.db.exec('ROLLBACK'); } catch { /* 已回滚 */ }
      throw err;
    }
  }

  // ---------- 用户 ----------

  createUser(id, name, tokenRandom) {
    this.stmt.insertUser.run(id, name, tokenRandom, now());
    return this.stmt.userById.get(id);
  }

  getUserByName(name) { return this.stmt.userByName.get(name); }
  getUserById(id) { return this.stmt.userById.get(id); }

  // ---------- 房间与成员 ----------

  createRoom(id, name, creatorId) {
    return this._tx(() => {
      this.stmt.insertRoom.run(id, name, creatorId, now());
      // 创建者即管理员
      this.stmt.upsertMember.run(id, creatorId, 'admin', now());
      return this.stmt.roomById.get(id);
    });
  }

  getRoom(id) { return this.stmt.roomById.get(id); }
  getRoomByName(name) { return this.stmt.roomByName.get(name); }
  listRoomsForUser(userId) { return this.stmt.roomsForUser.all(userId); }
  listMembers(roomId) { return this.stmt.membersOfRoom.all(roomId); }

  joinRoom(roomId, userId) {
    this.stmt.upsertMember.run(roomId, userId, 'member', now());
    return this.stmt.member.get(roomId, userId);
  }

  getMember(roomId, userId) { return this.stmt.member.get(roomId, userId); }

  /** 设置禁言截止时间（0 表示解除禁言） */
  setMuted(roomId, userId, mutedUntil) {
    this.stmt.setMuted.run(mutedUntil, roomId, userId);
    return this.stmt.member.get(roomId, userId);
  }

  // ---------- 消息 ----------

  /**
   * 幂等写入消息。
   * 返回 { message, duplicate }：
   *  - duplicate=false：新消息，已分配 seq 并落库（调用方负责广播）；
   *  - duplicate=true ：同 clientMsgId 的消息已存在，直接返回原消息（调用方只回 ACK，不再广播）。
   */
  insertMessage({ roomId, clientMsgId, senderId, content }) {
    return this._tx(() => {
      const existing = this.stmt.msgByClientId.get(roomId, senderId, clientMsgId);
      if (existing) return { message: existing, duplicate: true };

      const { last_seq: seq } = this.stmt.bumpSeq.get(roomId);
      const ts = now();
      this.stmt.insertMsg.run(roomId, seq, clientMsgId, senderId, content, ts);
      const message = this.stmt.msgByClientId.get(roomId, senderId, clientMsgId);
      return { message, duplicate: false };
    });
  }

  /** 断线补发：取 seq > afterSeq 的消息（升序，最多 limit 条） */
  getMessagesAfter(roomId, afterSeq, limit) {
    return this.stmt.msgsAfter.all(roomId, afterSeq, limit);
  }

  /** 历史翻页：取 seq < beforeSeq 的消息，返回时按升序排列 */
  getMessagesBefore(roomId, beforeSeq, limit) {
    return this.stmt.msgsBefore.all(roomId, beforeSeq, limit).reverse();
  }

  // ---------- 游标 ----------

  saveCursor(roomId, userId, lastAckSeq) {
    this.stmt.upsertCursor.run(roomId, userId, lastAckSeq, now());
  }

  getCursor(roomId, userId) {
    const row = this.stmt.cursor.get(roomId, userId);
    return row ? row.lastAckSeq : 0;
  }

  // ---------- 邀请 ----------

  /**
   * 创建定向邀请。同房间对同一用户已有待处理邀请时，刷新其有效期（幂等重发）。
   * 返回邀请行（含房间/邀请人/被邀请人名称）。
   */
  createInvite({ id, roomId, inviterId, inviteeId, expiresAt }) {
    return this._tx(() => {
      const open = this.stmt.openInviteForUser.get(roomId, inviteeId);
      if (open) {
        this.stmt.refreshInvite.run(expiresAt, inviterId, open.id);
        return this.stmt.inviteById.get(open.id);
      }
      this.stmt.insertInvite.run(id, roomId, inviterId, inviteeId, expiresAt, now());
      return this.stmt.inviteById.get(id);
    });
  }

  getInvite(id) { return this.stmt.inviteById.get(id); }

  listInvitesForRoom(roomId) {
    this.markExpiredInvites();
    return this.stmt.invitesForRoom.all(roomId);
  }

  listPendingInvitesForUser(userId) {
    this.markExpiredInvites();
    return this.stmt.pendingInvitesForUser.all(userId);
  }

  /** 把所有到期的待处理邀请置为 expired，返回受影响行数 */
  markExpiredInvites(t = now()) {
    return this.stmt.expireInvites.run(t).changes;
  }

  /** 撤销邀请：仅 pending 可撤销，成功返回 true */
  revokeInvite(id) {
    return this.stmt.bumpInviteIfPending.run('revoked', 0, id).changes > 0;
  }

  /** 拒绝邀请：仅 pending 可拒绝，成功返回 true */
  declineInvite(id) {
    return this.stmt.bumpInviteIfPending.run('declined', 0, id).changes > 0;
  }

  memberCount(roomId) {
    return this.stmt.countMembers.get(roomId).n;
  }

  /**
   * 接受邀请（原子操作）：
   * 校验「仍 pending、未过期、未入组、未超人数上限」通过后，
   * 在同一事务内把邀请置为 accepted 并把成员关系落库 —— 成员身份持久生效，
   * 不是只在当前连接的房间索引里临时挂接。
   * 返回 { ok:true, invite } 或 { ok:false, reason, invite }。
   */
  acceptInvite(id, userId, maxMembers) {
    return this._tx(() => {
      const invite = this.stmt.inviteById.get(id);
      if (!invite) return { ok: false, reason: 'NO_SUCH_INVITE', invite: null };
      if (invite.inviteeId !== userId) return { ok: false, reason: 'FORBIDDEN', invite };
      if (invite.status !== 'pending') return { ok: false, reason: 'INVITE_NOT_OPEN', invite };
      if (invite.expiresAt <= now()) {
        this.stmt.bumpInviteIfPending.run('expired', 0, id);
        return { ok: false, reason: 'INVITE_EXPIRED', invite: { ...invite, status: 'expired' } };
      }
      if (this.stmt.member.get(invite.roomId, userId)) {
        // 已在邀请生成后通过其他方式入组：关闭邀请，视为成功（幂等）
        this.stmt.bumpInviteIfPending.run('accepted', now(), id);
        return { ok: true, invite: this.stmt.inviteById.get(id), alreadyMember: true };
      }
      if (this.stmt.countMembers.get(invite.roomId).n >= maxMembers) {
        return { ok: false, reason: 'ROOM_FULL', invite };
      }
      this.stmt.bumpInviteIfPending.run('accepted', now(), id);
      this.stmt.upsertMember.run(invite.roomId, userId, 'member', now());
      return { ok: true, invite: this.stmt.inviteById.get(id), alreadyMember: false };
    });
  }

  close() {
    try { this.db.exec('PRAGMA wal_checkpoint(TRUNCATE)'); } catch { /* 内存库无 WAL */ }
    this.db.close();
  }
}

module.exports = { ChatDB };
