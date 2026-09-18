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
  last_seq   INTEGER NOT NULL DEFAULT 0,
  max_occupancy INTEGER NOT NULL DEFAULT 50  -- 房间人数上限（含管理员），<=0 表示不限
);

CREATE TABLE IF NOT EXISTS members (
  room_id     TEXT NOT NULL REFERENCES rooms(id),
  user_id     TEXT NOT NULL REFERENCES users(id),
  role        TEXT NOT NULL DEFAULT 'member' CHECK (role IN ('admin','member')),
  muted_until INTEGER NOT NULL DEFAULT 0,
  joined_at   INTEGER NOT NULL,
  PRIMARY KEY (room_id, user_id)
);

-- 房间邀请：管理员定向发给某个已注册用户。
-- status: active（可用）/ used（已接受）/ revoked（已撤销）。
-- (room_id, invitee_id) 仅在 active 时唯一 —— 同一房间对同一用户同时只存在
-- 一张有效邀请，接受/撤销/过期后方可重新发起。
CREATE TABLE IF NOT EXISTS invites (
  id          TEXT PRIMARY KEY,
  room_id     TEXT NOT NULL REFERENCES rooms(id),
  inviter_id  TEXT NOT NULL REFERENCES users(id),
  invitee_id  TEXT NOT NULL REFERENCES users(id),
  status      TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','used','revoked')),
  created_at  INTEGER NOT NULL,
  expires_at  INTEGER NOT NULL,
  used_at     INTEGER,
  revoked_at  INTEGER,
  revoked_by  TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_invites_active_pair
  ON invites (room_id, invitee_id) WHERE status = 'active';

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
`;

const MSG_SELECT = `
  SELECT m.room_id AS roomId, m.seq, m.client_msg_id AS clientMsgId,
         m.sender_id AS "from", u.name AS fromName, m.content, m.ts
    FROM messages m JOIN users u ON u.id = m.sender_id
`;

class ChatDB {
  constructor(dbPath) {
    this.db = new DatabaseSync(dbPath);
    this.db.exec(SCHEMA);
    this._migrate();
    this._prepare();
  }

  /** 旧库迁移：为已存在的 rooms 补上新增列（CREATE TABLE IF NOT EXISTS 不会改已建表） */
  _migrate() {
    const cols = this.db.prepare('PRAGMA table_info(rooms)').all();
    if (!cols.some((c) => c.name === 'max_occupancy')) {
      this.db.exec('ALTER TABLE rooms ADD COLUMN max_occupancy INTEGER NOT NULL DEFAULT 50');
    }
  }

  _prepare() {
    const d = this.db;
    this.stmt = {
      insertUser: d.prepare('INSERT INTO users (id, name, token_random, created_at) VALUES (?, ?, ?, ?)'),
      userByName: d.prepare('SELECT * FROM users WHERE name = ?'),
      userById: d.prepare('SELECT * FROM users WHERE id = ?'),

      insertRoom: d.prepare(
        'INSERT INTO rooms (id, name, created_by, created_at, max_occupancy) VALUES (?, ?, ?, ?, ?)'
      ),
      roomById: d.prepare('SELECT * FROM rooms WHERE id = ?'),
      roomByName: d.prepare('SELECT * FROM rooms WHERE name = ?'),
      roomsForUser: d.prepare(
        `SELECT r.id, r.name, r.last_seq AS lastSeq, r.max_occupancy AS maxOccupancy,
                m.role, m.muted_until AS mutedUntil
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
        `SELECT m.user_id AS userId, u.name, m.role, m.muted_until AS mutedUntil, m.joined_at AS joinedAt
           FROM members m JOIN users u ON u.id = m.user_id WHERE m.room_id = ?`
      ),
      countMembers: d.prepare('SELECT COUNT(*) AS n FROM members WHERE room_id = ?'),

      // —— 邀请 ——
      insertInvite: d.prepare(
        `INSERT INTO invites (id, room_id, inviter_id, invitee_id, status, created_at, expires_at)
         VALUES (?, ?, ?, ?, 'active', ?, ?)`
      ),
      inviteById: d.prepare('SELECT * FROM invites WHERE id = ?'),
      activeInviteForPair: d.prepare(
        `SELECT * FROM invites WHERE room_id = ? AND invitee_id = ? AND status = 'active'`
      ),
      // 「我收到的」邀请（默认只列 active，可通过 includeAll 拉历史）
      invitesForInvitee: d.prepare(
        `SELECT i.id, i.room_id AS roomId, r.name AS roomName, i.inviter_id AS inviterId,
                u.name AS inviterName, i.status, i.created_at AS createdAt,
                i.expires_at AS expiresAt, i.used_at AS usedAt, i.revoked_at AS revokedAt
           FROM invites i
           JOIN rooms r ON r.id = i.room_id
           JOIN users u ON u.id = i.inviter_id
          WHERE i.invitee_id = ? AND i.status = 'active'
          ORDER BY i.created_at DESC`
      ),
      invitesForRoom: d.prepare(
        `SELECT i.id, i.room_id AS roomId, i.invitee_id AS inviteeId, u.name AS inviteeName,
                i.inviter_id AS inviterId, i.status, i.created_at AS createdAt,
                i.expires_at AS expiresAt, i.used_at AS usedAt, i.revoked_at AS revokedAt
           FROM invites i JOIN users u ON u.id = i.invitee_id
          WHERE i.room_id = ? ORDER BY i.created_at DESC`
      ),
      markInviteUsed: d.prepare(
        `UPDATE invites SET status = 'used', used_at = ? WHERE id = ? AND status = 'active'`
      ),
      revokeInvite: d.prepare(
        `UPDATE invites SET status = 'revoked', revoked_at = ?, revoked_by = ?
         WHERE id = ? AND status = 'active'`
      ),
      expiredInviteIds: d.prepare(
        `SELECT id FROM invites WHERE status = 'active' AND expires_at <= ?`
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

  createRoom(id, name, creatorId, maxOccupancy = 50) {
    return this._tx(() => {
      this.stmt.insertRoom.run(id, name, creatorId, now(), maxOccupancy);
      // 创建者即管理员
      this.stmt.upsertMember.run(id, creatorId, 'admin', now());
      return this.stmt.roomById.get(id);
    });
  }

  getRoom(id) { return this.stmt.roomById.get(id); }
  getRoomByName(name) { return this.stmt.roomByName.get(name); }
  listRoomsForUser(userId) { return this.stmt.roomsForUser.all(userId); }
  listMembers(roomId) { return this.stmt.membersOfRoom.all(roomId); }
  countMembers(roomId) { return this.stmt.countMembers.get(roomId).n; }

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

  // ---------- 邀请 ----------

  createInvite({ id, roomId, inviterId, inviteeId, expiresAt }) {
    this.stmt.insertInvite.run(id, roomId, inviterId, inviteeId, now(), expiresAt);
    return this.stmt.inviteById.get(id);
  }

  getInvite(id) { return this.stmt.inviteById.get(id); }

  /** 该房间对该用户当前是否已有 active 邀请（partial unique index 的应用侧预判） */
  getActiveInviteForPair(roomId, inviteeId) {
    return this.stmt.activeInviteForPair.get(roomId, inviteeId);
  }

  listInvitesForInvitee(inviteeId) { return this.stmt.invitesForInvitee.all(inviteeId); }
  listInvitesForRoom(roomId) { return this.stmt.invitesForRoom.all(roomId); }

  /**
   * 原子接受邀请：在同一 IMMEDIATE 事务内
   *   1) 把指定 active 邀请标记为 used（条件 UPDATE，影响行数为 0 说明已被并发用掉/撤销）；
   *   2) 校验成员上限；
   *   3) upsert 成员关系（持久生效）。
   * 返回 { ok:true, member, alreadyMember } 或 { ok:false, reason }。
   * 过期判定由 expiresAt 与当前时间比较在应用层完成。
   */
  acceptInvite(inviteId, { maxOccupancy }) {
    return this._tx(() => {
      const invite = this.stmt.inviteById.get(inviteId);
      if (!invite) return { ok: false, reason: 'NO_SUCH_INVITE' };
      if (invite.status !== 'active') {
        return { ok: false, reason: invite.status === 'revoked' ? 'INVITE_REVOKED' : 'INVITE_USED' };
      }
      if (invite.expires_at <= now()) return { ok: false, reason: 'INVITE_EXPIRED' };

      const existing = this.stmt.member.get(invite.room_id, invite.invitee_id);
      if (existing) {
        // 已是成员：把邀请收口为 used，避免悬挂的 active 邀请
        this.stmt.markInviteUsed.run(now(), inviteId);
        return { ok: true, alreadyMember: true, member: existing, invite };
      }

      const n = this.stmt.countMembers.get(invite.room_id).n;
      if (maxOccupancy > 0 && n >= maxOccupancy) return { ok: false, reason: 'ROOM_FULL' };

      // 条件 UPDATE：仅当仍为 active 才置 used，返回变化行数
      const info = this.stmt.markInviteUsed.run(now(), inviteId);
      if (info.changes === 0) return { ok: false, reason: 'INVITE_UNAVAILABLE' };

      const ts = now();
      this.stmt.upsertMember.run(invite.room_id, invite.invitee_id, 'member', ts);
      const member = this.stmt.member.get(invite.room_id, invite.invitee_id);
      return { ok: true, alreadyMember: false, member, invite };
    });
  }

  /** 撤销邀请。仅 active 可撤销，返回是否实际撤销 */
  revokeInvite(inviteId, revokedBy) {
    const info = this.stmt.revokeInvite.run(now(), revokedBy, inviteId);
    return info.changes > 0;
  }

  /**
   * 把过期的 active 邀请批量标记为 revoked，返回被收口的邀请（含 roomId/inviteeId，
   * 供服务端通知目标用户刷新邀请列表）。
   */
  sweepExpiredInvites() {
    return this._tx(() => {
      const due = this.stmt.expiredInviteIds.all(now());
      if (due.length === 0) return [];
      const expired = [];
      for (const { id } of due) {
        const invite = this.stmt.inviteById.get(id);
        const info = this.stmt.revokeInvite.run(now(), null, id);
        if (info.changes > 0) expired.push(invite);
      }
      return expired;
    });
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

  close() {
    try { this.db.exec('PRAGMA wal_checkpoint(TRUNCATE)'); } catch { /* 内存库无 WAL */ }
    this.db.close();
  }
}

module.exports = { ChatDB };
