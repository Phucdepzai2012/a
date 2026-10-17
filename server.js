'use strict';

// Minimal Socket.IO-backed realtime store for the supplied Caro client.
// This is a migration bridge, not a production-secure database.
const express = require('express');
const http = require('http');
const crypto = require('node:crypto');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  pingInterval: 25000,
  pingTimeout: 20000,
});

// In-memory only: data resets when the Render instance restarts/redeploys.
const data = { players: {}, lobby: null, invite: {}, games: {}, history: {} };
// Temporary in-memory account/session store. Passwords are stored only as scrypt hashes.
const accountsByName = new Map();
const accountsByUid = new Map();
const sessions = new Map();
let version = 0;

function parseAccountName(value) {
  const name = String(value || '').trim().replace(/\s+/g, ' ').normalize('NFKC');
  if (name.length < 3 || name.length > 14) throw new Error('Tên đăng nhập phải dài từ 3 đến 14 ký tự.');
  if (!/^[\p{L}\p{N}_ -]+$/u.test(name)) throw new Error('Tên chỉ được gồm chữ cái, số, dấu cách, gạch dưới hoặc gạch ngang.');
  return { name, key: name.toLocaleLowerCase('vi-VN') };
}

function makeAccountSession(uid) {
  const token = crypto.randomBytes(32).toString('hex');
  sessions.set(token, uid);
  const account = accountsByUid.get(uid);
  const profile = data.players[uid] || {};
  return {
    ok: true, token, uid, name: account.name,
    elo: Number(profile.elo) || 1000,
    w: Number(profile.w) || 0, l: Number(profile.l) || 0, d: Number(profile.d) || 0
  };
}

function requireSession(payload) {
  const token = String(payload && payload.sessionToken || '');
  const uid = sessions.get(token);
  if (!uid || !accountsByUid.has(uid)) throw new Error('Phiên đăng nhập không hợp lệ. Hãy đăng nhập lại.');
  return uid;
}

function assertWriteAccess(path, uid, value) {
  const parts = cleanPath(path);
  if (parts[0] === 'players' || parts[0] === 'history') {
    if (parts.length < 2 || parts[1] !== uid) {
      throw new Error('Bạn chỉ được sửa hồ sơ và lịch sử của chính mình.');
    }
  }
  if (parts[0] === 'lobby' && value != null && value.uid && value.uid !== uid) {
    throw new Error('Không thể tạo hàng chờ thay cho tài khoản khác.');
  }
}

app.get('/', (_req, res) => res.status(200).json({ service: 'caro-realtime', status: 'ok' }));
app.get('/healthz', (_req, res) => res.status(200).json({ status: 'ok' }));

function cleanPath(path) {
  if (typeof path !== 'string') throw new Error('Đường dẫn không hợp lệ.');
  const p = path.replace(/^\/+|\/+$/g, '');
  if (!p) throw new Error('Không hỗ trợ truy cập toàn bộ database.');
  const parts = p.split('/');
  if (parts.some(x => !x || x === '.' || x === '..' || x.includes('\\'))) throw new Error('Đường dẫn không hợp lệ.');
  if (parts[0] === '.info') {
    if (p !== '.info/serverTimeOffset') throw new Error('Đường dẫn hệ thống không hợp lệ.');
    return parts;
  }
  if (!['players', 'lobby', 'invite', 'games', 'history'].includes(parts[0])) throw new Error('Đường dẫn không được phép.');
  return parts;
}

function getAt(path) {
  const parts = cleanPath(path);
  if (parts[0] === '.info') return 0;
  let cur = data;
  for (const part of parts) {
    if (cur == null || typeof cur !== 'object' || !Object.prototype.hasOwnProperty.call(cur, part)) return null;
    cur = cur[part];
  }
  return cur === undefined ? null : clone(cur);
}

function setAt(path, value) {
  const parts = cleanPath(path);
  if (parts[0] === '.info') throw new Error('Không thể ghi vào đường dẫn hệ thống.');
  let cur = data;
  for (let i = 0; i < parts.length - 1; i++) {
    const key = parts[i];
    if (!cur[key] || typeof cur[key] !== 'object' || Array.isArray(cur[key])) cur[key] = {};
    cur = cur[key];
  }
  const last = parts[parts.length - 1];
  if (value === null || value === undefined) delete cur[last];
  else cur[last] = clone(value);
}

function clone(value) {
  if (value === undefined) return undefined;
  return JSON.parse(JSON.stringify(value));
}

function queryValue(path, query = {}) {
  const raw = getAt(path);
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return raw;
  let entries = Object.entries(raw);
  if (query.orderByChild) {
    const child = String(query.orderByChild);
    entries.sort((a, b) => {
      const av = a[1] && a[1][child] != null ? a[1][child] : null;
      const bv = b[1] && b[1][child] != null ? b[1][child] : null;
      if (av === bv) return a[0].localeCompare(b[0]);
      if (av == null) return -1;
      if (bv == null) return 1;
      return typeof av === 'number' && typeof bv === 'number' ? av - bv : String(av).localeCompare(String(bv));
    });
  }
  if (Number.isInteger(query.limitToFirst) && query.limitToFirst >= 0) entries = entries.slice(0, query.limitToFirst);
  if (Number.isInteger(query.limitToLast) && query.limitToLast >= 0) entries = entries.slice(Math.max(0, entries.length - query.limitToLast));
  return Object.fromEntries(entries);
}

function notifySubscribers() {
  for (const client of io.sockets.sockets.values()) {
    const subs = client.data.subscriptions;
    if (!subs) continue;
    for (const [subId, sub] of subs.entries()) {
      client.emit('db:value', { subId, data: queryValue(sub.path, sub.query), version });
    }
  }
}

function reply(ack, result) {
  if (typeof ack === 'function') ack(result);
}

io.on('connection', socket => {
  socket.data.subscriptions = new Map();

  socket.on('auth:register', (payload, ack) => {
    try {
      const { name, key } = parseAccountName(payload && payload.name);
      const password = String(payload && payload.password || '');
      if (password.length < 8 || Buffer.byteLength(password, 'utf8') > 128) {
        throw new Error('Mật khẩu phải có ít nhất 8 ký tự và tối đa 128 byte.');
      }
      if (accountsByName.has(key)) throw new Error('Tên đăng nhập đã được sử dụng.');
      const uid = 'u_' + crypto.randomBytes(18).toString('hex');
      const salt = crypto.randomBytes(16);
      const passwordHash = crypto.scryptSync(password, salt, 64);
      const account = { uid, name, key, salt: salt.toString('hex'), passwordHash: passwordHash.toString('hex'), createdAt: Date.now() };
      accountsByName.set(key, uid);
      accountsByUid.set(uid, account);
      data.players[uid] = { name, elo: 1000, w: 0, l: 0, d: 0 };
      version++;
      notifySubscribers();
      reply(ack, makeAccountSession(uid));
    } catch (error) {
      reply(ack, { ok: false, error: error.message || 'Không thể đăng ký tài khoản.' });
    }
  });

  socket.on('auth:login', (payload, ack) => {
    try {
      const { key } = parseAccountName(payload && payload.name);
      const password = String(payload && payload.password || '');
      const uid = accountsByName.get(key);
      const account = uid && accountsByUid.get(uid);
      if (!account || Buffer.byteLength(password, 'utf8') > 128) throw new Error('Tên đăng nhập hoặc mật khẩu không đúng.');
      const salt = Buffer.from(account.salt, 'hex');
      const storedHash = Buffer.from(account.passwordHash, 'hex');
      const suppliedHash = crypto.scryptSync(password, salt, storedHash.length);
      if (storedHash.length !== suppliedHash.length || !crypto.timingSafeEqual(storedHash, suppliedHash)) {
        throw new Error('Tên đăng nhập hoặc mật khẩu không đúng.');
      }
      reply(ack, makeAccountSession(uid));
    } catch (error) {
      reply(ack, { ok: false, error: error.message || 'Không thể đăng nhập.' });
    }
  });

  socket.on('auth:session', (payload, ack) => {
    try {
      const uid = requireSession(payload);
      const account = accountsByUid.get(uid);
      const profile = data.players[uid] || {};
      reply(ack, {
        ok: true, uid, name: account.name,
        elo: Number(profile.elo) || 1000,
        w: Number(profile.w) || 0, l: Number(profile.l) || 0, d: Number(profile.d) || 0
      });
    } catch (error) {
      reply(ack, { ok: false, error: error.message || 'Phiên đăng nhập không hợp lệ.' });
    }
  });

  socket.on('auth:logout', (payload, ack) => {
    const token = String(payload && payload.sessionToken || '');
    sessions.delete(token);
    reply(ack, { ok: true });
  });

  socket.on('room:create', (payload, ack) => {
    try {
      const uid = String(payload && payload.uid || '').trim();
      const authenticatedUid = requireSession(payload);
      if (uid !== authenticatedUid) throw new Error('Phiên đăng nhập không trùng người chơi.');
      const account = accountsByUid.get(uid);
      const profile = data.players[uid] || {};
      const name = account.name;
      const elo = Number(profile.elo) || 1000;
      let code = '';
      for (let attempt = 0; attempt < 100; attempt++) {
        const candidate = String(require('node:crypto').randomInt(1000, 10000));
        if (!data.games[candidate]) { code = candidate; break; }
      }
      if (!code) throw new Error('Không tạo được mã phòng. Hãy thử lại.');
      data.games[code] = { roomCode: code, p: [{ uid, name, elo }], moves: {}, turn: 0, status: 'waiting', dl: 0, createdAt: Date.now() };
      version++;
      notifySubscribers();
      reply(ack, { ok: true, code });
    } catch (error) { reply(ack, { ok: false, error: error.message || 'Không tạo được phòng.' }); }
  });

  socket.on('room:join', (payload, ack) => {
    try {
      const code = String(payload && payload.code || '').trim();
      const uid = String(payload && payload.uid || '').trim();
      const authenticatedUid = requireSession(payload);
      if (uid !== authenticatedUid) throw new Error('Phiên đăng nhập không trùng người chơi.');
      if (!/^[0-9]{4}$/.test(code)) throw new Error('Mã phòng phải gồm đúng 4 chữ số.');
      const game = data.games[code];
      if (!game || game.roomCode !== code) throw new Error('Không tìm thấy phòng. Hãy kiểm tra mã.');
      if (game.status !== 'waiting' || !Array.isArray(game.p) || game.p.length !== 1) throw new Error('Phòng này đã bắt đầu hoặc đã đủ người.');
      if (game.p[0].uid === uid) throw new Error('Hãy dùng trình duyệt hoặc tài khoản khác để vào phòng.');
      const account = accountsByUid.get(uid);
      const profile = data.players[uid] || {};
      const name = account.name;
      const elo = Number(profile.elo) || 1000;
      game.p.push({ uid, name, elo });
      if (Math.random() < 0.5) game.p.reverse();
      game.status = 'play'; game.turn = 0; game.dl = Date.now() + 30000;
      version++; notifySubscribers();
      reply(ack, { ok: true, code });
    } catch (error) { reply(ack, { ok: false, error: error.message || 'Không thể vào phòng.' }); }
  });

  socket.on('db:read', (payload, ack) => {
    try {
      const path = String(payload && payload.path || '');
      const query = payload && payload.query || {};
      const value = queryValue(path, query);
      reply(ack, { ok: true, data: value, version, key: path.split('/').filter(Boolean).pop() || null });
    } catch (error) {
      reply(ack, { ok: false, error: error.message || 'Lỗi đọc dữ liệu.' });
    }
  });

  socket.on('db:subscribe', (payload, ack) => {
    try {
      const subId = String(payload && payload.subId || '');
      const path = String(payload && payload.path || '');
      const query = payload && payload.query || {};
      if (!subId) throw new Error('Thiếu mã đăng ký theo dõi.');
      cleanPath(path);
      socket.data.subscriptions.set(subId, { path, query });
      socket.emit('db:value', { subId, data: queryValue(path, query), version });
      reply(ack, { ok: true });
    } catch (error) {
      reply(ack, { ok: false, error: error.message || 'Không thể theo dõi dữ liệu.' });
    }
  });

  socket.on('db:unsubscribe', (payload, ack) => {
    const subId = String(payload && payload.subId || '');
    socket.data.subscriptions.delete(subId);
    reply(ack, { ok: true });
  });

  socket.on('db:write', (payload, ack) => {
    try {
      const op = String(payload && payload.op || '');
      const path = String(payload && payload.path || '');
      const uid = requireSession(payload);
      assertWriteAccess(path, uid, payload.data);
      cleanPath(path);
      if (op === 'set') {
        setAt(path, payload.data);
      } else if (op === 'remove') {
        setAt(path, null);
      } else if (op === 'update') {
        const updates = payload.data;
        if (!updates || typeof updates !== 'object' || Array.isArray(updates)) throw new Error('Dữ liệu update không hợp lệ.');
        for (const [relativePath, value] of Object.entries(updates)) {
          const target = relativePath ? `${path}/${relativePath}` : path;
          setAt(target, value);
        }
      } else {
        throw new Error('Thao tác ghi không hợp lệ.');
      }
      version++;
      notifySubscribers();
      reply(ack, { ok: true, version });
    } catch (error) {
      reply(ack, { ok: false, error: error.message || 'Lỗi ghi dữ liệu.' });
    }
  });

  socket.on('db:commit', (payload, ack) => {
    try {
      const path = String(payload && payload.path || '');
      const uid = requireSession(payload);
      assertWriteAccess(path, uid, payload.value);
      cleanPath(path);
      if (Number(payload && payload.baseVersion) !== version) {
        reply(ack, { ok: true, committed: false, version });
        return;
      }
      setAt(path, payload.value);
      version++;
      notifySubscribers();
      reply(ack, { ok: true, committed: true, version, data: getAt(path) });
    } catch (error) {
      reply(ack, { ok: false, error: error.message || 'Lỗi transaction.' });
    }
  });
});

const port = Number(process.env.PORT) || 10000;
server.listen(port, '0.0.0.0', () => {
  console.log(`Caro realtime server listening on 0.0.0.0:${port}`);
});
