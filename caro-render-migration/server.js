'use strict';

// Minimal Socket.IO-backed realtime store for the supplied Caro client.
// This is a migration bridge, not a production-secure database.
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  pingInterval: 25000,
  pingTimeout: 20000,
});

// In-memory only: data resets when the Render instance restarts/redeploys.
const data = { players: {}, lobby: null, invite: {}, games: {} };
let version = 0;

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
  if (!['players', 'lobby', 'invite', 'games'].includes(parts[0])) throw new Error('Đường dẫn không được phép.');
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
