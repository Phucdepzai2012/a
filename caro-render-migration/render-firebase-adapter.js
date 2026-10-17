/*
 * Compatibility adapter: preserves the existing Caro client calls (ref/on/once/update/set/remove/transaction)
 * while routing them to the Render Socket.IO server instead of Firebase.
 * Place this file beside index.html. The actual Firebase SDK scripts must be removed.
 */
(function () {
  'use strict';

  const SERVER_URL = 'https://a-sl8r.onrender.com';
  if (typeof window.io !== 'function') {
    console.error('Socket.IO client chưa được tải. Hãy thêm script Socket.IO trước adapter.');
    window.firebase = undefined;
    return;
  }

  const socket = window.io(SERVER_URL, {
    transports: ['websocket', 'polling'],
    reconnection: true,
    reconnectionAttempts: Infinity,
    reconnectionDelay: 1000,
    reconnectionDelayMax: 8000,
    timeout: 12000
  });

  let localUid = '';
  try {
    localUid = localStorage.getItem('caroRenderUid') || '';
    if (!localUid) {
      localUid = 'u_' + (window.crypto && crypto.randomUUID ? crypto.randomUUID().replace(/-/g, '') : Math.random().toString(36).slice(2) + Date.now().toString(36));
      localStorage.setItem('caroRenderUid', localUid);
    }
  } catch (_) {
    localUid = 'u_' + Math.random().toString(36).slice(2) + Date.now().toString(36);
  }

  function waitForConnection() {
    if (socket.connected) return Promise.resolve();
    return new Promise((resolve, reject) => {
      let done = false;
      const timer = setTimeout(() => finish(new Error('Máy chủ Render chưa kết nối. Hãy kiểm tra trạng thái deploy của dịch vụ.')), 15000);
      function finish(err) {
        if (done) return;
        done = true;
        clearTimeout(timer);
        socket.off('connect', onConnect);
        socket.off('connect_error', onError);
        if (err) reject(err); else resolve();
      }
      function onConnect() { finish(); }
      function onError(err) { finish(new Error('Không kết nối được Render: ' + (err && err.message || 'kết nối thất bại'))); }
      socket.once('connect', onConnect);
      socket.once('connect_error', onError);
    });
  }

  async function rpc(event, payload) {
    await waitForConnection();
    return new Promise((resolve, reject) => {
      let done = false;
      const timer = setTimeout(() => finish(new Error('Máy chủ phản hồi quá lâu.')), 15000);
      function finish(err, result) {
        if (done) return;
        done = true;
        clearTimeout(timer);
        if (err) reject(err); else if (!result || result.ok !== true) reject(new Error(result && result.error || 'Yêu cầu máy chủ thất bại.'));
        else resolve(result);
      }
      socket.emit(event, payload, result => finish(null, result));
    });
  }

  function copy(value) {
    if (value === undefined) return undefined;
    return JSON.parse(JSON.stringify(value));
  }

  function makeSnapshot(value, path, query) {
    const key = String(path || '').split('/').filter(Boolean).pop() || null;
    let val = value == null ? null : copy(value);
    if (val && typeof val === 'object' && !Array.isArray(val)) {
      let entries = Object.entries(val);
      if (query && query.orderByChild) {
        const field = query.orderByChild;
        entries.sort((a, b) => {
          const av = a[1] && a[1][field] != null ? a[1][field] : null;
          const bv = b[1] && b[1][field] != null ? b[1][field] : null;
          if (av === bv) return a[0].localeCompare(b[0]);
          if (av == null) return -1;
          if (bv == null) return 1;
          return typeof av === 'number' && typeof bv === 'number' ? av - bv : String(av).localeCompare(String(bv));
        });
      }
      if (query && Number.isInteger(query.limitToFirst)) entries = entries.slice(0, query.limitToFirst);
      if (query && Number.isInteger(query.limitToLast)) entries = entries.slice(Math.max(0, entries.length - query.limitToLast));
      val = Object.fromEntries(entries);
    }
    return {
      key,
      val: () => val == null ? null : copy(val),
      exists: () => val !== null && val !== undefined,
      forEach: callback => {
        if (!val || typeof val !== 'object') return false;
        for (const [childKey, childValue] of Object.entries(val)) {
          const stop = callback(makeSnapshot(childValue, `${path}/${childKey}`, {}));
          if (stop === true) return true;
        }
        return false;
      }
    };
  }

  let nextSubId = 0;
  const subscriptions = new Map();
  socket.on('db:value', message => {
    const sub = subscriptions.get(message && message.subId);
    if (!sub) return;
    const fingerprint = JSON.stringify(message.data === undefined ? null : message.data);
    if (sub.lastFingerprint === fingerprint && sub.hasValue) return;
    sub.lastFingerprint = fingerprint;
    sub.hasValue = true;
    const snap = makeSnapshot(message.data, sub.path, sub.query);
    for (const cb of [...sub.callbacks]) {
      try { cb(snap); } catch (error) { console.error('Lỗi callback đồng bộ Caro:', error); }
    }
  });

  socket.on('connect', () => {
    for (const [subId, sub] of subscriptions) {
      sub.hasValue = false;
      socket.emit('db:subscribe', { subId, path: sub.path, query: sub.query }, result => {
        if (!result || result.ok !== true) console.error(result && result.error || 'Không thể đăng ký đồng bộ.');
      });
    }
  });

  class Ref {
    constructor(path, query = {}) {
      this.path = String(path || '').replace(/^\/+|\/+$/g, '');
      this.query = { ...query };
      this.localSubIds = new Set();
    }
    limitToFirst(n) { return new Ref(this.path, { ...this.query, limitToFirst: Math.max(0, Number(n) || 0) }); }
    limitToLast(n) { return new Ref(this.path, { ...this.query, limitToLast: Math.max(0, Number(n) || 0) }); }
    orderByChild(field) { return new Ref(this.path, { ...this.query, orderByChild: String(field) }); }
    async once(eventName) {
      if (eventName !== 'value') throw new Error('Adapter chỉ hỗ trợ sự kiện value.');
      const result = await rpc('db:read', { path: this.path, query: this.query });
      return makeSnapshot(result.data, this.path, this.query);
    }
    on(eventName, callback, errorCallback) {
      if (eventName !== 'value' || typeof callback !== 'function') throw new Error('Sự kiện Firebase không được hỗ trợ.');
      const subId = 's' + (++nextSubId);
      const sub = { path: this.path, query: this.query, callbacks: new Set([callback]), hasValue: false, lastFingerprint: null };
      subscriptions.set(subId, sub);
      this.localSubIds.add(subId);
      if (socket.connected) {
        socket.emit('db:subscribe', { subId, path: this.path, query: this.query }, result => {
          if (!result || result.ok !== true) {
            const err = new Error(result && result.error || 'Không thể đăng ký đồng bộ.');
            if (typeof errorCallback === 'function') errorCallback(err);
            else console.error(err);
          }
        });
      }
      return callback;
    }
    off(eventName, callback) {
      for (const subId of this.localSubIds) {
        const sub = subscriptions.get(subId);
        if (!sub) continue;
        if (callback) sub.callbacks.delete(callback); else sub.callbacks.clear();
        if (!sub.callbacks.size) {
          subscriptions.delete(subId);
          if (socket.connected) socket.emit('db:unsubscribe', { subId }, () => {});
        }
      }
      if (!callback) this.localSubIds.clear();
    }
    async set(value) {
      await rpc('db:write', { op: 'set', path: this.path, data: value });
    }
    async update(value) {
      await rpc('db:write', { op: 'update', path: this.path, data: value });
    }
    async remove() {
      await rpc('db:write', { op: 'remove', path: this.path });
    }
    async transaction(updateFunction) {
      if (typeof updateFunction !== 'function') throw new Error('Transaction thiếu hàm cập nhật.');
      for (let attempt = 0; attempt < 12; attempt++) {
        const current = await rpc('db:read', { path: this.path, query: this.query });
        const oldValue = current.data == null ? null : copy(current.data);
        const newValue = updateFunction(oldValue);
        if (newValue === undefined) {
          return { committed: false, snapshot: makeSnapshot(oldValue, this.path, this.query) };
        }
        const result = await rpc('db:commit', { path: this.path, baseVersion: current.version, value: newValue });
        if (result.committed) return { committed: true, snapshot: makeSnapshot(result.data, this.path, this.query) };
      }
      throw new Error('Transaction bị tranh chấp quá nhiều lần. Hãy thử lại.');
    }
    push() {
      const key = '-R' + Date.now().toString(36) + Math.random().toString(36).slice(2, 10);
      return { key, set: value => new Ref(`${this.path}/${key}`).set(value), update: value => new Ref(`${this.path}/${key}`).update(value) };
    }
  }

  const firebaseApps = [];
  function makeApp(config, name) {
    const auth = {
      currentUser: null,
      async signInAnonymously() {
        await waitForConnection();
        this.currentUser = { uid: localUid, isAnonymous: true };
        return { user: this.currentUser };
      }
    };
    const db = { ref: path => new Ref(path) };
    return { name: name || '[DEFAULT]', options: config || {}, auth: () => auth, database: () => db };
  }

  window.firebase = {
    apps: firebaseApps,
    initializeApp(config, name) {
      const existing = firebaseApps.find(app => app.name === (name || '[DEFAULT]'));
      if (existing) return existing;
      const app = makeApp(config, name);
      firebaseApps.push(app);
      return app;
    }
  };

  // Allows connection-state diagnostics without changing the Caro UI or game logic.
  window.caroRenderSocket = socket;
})();
