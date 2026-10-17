'use strict';

// Minimal Socket.IO-backed realtime store for the supplied Caro client.
// This is a migration bridge, not a production-secure database.
const express = require('express');
const http = require('http');
const crypto = require('node:crypto');
const { Pool } = require('pg');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  pingInterval: 25000,
  pingTimeout: 20000,
});

// Live cache. PostgreSQL is the durable source when DATABASE_URL is configured.
const data = { players: {}, lobby: null, invite: {}, games: {}, history: {}, social: { friends: {}, requests: {} } };
// Temporary in-memory account/session store. Passwords are stored only as scrypt hashes.
const accountsByName = new Map();
const accountsByUid = new Map();
const sessions = new Map();
const bannedUids = new Map();
const loginFailures = new Map();
const chatRateLimits = new Map();
const friendRequestRateLimits = new Map();
const roomCreateEvents = new Map();
const adminAudit = [];
const databaseUrl = String(process.env.DATABASE_URL || '').trim();
const pool = databaseUrl ? new Pool({ connectionString: databaseUrl, max: 5, connectionTimeoutMillis: 10000 }) : null;
if (pool) pool.on('error', error => console.error('PostgreSQL pool error:', error.message));
let persistenceReady = false;
let persistQueue = Promise.resolve();
const ADMIN_UID = '__caro_admin__';
const ADMIN_USERNAME = String(process.env.CARO_ADMIN_USERNAME || 'admin').trim().slice(0, 14) || 'admin';
const ADMIN_NAME_KEY = ADMIN_USERNAME.normalize('NFKC').toLocaleLowerCase('vi-VN');
let version = 0;

function persistentSnapshot() {
  return {
    data: clone(data),
    accounts: [...accountsByUid.values()],
    sessions: [...sessions.entries()],
    bans: [...bannedUids.entries()],
    roomCreateEvents: [...roomCreateEvents.entries()],
    adminAudit: adminAudit.slice(-1000),
    version
  };
}

function restorePersistentSnapshot(raw) {
  const saved = typeof raw === 'string' ? JSON.parse(raw) : raw;
  if (!saved || typeof saved !== 'object') throw new Error('Dữ liệu PostgreSQL không hợp lệ.');
  const stored = saved.data && typeof saved.data === 'object' ? saved.data : {};
  Object.assign(data, {
    players: stored.players && typeof stored.players === 'object' ? stored.players : {},
    lobby: stored.lobby == null ? null : stored.lobby,
    invite: stored.invite && typeof stored.invite === 'object' ? stored.invite : {},
    games: stored.games && typeof stored.games === 'object' ? stored.games : {},
    history: stored.history && typeof stored.history === 'object' ? stored.history : {},
    social: stored.social && typeof stored.social === 'object' ? stored.social : { friends: {}, requests: {} }
  });
  if (!data.social.friends || typeof data.social.friends !== 'object') data.social.friends = {};
  if (!data.social.requests || typeof data.social.requests !== 'object') data.social.requests = {};
  accountsByName.clear();
  accountsByUid.clear();
  for (const item of Array.isArray(saved.accounts) ? saved.accounts : []) {
    if (!item || typeof item.uid !== 'string' || typeof item.name !== 'string') continue;
    const account = { ...item };
    account.key = String(account.key || account.name).trim().replace(/\s+/g, ' ').normalize('NFKC').toLocaleLowerCase('vi-VN');
    accountsByUid.set(account.uid, account);
    accountsByName.set(account.key, account.uid);
  }
  sessions.clear();
  for (const item of Array.isArray(saved.sessions) ? saved.sessions : []) {
    if (Array.isArray(item) && item.length === 2 && typeof item[0] === 'string' && typeof item[1] === 'string') {
      if (item[1] === ADMIN_UID || accountsByUid.has(item[1])) sessions.set(item[0], item[1]);
    }
  }
  bannedUids.clear();
  for (const item of Array.isArray(saved.bans) ? saved.bans : []) {
    if (Array.isArray(item) && item.length === 2 && typeof item[0] === 'string' && accountsByUid.has(item[0])) bannedUids.set(item[0], item[1]);
  }
  roomCreateEvents.clear();
  for (const item of Array.isArray(saved.roomCreateEvents) ? saved.roomCreateEvents : []) {
    if (Array.isArray(item) && typeof item[0] === 'string' && Array.isArray(item[1])) roomCreateEvents.set(item[0], item[1].filter(Number.isFinite));
  }
  adminAudit.splice(0, adminAudit.length, ...(Array.isArray(saved.adminAudit) ? saved.adminAudit.slice(-1000) : []));
  version = Math.max(0, Number(saved.version) || 0);
}

async function initializePersistence() {
  if (!pool) {
    console.warn('DATABASE_URL is not configured. Storage is temporary and will reset when the service restarts.');
    return;
  }
  await pool.query(`
    CREATE TABLE IF NOT EXISTS caro_state (
      state_key TEXT PRIMARY KEY,
      payload JSONB NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  const result = await pool.query('SELECT payload FROM caro_state WHERE state_key = $1', ['main']);
  if (result.rows.length) {
    restorePersistentSnapshot(result.rows[0].payload);
    console.log('Loaded persistent Caro data from PostgreSQL.');
  } else {
    await pool.query(
      'INSERT INTO caro_state (state_key, payload) VALUES ($1, $2::jsonb) ON CONFLICT (state_key) DO NOTHING',
      ['main', JSON.stringify(persistentSnapshot())]
    );
    console.log('Initialized empty persistent Caro database.');
  }
  persistenceReady = true;
}

function persistState() {
  if (!pool || !persistenceReady) return Promise.resolve(false);
  const payload = JSON.stringify(persistentSnapshot());
  persistQueue = persistQueue.catch(() => {}).then(async () => {
    await pool.query(
      'INSERT INTO caro_state (state_key, payload, updated_at) VALUES ($1, $2::jsonb, NOW()) ON CONFLICT (state_key) DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW()',
      ['main', payload]
    );
    return true;
  });
  persistQueue.catch(error => console.error('Could not save Caro state to PostgreSQL:', error.message));
  return persistQueue;
}

function schedulePersist() {
  persistState().catch(error => console.error('Persistent save failed:', error.message));
}

function requirePersistentStorage() {
  if (!persistenceReady) {
    throw new Error('Máy chủ chưa kết nối PostgreSQL nên không thể bảo đảm lưu tài khoản. Hãy cấu hình DATABASE_URL trên Render rồi deploy lại.');
  }
}

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
    ok: true, token, uid, name: account.name, role: 'user',
    elo: Number(profile.elo) || 1000,
    w: Number(profile.w) || 0, l: Number(profile.l) || 0, d: Number(profile.d) || 0,
    avatar: String(profile.avatar || '')
  };
}

function makeAdminSession() {
  const token = crypto.randomBytes(32).toString('hex');
  sessions.set(token, ADMIN_UID);
  const profile = data.players[ADMIN_UID] || (data.players[ADMIN_UID] = {name: ADMIN_USERNAME, elo: 1000, w: 0, l: 0, d: 0});
  return {ok: true, token, uid: ADMIN_UID, name: ADMIN_USERNAME, role: 'admin', elo: Number(profile.elo) || 1000, w: 0, l: 0, d: 0};
}

function requireSession(payload) {
  const token = String(payload && payload.sessionToken || '');
  const uid = sessions.get(token);
  if (!uid || (uid !== ADMIN_UID && !accountsByUid.has(uid))) throw new Error('Phiên đăng nhập không hợp lệ. Hãy đăng nhập lại.');
  if (uid !== ADMIN_UID && bannedUids.has(uid)) throw new Error('Tài khoản đã bị khóa quản trị. Hãy liên hệ quản trị viên.');
  return uid;
}

function requireAdmin(payload) {
  const uid = requireSession(payload);
  if (uid !== ADMIN_UID) throw new Error('Bạn không có quyền quản trị.');
  return uid;
}

function publicProfile(uid) {
  if (!uid || uid === ADMIN_UID || !accountsByUid.has(uid)) return null;
  const profile = data.players[uid] || {};
  return {
    uid,
    name: String(profile.name || accountsByUid.get(uid).name || 'Người chơi'),
    avatar: String(profile.avatar || ''),
    elo: Number(profile.elo) || 1000,
    w: Number(profile.w) || 0,
    l: Number(profile.l) || 0,
    d: Number(profile.d) || 0
  };
}

function socialStore() {
  if (!data.social || typeof data.social !== 'object') data.social = { friends: {}, requests: {} };
  if (!data.social.friends || typeof data.social.friends !== 'object') data.social.friends = {};
  if (!data.social.requests || typeof data.social.requests !== 'object') data.social.requests = {};
  return data.social;
}

function recordRateLimit(store, key, limit, windowMs, message) {
  const now = Date.now();
  const recent = (store.get(key) || []).filter(time => time > now - windowMs);
  if (recent.length >= limit) throw new Error(message);
  recent.push(now);
  store.set(key, recent);
  return now;
}

function friendListFor(uid) {
  const social = socialStore();
  const friendIds = Object.keys(social.friends[uid] || {});
  const incomingIds = Object.keys(social.requests[uid] || {});
  const outgoingIds = [];
  for (const [recipientUid, requests] of Object.entries(social.requests)) {
    if (requests && Object.prototype.hasOwnProperty.call(requests, uid) && recipientUid !== uid) outgoingIds.push(recipientUid);
  }
  const mapProfiles = ids => ids.map(publicProfile).filter(Boolean).sort((a, b) => a.name.localeCompare(b.name, 'vi'));
  return {
    ok: true,
    friends: mapProfiles(friendIds),
    incoming: incomingIds.map(fromUid => {
      const profile = publicProfile(fromUid);
      return profile ? { ...profile, at: Number(social.requests[uid][fromUid].at) || 0 } : null;
    }).filter(Boolean).sort((a, b) => b.at - a.at),
    outgoing: mapProfiles(outgoingIds)
  };
}

function notifySocialUser(uid) {
  if (!uid) return;
  for (const client of io.sockets.sockets.values()) {
    if (client.data && client.data.uid === uid) client.emit('social:update', { at: Date.now() });
  }
}

function assertLoginAllowed(key) {
  const record=loginFailures.get(key);
  if(!record)return;
  if(record.lockedUntil>Date.now())throw new Error('Quá nhiều lần đăng nhập thất bại. Hãy thử lại sau 15 phút.');
  if(Date.now()-record.startedAt>15*60*1000)loginFailures.delete(key);
}
function recordLoginFailure(key) {
  const now=Date.now(),current=loginFailures.get(key);
  if(current&&current.lockedUntil>now)return;
  const record=!current||now-current.startedAt>15*60*1000?{startedAt:now,count:0,lockedUntil:0}:current;
  record.count++;
  if(record.count>=5)record.lockedUntil=now+15*60*1000;
  loginFailures.set(key,record);
}
function clearLoginFailures(key){loginFailures.delete(key)}

function hasFiveInMoves(moves, winner) {
  const board = new Map();
  const ordered = Object.entries(moves && typeof moves === 'object' ? moves : {}).sort((a,b)=>Number(a[0])-Number(b[0]));
  for (const [, move] of ordered) {
    if (!move || !Number.isInteger(Number(move.x)) || !Number.isInteger(Number(move.y)) || !Number.isInteger(Number(move.p))) continue;
    const x=Number(move.x),y=Number(move.y),p=Number(move.p);
    if(x<0||x>14||y<0||y>14||p<0||p>1)continue;
    board.set(x+','+y,p);
    if(p===winner){
      for(const [dx,dy] of [[1,0],[0,1],[1,1],[1,-1]]){
        let count=1;
        for(const sign of [1,-1]){
          let xx=x+dx*sign,yy=y+dy*sign;
          while(board.get(xx+','+yy)===p){count++;xx+=dx*sign;yy+=dy*sign}
        }
        if(count>=5)return true;
      }
    }
  }
  return false;
}

function getAdminOverview() {
  const users = [...accountsByUid.values()].map(account => {
    const profile=data.players[account.uid]||{};
    const ban=bannedUids.get(account.uid);
    return {uid:account.uid,name:account.name,createdAt:account.createdAt,elo:Number(profile.elo)||1000,w:Number(profile.w)||0,l:Number(profile.l)||0,d:Number(profile.d)||0,banned:!!ban,banReason:ban&&ban.reason||''};
  }).sort((a,b)=>b.elo-a.elo);
  const matches=[];
  for(const [ownerUid,history] of Object.entries(data.history||{})){
    if(!history||typeof history!=='object')continue;
    for(const [key,record] of Object.entries(history)){
      if(!record||typeof record!=='object')continue;
      matches.push({id:key,timestamp:Number(record.timestamp)||0,playerUid:ownerUid,playerName:String(record.playerName||data.players[ownerUid]?.name||'Người chơi'),opponentUid:String(record.opponentUid||''),opponentName:String(record.opponentName||'Đối thủ'),gameType:String(record.gameType||'Caro'),result:String(record.result||''),eloBefore:Number(record.playerEloBefore)||1000,eloAfter:Number(record.playerEloAfter)||1000,moveCount:Number(record.moveCount)||0,durationMs:Number(record.durationMs)||0});
    }
  }
  matches.sort((a,b)=>b.timestamp-a.timestamp);
  const uniqueMatches=new Map();
  for(const match of matches){
    const id=match.id.startsWith('g_')?match.id:`${match.playerUid}:${match.id}`;
    if(!uniqueMatches.has(id))uniqueMatches.set(id,match);
  }
  const ranked=[...uniqueMatches.values()].filter(m=>m.gameType==='Xếp hạng'&&m.timestamp>Date.now()-24*60*60*1000&&m.opponentUid);
  const pairs=new Map();
  for(const match of ranked){
    const ids=[match.playerUid,match.opponentUid].sort();
    const key=ids.join('|');
    if(!pairs.has(key))pairs.set(key,{uids:ids,games:0,names:[match.playerName,match.opponentName],lastAt:0});
    const pair=pairs.get(key);pair.games++;pair.lastAt=Math.max(pair.lastAt,match.timestamp);
  }
  const alerts=[...pairs.values()].filter(pair=>pair.games>=4).map(pair=>({
    type:'repeat_ranked_pair',uids:pair.uids,games:pair.games,lastAt:pair.lastAt,
    message:`${pair.names.join(' / ')} đã có ${pair.games} trận xếp hạng trong 24 giờ qua; cần kiểm tra thủ công.`
  }));
  for(const [uid,times] of roomCreateEvents.entries()){
    const recent=times.filter(t=>t>Date.now()-5*60*1000);
    roomCreateEvents.set(uid,recent);
    if(recent.length>=10){
      const account=accountsByUid.get(uid);
      alerts.push({type:'room_spam',uids:[uid],games:recent.length,lastAt:Math.max(...recent),message:`${account&&account.name||uid} đã tạo ${recent.length} phòng trong 5 phút; cần kiểm tra thủ công.`});
    }
  }
  alerts.sort((a,b)=>(b.games||0)-(a.games||0));
  return {ok:true,configuredAdmin:!!process.env.CARO_ADMIN_PASSWORD,storageMode:persistenceReady?'postgres':'memory',persistentStorage:persistenceReady,totalAccounts:users.length,bannedAccounts:bannedUids.size,users:users.slice(0,200),recentMatches:matches.slice(0,100),alerts:alerts.slice(0,50),audit:adminAudit.slice(-100).reverse(),volatileStorage:!persistenceReady};
}

function validateGameRootPatch(gid, uid, patch) {
  const game=data.games[gid];
  if(!game||!Array.isArray(game.p)||game.p.length!==2)throw new Error('Không tìm thấy trận đấu hợp lệ.');
  const playerIndex=game.p.findIndex(player=>player&&player.uid===uid);
  if(playerIndex<0)throw new Error('Bạn không thuộc trận đấu này.');
  if(game.status!=='play')throw new Error('Trận đấu đã kết thúc hoặc không còn hoạt động.');
  if(!patch||typeof patch!=='object'||Array.isArray(patch))throw new Error('Dữ liệu nước đi không hợp lệ.');
  if(['p','roomCode','createdAt','ratingResult','drawOffer','drawResponse','drawAgreedAt','chat'].some(field=>Object.prototype.hasOwnProperty.call(patch,field)))throw new Error('Không được sửa thông tin cố định hoặc trạng thái hòa của trận đấu.');

  const moveKeys=Object.keys(patch).filter(key=>key.startsWith('moves/'));
  if(Object.keys(patch).some(key=>key.startsWith('moves/')&&!/^moves\/\d+$/.test(key)))throw new Error('Chỉ được ghi từng nước đi theo thứ tự.');
  if(moveKeys.length>1)throw new Error('Mỗi lượt chỉ được ghi một nước đi.');
  if(moveKeys.length===1){
    const allowedPatchKeys=new Set([moveKeys[0],'turn','dl','status','winner','why']);
    if(Object.keys(patch).some(key=>!allowedPatchKeys.has(key)))throw new Error('Lượt đi chứa trường dữ liệu không được phép.');
    const moveKey=moveKeys[0],index=Number(moveKey.slice(6)),move=patch[moveKey];
    const existing=game.moves&&typeof game.moves==='object'?game.moves:{};
    if(index!==Object.keys(existing).length||Object.prototype.hasOwnProperty.call(existing,String(index)))throw new Error('Nước đi không đúng thứ tự.');
    if(playerIndex!==Number(game.turn)||!move||Number(move.p)!==playerIndex)throw new Error('Chưa đến lượt tài khoản này.');
    const x=Number(move.x),y=Number(move.y);
    if(!Number.isInteger(x)||!Number.isInteger(y)||x<0||x>14||y<0||y>14)throw new Error('Tọa độ nước đi không hợp lệ.');
    if(Object.values(existing).some(m=>m&&Number(m.x)===x&&Number(m.y)===y))throw new Error('Ô này đã có quân.');
    const completeMoves={...existing,[String(index)]:move};
    if(patch.status==='over'){
      if(patch.why!=='win'||Number(patch.winner)!==playerIndex||!hasFiveInMoves(completeMoves,playerIndex))throw new Error('Máy chủ chưa xác nhận nước thắng hợp lệ.');
      if('resignedBy' in patch)throw new Error('Dữ liệu kết thúc trận không hợp lệ.');
    }else{
      if('winner' in patch||'why' in patch||'resignedBy' in patch)throw new Error('Kết quả chỉ được đặt khi ván đấu thực sự kết thúc.');
      if(Number(patch.turn)!==1-playerIndex)throw new Error('Đổi lượt không hợp lệ.');
      const deadline=Number(patch.dl),now=Date.now();
      if(!Number.isFinite(deadline)||deadline<now+25000||deadline>now+35000)throw new Error('Thời hạn lượt đi không hợp lệ.');
    }
    return;
  }

  if(patch.status==='over'&&patch.why==='resign'){
    if(Object.keys(patch).some(key=>!['status','winner','why','resignedBy'].includes(key)))throw new Error('Kết quả đầu hàng chứa trường dữ liệu không được phép.');
    if(patch.resignedBy!==uid||Number(patch.winner)!==1-playerIndex)throw new Error('Kết quả đầu hàng không hợp lệ.');
    if('turn' in patch||'dl' in patch)throw new Error('Không được đổi lượt khi đầu hàng.');
    return;
  }
  if(['status','winner','why','resignedBy','turn','dl'].some(field=>Object.prototype.hasOwnProperty.call(patch,field))) {
    throw new Error('Chỉ máy chủ mới được xử lý timeout; lượt thường phải gửi nước đi hợp lệ.');
  }
  throw new Error('Cập nhật trận đấu không hợp lệ.');
}

function validateGameTransaction(gid,uid,nextValue){
  const game=data.games[gid];
  if(!game||!Array.isArray(game.p)||game.p.length!==2||!game.p.some(player=>player&&player.uid===uid))throw new Error('Bạn không thuộc trận đấu này.');
  if(game.status!=='play'||!nextValue||nextValue.status!=='over'||nextValue.why!=='timeout')throw new Error('Transaction trận đấu không hợp lệ.');
  if(Date.now()<=Number(game.dl||0)+1800||Number(nextValue.winner)!==1-Number(game.turn))throw new Error('Chưa hết giờ hoặc kết quả timeout không hợp lệ.');
  if(JSON.stringify(nextValue.p)!==JSON.stringify(game.p)||JSON.stringify(nextValue.moves||{})!==JSON.stringify(game.moves||{})||Number(nextValue.turn)!==Number(game.turn)||Number(nextValue.dl)!==Number(game.dl))throw new Error('Transaction không được sửa nước đi, người chơi hoặc đồng hồ.');
  const allowed=new Set(['status','winner','why']);
  const keys=new Set([...Object.keys(game),...Object.keys(nextValue)]);
  for(const key of keys)if(!allowed.has(key)&&JSON.stringify(nextValue[key])!==JSON.stringify(game[key]))throw new Error('Transaction chứa thay đổi không được phép.');
}

function assertWriteAccess(path, uid, value, mode='patch') {
  const parts = cleanPath(path);
  if (parts[0] === 'players') {
    if (parts.length < 2 || parts[1] !== uid) throw new Error('Bạn chỉ được xem hoặc sửa hồ sơ của chính mình.');
    throw new Error('Hồ sơ và ELO chỉ được cập nhật qua chức năng máy chủ.');
  }
  if (parts[0] === 'history') {
    if (parts.length < 2 || parts[1] !== uid) throw new Error('Bạn chỉ được sửa lịch sử của chính mình.');
  }
  if(parts[0]==='games'){
    const gid=parts[1];
    if(!gid)throw new Error('Phải chỉ rõ mã trận.');
    const game=data.games[gid];
    if(mode==='set'&&parts.length===2&&!game){
      const p=value&&Array.isArray(value.p)?value.p:[];
      const creator=p[0],opponent=p[1],creatorProfile=creator&&data.players[creator.uid],opponentProfile=opponent&&data.players[opponent.uid];
      const currentTime=Date.now(),deadline=Number(value&&value.dl);
      if(/^\d{4}$/.test(gid)||!value||['ratingResult','drawOffer','drawResponse','drawAgreedAt','chat'].some(field=>Object.prototype.hasOwnProperty.call(value,field))||p.length!==2||!creator||!opponent||creator.uid!==uid||!accountsByUid.has(creator.uid)||!accountsByUid.has(opponent.uid)||creator.uid===opponent.uid||value.status!=='play'||Number(value.turn)!==0||Object.keys(value.moves||{}).length!==0||Number(value.createdAt)<currentTime-10000||Number(value.createdAt)>currentTime+10000||!Number.isFinite(deadline)||deadline<currentTime+25000||deadline>currentTime+35000||creator.name!==accountsByUid.get(creator.uid).name||opponent.name!==accountsByUid.get(opponent.uid).name||Number(creator.elo)!==(Number(creatorProfile&&creatorProfile.elo)||1000)||Number(opponent.elo)!==(Number(opponentProfile&&opponentProfile.elo)||1000))throw new Error('Không được tự tạo hoặc giả mạo trận xếp hạng.');
      return;
    }
    if(parts.length!==2)throw new Error('Chỉ được cập nhật toàn bộ lượt chơi qua thao tác hợp lệ.');
    if(mode==='set')throw new Error('Không được ghi đè trận đã tạo.');
    if(mode==='update'){validateGameRootPatch(gid,uid,value);return}
    if(mode==='transaction'){validateGameTransaction(gid,uid,value);return}
    throw new Error('Thao tác trên trận đấu không được phép.');
  }
  if (parts[0] === 'lobby' && value != null && value.uid && value.uid !== uid) {
    throw new Error('Không thể tạo hàng chờ thay cho tài khoản khác.');
  }
}

app.get('/', (_req, res) => res.status(200).json({ service: 'caro-realtime', status: 'ok', storage: persistenceReady ? 'postgres' : 'memory' }));
app.get('/healthz', (_req, res) => res.status(200).json({ status: 'ok', persistentStorage: persistenceReady }));

function assertReadAccess(path, payload) {
  const parts = cleanPath(path);
  if (parts[0] === 'games') {
    const uid = requireSession(payload);
    const game = parts[1] && data.games[parts[1]];
    if (parts.length < 2 || !game || !Array.isArray(game.p) || !game.p.some(player => player && player.uid === uid)) {
      throw new Error('Chỉ người chơi trong ván mới được xem dữ liệu và chat của trận đó.');
    }
  }
  if (parts[0] === 'invite') {
    const uid = requireSession(payload);
    if (parts.length < 2 || parts[1] !== uid) throw new Error('Chỉ được đọc lời mời của chính tài khoản.');
  }
}

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
  schedulePersist();
}

function reply(ack, result) {
  if (typeof ack === 'function') ack(result);
}

io.on('connection', socket => {
  socket.data.subscriptions = new Map();

  socket.on('auth:register', async (payload, ack) => {
    try {
      requirePersistentStorage();
      const { name, key } = parseAccountName(payload && payload.name);
      const password = String(payload && payload.password || '');
      if (password.length < 8 || Buffer.byteLength(password, 'utf8') > 128) {
        throw new Error('Mật khẩu phải có ít nhất 8 ký tự và tối đa 128 byte.');
      }
      if (key === ADMIN_NAME_KEY) throw new Error('Tên này được dành riêng cho quản trị viên.');
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
      const session = makeAccountSession(uid);
      socket.data.uid = uid;
      await persistState();
      reply(ack, session);
    } catch (error) {
      reply(ack, { ok: false, error: error.message || 'Không thể đăng ký tài khoản.' });
    }
  });

  socket.on('auth:login', async (payload, ack) => {
    let throttleKey=String(socket.handshake.address||'unknown')+'|invalid';
    try {
      requirePersistentStorage();
      const { key } = parseAccountName(payload && payload.name);
      throttleKey=String(socket.handshake.address||'unknown')+'|'+key;
      assertLoginAllowed(throttleKey);
      const password = String(payload && payload.password || '');
      if (key === ADMIN_NAME_KEY) {
        const configuredPassword = String(process.env.CARO_ADMIN_PASSWORD || '');
        if (!configuredPassword) throw new Error('Quản trị viên chưa được cấu hình CARO_ADMIN_PASSWORD trên Render.');
        const suppliedHash=crypto.createHash('sha256').update(password).digest();
        const expectedHash=crypto.createHash('sha256').update(configuredPassword).digest();
        if(!password || !crypto.timingSafeEqual(suppliedHash,expectedHash)) throw new Error('Tên đăng nhập hoặc mật khẩu không đúng.');
        clearLoginFailures(throttleKey);
        const session = makeAdminSession();
        socket.data.uid = session.uid;
        await persistState();
        reply(ack, session);
        return;
      }
      const uid = accountsByName.get(key);
      const account = uid && accountsByUid.get(uid);
      if (!account || Buffer.byteLength(password, 'utf8') > 128) throw new Error('Tên đăng nhập hoặc mật khẩu không đúng.');
      if (bannedUids.has(uid)) throw new Error('Tài khoản đã bị khóa quản trị. Hãy liên hệ quản trị viên.');
      const salt = Buffer.from(account.salt, 'hex');
      const storedHash = Buffer.from(account.passwordHash, 'hex');
      const suppliedHash = crypto.scryptSync(password, salt, storedHash.length);
      if (storedHash.length !== suppliedHash.length || !crypto.timingSafeEqual(storedHash, suppliedHash)) {
        throw new Error('Tên đăng nhập hoặc mật khẩu không đúng.');
      }
      clearLoginFailures(throttleKey);
      const session = makeAccountSession(uid);
      socket.data.uid = uid;
      await persistState();
      reply(ack, session);
    } catch (error) {
      recordLoginFailure(throttleKey);
      reply(ack, { ok: false, error: error.message || 'Không thể đăng nhập.' });
    }
  });

  socket.on('auth:session', (payload, ack) => {
    try {
      requirePersistentStorage();
      const uid = requireSession(payload);
      socket.data.uid = uid;
      const profile = data.players[uid] || {};
      const account = uid === ADMIN_UID ? {name:ADMIN_USERNAME} : accountsByUid.get(uid);
      reply(ack, {
        ok: true, uid, name: account.name, role:uid===ADMIN_UID?'admin':'user',
        elo: Number(profile.elo) || 1000,
        w: Number(profile.w) || 0, l: Number(profile.l) || 0, d: Number(profile.d) || 0,
        avatar: String(profile.avatar || '')
      });
    } catch (error) {
      reply(ack, { ok: false, error: error.message || 'Phiên đăng nhập không hợp lệ.' });
    }
  });

  socket.on('auth:logout', async (payload, ack) => {
    const token = String(payload && payload.sessionToken || '');
    const uid = sessions.get(token);
    sessions.delete(token);
    if (socket.data.uid === uid) delete socket.data.uid;
    try { await persistState(); reply(ack, { ok: true }); }
    catch (error) { reply(ack, { ok: false, error: error.message || 'Không thể lưu trạng thái đăng xuất.' }); }
  });

  socket.on('profile:update', async (payload, ack) => {
    try {
      const uid = requireSession(payload);
      if (uid === ADMIN_UID) throw new Error('Không thể sửa ảnh của tài khoản quản trị.');
      const avatar = String(payload && payload.avatar || '');
      if (avatar && (!/^data:image\/jpeg;base64,[A-Za-z0-9+/]+={0,2}$/.test(avatar) || avatar.length > 100000 || Buffer.from(avatar.slice(avatar.indexOf(',') + 1), 'base64').length > 72 * 1024)) {
        throw new Error('Ảnh phải là JPEG đã thu nhỏ dưới 72 KB. Hãy chọn ảnh khác.');
      }
      const profile = data.players[uid] || (data.players[uid] = { name: accountsByUid.get(uid).name, elo: 1000, w: 0, l: 0, d: 0 });
      profile.avatar = avatar;
      version++;
      notifySubscribers();
      notifySocialUser(uid);
      const social = socialStore();
      for (const friendUid of Object.keys(social.friends[uid] || {})) notifySocialUser(friendUid);
      await persistState();
      reply(ack, { ok: true, avatar });
    } catch (error) {
      reply(ack, { ok: false, error: error.message || 'Không thể cập nhật ảnh đại diện.' });
    }
  });

  socket.on('friends:search', (payload, ack) => {
    try {
      const uid = requireSession(payload);
      const query = String(payload && payload.query || '').trim().replace(/\s+/g, ' ').toLocaleLowerCase('vi-VN');
      if (query.length < 2) throw new Error('Nhập ít nhất 2 ký tự để tìm người chơi.');
      const social = socialStore();
      const results = [...accountsByUid.keys()]
        .filter(targetUid => targetUid !== ADMIN_UID)
        .map(targetUid => publicProfile(targetUid))
        .filter(profile => profile && profile.name.toLocaleLowerCase('vi-VN').includes(query))
        .sort((a, b) => {
          const aExact = a.name.toLocaleLowerCase('vi-VN') === query ? 1 : 0;
          const bExact = b.name.toLocaleLowerCase('vi-VN') === query ? 1 : 0;
          return bExact - aExact || a.name.localeCompare(b.name, 'vi');
        })
        .slice(0, 12)
        .map(profile => ({
          ...profile,
          relationship: profile.uid === uid ? 'self'
            : social.friends[uid] && social.friends[uid][profile.uid] ? 'friend'
            : social.requests[uid] && social.requests[uid][profile.uid] ? 'incoming'
            : social.requests[profile.uid] && social.requests[profile.uid][uid] ? 'outgoing'
            : 'none'
        }));
      reply(ack, { ok: true, results });
    } catch (error) {
      reply(ack, { ok: false, error: error.message || 'Không tìm được người chơi.' });
    }
  });

  socket.on('friends:list', (payload, ack) => {
    try {
      const uid = requireSession(payload);
      reply(ack, friendListFor(uid));
    } catch (error) {
      reply(ack, { ok: false, error: error.message || 'Không tải được danh sách bạn bè.' });
    }
  });

  socket.on('friends:request', async (payload, ack) => {
    try {
      const uid = requireSession(payload);
      const targetUid = String(payload && payload.targetUid || '');
      if (!targetUid || targetUid === uid || targetUid === ADMIN_UID || !accountsByUid.has(targetUid)) throw new Error('Không tìm thấy tài khoản để kết bạn.');
      recordRateLimit(friendRequestRateLimits, uid, 12, 60 * 1000, 'Bạn gửi lời mời quá nhanh. Hãy thử lại sau.');
      const social = socialStore();
      if (social.friends[uid] && social.friends[uid][targetUid]) throw new Error('Hai người đã là bạn bè.');
      if (social.requests[uid] && social.requests[uid][targetUid]) throw new Error('Người này đã gửi lời mời cho bạn. Hãy chấp nhận trong danh sách yêu cầu.');
      if (social.requests[targetUid] && social.requests[targetUid][uid]) {
        reply(ack, { ok: true, alreadySent: true });
        return;
      }
      const pending = social.requests[targetUid] || {};
      if (Object.keys(pending).length >= 100) throw new Error('Người chơi này đang có quá nhiều lời mời chưa xử lý.');
      if (!social.requests[targetUid]) social.requests[targetUid] = {};
      social.requests[targetUid][uid] = { at: Date.now() };
      version++;
      notifySocialUser(targetUid);
      notifySocialUser(uid);
      await persistState();
      reply(ack, { ok: true });
    } catch (error) {
      reply(ack, { ok: false, error: error.message || 'Không gửi được lời mời kết bạn.' });
    }
  });

  socket.on('friends:respond', async (payload, ack) => {
    try {
      const uid = requireSession(payload);
      const targetUid = String(payload && payload.targetUid || '');
      const accept = payload && payload.accept === true;
      const social = socialStore();
      if (!targetUid || targetUid === uid || !social.requests[uid] || !social.requests[uid][targetUid]) throw new Error('Không tìm thấy lời mời kết bạn này.');
      delete social.requests[uid][targetUid];
      if (!Object.keys(social.requests[uid]).length) delete social.requests[uid];
      if (accept) {
        if (!social.friends[uid]) social.friends[uid] = {};
        if (!social.friends[targetUid]) social.friends[targetUid] = {};
        const since = Date.now();
        social.friends[uid][targetUid] = { since };
        social.friends[targetUid][uid] = { since };
      }
      if (social.requests[targetUid]) {
        delete social.requests[targetUid][uid];
        if (!Object.keys(social.requests[targetUid]).length) delete social.requests[targetUid];
      }
      version++;
      notifySocialUser(uid);
      notifySocialUser(targetUid);
      await persistState();
      reply(ack, { ok: true, accepted: accept });
    } catch (error) {
      reply(ack, { ok: false, error: error.message || 'Không xử lý được lời mời kết bạn.' });
    }
  });

  socket.on('friends:remove', async (payload, ack) => {
    try {
      const uid = requireSession(payload);
      const targetUid = String(payload && payload.targetUid || '');
      const social = socialStore();
      if (!targetUid || targetUid === uid || !(social.friends[uid] && social.friends[uid][targetUid])) throw new Error('Người chơi này không nằm trong danh sách bạn bè.');
      delete social.friends[uid][targetUid];
      delete social.friends[targetUid]?.[uid];
      if (!Object.keys(social.friends[uid]).length) delete social.friends[uid];
      if (social.friends[targetUid] && !Object.keys(social.friends[targetUid]).length) delete social.friends[targetUid];
      version++;
      notifySocialUser(uid);
      notifySocialUser(targetUid);
      await persistState();
      reply(ack, { ok: true });
    } catch (error) {
      reply(ack, { ok: false, error: error.message || 'Không thể xóa bạn bè.' });
    }
  });

  socket.on('chat:send', async (payload, ack) => {
    try {
      const uid = requireSession(payload);
      const gid = String(payload && payload.gid || '');
      const text = String(payload && payload.text || '').replace(/[\u0000-\u001f\u007f]/g, ' ').trim();
      const game = data.games[gid];
      if (!game || game.status !== 'play' || !Array.isArray(game.p) || game.p.length !== 2) throw new Error('Chat chỉ dùng trong ván đang diễn ra.');
      const player = game.p.find(item => item && item.uid === uid);
      if (!player) throw new Error('Bạn không thuộc trận đấu này.');
      if (!text || text.length > 240) throw new Error('Tin nhắn phải dài từ 1 đến 240 ký tự.');
      const rate = chatRateLimits.get(uid) || { times: [], lastAt: 0 };
      const now = Date.now();
      rate.times = rate.times.filter(time => time > now - 10 * 1000);
      if (now - rate.lastAt < 500 || rate.times.length >= 8) throw new Error('Bạn gửi tin nhắn quá nhanh. Hãy chờ một chút.');
      rate.lastAt = now;
      rate.times.push(now);
      chatRateLimits.set(uid, rate);
      const profile = data.players[uid] || {};
      const message = {
        id: crypto.randomBytes(8).toString('hex'),
        uid,
        name: String(player.name || (accountsByUid.get(uid) && accountsByUid.get(uid).name) || 'Người chơi'),
        avatar: String(profile.avatar || ''),
        text,
        at: now
      };
      if (!Array.isArray(game.chat)) game.chat = [];
      game.chat.push(message);
      if (game.chat.length > 100) game.chat.splice(0, game.chat.length - 100);
      version++;
      notifySubscribers();
      await persistState();
      reply(ack, { ok: true, message });
    } catch (error) {
      reply(ack, { ok: false, error: error.message || 'Không gửi được tin nhắn.' });
    }
  });

  socket.on('draw:offer', async (payload, ack) => {
    try {
      const uid = requireSession(payload);
      const gid = String(payload && payload.gid || '');
      const game = data.games[gid];
      if (!game || game.status !== 'play' || !Array.isArray(game.p) || game.p.length !== 2) {
        throw new Error('Không tìm thấy trận đang diễn ra để xin hòa.');
      }
      const player = game.p.find(item => item && item.uid === uid);
      if (!player) throw new Error('Bạn không thuộc trận đấu này.');
      if (game.drawOffer) {
        throw new Error(game.drawOffer.uid === uid ? 'Bạn đã gửi lời hòa, đang chờ đối thủ trả lời.' : 'Đối thủ đã gửi lời hòa. Hãy chọn chấp nhận hoặc từ chối.');
      }
      game.drawOffer = {
        uid,
        name: String(player.name || (accountsByUid.get(uid) && accountsByUid.get(uid).name) || 'Đối thủ'),
        at: Date.now()
      };
      game.drawResponse = null;
      version++;
      notifySubscribers();
      await persistState();
      reply(ack, { ok: true, gid, drawOffer: game.drawOffer });
    } catch (error) {
      reply(ack, { ok: false, error: error.message || 'Không thể gửi lời xin hòa.' });
    }
  });

  socket.on('draw:respond', async (payload, ack) => {
    try {
      const uid = requireSession(payload);
      const gid = String(payload && payload.gid || '');
      const accept = payload && payload.accept === true;
      const game = data.games[gid];
      if (!game || game.status !== 'play' || !Array.isArray(game.p) || game.p.length !== 2) {
        throw new Error('Trận đấu không còn hoạt động.');
      }
      if (!game.p.some(item => item && item.uid === uid)) throw new Error('Bạn không thuộc trận đấu này.');
      const offer = game.drawOffer;
      if (!offer || !offer.uid || offer.uid === uid) throw new Error('Không có lời xin hòa từ đối thủ để phản hồi.');
      const now = Date.now();
      game.drawOffer = null;
      game.drawResponse = { offerUid: offer.uid, uid, accepted: accept, at: now };
      if (accept) {
        game.status = 'over';
        game.winner = -1;
        game.why = 'draw';
        game.drawAgreedAt = now;
      }
      version++;
      notifySubscribers();
      await persistState();
      reply(ack, { ok: true, accepted: accept, gid });
    } catch (error) {
      reply(ack, { ok: false, error: error.message || 'Không thể phản hồi lời xin hòa.' });
    }
  });

  socket.on('admin:overview', (payload, ack) => {
    try { requireAdmin(payload); reply(ack, getAdminOverview()); }
    catch(error) { reply(ack, {ok:false,error:error.message||'Không thể đọc bảng quản trị.'}); }
  });

  socket.on('admin:ban', async (payload, ack) => {
    try {
      requireAdmin(payload);
      const uid=String(payload&&payload.uid||'').trim();
      const shouldBan=payload&&payload.banned===true;
      if(!uid||uid===ADMIN_UID||!accountsByUid.has(uid))throw new Error('Không tìm thấy tài khoản để thao tác.');
      if(shouldBan){
        const reason=String(payload&&payload.reason||'Vi phạm quy định').trim().slice(0,200)||'Vi phạm quy định';
        bannedUids.set(uid,{reason,at:Date.now()});
        adminAudit.push({at:Date.now(),action:'ban',targetUid:uid,targetName:accountsByUid.get(uid).name,reason});
      }else{
        bannedUids.delete(uid);
        adminAudit.push({at:Date.now(),action:'unban',targetUid:uid,targetName:accountsByUid.get(uid).name});
      }
      await persistState();
      reply(ack,{ok:true,uid,banned:bannedUids.has(uid)});
    }catch(error){reply(ack,{ok:false,error:error.message||'Không thể khóa tài khoản.'})}
  });

  socket.on('ranked:finish', async (payload, ack) => {
    try {
      const callerUid=requireSession(payload);
      const gid=String(payload&&payload.gid||'');
      const game=data.games[gid];
      if(!game||!Array.isArray(game.p)||game.p.length!==2)throw new Error('Không tìm thấy trận xếp hạng.');
      if(/^\d{4}$/.test(gid)||game.roomCode)throw new Error('Đấu phòng riêng không được cộng hoặc trừ ELO.');
      const callerIndex=game.p.findIndex(player=>player&&player.uid===callerUid);
      if(callerIndex<0)throw new Error('Bạn không thuộc trận này.');
      const winnerIndex=Number(payload&&payload.winnerIndex);
      if(!Number.isInteger(winnerIndex)||![-1,0,1].includes(winnerIndex))throw new Error('Kết quả trận không hợp lệ.');
      if(game.status!=='over'||Number(game.winner)!==winnerIndex)throw new Error('Máy chủ chưa xác nhận trạng thái kết thúc trận.');
      if(game.ratingResult){
        const profile=data.players[callerUid]||{};
        const prior=game.ratingResult.playerResults[callerUid];
        reply(ack,{ok:true,elo:Number(profile.elo)||1000,w:Number(profile.w)||0,l:Number(profile.l)||0,d:Number(profile.d)||0,delta:prior?prior.elo-prior.before:0,alreadyApplied:true});
        return;
      }
      const reason=String(payload&&payload.reason||game.why||'');
      if(reason==='win'){
        if(!hasFiveInMoves(game.moves,winnerIndex))throw new Error('Máy chủ chưa xác nhận đủ 5 quân liên tiếp.');
      }else if(reason==='resign'){
        if(winnerIndex===-1||!game.resignedBy||game.resignedBy!==game.p[1-winnerIndex]?.uid)throw new Error('Kết quả đầu hàng không hợp lệ.');
      }else if(reason==='timeout'){
        if(Date.now()<=Number(game.dl||0)+1800||winnerIndex!==1-Number(game.turn))throw new Error('Thời gian trận chưa hết hợp lệ.');
      }else if(reason!=='draw')throw new Error('Lý do kết thúc trận không hợp lệ.');

      const playerResults={};
      const winnerUid=winnerIndex===-1?null:game.p[winnerIndex].uid;
      const beforeRatings=game.p.map(player=>Number(data.players[player.uid]&&data.players[player.uid].elo)||Number(player.elo)||1000);
      for(let i=0;i<2;i++){
        const player=game.p[i],profile=data.players[player.uid]||(data.players[player.uid]={name:player.name||'Người chơi',elo:1000,w:0,l:0,d:0});
        const before=beforeRatings[i];
        const score=winnerIndex===-1?0.5:winnerIndex===i?1:0;
        const expected=1/(1+10**((beforeRatings[1-i]-before)/400));
        const after=Math.max(100,Math.round(before+32*(score-expected)));
        profile.name=player.name||profile.name||'Người chơi';
        profile.elo=after;
        if(score===1)profile.w=(Number(profile.w)||0)+1;
        else if(score===0)profile.l=(Number(profile.l)||0)+1;
        else profile.d=(Number(profile.d)||0)+1;
        playerResults[player.uid]={elo:after,w:profile.w,l:profile.l,d:profile.d,before};
      }
      game.ratingResult={winnerUid,reason,at:Date.now(),playerResults};
      version++;notifySubscribers();
      await persistState();
      const result=playerResults[callerUid];
      reply(ack,{ok:true,elo:result.elo,w:result.w,l:result.l,d:result.d,delta:result.elo-result.before,alreadyApplied:false});
    }catch(error){reply(ack,{ok:false,error:error.message||'Không thể cập nhật ELO xếp hạng.'})}
  });

  socket.on('room:create', async (payload, ack) => {
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
      const createdAt=Date.now(),recent=(roomCreateEvents.get(uid)||[]).filter(t=>t>createdAt-5*60*1000);
      recent.push(createdAt);roomCreateEvents.set(uid,recent);
      if(recent.length===10)adminAudit.push({at:createdAt,action:'room_spam_warning',targetUid:uid,targetName:name,count:recent.length,reason:'10 phòng được tạo trong vòng 5 phút'});
      data.games[code] = { roomCode: code, p: [{ uid, name, elo }], moves: {}, turn: 0, status: 'waiting', dl: 0, createdAt };
      version++;
      notifySubscribers();
      await persistState();
      reply(ack, { ok: true, code });
    } catch (error) { reply(ack, { ok: false, error: error.message || 'Không tạo được phòng.' }); }
  });

  socket.on('room:join', async (payload, ack) => {
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
      await persistState();
      reply(ack, { ok: true, code });
    } catch (error) { reply(ack, { ok: false, error: error.message || 'Không thể vào phòng.' }); }
  });

  socket.on('db:read', (payload, ack) => {
    try {
      const path = String(payload && payload.path || '');
      const query = payload && payload.query || {};
      assertReadAccess(path, payload);
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
      if (payload && payload.sessionToken) socket.data.uid = requireSession(payload);
      assertReadAccess(path, payload);
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

  socket.on('db:write', async (payload, ack) => {
    try {
      const op = String(payload && payload.op || '');
      const path = String(payload && payload.path || '');
      const uid = requireSession(payload);
      assertWriteAccess(path, uid, payload.data, op);
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
      await persistState();
      reply(ack, { ok: true, version });
    } catch (error) {
      reply(ack, { ok: false, error: error.message || 'Lỗi ghi dữ liệu.' });
    }
  });

  socket.on('db:commit', async (payload, ack) => {
    try {
      const path = String(payload && payload.path || '');
      const uid = requireSession(payload);
      assertWriteAccess(path, uid, payload.value, 'transaction');
      cleanPath(path);
      if (Number(payload && payload.baseVersion) !== version) {
        reply(ack, { ok: true, committed: false, version });
        return;
      }
      setAt(path, payload.value);
      version++;
      notifySubscribers();
      await persistState();
      reply(ack, { ok: true, committed: true, version, data: getAt(path) });
    } catch (error) {
      reply(ack, { ok: false, error: error.message || 'Lỗi transaction.' });
    }
  });
});

const port = Number(process.env.PORT) || 10000;

async function startServer() {
  try {
    await initializePersistence();
    server.listen(port, '0.0.0.0', () => {
      console.log(`Caro realtime server listening on 0.0.0.0:${port}`);
    });
  } catch (error) {
    console.error('Could not initialize Caro PostgreSQL storage:', error.message);
    if (pool) await pool.end().catch(() => {});
    process.exitCode = 1;
  }
}

async function shutdown() {
  try { await persistState(); } catch (error) { console.error('Final state save failed:', error.message); }
  if (pool) await pool.end().catch(() => {});
  server.close(() => process.exit(0));
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

startServer();
