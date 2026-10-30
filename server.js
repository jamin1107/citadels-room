/* 富饶之城 · 在线房间服务（Node 版，可部署到 Render / 自己服务器）
 *
 * 与 Cloudflare Worker 版协议完全相同：
 *   POST /api/room   创建房间，返回 4 位房间号
 *   GET  /ws?room=XXXX&host=1&count=4   房主
 *   GET  /ws?room=XXXX&resume=1         客人 / 断线重连
 *   GET  /health     健康检查
 */
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT) || 8788;
const MAX_SEATS = 4;
const ROOM_TTL = 6 * 60 * 60 * 1000;

const rooms = new Map();

function roomCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let out = '';
  for (let i = 0; i < 4; i++) out += chars[Math.floor(Math.random() * chars.length)];
  return out;
}

function seatsOf(room) {
  return Array.from(room.sockets.keys()).sort((a, b) => a - b);
}
function send(ws, obj) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}
function broadcast(room, obj, exceptSeat) {
  for (const [seat, ws] of room.sockets) {
    if (seat === exceptSeat) continue;
    send(ws, obj);
  }
}
function sendTo(room, seat, obj) {
  const ws = room.sockets.get(seat);
  if (!ws) return false;
  send(ws, obj);
  return true;
}

function cors() {
  return {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'content-type',
    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
  };
}
function json(res, obj, status = 200) {
  const body = JSON.stringify(obj);
  res.writeHead(status, Object.assign({ 'content-type': 'application/json; charset=utf-8' }, cors()));
  res.end(body);
}

function expire(room) {
  clearTimeout(room.timer);
  room.timer = setTimeout(() => {
    for (const ws of room.sockets.values()) {
      try { send(ws, { t: 'closed', message: '房间已过期' }); ws.close(1000, 'expired'); } catch (e) { /* ignore */ }
    }
    rooms.delete(room.code);
  }, ROOM_TTL);
  if (room.timer.unref) room.timer.unref();
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://localhost');
  if (req.method === 'OPTIONS') { res.writeHead(204, cors()); return res.end(); }

  if (url.pathname === '/' || url.pathname === '/health') {
    res.writeHead(200, Object.assign({ 'content-type': 'text/plain; charset=utf-8' }, cors()));
    return res.end('citadels room service ok');
  }

  if (url.pathname === '/api/room') {
    if (req.method !== 'POST') return json(res, { error: '只支持 POST' }, 405);
    let raw = '';
    req.on('data', (c) => { raw += c; });
    req.on('end', () => {
      let count = 4;
      try { count = Number(JSON.parse(raw || '{}').count) || 4; } catch (e) { /* ignore */ }
      count = Math.min(MAX_SEATS, Math.max(3, count));
      let code = roomCode();
      for (let i = 0; i < 20 && rooms.has(code); i++) code = roomCode();
      const room = { code, count, sockets: new Map(), createdAt: Date.now() };
      expire(room);
      rooms.set(code, room);
      json(res, { code });
    });
    return;
  }

  json(res, { error: 'not found' }, 404);
});

const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, 'http://localhost');
  if (url.pathname !== '/ws') return socket.destroy();

  const code = (url.searchParams.get('room') || '').toUpperCase();
  if (!/^[A-Z0-9]{3,8}$/.test(code)) return socket.destroy();

  const room = rooms.get(code);
  const current = room ? seatsOf(room) : [];
  const host = url.searchParams.get('host') === '1';
  const resume = url.searchParams.get('resume');

  let seat = null;
  if (room) {
    if (host) {
      if (!current.includes(0)) seat = 0;
    } else if (resume != null && !current.includes(Number(resume)) && Number(resume) >= 1 && Number(resume) < room.count) {
      seat = Number(resume);
    } else {
      for (let i = 1; i < room.count; i++) {
        if (!current.includes(i)) { seat = i; break; }
      }
    }
  }

  wss.handleUpgrade(req, socket, head, (ws) => {
    if (!room) {
      send(ws, { t: 'error', message: '房间不存在或已过期' });
      send(ws, { t: 'closed', message: '房间不存在或已过期' });
      return ws.close(1000, 'no-room');
    }
    if (seat == null) {
      send(ws, { t: 'error', message: host ? '这个房间号已被占用，请重新创建' : '房间坐满了，等下一局吧' });
      send(ws, { t: 'closed', message: '无法入座' });
      return ws.close(1000, 'rejected');
    }

    expire(room);
    room.sockets.set(seat, ws);
    const seats = seatsOf(room);
    send(ws, { t: 'room', room: code, seat, count: room.count, seats });
    broadcast(room, { t: 'seats', seats }, seat);
    if (seat !== 0) sendTo(room, 0, { t: 'peer', event: 'join', seat });

    ws.on('message', (buf) => {
      let msg;
      try { msg = JSON.parse(buf.toString()); } catch (e) { return; }
      if (!msg || typeof msg !== 'object') return;
      if (msg.t === 'pulse') return send(ws, { t: 'pulse' });
      if (msg.t === 'relay' && typeof msg.to === 'number') {
        if (!sendTo(room, msg.to, { t: 'payload', from: seat, payload: msg.payload })) {
          send(ws, { t: 'peer', event: 'gone', seat: msg.to });
        }
        return;
      }
      if (msg.t === 'leave') ws.close(1000, 'left');
    });

    ws.on('close', () => {
      if (room.sockets.get(seat) !== ws) return; // 同座位已重连，不算掉线
      room.sockets.delete(seat);
      const left = seatsOf(room);
      if (!left.length) {
        clearTimeout(room.timer);
        rooms.delete(code);
        return;
      }
      broadcast(room, { t: 'peer', event: 'leave', seat, seats: left }, seat);
    });

    ws.on('error', () => { /* close 会跟着触发 */ });
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('citadels room service listening on ' + PORT);
});
