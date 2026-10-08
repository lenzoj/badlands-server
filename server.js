// Badlands multiplayer server: relays player presence inside rooms (lobby, parties) and keeps a small
// shared database (trophies, tournaments, the hourly chest, cloud saves) in data.json.
// Start: npm install && npm start      (listens on PORT, default 8080)
const http = require('http');
const fs = require('fs');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const DATA = process.env.DATA_FILE || __dirname + '/data.json';
let db = {}; try { db = JSON.parse(fs.readFileSync(DATA, 'utf8')); } catch (e) { db = {}; }
let dirty = false;
setInterval(() => { if (!dirty) return; dirty = false; fs.writeFile(DATA, JSON.stringify(db), () => {}); }, 5000);

const server = http.createServer((req, res) => { res.writeHead(200, { 'content-type': 'text/plain', 'access-control-allow-origin': '*' }); res.end('Badlands server is running'); });
const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 });
const rooms = new Map(); // room name -> Map(peerId -> {ws, presence})
const subs = new Map();  // db path -> Set(ws)
let nextId = 1;

function send(ws, m) { if (ws.readyState === 1) ws.send(JSON.stringify(m)); }
function roomOf(name) { let r = rooms.get(name); if (!r) { r = new Map(); rooms.set(name, r); } return r; }
function leave(ws, name) {
  const r = rooms.get(name); if (!r || !r.has(ws.pid)) return; r.delete(ws.pid);
  for (const o of r.values()) send(o.ws, { t: 'gone', room: name, peer: ws.pid });
  if (!r.size) rooms.delete(name);
}
const okPath = p => typeof p === 'string' && p.length < 200 && /^[\w\/\-]+$/.test(p);
function collectionDocs(coll) { const pre = coll + '/', out = []; for (const k in db) if (k.startsWith(pre) && !k.slice(pre.length).includes('/')) out.push({ id: k.slice(pre.length), data: db[k] }); return out; }

wss.on('connection', ws => {
  ws.pid = 'p' + (nextId++) + Math.random().toString(36).slice(2, 6); ws.rooms = new Set(); ws.alive = true;
  send(ws, { t: 'hello', peer: ws.pid });
  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (m.t === 'join' && typeof m.room === 'string' && m.room.length < 64) {
      const r = roomOf(m.room); r.set(ws.pid, { ws, presence: {} }); ws.rooms.add(m.room);
      send(ws, { t: 'joined', room: m.room, id: m.id, peers: [...r.entries()].filter(([k]) => k !== ws.pid).map(([k, v]) => ({ peer: k, presence: v.presence })) });
    } else if (m.t === 'leave') { leave(ws, m.room); ws.rooms.delete(m.room); }
    else if (m.t === 'pres' && ws.rooms.has(m.room) && m.data && typeof m.data === 'object') {
      const r = rooms.get(m.room), me = r && r.get(ws.pid); if (!me) return; me.presence = m.data;
      for (const [k, o] of r) if (k !== ws.pid) send(o.ws, { t: 'pres', room: m.room, peer: ws.pid, data: m.data });
    } else if (m.t === 'get' && okPath(m.path)) send(ws, { t: 'res', id: m.id, data: db[m.path] === undefined ? null : db[m.path] });
    else if (m.t === 'set' && okPath(m.path) && m.data && typeof m.data === 'object') {
      if (JSON.stringify(m.data).length > 200000) { send(ws, { t: 'res', id: m.id, err: 'invalid_argument' }); return; }
      db[m.path] = m.data; dirty = true; send(ws, { t: 'res', id: m.id, ok: 1 });
      for (const s of subs.get(m.path) || []) send(s, { t: 'snap', path: m.path, data: m.data });
    } else if (m.t === 'list' && okPath(m.coll) && !m.coll.startsWith('data')) send(ws, { t: 'res', id: m.id, docs: collectionDocs(m.coll).slice(0, Math.min(500, m.limit || 500)) });
    else if (m.t === 'sub' && okPath(m.path)) { let s = subs.get(m.path); if (!s) { s = new Set(); subs.set(m.path, s); } s.add(ws); send(ws, { t: 'snap', path: m.path, data: db[m.path] === undefined ? null : db[m.path] }); }
  });
  ws.on('close', () => { for (const r of ws.rooms) leave(ws, r); for (const s of subs.values()) s.delete(ws); });
});
setInterval(() => { for (const ws of wss.clients) { if (!ws.alive) { ws.terminate(); continue; } ws.alive = false; ws.ping(); } }, 20000);
server.listen(PORT, () => console.log('Badlands server on port ' + PORT));
