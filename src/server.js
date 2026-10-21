import http from 'node:http';
import fs from 'node:fs';
import crypto from 'node:crypto';
import { MemoryStore, STRATEGIES } from './store-memory.js';

const env = (k, d) => process.env[k] ?? d;
const PORT = +env('PORT', 3000), ROLE = env('ROLE', 'all');            // all | api | worker
const DEFAULT_STRATEGY = env('STRATEGY', 'lua');
const WAITING_ROOM = env('WAITING_ROOM', 'off') === 'on';
const ALLOW_OVERRIDE = env('ALLOW_STRATEGY_OVERRIDE', 'true') === 'true'; // benchmark only; disable in prod
const RL_BURST = +env('RL_BURST', 10), RL_RATE = +env('RL_RATE', 5);     // per-user token bucket
const SECRET = env('PASS_SECRET', 'dev-secret'), ADMIN = env('ADMIN_TOKEN', 'dev-admin');
const INITIAL_STOCK = +env('INITIAL_STOCK', 500);

const store = env('REDIS_URL', '')
  ? new (await import('./store-redis.js')).RedisStore(env('REDIS_URL'))
  : new MemoryStore();
await store.start({ worker: ROLE !== 'api' });
if (ROLE !== 'api' && !env('KEEP_STATE', '')) await store.reset(INITIAL_STOCK);
if (ROLE === 'worker') { console.log('worker running'); await new Promise(() => {}); }

const sign = (u, n) => crypto.createHmac('sha256', SECRET).update(`${u}:${n}`).digest('hex').slice(0, 16);
const safeEq = (a, b) => a.length === b.length && crypto.timingSafeEqual(Buffer.from(a), Buffer.from(b));
const readBody = req => new Promise(r => { let d = ''; req.on('data', c => { d += c; if (d.length > 1e4) req.destroy(); });
  req.on('end', () => { try { r(JSON.parse(d || '{}')); } catch { r({}); } }); });
const m = { requests: 0, byStatus: {} };
const send = (res, code, obj) => { m.byStatus[code] = (m.byStatus[code] || 0) + 1;
  res.writeHead(code, { 'content-type': 'application/json' }); res.end(JSON.stringify(obj)); };
const validUser = u => typeof u === 'string' && /^[\w.-]{1,64}$/.test(u);

async function queueState(userId) {
  const n = await store.ticket(userId), adm = await store.admittedUpTo();
  return { ticket: n, admitted: !WAITING_ROOM || n <= adm, position: Math.max(0, n - adm), pass: `${n}.${sign(userId, n)}` };
}

const routes = {
  'POST /api/queue/join': async (req, res) => {          // idempotent: also used for polling
    const { userId } = await readBody(req);
    if (!validUser(userId)) return send(res, 400, { error: 'userId required' });
    if (!await store.allow(`join:${userId}`, 30, 20)) return send(res, 429, { error: 'slow_down' });
    send(res, 200, await queueState(userId));
  },
  'POST /api/purchase': async (req, res) => {
    const { userId, strategy } = await readBody(req);
    if (!validUser(userId)) return send(res, 400, { error: 'userId required' });
    if (!await store.allow(`buy:${userId}`, RL_BURST, RL_RATE)) return send(res, 429, { error: 'rate_limited' });
    if (WAITING_ROOM) {                                  // must hold a pass AND be admitted
      const [n, sig = ''] = String(req.headers['x-pass'] || '').split('.');
      if (!n || !safeEq(sig, sign(userId, n)) || +n > await store.admittedUpTo())
        return send(res, 403, { error: 'not_admitted' });
    }
    const s = ALLOW_OVERRIDE && STRATEGIES.includes(strategy) ? strategy : DEFAULT_STRATEGY;
    const r = await store.purchase(s, userId);
    const code = { ok: 201, duplicate: 200, sold_out: 409, busy: 503 }[r.status];
    send(res, code, { ...r, strategy: s });
  },
  'GET /api/stats': async (_, res) => send(res, 200, await store.stats()),
  'POST /api/admin/reset': async (req, res) => {
    if (req.headers['x-admin-token'] !== ADMIN) return send(res, 401, { error: 'unauthorized' });
    const { stock } = await readBody(req); await store.reset(Math.max(0, +stock || INITIAL_STOCK));
    send(res, 200, await store.stats());
  },
  'GET /health': async (_, res) => send(res, 200, { ok: true }),
  'GET /metrics': async (_, res) => {
    const s = await store.stats();
    const lines = [`flash_stock ${s.stock}`, `flash_sold ${s.sold}`, `flash_oversold ${s.oversold}`,
      `flash_conflicts ${s.conflicts}`, ...Object.entries(m.byStatus).map(([c, v]) => `flash_http_responses{code="${c}"} ${v}`)];
    res.writeHead(200, { 'content-type': 'text/plain' }); res.end(lines.join('\n') + '\n');
  },
};

const page = fs.readFileSync(new URL('../public/index.html', import.meta.url));
const server = http.createServer(async (req, res) => {
  m.requests++;
  const path = req.url.split('?')[0];
  if (req.method === 'GET' && path === '/') { res.writeHead(200, { 'content-type': 'text/html' }); return res.end(page); }
  const h = routes[`${req.method} ${path}`];
  if (!h) return send(res, 404, { error: 'not_found' });
  try { await h(req, res); } catch (e) { console.error(e); send(res, 500, { error: 'internal' }); }
});
server.keepAliveTimeout = 65000;
server.listen(PORT, () => console.log(`flash-sale on :${PORT} role=${ROLE} store=${env('REDIS_URL', '') ? 'redis' : 'memory'} strategy=${DEFAULT_STRATEGY} waitingRoom=${WAITING_ROOM}`));
process.on('SIGTERM', () => server.close(() => process.exit(0)));
