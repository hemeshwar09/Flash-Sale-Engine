// Redis store: the production backend. Safe across many API instances.
import Redis from 'ioredis';
import crypto from 'node:crypto';
const sleep = ms => new Promise(r => setTimeout(r, ms));
export const STRATEGIES = ['naive', 'optimistic', 'lua', 'queue'];
const K = { stock: 'sale:stock', orders: 'sale:orders', init: 'sale:initial', stats: 'sale:stats',
  tail: 'wr:tail', adm: 'wr:admitted', users: 'wr:users', jobs: 'sale:jobs', chan: 'sale:results' };

// Atomic: dedupe per user + stock check + decrement + order write, in ONE server-side step.
const LUA_BUY = `
local o = redis.call('HGET', KEYS[2], ARGV[1])
if o then return {2, o} end
local s = tonumber(redis.call('GET', KEYS[1]) or '0')
if s <= 0 then return {0, ''} end
redis.call('DECR', KEYS[1])
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
return {1, ARGV[2]}`;

// Token bucket using Redis TIME so all API instances share one clock.
const LUA_BUCKET = `
local t = redis.call('TIME'); local now = t[1] * 1000 + math.floor(t[2] / 1000)
local cap = tonumber(ARGV[1]); local rate = tonumber(ARGV[2])
local d = redis.call('HMGET', KEYS[1], 't', 'ts')
local tokens = tonumber(d[1]) or cap; local ts = tonumber(d[2]) or now
tokens = math.min(cap, tokens + (now - ts) / 1000 * rate)
local ok = 0
if tokens >= 1 then tokens = tokens - 1; ok = 1 end
redis.call('HSET', KEYS[1], 't', tokens, 'ts', now); redis.call('PEXPIRE', KEYS[1], 60000)
return ok`;

export class RedisStore {
  constructor(url) {
    this.url = url; this.r = new Redis(url); this.pending = new Map(); this.waiters = [];
    this.r.defineCommand('buy', { numberOfKeys: 2, lua: LUA_BUY });
    this.r.defineCommand('bucket', { numberOfKeys: 1, lua: LUA_BUCKET });
  }
  async start({ worker = true } = {}) {
    // connection pool for WATCH/MULTI (WATCH is per-connection, so it can't share one socket)
    this.free = Array.from({ length: 32 }, () => this.r.duplicate());
    this.sub = this.r.duplicate(); await this.sub.subscribe(K.chan);
    this.sub.on('message', (_, m) => { const x = JSON.parse(m); this.pending.get(x.id)?.(x); });
    if (worker) {                       // run exactly ONE worker for the queue strategy
      const admit = Number(process.env.ADMIT_PER_TICK || 200);
      setInterval(() => this.r.incrby(K.adm, admit).catch(() => {}), 100);
      this._loop();
    }
  }
  async acquire() { return this.free.pop() || new Promise(r => this.waiters.push(r)); }
  release(c) { const w = this.waiters.shift(); w ? w(c) : this.free.push(c); }
  bump(f) { this.r.hincrby(K.stats, f, 1).catch(() => {}); }
  async reset(stock) {
    await this.r.multi().del(K.orders, K.stats, K.tail, K.adm, K.users, K.jobs)
      .set(K.stock, stock).set(K.init, stock).exec();
  }
  purchase(strategy, u) { return this[strategy](u); }

  async naive(u) {                      // read -> (race window) -> write
    const ex = await this.r.hget(K.orders, u); if (ex) return { status: 'duplicate', orderId: ex };
    const s = +await this.r.get(K.stock);
    if (s <= 0) { this.bump('soldOut'); return { status: 'sold_out' }; }
    await this.r.set(K.stock, s - 1);
    const orderId = crypto.randomUUID(); await this.r.hset(K.orders, u, orderId);
    return { status: 'ok', orderId };
  }
  async optimistic(u) {                 // WATCH + MULTI/EXEC, retry on conflict
    for (let i = 0; i < 25; i++) {
      const c = await this.acquire();
      try {
        await c.watch(K.stock, K.orders);
        const ex = await c.hget(K.orders, u);
        if (ex) { await c.unwatch(); return { status: 'duplicate', orderId: ex }; }
        const s = +await c.get(K.stock);
        if (s <= 0) { await c.unwatch(); this.bump('soldOut'); return { status: 'sold_out' }; }
        const orderId = crypto.randomUUID();
        const out = await c.multi().decr(K.stock).hset(K.orders, u, orderId).exec();
        if (out) return { status: 'ok', orderId };          // null => someone else won the race
      } finally { this.release(c); }
      this.bump('conflicts'); await sleep(Math.random() * 2 * (i + 1));
    }
    this.bump('exhausted'); return { status: 'busy' };
  }
  async lua(u) {                        // single atomic server-side script
    const [code, orderId] = await this.r.buy(K.stock, K.orders, u, crypto.randomUUID());
    if (code === 1) return { status: 'ok', orderId };
    if (code === 2) { this.bump('duplicate'); return { status: 'duplicate', orderId }; }
    this.bump('soldOut'); return { status: 'sold_out' };
  }
  queue(u) {                            // API enqueues; single worker applies serially
    const id = crypto.randomUUID();
    return new Promise(res => {
      const t = setTimeout(() => { this.pending.delete(id); res({ status: 'busy' }); }, 5000);
      this.pending.set(id, x => { clearTimeout(t); this.pending.delete(id); res(x); });
      this.r.lpush(K.jobs, JSON.stringify({ id, u }));
    });
  }
  async _loop() {
    const b = this.r.duplicate();
    for (;;) {
      const j = await b.brpop(K.jobs, 1).catch(() => null); if (!j) continue;
      const { id, u } = JSON.parse(j[1]);
      const ex = await this.r.hget(K.orders, u); let res;
      if (ex) res = { status: 'duplicate', orderId: ex };
      else if (+await this.r.get(K.stock) <= 0) { this.bump('soldOut'); res = { status: 'sold_out' }; }
      else { const orderId = crypto.randomUUID(); await this.r.decr(K.stock); await this.r.hset(K.orders, u, orderId); res = { status: 'ok', orderId }; }
      await this.r.publish(K.chan, JSON.stringify({ id, ...res }));
    }
  }

  async ticket(u) {
    let n = await this.r.hget(K.users, u); if (n) return +n;
    n = await this.r.incr(K.tail);
    if (!await this.r.hsetnx(K.users, u, n)) n = await this.r.hget(K.users, u);
    return +n;
  }
  async admittedUpTo() { return +(await this.r.get(K.adm) || 0); }
  async allow(key, cap, rate) { return (await this.r.bucket(`rl:${key}`, cap, rate)) === 1; }
  async stats() {
    const [stock, init, sold, st, tail, adm] = await Promise.all([this.r.get(K.stock), this.r.get(K.init),
      this.r.hlen(K.orders), this.r.hgetall(K.stats), this.r.get(K.tail), this.r.get(K.adm)]);
    const initial = +init || 0;
    return { backend: 'redis', initial, stock: +stock || 0, sold, oversold: Math.max(0, sold - initial),
      conflicts: +st.conflicts || 0, soldOut: +st.soldOut || 0, duplicate: +st.duplicate || 0,
      exhausted: +st.exhausted || 0, ticketsIssued: +tail || 0, admitted: +adm || 0 };
  }
}
