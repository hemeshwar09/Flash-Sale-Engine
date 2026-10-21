// In-memory store: zero dependencies, used for local demos / CI.
// Models network latency with `tick()` so races and CAS conflicts are real.
import crypto from 'node:crypto';
const tick = () => new Promise(r => setImmediate(r));
const sleep = ms => new Promise(r => setTimeout(r, ms));
export const STRATEGIES = ['naive', 'optimistic', 'lua', 'queue'];

export class MemoryStore {
  constructor() { this.buckets = new Map(); this.users = new Map(); this.reset(0); }
  async start() {
    this.timer = setInterval(() => { this.admitted += Number(process.env.ADMIT_PER_TICK || 200); }, 100);
  }
  async reset(stock) {
    Object.assign(this, { initial: stock, stock, version: 0, orders: new Map(), tail: 0, admitted: 0,
      c: { conflicts: 0, soldOut: 0, duplicate: 0, exhausted: 0 }, q: [], draining: false });
    this.users.clear(); this.buckets.clear();
  }
  purchase(strategy, u) { return this[strategy](u); }

  // atomic section (synchronous => cannot interleave in JS)
  _sell(u) {
    if (this.orders.has(u)) { this.c.duplicate++; return { status: 'duplicate', orderId: this.orders.get(u) }; }
    if (this.stock <= 0) { this.c.soldOut++; return { status: 'sold_out' }; }
    this.stock--; this.version++;
    const orderId = crypto.randomUUID(); this.orders.set(u, orderId);
    return { status: 'ok', orderId };
  }
  // 1) NAIVE: read-then-write with no protection (kept to PROVE the bug)
  async naive(u) {
    if (this.orders.has(u)) return this._sell(u);
    const s = this.stock; await tick();
    if (s <= 0) { this.c.soldOut++; return { status: 'sold_out' }; }
    this.stock = s - 1;
    const orderId = crypto.randomUUID(); this.orders.set(u, orderId);
    return { status: 'ok', orderId };
  }
  // 2) OPTIMISTIC: version check (compare-and-set) with jittered retries
  async optimistic(u) {
    for (let i = 0; i < 25; i++) {
      if (this.orders.has(u)) return this._sell(u);
      const v = this.version;
      if (this.stock <= 0) return this._sell(u);
      await tick();
      if (this.version === v) return this._sell(u);       // CAS success
      this.c.conflicts++; await sleep(Math.random() * 2 * (i + 1));
    }
    this.c.exhausted++; return { status: 'busy' };
  }
  // 3) LUA-equivalent: one atomic check-and-decrement
  async lua(u) { return this._sell(u); }
  // 4) QUEUE: all writes serialized through one consumer
  queue(u) {
    return new Promise(res => {
      this.q.push([u, res]);
      if (!this.draining) { this.draining = true; setImmediate(() => this._drain()); }
    });
  }
  _drain() { while (this.q.length) { const [u, res] = this.q.shift(); res(this._sell(u)); } this.draining = false; }

  // waiting room: monotonically increasing ticket numbers, admitted-up-to watermark
  async ticket(u) { if (!this.users.has(u)) this.users.set(u, ++this.tail); return this.users.get(u); }
  async admittedUpTo() { return this.admitted; }

  // token bucket rate limiter
  async allow(key, cap, rate) {
    const now = Date.now(); const b = this.buckets.get(key) || { t: cap, ts: now };
    b.t = Math.min(cap, b.t + (now - b.ts) / 1000 * rate); b.ts = now;
    const ok = b.t >= 1; if (ok) b.t--; this.buckets.set(key, b); return ok;
  }
  async stats() {
    const sold = this.orders.size;
    return { backend: 'memory', initial: this.initial, stock: this.stock, sold,
      oversold: Math.max(0, sold - this.initial), ...this.c, ticketsIssued: this.tail, admitted: this.admitted };
  }
}
