// Usage: node loadtest/run.js [--users 50000] [--stock 500] [--concurrency 500] [--spam 0.02]
//        [--strategies naive,optimistic,lua,queue] [--wr]   (server needs WAITING_ROOM=on for --wr)
const BASE = process.env.BASE || 'http://localhost:3000';
const arg = (k, d) => { const i = process.argv.indexOf('--' + k); return i > 0 ? process.argv[i + 1] : d; };
const USERS = +arg('users', 50000), STOCK = +arg('stock', 500), CONC = +arg('concurrency', 500);
const SPAM = +arg('spam', 0.02), WR = process.argv.includes('--wr');
const strategies = arg('strategies', 'naive,optimistic,lua,queue').split(',');
const ADMIN = process.env.ADMIN_TOKEN || 'dev-admin';
const sleep = ms => new Promise(r => setTimeout(r, ms));
const pct = (a, p) => a[Math.min(a.length - 1, Math.floor(a.length * p))] ?? 0;
async function post(path, body, headers = {}) {
  const t = performance.now();
  try {
    const r = await fetch(BASE + path, { method: 'POST', headers: { 'content-type': 'application/json', ...headers }, body: JSON.stringify(body) });
    return { status: r.status, j: await r.json().catch(() => ({})), ms: performance.now() - t };
  } catch { return { status: 0, j: {}, ms: performance.now() - t }; }
}
async function virtualUser(i, strategy, lat, tally) {
  const userId = 'u' + i; let pass = '';
  if (WR) for (;;) {                                      // wait in the virtual waiting room
    const r = await post('/api/queue/join', { userId }); pass = r.j.pass;
    if (r.j.admitted) break; await sleep(300 + Math.random() * 400);
  }
  const n = Math.random() < SPAM ? 20 : 1;                // ~2% of users hammer the buy button
  const rs = await Promise.all(Array.from({ length: n }, () => post('/api/purchase', { userId, strategy }, { 'x-pass': pass })));
  for (const r of rs) { lat.push(r.ms); tally[r.status] = (tally[r.status] || 0) + 1; }
}
const rows = []; let failed = false;
for (const strategy of strategies) {
  await post('/api/admin/reset', { stock: STOCK }, { 'x-admin-token': ADMIN });
  const lat = [], tally = {}; let next = 0; const t0 = performance.now();
  await Promise.all(Array.from({ length: CONC }, async () => { while (next < USERS) await virtualUser(next++, strategy, lat, tally); }));
  const secs = (performance.now() - t0) / 1000;
  const st = await (await fetch(BASE + '/api/stats')).json(); lat.sort((a, b) => a - b);
  const safe = strategy !== 'naive';
  const ok = st.oversold === 0 && st.sold === Math.min(STOCK, USERS);
  if (safe && !ok) failed = true;
  rows.push({ strategy, sold: st.sold, oversold: st.oversold, stockLeft: st.stock, '201': tally[201] || 0, '409': tally[409] || 0,
    '429': tally[429] || 0, '503': tally[503] || 0, conflicts: st.conflicts, rps: Math.round(lat.length / secs),
    p50: pct(lat, .5).toFixed(0), p95: pct(lat, .95).toFixed(0), p99: pct(lat, .99).toFixed(0), result: ok ? 'PASS' : (safe ? 'FAIL' : 'OVERSOLD (expected)') });
}
console.log(`\nusers=${USERS} stock=${STOCK} concurrency=${CONC} waitingRoom=${WR}\n`);
console.table(rows); process.exit(failed ? 1 : 0);
