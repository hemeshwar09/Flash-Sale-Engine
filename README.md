# Flash-Sale Engine: 500 items, 50,000 users

A Prime-Day-style inventory engine that **never oversells**: four interchangeable stock-reservation
strategies, a signed virtual waiting room, per-user token-bucket rate limiting, a live UI, metrics,
and a load test that proves correctness and compares the strategies.

## Architecture
```
 browser / loadtest
        │
   N x API (stateless Node) ─ POST /api/queue/join → ticket + HMAC-signed pass
        │                   ─ POST /api/purchase  → rate limit → pass check → strategy
        ▼
      Redis ─ stock, orders (user→orderId), waiting-room counters, token buckets, job list
        ▲
   1 x Worker ─ admits users (INCRBY watermark every 100 ms) + serial queue consumer
```

## Strategies (`STRATEGY=` env, or per request while benchmarking)
| Strategy | How | Correct? | Trade-off |
|---|---|---|---|
| `naive` | GET stock, then SET stock-1 | No (lost updates) | Kept to prove the bug |
| `optimistic` | Redis WATCH/MULTI/EXEC + jittered retries | Yes | Retry storms on a hot key; some 503s |
| `lua` | One atomic script: dedupe + check + decrement + order | Yes | Fastest/simplest; one hot key |
| `queue` | API enqueues, single worker applies serially, result via pub/sub | Yes | Strict FIFO fairness; extra hop, single-consumer ceiling |

Default: `lua`. Use `queue` when you need strict ordering or serial payment/hold steps.

## Features
- Zero oversell, one order per user (repeat requests return the same order)
- Virtual waiting room with signed, user-bound passes
- Token-bucket rate limiter (Redis Lua, shared clock)
- Load test with p50/p95/p99, status breakdown, conflict counts, PASS/FAIL exit code
- `/metrics`, `/health`, `/api/stats`, token-protected `/api/admin/reset`, web UI at `/`
- Dockerfile + docker-compose (scalable API replicas); no-dependency in-memory mode

## Run
```bash
# A) No Redis (in-memory store)
node src/server.js                                   # http://localhost:3000
node loadtest/run.js --users 50000 --stock 500

# B) Redis + worker + 3 API replicas
docker compose up --build --scale api=3
node loadtest/run.js --users 50000 --stock 500
# Waiting room flow (compose API has WAITING_ROOM=on):
node loadtest/run.js --users 5000 --concurrency 5000 --strategies lua,queue --wr
```
Exit code is non-zero if a safe strategy oversells or undersells.

## Measured here (in-memory mode, 20,000 users, 500 stock, 2% of users spam 20 requests)
| strategy | sold | oversold | 503 | conflicts | verdict |
|---|---|---|---|---|---|
| naive | 1,371 | **871** | 0 | 0 | broken, as expected |
| optimistic | 500 | 0 | 360 | 14,988 | PASS (high retry cost) |
| lua | 500 | 0 | 0 | 0 | PASS |
| queue | 500 | 0 | 0 | 0 | PASS |

Waiting-room run (1,500 users): unadmitted purchase returns 403; lua and queue both sold exactly 500, 0 oversold.

**Caveats:** latency from a single-process load generator is not meaningful. The Redis code path
(Lua, WATCH/MULTI, worker) was written but not executed in my sandbox (no Redis/Docker there), so run
the Docker benchmark yourself and use your own numbers before putting them on a resume.

