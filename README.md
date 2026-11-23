# NexQueue

<div align="center">

<br/>

> **A production-inspired distributed job queue built with Node.js, Redis, and BullMQ**

<br/>

</div>

---

## What is NexQueue?

Most backend tutorials stop at REST APIs and CRUD operations.

When a user places an order on an e-commerce platform, they should not wait for the confirmation email to send, the inventory to update, and the analytics event to fire before getting a response. All of that is background work. It goes into a queue. Workers pick it up asynchronously. The user gets a near-instant response, and all the heavy lifting happens behind the scenes.

NexQueue is my ground-up implementation of this exact pattern - a fully working distributed job queue built with Node.js, Redis, and BullMQ. Independent microservices communicate through a shared in-memory queue broker.
This follows the same architectural pattern used in systems like Shopify's order pipeline, GitHub's notification system, and Stripe's payment processing, where background work is processed asynchronously.

---

## Architecture

```
                      ┌──────────────────────────┐
   HTTP Request  ───▶ │      user-service         │  :5001
                      │  (HTTP entrypoint +       │
                      │   verification worker)    │
                      └────────────┬─────────────┘
                                   │  enqueue "Process Order"
                                   ▼
                      ┌───────────────────────────┐
                      │      Redis + BullMQ       │  :6379
                      │    (in-memory broker)     │
                      │                           │
                      │  order-queue              │
                      │  user-verification-queue  │
                      │  mail-queue               │
                      └─────────┬────────┬────────┘
                                │        │  consume jobs
                    ┌───────────┘        └──────────────┐
                    ▼                                    ▼
     ┌─────────────────────────┐         ┌─────────────────────────┐
     │      order-server       │         │      mail-server        │
     │  (order-queue worker —  │────────▶│   (mail-queue worker,   │
     │   calls back into       │  enqueue │    no HTTP port)        │
     │   user-verification)    │  mail job│                        │
     └─────────────────────────┘         └─────────────────────────┘
```

**Independent microservices. One Redis brain. Zero blocking on the main thread.**

| Service | Port | Role |
|---|---|---|
| `user-service` | 5001 | HTTP entrypoint — accepts `POST /order`, validates input, enqueues jobs into `order-queue`. Also owns user data, so it runs the worker that answers "is this user valid?" requests on `user-verification-queue`. |
| `order-server` | — (no HTTP) | Pure worker. Subscribes to `order-queue`, asks `user-service` to verify the user (via a queue-based request/reply on `user-verification-queue`), and on success enqueues a job onto `mail-queue`. |
| `mail-server` | — (no HTTP) | Pure worker. Subscribes to `mail-queue`, dispatches (simulated) notification emails. |

> `order-server` does not expose an HTTP port — it is a background worker only, matching the "the HTTP response and the actual work are separate things" principle this project is built around.

**Why Redis as the broker?**
Redis is an in-memory data store—reads and writes happen in microseconds. BullMQ stores job data in Redis, allowing jobs to persist even if a worker service crashes and restarts. Jobs remain available until successfully processed by a worker.

**Why BullMQ over a raw Redis queue?**
A raw Redis list has no concept of retries, delayed execution, or worker coordination. You would need to build these capabilities yourself. BullMQ provides them through a clean API while handling the underlying Redis data structures, making reliable background job processing significantly easier.

> **Reliability Note (SOLVED):** In initial versions, jobs had no retry policy. All queues are now configured with 3 automatic attempts and exponential backoff (`delay: 1000ms`, doubling on each attempt) to survive transient network or downstream microservice hiccups without dropping jobs.

**Why three microservices instead of one app?**
Fault isolation. If `mail-server` crashes because of a bad email template or a third-party API outage, `order-server` keeps processing orders completely unaffected. One service's failure does not cascade into the others which is just perfect.

---

## Features

| Feature | Description |
|---|---|
| Background job processing | The HTTP request returns immediately; order verification and mail sending happen asynchronously in separate worker processes |
| Service fault isolation | Each microservice fails independently - no cascading system failures |
| Job persistence | Job state lives in Redis - workers can restart without losing queued/in-flight jobs |
| Queue-based request/reply | `order-server` calls into `user-service`'s verification worker over a queue (`job.waitUntilFinished`) instead of a direct HTTP call, so the two stay fully decoupled |
| Idempotency & deduplication | Prevent double ordering on network retries or duplicate clicks via `Idempotency-Key` header or `idempotencyKey` body |
| Automatic retries & backoff | Configured 3 retry attempts with exponential backoff on all queues to survive transient downstream failures |
| Order cancellation | Cancel pending or waiting jobs before workers start execution via `DELETE /order/job/:jobId` |
| Live queue stats | Inspect real-time counts of waiting, active, completed, and failed jobs via `GET /stats` |
| Recent orders list | Inspect recent order history and their execution outcomes via `GET /orders` |
| Deep health check | Live Redis ping and uptime probe via `GET /health` for container and load-balancer readiness |
| Input validation | Strict field-by-field validation with descriptive error messages for `POST /order` |
| Distributed tracing | Trace and correlate logs across all microservices using correlated `correlationId` |
| Queue cleanup | Purge completed and failed jobs on-demand to free Redis memory via `POST /queue/clean` |
| Graceful shutdown | Each service closes its BullMQ workers/queues cleanly on `SIGINT`/`SIGTERM` |

---

## Tech Stack

| Layer | Technology | Why This Choice |
|---|---|---|
| Runtime | Node.js 18+ | Non-blocking I/O is a natural fit for queue workers that spend time waiting |
| Framework | Express.js | Minimal HTTP layer for the producer service (`user-service` only) — no unnecessary overhead |
| Queue library | BullMQ | Production-grade queue abstraction for reliable background job processing on top of Redis |
| Broker | Redis 7+ | In-memory speed with list/sorted-set data structures that map perfectly to queue semantics |
| Config | dotenv | Each service reads Redis host/port/timeouts from its own `.env` file instead of hardcoded values |
| Local Redis | Docker | One command to spin up Redis locally, reproducible across any machine (the Node services themselves are not containerized yet — see Roadmap) |

---

## Project Structure

```
NexQueue/
│
├── user-service/               # HTTP entrypoint + verification worker
│   ├── app.js                  # Express POST /order producer + BullMQ verification Worker
│   ├── package.json
│   └── .env.example
│
├── order-server/                # Order worker - consumes order-queue, calls verification, enqueues mail
│   ├── app.js                  # BullMQ Worker + order business logic
│   ├── package.json
│   └── .env.example
│
├── mail-server/                # Mail worker - consumes and processes email jobs
│   ├── app.js                  # BullMQ Worker + email dispatch logic
│   ├── package.json
│   └── .env.example
│
├── .gitignore
└── README.md
```

---

## Getting Started

### Prerequisites

- Node.js 18+
- Docker (for running Redis locally)

### 1. Clone the repository

```bash
git clone https://github.com/ayushhh-here/NexQueue.git
cd NexQueue
```

### 2. Start Redis

```bash
docker run -d -p 6379:6379 redis
```

### 3. Install dependencies for each service

```bash
cd user-service && npm install && cd ..
cd order-server && npm install && cd ..
cd mail-server && npm install && cd ..
```

### 4. Configure environment variables

Each service has a `.env.example` — copy it to `.env`:

```bash
cp user-service/.env.example user-service/.env
cp order-server/.env.example order-server/.env
cp mail-server/.env.example mail-server/.env
```

The defaults (`127.0.0.1:6379`) work out of the box with the Redis container from step 2.

---

## Running the Project

You can run all three services concurrently in a single terminal from the repository root:

```bash
npm run dev
```

Alternatively, open three separate terminal windows and start each service individually:

```bash
# Terminal 1 — HTTP entrypoint + verification worker
cd user-service && node app.js

# Terminal 2 — order worker
cd order-server && node app.js

# Terminal 3 — mail worker
cd mail-server && node app.js
```

Send a test order (note: the request goes to `user-service` on **5001**, not `order-server`, since `order-server` has no HTTP port):

```bash
curl -X POST http://localhost:5001/order \
  -H "Content-Type: application/json" \
  -d '{"userId": "user_123", "productName": "mechanical keyboard", "price": 4999}'
```

`user-service` responds immediately with `202 Accepted`, an `orderId`, and a `jobId`. Watch the other two terminals — `order-server` picks up the order, verifies the user, and `mail-server` picks up the resulting mail job — all asynchronously.

Check the background processing status using the returned `jobId`:

```bash
curl http://localhost:5001/order/job/<jobId>
```

To see the rejection path, send a `userId` that isn't in the mock user DB (anything other than `"user_123"`) — `order-server` will log the order as rejected and no mail job will be created.

To test duplicate order prevention, pass an `Idempotency-Key` header:

```bash
curl -X POST http://localhost:5001/order \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-key-101" \
  -d '{"userId": "user_123", "productName": "mechanical keyboard", "price": 4999}'
```

Re-running the exact request with the same `Idempotency-Key` immediately returns `409 Conflict`, ensuring the background worker never processes duplicate orders.

Cancel a pending order before workers begin processing it:

```bash
curl -X DELETE http://localhost:5001/order/job/<jobId>
```

Inspect real-time queue metrics and job counts:

```bash
curl http://localhost:5001/stats
```

List recent orders and their processing outcomes:

```bash
curl http://localhost:5001/orders
```

Check service and Redis readiness health:

```bash
curl http://localhost:5001/health
```

Purge completed and failed jobs from Redis on demand:

```bash
curl -X POST http://localhost:5001/queue/clean
```

---

## What I Learned Building This

**Async task decoupling**
The core insight is that the HTTP response and the actual work are two separate things. The user cares that their order was accepted—not that the confirmation email was sent. Decoupling these with a queue makes the system faster for users and more resilient to downstream failures.

**Redis data structures under the hood**
BullMQ stores delayed jobs in Redis sorted sets, manages active jobs internally, and tracks failed jobs separately. Understanding these data structures made Redis genuinely click for me—I now think of Redis as a toolkit, not just a cache.

**Distributed failure modes**
Three services communicating through a shared broker forces you to think about every failure scenario: what if a worker crashes mid-job? Does the job get processed twice? Does it get lost? BullMQ's acknowledgement model answers each of these correctly, and understanding why required reading through its internals.

**Queue-based request/reply, and its pitfalls**
`order-server` needs an answer from `user-service` before it can continue, so it enqueues a verification job and waits for the result. My first version of this wired up manual `QueueEvents` listeners per request and never cleaned them up — a textbook `EventEmitter` memory leak. Switching to BullMQ's built-in `job.waitUntilFinished()` fixed it, and taught me to look for exactly this kind of leak anywhere I'm attaching listeners inside a request handler.

**Microservice tradeoffs**
Multiple `package.json` files, processes, and ports—the operational overhead of microservices is real.

---

## Known Limitations

Being upfront about what this project doesn't do (yet), rather than overstating it:

- ~~No retries/backoff configured on jobs~~ **[SOLVED]**: Jobs now configure 3 attempts with exponential backoff (`delay: 1000ms`, `type: 'exponential'`) and automated Redis cleanup (`removeOnComplete: 100`, `removeOnFail: 500`), addressing the earlier single-attempt limitation.
- Order data is stored in an in-memory array in `order-server`, not a real database — restarting the process loses all order history.
- The mock user "database" in `user-service` is a hardcoded array with a plaintext password field, purely for demoing the verification flow — it is not a real auth system.
- No monitoring/dashboard for queue state (Bull Board is on the roadmap).
- No automated tests.

---

## Roadmap

- [ ] Live monitoring dashboard using Bull Board UI
- [x] Real email dispatch via Nodemailer (integrated with Ethereal test inbox and preview URLs)
- [x] Configure retries/backoff on job creation (3 attempts + exponential backoff wired across all queues)
- [ ] Persist orders to a real database (Postgres/Mongo)
- [ ] Kubernetes deployment with horizontal pod autoscaling
- [ ] Prometheus metrics + Grafana dashboards
- [ ] WebSocket-based live job log streaming in the terminal
- [ ] CI/CD pipeline with GitHub Actions
- [ ] Automated tests (unit + integration against a test Redis instance)

---

## License

MIT — use it, fork it, break it, learn from it.

---

<div align="center">

Built by [ayushhh-here](https://github.com/ayushhh-here) · 3rd year B.Tech · NIT Agartala

*cheers !!!*

</div>
