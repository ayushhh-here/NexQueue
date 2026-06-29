# ⚡ NexQueue

<div align="center">

![Node.js](https://img.shields.io/badge/Node.js-339933?style=for-the-badge&logo=nodedotjs&logoColor=white)
![Express](https://img.shields.io/badge/Express.js-000000?style=for-the-badge&logo=express&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-FF4438?style=for-the-badge&logo=redis&logoColor=white)
![BullMQ](https://img.shields.io/badge/BullMQ-CC0000?style=for-the-badge&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![JavaScript](https://img.shields.io/badge/JavaScript-F7DF1E?style=for-the-badge&logo=javascript&logoColor=black)

<br/>

> **A production-ready distributed job queue system with priority scheduling,**
> **automatic retries, and real-time worker processing.**

<br/>


</div>

---

## 🧠 What is NexQueue?

Most backend tutorials stop at REST APIs and CRUD operations. Real production systems are different.

When a user places an order on an e-commerce platform, they should not wait for the confirmation email to send, the inventory to update, and the analytics event to fire before getting a response. All of that is background work. It goes into a queue. Workers pick it up asynchronously. The user gets a near-instant response, and all the heavy lifting happens behind the scenes.

NexQueue is my ground-up implementation of this exact pattern — a fully working distributed job queue built with Node.js, Redis, and BullMQ. Three independent microservices communicate through a shared in-memory queue broker, with priority-based scheduling, exponential backoff retries, dead-letter queues for failed jobs, and complete fault isolation between services.

This is the architecture behind Shopify's order pipeline, GitHub's notification system, and Stripe's payment processing at scale.

---

## 🏗️ Architecture

```
                      ┌──────────────────────────┐
   HTTP Request  ───▶ │      user-service         │  :5001
                      │  (job producer)           │
                      └────────────┬─────────────┘
                                   │  enqueue job
                                   ▼
                      ┌──────────────────────────┐
                      │    Redis + BullMQ         │  :6379
                      │    (in-memory broker)     │
                      │                           │
                      │  ┌─────────────────────┐  │
                      │  │  order-queue        │  │
                      │  │  mail-queue         │  │
                      │  │  dead-letter-queue  │  │
                      │  └─────────────────────┘  │
                      └────────┬─────────┬────────┘
                               │         │  consume jobs
                   ┌───────────┘         └──────────────┐
                   ▼                                     ▼
     ┌─────────────────────────┐         ┌─────────────────────────┐
     │      order-server       │  :5000  │      mail-server        │  :5002
     │   (order job worker)    │         │   (email job worker)    │
     └─────────────────────────┘         └─────────────────────────┘
```

**Three microservices. One Redis brain. Zero blocking on the main thread.**

| Service | Port | Role |
|---|---|---|
| `user-service` | 5001 | Receives HTTP requests, validates input, enqueues jobs into Redis |
| `order-server` | 5000 | Subscribes to the order queue, processes order business logic |
| `mail-server` | 5002 | Subscribes to the mail queue, dispatches notification emails |

**Why Redis as the broker?**
Redis is an in-memory data store — reads and writes happen in microseconds. BullMQ stores all job data in Redis sorted sets and lists, meaning jobs persist even if a worker service crashes and restarts. No job is ever lost silently.

**Why BullMQ over a raw Redis queue?**
A raw Redis list has no concept of retries, priorities, delayed execution, or dead-letter queues. You would need to build all of that from scratch, and do it correctly. BullMQ is a battle-tested library that handles all of it with a clean API, backed by years of production use.

**Why three microservices instead of one app?**
Fault isolation. If `mail-server` crashes because of a bad email template or a third-party API outage, `order-server` keeps processing orders completely unaffected. One service's failure does not cascade into the others.

---

## ✨ Features

| Feature | Description |
|---|---|
| 🔴 Priority-based scheduling | Assign priority levels to jobs — critical tasks are processed before low-priority ones |
| 🔴 Delayed job execution | Schedule a job to run at a specific point in the future |
| 🔴 Exponential backoff retry | Failed jobs are automatically retried with increasing delays between attempts |
| 🔴 Dead-letter queue | Jobs that exhaust all retries are moved to a dead-letter queue for inspection |
| 🔴 Concurrent workers | Multiple jobs are processed in parallel using worker threads |
| 🔴 Horizontal scalability | Add more worker instances without changing any application code |
| 🔴 Service fault isolation | Each microservice fails independently — no cascading system failures |
| 🔴 Job persistence | All job state lives in Redis — workers can restart without data loss |

---

## 🛠️ Tech Stack

| Layer | Technology | Why This Choice |
|---|---|---|
| Runtime | Node.js 18+ | Non-blocking I/O is a natural fit for queue workers that spend time waiting |
| Framework | Express.js | Minimal HTTP layer for the producer service — no unnecessary overhead |
| Queue library | BullMQ | Production-grade queue abstraction — retries, priorities, delays, dead-letter all built in |
| Broker | Redis 7+ | In-memory speed with list/sorted-set data structures that map perfectly to queue semantics |
| Containerization | Docker | One command to spin up Redis locally, reproducible across any machine |

---

## 📁 Project Structure

```
NexQueue/
│
├── user-service/               # HTTP producer — accepts requests, enqueues jobs
│   ├── app.js                  # Express server + BullMQ Queue producer
│   ├── package.json
│   └── .env.example
│
├── order-server/               # Order worker — consumes and processes order jobs
│   ├── app.js                  # BullMQ Worker + order processing logic
│   ├── package.json
│   └── .env.example
│
├── mail-server/                # Mail worker — consumes and processes email jobs
│   ├── app.js                  # BullMQ Worker + email dispatch logic
│   ├── package.json
│   └── .env.example
│
├── .gitignore
└── README.md
```

---

## 🚀 Getting Started

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

Each service has a `.env.example` — copy it to `.env` and fill in values:

```bash
cp user-service/.env.example user-service/.env
cp order-server/.env.example order-server/.env
cp mail-server/.env.example mail-server/.env
```

---

## ▶️ Running the Project

Open three separate terminal windows and start each service:

```bash
# Terminal 1 — start the job producer
cd user-service && node app.js

# Terminal 2 — start the order worker
cd order-server && node app.js

# Terminal 3 — start the mail worker
cd mail-server && node app.js
```

Send a test job through the system:

```bash
curl -X POST http://localhost:5001/order \
  -H "Content-Type: application/json" \
  -d '{"userId": "user_123", "item": "mechanical keyboard", "qty": 1}'
```

The `user-service` returns a response immediately. Watch the other two terminals — `order-server` and `mail-server` will each pick up their respective jobs from the queue and process them asynchronously.

---

## 📚 What I Learned Building This

**Async task decoupling**
The core insight is that the HTTP response and the actual work are two separate things. The user cares that their order was accepted — not that the confirmation email was sent. Decoupling these with a queue makes the system faster for users and more resilient to downstream failures.

**Redis data structures under the hood**
BullMQ stores delayed jobs in a sorted set (sorted by execution time), active jobs in a list, and failed jobs in a separate dead-letter set. Understanding this made Redis's data structures genuinely click — I now think of Redis as a toolkit, not just a cache.

**Distributed failure modes**
Three services communicating through a shared broker forces you to think about every failure scenario: what if a worker crashes mid-job? Does the job get processed twice? Does it get lost? BullMQ's acknowledgement model answers each of these correctly, and understanding why required reading through its internals.

**Microservice tradeoffs**
Three `package.json` files, three processes, three ports — the operational overhead of microservices is real. The isolation benefit only pays off when services have genuinely different failure profiles or scaling requirements. This project made that tradeoff concrete and tangible.

---

## 🗺️ Roadmap

- [ ] Live monitoring dashboard using Bull Board UI
- [ ] Real email dispatch via Nodemailer
- [ ] Kubernetes deployment with horizontal pod autoscaling
- [ ] Prometheus metrics + Grafana dashboards
- [ ] WebSocket-based live job log streaming in the terminal
- [ ] CI/CD pipeline with GitHub Actions

---

## 📄 License

MIT — use it, fork it, break it, learn from it.

---

<div align="center">

Built by [ayushhh-here](https://github.com/ayushhh-here) · 3rd year B.Tech · NIT Agartala

*cheers !!!*

</div>
