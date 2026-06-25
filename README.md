# 🔁 Distributed Job Queue System

Not every task needs to happen right now.

This system is built around one idea — decouple the work from 
the request. When a user places an order, they shouldn't wait 
for emails to send, inventory to update, and notifications to 
fire before getting a response. Those jobs go into a queue. 
Workers pick them up. Life goes on.

This is how production systems at scale actually work.

---

## 🧠 Architecture
User Request → user-service → Redis Queue → Workers
                                                ↓
                                              order-server  +  mail-server

Three microservices. One Redis brain. Zero blocking.

| Service | Port | Job |
|---|---|---|
| user-service | 5001 | Receives requests, queues jobs |
| order-server | 5000 | Processes order tasks |
| mail-server | 5002 | Handles email jobs |

---

## ⚙️ Features

- Priority-based job scheduling
- Delayed execution
- Auto-retry with exponential backoff
- Dead-letter queue for failed jobs  
- Concurrent workers
- Horizontally scalable

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Runtime | Node.js |
| Framework | Express.js |
| Queue | BullMQ |
| Broker | Redis |
| Container | Docker |

---


## 🧩 Key Concepts

**Why Redis?**
Redis keeps all jobs in memory — making reads/writes 
microseconds fast. Even if a service crashes, jobs 
stay in Redis and get picked up when it restarts.

**Why BullMQ?**
Raw Redis queues have no retry logic, no priority system, 
no delayed jobs. BullMQ adds all of that on top of Redis 
with a clean API.

**Why Microservices?**
If mail-server crashes, order-server keeps running. 
Services fail independently — the whole system doesn't go down.


## ⚡ Performance

- Redis can handle **1 million operations per second**
- BullMQ supports **thousands of concurrent jobs**
- Each worker processes jobs in **parallel threads**
- Failed jobs **auto-retry** up to configured max attempts


## 🚀 Quick Start

```bash
# 1. Start Redis
docker run -d -p 6379:6379 redis

# 2. Install dependencies
cd mail-server && npm install
cd ../order-server && npm install
cd ../user-service && npm install

# 3. Start services (3 separate terminals)
cd user-service && node app.js    # :5001
cd order-server && node app.js    # :5000
cd mail-server && node app.js     # :5002
```

---

## 💡 Why I Built This

Most tutorials teach you CRUD apps. I wanted to understand 
how real companies handle millions of background tasks — 
emails, notifications, order processing — without slowing 
down the main application. This project is my answer to that.
> 🚀 This is my first backend project — built from scratch to understand how real-world distributed systems work.


## 🔮 Roadmap

- [ ] Live monitoring dashboard
- [ ] Real email integration via Nodemailer
- [ ] Kubernetes deployment
- [ ] Prometheus + Grafana metrics
- [ ] WebSocket live job logs
- [ ] CI/CD with GitHub Actions
