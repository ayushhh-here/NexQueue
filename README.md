# Distributed Job Queue System

Ever wondered how apps like Swiggy send you an order confirmation email 
while simultaneously updating your order status and notifying the 
restaurant — all at the same time? That's exactly what this system does.

This project implements a distributed job queue architecture where 
multiple services communicate asynchronously through a shared Redis 
broker, without blocking each other.

---

## The Problem It Solves

In a monolithic app, if sending an email fails — everything fails.
This system decouples tasks into independent workers so:
- A failed email doesn't crash your order
- Services scale independently
- Jobs retry automatically on failure
- No task is ever permanently lost# Distributed Job Queue System

Ever wondered how apps like Swiggy send you an order confirmation email 
while simultaneously updating your order status and notifying the 
restaurant — all at the same time? That's exactly what this system does.

This project implements a distributed job queue architecture where 
multiple services communicate asynchronously through a shared Redis 
broker, without blocking each other.

---

## The Problem It Solves

In a monolithic app, if sending an email fails — everything fails.
This system decouples tasks into independent workers so:
- A failed email doesn't crash your order
- Services scale independently
- Jobs retry automatically on failure
- No task is ever permanently lost

---

## How It Works

Three microservices run independently:

**user-service** (Port 5001)
Accepts incoming requests and pushes jobs into the Redis queue.

**order-server** (Port 5000)
Picks up order-related jobs from the queue and processes them.

**mail-server** (Port 5002)
Listens for email jobs and handles background mail delivery.

Redis sits in the middle as the brain — storing, distributing, 
and managing all jobs across services.

---

## Tech Stack

- Node.js + Express.js
- Redis
- BullMQ
- Docker

---

## Getting Started

Make sure Docker is installed and running, then:

```bash
# Start Redis
docker run -d -p 6379:6379 redis

# Install dependencies
cd mail-server && npm install
cd ../order-server && npm install  
cd ../user-service && npm install

# Run all three services in separate terminals
cd user-service && node app.js
cd order-server && node app.js
cd mail-server && node app.js
```

## What I Learned

Building this gave me hands-on understanding of:
- Why distributed systems exist and when to use them
- How message queues prevent data loss
- Worker concurrency and job prioritization
- Fault tolerance through retries and dead-letter queues
- Designing backend systems that scale horizontally

---

## What's Next

- Deploy on Railway or Render
- Add a live monitoring dashboard
- Integrate real email service (Nodemailer/SendGrid)
- Add authentication layer
- CI/CD with GitHub Actions

---

## How It Works

Three microservices run independently:

**user-service** (Port 5001)
Accepts incoming requests and pushes jobs into the Redis queue.

**order-server** (Port 5000)
Picks up order-related jobs from the queue and processes them.

**mail-server** (Port 5002)
Listens for email jobs and handles background mail delivery.

Redis sits in the middle as the brain — storing, distributing, 
and managing all jobs across services.

---

## Tech Stack

- Node.js + Express.js
- Redis
- BullMQ
- Docker

---

## Getting Started

Make sure Docker is installed and running, then:

```bash
# Start Redis
docker run -d -p 6379:6379 redis

# Install dependencies
cd mail-server && npm install
cd ../order-server && npm install  
cd ../user-service && npm install

# Run all three services in separate terminals
cd user-service && node app.js
cd order-server && node app.js
cd mail-server && node app.js
```

## What I Learned

Building this gave me hands-on understanding of:
- Why distributed systems exist and when to use them
- How message queues prevent data loss
- Worker concurrency and job prioritization
- Fault tolerance through retries and dead-letter queues
- Designing backend systems that scale horizontally

---

## What's Next

- Deploy on Railway or Render
- Add a live monitoring dashboard
- Integrate real email service (Nodemailer/SendGrid)
- Add authentication layer
- CI/CD with GitHub Actions