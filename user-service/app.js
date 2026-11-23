require("dotenv").config({ quiet: true });
const express = require("express");
const { Queue, Worker } = require("bullmq");

const app = express();
const PORT = process.env.PORT || 5001;

const connection = process.env.REDIS_URL
  ? {
      url: process.env.REDIS_URL,
      ...(process.env.REDIS_PASSWORD ? { password: process.env.REDIS_PASSWORD } : {}),
      maxRetriesPerRequest: null,
    }
  : {
      host: process.env.REDIS_HOST || "127.0.0.1",
      port: Number(process.env.REDIS_PORT) || 6379,
      ...(process.env.REDIS_PASSWORD ? { password: process.env.REDIS_PASSWORD } : {}),
      maxRetriesPerRequest: null,
    };

app.use(express.json());

// ---------------------------------------------------------------------------
// Mock "database" of users. In a real system this would live in Postgres/Mongo
// etc. user-service owns this data, which is why the verification worker
// lives here rather than in order-server.
// ---------------------------------------------------------------------------
const userDb = [
  {
    id: "user_123",
    name: "Rahul Nikam",
    password: "123456", // NOTE: plaintext for demo purposes only, never do this in real auth
    email: "rahul@gmail.com",
  },
];

const DEFAULT_JOB_OPTIONS = {
  attempts: 3,
  backoff: {
    type: "exponential",
    delay: 1000,
  },
  removeOnComplete: { count: 100 },
  removeOnFail: { count: 500 },
};

// ---------------------------------------------------------------------------
// Producer: this is the public HTTP entrypoint of the system. It validates
// the incoming request, then hands the work off to order-server by placing
// a job on "order-queue". The HTTP response returns immediately — it does
// NOT wait for the order to actually be verified/processed. That work
// happens asynchronously in order-server.
// ---------------------------------------------------------------------------
const orderQueue = new Queue("order-queue", { connection });

app.post("/order", async (req, res) => {
  const { userId, productName, price, idempotencyKey: bodyIdempotencyKey } = req.body || {};
  const idempotencyKey = req.headers["idempotency-key"] || bodyIdempotencyKey;

  if (!userId || typeof userId !== "string" || !userId.trim()) {
    return res.status(400).json({ error: "userId is required and must be a non-empty string" });
  }

  if (!productName || typeof productName !== "string" || !productName.trim()) {
    return res.status(400).json({ error: "productName is required and cannot be empty" });
  }

  if (typeof price !== "number" || Number.isNaN(price) || price <= 0) {
    return res.status(400).json({ error: "price must be a positive number greater than 0" });
  }

  const cleanUserId = userId.trim();
  const cleanProductName = productName.trim();
  const correlationId =
    req.headers["x-correlation-id"] ||
    `corr_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;

  try {
    // Idempotency check: prevent duplicate job creation if key was already submitted
    if (idempotencyKey) {
      const existingJob = await orderQueue.getJob(idempotencyKey);
      if (existingJob) {
        const state = await existingJob.getState();
        return res.status(409).json({
          message: "Duplicate order: an order with this idempotency key already exists",
          orderId: existingJob.data?.orderId,
          jobId: existingJob.id,
          state,
        });
      }
    }

    const orderId = idempotencyKey ? `order_${idempotencyKey}` : `order_${Date.now()}`;
    const jobOptions = {
      ...DEFAULT_JOB_OPTIONS,
      ...(idempotencyKey ? { jobId: idempotencyKey } : {}),
    };

    const job = await orderQueue.add(
      "Process Order",
      {
        orderId,
        userId: cleanUserId,
        productName: cleanProductName,
        price,
        correlationId,
        ...(idempotencyKey ? { idempotencyKey } : {}),
      },
      jobOptions
    );

    console.log(`[user-service][${correlationId}] accepted order ${orderId} (job ${job.id})`);

    return res.status(202).json({
      message: "Order accepted, processing in background",
      orderId,
      jobId: job.id,
      correlationId,
    });
  } catch (error) {
    console.error("Failed to enqueue order:", error);
    return res.status(500).json({ message: "Could not accept order" });
  }
});

app.get("/order/job/:jobId", async (req, res) => {
  const { jobId } = req.params;

  try {
    const job = await orderQueue.getJob(jobId);

    if (!job) {
      return res.status(404).json({ message: `Job ${jobId} not found` });
    }

    const state = await job.getState();

    return res.json({
      jobId: job.id,
      state,
      data: job.data,
      result: job.returnvalue || null,
      failedReason: job.failedReason || null,
    });
  } catch (error) {
    console.error(`Failed to fetch status for job ${jobId}:`, error);
    return res.status(500).json({ message: "Could not retrieve job status" });
  }
});

app.get("/orders", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit) || 20, 100);
    const jobs = await orderQueue.getJobs(
      ["completed", "active", "waiting", "delayed", "failed"],
      0,
      limit - 1
    );

    const orders = await Promise.all(
      jobs.map(async (job) => {
        const state = await job.getState();
        return {
          jobId: job.id,
          orderId: job.data?.orderId,
          userId: job.data?.userId,
          productName: job.data?.productName,
          price: job.data?.price,
          state,
          timestamp: job.timestamp,
          result: job.returnvalue || null,
          failedReason: job.failedReason || null,
        };
      })
    );

    return res.json({ count: orders.length, orders });
  } catch (error) {
    console.error("Failed to fetch recent orders:", error);
    return res.status(500).json({ message: "Could not retrieve recent orders" });
  }
});

app.delete("/order/job/:jobId", async (req, res) => {
  const { jobId } = req.params;

  try {
    const job = await orderQueue.getJob(jobId);

    if (!job) {
      return res.status(404).json({ message: `Job ${jobId} not found` });
    }

    const state = await job.getState();

    // Only waiting or delayed jobs can be cleanly removed before processing begins
    if (state !== "waiting" && state !== "delayed") {
      return res.status(400).json({
        message: `Cannot cancel order: job is already in '${state}' state`,
      });
    }

    await job.remove();

    return res.json({
      message: "Order cancelled successfully",
      jobId,
      orderId: job.data?.orderId,
    });
  } catch (error) {
    console.error(`Failed to cancel job ${jobId}:`, error);
    return res.status(500).json({ message: "Could not cancel order" });
  }
});

app.get("/stats", async (_req, res) => {
  try {
    const counts = await orderQueue.getJobCounts();

    return res.json({
      queue: "order-queue",
      waiting: counts.waiting || 0,
      active: counts.active || 0,
      completed: counts.completed || 0,
      failed: counts.failed || 0,
      delayed: counts.delayed || 0,
      paused: counts.paused || 0,
      uptimeSeconds: Math.floor(process.uptime()),
    });
  } catch (error) {
    console.error("Failed to fetch queue stats:", error);
    return res.status(500).json({ message: "Could not retrieve queue stats" });
  }
});

app.get("/health", async (_req, res) => {
  try {
    const client = await orderQueue.client;
    await client.ping();

    return res.json({
      status: "healthy",
      redis: "connected",
      uptimeSeconds: Math.floor(process.uptime()),
    });
  } catch (error) {
    return res.status(503).json({
      status: "unhealthy",
      redis: "disconnected",
      error: error.message,
    });
  }
});

// ---------------------------------------------------------------------------
// Consumer: user-service also runs the worker that answers "is this user
// valid?" requests coming from order-server, since it is the service that
// owns user data.
// ---------------------------------------------------------------------------
const verificationWorker = new Worker(
  "user-verification-queue",
  async (job) => {
    const { userId, correlationId } = job.data;
    const prefix = correlationId ? `[verification][${correlationId}]` : `[verification]`;
    console.log(`${prefix} received job ${job.id} for userId=${userId}`);

    const foundUser = userDb.find((item) => item.id === userId);
    const isValidUser = Boolean(foundUser);

    // Never leak the password back through the queue payload.
    // eslint-disable-next-line no-unused-vars
    const { password: _password, ...rest } = foundUser || {};

    console.log(`${prefix} userId=${userId} valid=${isValidUser}`);
    return { isValidUser, user: isValidUser ? rest : null };
  },
  { connection }
);

verificationWorker.on("failed", (job, err) => {
  const attempts = job?.attemptsMade || 1;
  const maxAttempts = job?.opts?.attempts || 1;
  console.error(`[verification] job ${job?.id} failed (attempt ${attempts}/${maxAttempts}):`, err.message);
});

const server = app.listen(PORT, () =>
  console.log(`User service (HTTP producer + verification worker) started on port ${PORT}`)
);

// ---------------------------------------------------------------------------
// Graceful shutdown so Redis connections/workers close cleanly.
// ---------------------------------------------------------------------------
async function shutdown() {
  console.log("Shutting down user-service...");
  await verificationWorker.close();
  await orderQueue.close();
  server.close(() => process.exit(0));
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
