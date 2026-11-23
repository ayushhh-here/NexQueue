require("dotenv").config({ quiet: true });
const { Queue, QueueEvents, Worker } = require("bullmq");

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

const VERIFY_TIMEOUT_MS = Number(process.env.VERIFY_TIMEOUT_MS) || 10_000;

// ---------------------------------------------------------------------------
// order-server is a WORKER. It subscribes to "order-queue" and does the
// actual order business logic — it is not an HTTP server. The public HTTP
// entrypoint lives in user-service (see README architecture diagram).
// ---------------------------------------------------------------------------
const verifyUserQueue = new Queue("user-verification-queue", { connection });
const verificationQueueEvents = new QueueEvents("user-verification-queue", { connection });
const mailQueue = new Queue("mail-queue", { connection });

const DEFAULT_JOB_OPTIONS = {
  attempts: 3,
  backoff: {
    type: "exponential",
    delay: 1000,
  },
  removeOnComplete: { count: 100 },
  removeOnFail: { count: 500 },
};

// In-memory "orders table" standing in for a real database. Swap this out
// for Postgres/Mongo/etc. in a real deployment — see README roadmap.
const orders = [];

/**
 * Asks user-service (via the user-verification-queue) whether a user is
 * valid, and waits for the answer.
 *
 * Uses BullMQ's built-in `job.waitUntilFinished`, which internally manages
 * its own listener lifecycle per job and cleans up after itself. The
 * original version of this file wired up long-lived `.on("completed")`
 * listeners by hand — those were never removed, so they piled up as a
 * memory leak across requests. This avoids that entirely.
 */
async function verifyUser(userId, correlationId) {
  const job = await verifyUserQueue.add(
    "Verify User",
    { userId, correlationId },
    DEFAULT_JOB_OPTIONS
  );
  const result = await job.waitUntilFinished(verificationQueueEvents, VERIFY_TIMEOUT_MS);
  return result; // { isValidUser, user }
}

const orderWorker = new Worker(
  "order-queue",
  async (job) => {
    const { orderId, userId, productName, price, correlationId } = job.data;
    const prefix = correlationId ? `[order][${correlationId}]` : `[order]`;
    console.log(`${prefix} processing ${orderId} for userId=${userId}`);

    const { isValidUser, user } = await verifyUser(userId, correlationId);

    if (!isValidUser) {
      console.log(`${prefix} ${orderId} rejected — invalid userId=${userId}`);
      return { orderId, status: "rejected", reason: "invalid user" };
    }

    // "Save" the order. Replace with a real DB write in production.
    orders.push({ orderId, userId, productName, price, status: "confirmed" });

    const emailBody = [
      `Hi ${user.name || "Customer"},`,
      "",
      "Thank you for shopping with us! Here are your order details:",
      "----------------------------------------",
      `Order ID : ${orderId}`,
      `Item     : ${productName}`,
      `Total    : ₹${price}`,
      `Status   : Confirmed`,
      "----------------------------------------",
      "Your package is being prepared for shipment.",
    ].join("\n");

    const mailJob = await mailQueue.add(
      "Send Mail",
      {
        from: "noreply@nexqueue.dev",
        to: user.email,
        subject: `Order Confirmation - ${orderId}`,
        body: emailBody,
        correlationId,
      },
      DEFAULT_JOB_OPTIONS
    );

    console.log(`${prefix} ${orderId} confirmed, mail job ${mailJob.id} queued`);
    return { orderId, status: "confirmed", mailJobId: mailJob.id };
  },
  { connection }
);

orderWorker.on("failed", (job, err) => {
  const attempts = job?.attemptsMade || 1;
  const maxAttempts = job?.opts?.attempts || 1;
  console.error(`[order] job ${job?.id} failed (attempt ${attempts}/${maxAttempts}):`, err.message);
});

console.log("Order server (order-queue worker) started");

// ---------------------------------------------------------------------------
// Graceful shutdown so Redis connections/workers close cleanly.
// ---------------------------------------------------------------------------
async function shutdown() {
  console.log("Shutting down order-server...");
  await orderWorker.close();
  await verificationQueueEvents.close();
  await verifyUserQueue.close();
  await mailQueue.close();
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
