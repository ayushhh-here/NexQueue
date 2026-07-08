require("dotenv").config({ quiet: true });
const { Worker } = require("bullmq");

const connection = {
  host: process.env.REDIS_HOST || "127.0.0.1",
  port: Number(process.env.REDIS_PORT) || 6379,
};

/**
 * Stand-in for a real email provider (Nodemailer/SES/etc — see README
 * roadmap). Throwing here marks the BullMQ job as failed, which is the
 * correct way to signal an error to the worker.
 */
async function sendMail(from, to, subject, body) {
  if (!to) {
    throw new Error("Cannot send mail: recipient address missing");
  }
  console.log(`[mail] sending "${subject}" to ${to}`);
  console.log(`[mail]   from: ${from}`);
  console.log(`[mail]   body: ${body}`);
  // Simulated send — replace with a real transport in production.
  return { sent: true, to };
}

const mailWorker = new Worker(
  "mail-queue",
  async (job) => {
    const { from, to, subject, body } = job.data;
    console.log(`[mail] processing job ${job.id}`);
    return sendMail(from, to, subject, body);
  },
  { connection }
);

mailWorker.on("completed", (job) => {
  console.log(`[mail] job ${job.id} completed`);
});

mailWorker.on("failed", (job, err) => {
  console.error(`[mail] job ${job?.id} failed:`, err.message);
});

console.log("Mail server (mail-queue worker) started");

// ---------------------------------------------------------------------------
// Graceful shutdown so Redis connections/workers close cleanly.
// ---------------------------------------------------------------------------
async function shutdown() {
  console.log("Shutting down mail-server...");
  await mailWorker.close();
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
