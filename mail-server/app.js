require("dotenv").config({ quiet: true });
const { Worker } = require("bullmq");
const nodemailer = require("nodemailer");

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

let transporterPromise = null;

async function getTransporter() {
  if (!transporterPromise) {
    transporterPromise = (async () => {
      if (process.env.SMTP_HOST && process.env.SMTP_PORT) {
        return nodemailer.createTransport({
          host: process.env.SMTP_HOST,
          port: Number(process.env.SMTP_PORT),
          auth: {
            user: process.env.SMTP_USER,
            pass: process.env.SMTP_PASS,
          },
        });
      }

      // Generate a test Ethereal inbox on the fly for development
      const testAccount = await nodemailer.createTestAccount();
      console.log(`[mail] Created Ethereal test inbox: ${testAccount.user}`);
      return nodemailer.createTransport({
        host: "smtp.ethereal.email",
        port: 587,
        secure: false,
        auth: {
          user: testAccount.user,
          pass: testAccount.pass,
        },
      });
    })();
  }
  return transporterPromise;
}

/**
 * Dispatches emails using Nodemailer with automatic Ethereal test account fallback.
 * Generates and logs a clickable preview URL for local inspection.
 */
async function sendMail(from, to, subject, body, prefix = "[mail]") {
  if (!to) {
    throw new Error("Cannot send mail: recipient address missing");
  }
  console.log(`${prefix} sending "${subject}" to ${to}`);

  const transporter = await getTransporter();
  const info = await transporter.sendMail({
    from: from || "noreply@nexqueue.dev",
    to,
    subject,
    text: body,
  });

  const previewUrl = nodemailer.getTestMessageUrl(info);
  if (previewUrl) {
    console.log(`${prefix} Preview URL: ${previewUrl}`);
  }

  return { sent: true, to, messageId: info.messageId, previewUrl: previewUrl || null };
}

const mailWorker = new Worker(
  "mail-queue",
  async (job) => {
    const { from, to, subject, body, correlationId } = job.data;
    const prefix = correlationId ? `[mail][${correlationId}]` : `[mail]`;
    console.log(`${prefix} processing job ${job.id}`);
    return sendMail(from, to, subject, body, prefix);
  },
  { connection }
);

mailWorker.on("completed", (job) => {
  console.log(`[mail] job ${job.id} completed`);
});

mailWorker.on("failed", (job, err) => {
  const attempts = job?.attemptsMade || 1;
  const maxAttempts = job?.opts?.attempts || 1;
  console.error(`[mail] job ${job?.id} failed (attempt ${attempts}/${maxAttempts}):`, err.message);
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
