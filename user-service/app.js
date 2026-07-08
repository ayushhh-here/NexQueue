require("dotenv").config({ quiet: true });
const express = require("express");
const { Queue, Worker } = require("bullmq");

const app = express();
const PORT = process.env.PORT || 5001;

const connection = {
  host: process.env.REDIS_HOST || "127.0.0.1",
  port: Number(process.env.REDIS_PORT) || 6379,
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

// ---------------------------------------------------------------------------
// Producer: this is the public HTTP entrypoint of the system. It validates
// the incoming request, then hands the work off to order-server by placing
// a job on "order-queue". The HTTP response returns immediately — it does
// NOT wait for the order to actually be verified/processed. That work
// happens asynchronously in order-server.
// ---------------------------------------------------------------------------
const orderQueue = new Queue("order-queue", { connection });

app.post("/order", async (req, res) => {
  const { userId, productName, price } = req.body || {};

  if (
    !userId ||
    !productName ||
    typeof price !== "number" ||
    Number.isNaN(price) ||
    price < 0
  ) {
    return res.status(400).json({
      message: "userId, productName and a non-negative numeric price are required",
    });
  }

  try {
    const orderId = `order_${Date.now()}`;
    const job = await orderQueue.add("Process Order", {
      orderId,
      userId,
      productName,
      price,
    });

    return res.status(202).json({
      message: "Order accepted, processing in background",
      orderId,
      jobId: job.id,
    });
  } catch (error) {
    console.error("Failed to enqueue order:", error);
    return res.status(500).json({ message: "Could not accept order" });
  }
});

app.get("/health", (_req, res) => res.json({ status: "ok" }));

// ---------------------------------------------------------------------------
// Consumer: user-service also runs the worker that answers "is this user
// valid?" requests coming from order-server, since it is the service that
// owns user data.
// ---------------------------------------------------------------------------
const verificationWorker = new Worker(
  "user-verification-queue",
  async (job) => {
    const { userId } = job.data;
    console.log(`[verification] received job ${job.id} for userId=${userId}`);

    const foundUser = userDb.find((item) => item.id === userId);
    const isValidUser = Boolean(foundUser);

    // Never leak the password back through the queue payload.
    // eslint-disable-next-line no-unused-vars
    const { password: _password, ...rest } = foundUser || {};

    console.log(`[verification] userId=${userId} valid=${isValidUser}`);
    return { isValidUser, user: isValidUser ? rest : null };
  },
  { connection }
);

verificationWorker.on("failed", (job, err) => {
  console.error(`[verification] job ${job?.id} failed:`, err.message);
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
