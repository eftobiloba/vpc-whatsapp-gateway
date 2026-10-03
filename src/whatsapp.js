const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  proto
} = require("@whiskeysockets/baileys");
const qrcode = require("qrcode-terminal");
const pino = require("pino");
const https = require("https");
const http = require("http");
const { URL } = require("url");

const logger = pino({ level: process.env.LOG_LEVEL || "info" });

let sock = null;
let connected = false;
let lastConnection = null;
let lastDisconnect = null;
const sendQueue = [];
let isProcessingQueue = false;
const messageLog = [];
const QUEUE_INTERVAL_MS = Number(process.env.QUEUE_INTERVAL_MS || 500);
const WEBHOOK_URL = process.env.WEBHOOK_URL;

function normalizePhoneNumber(number) {
  return String(number).replace(/\D/g, "");
}

function isValidPhoneNumber(number) {
  return /^\d{8,15}$/.test(number);
}

function formatJid(number) {
  return `${normalizePhoneNumber(number)}@s.whatsapp.net`;
}

async function sendWebhookEvent(event, data) {
  if (!WEBHOOK_URL) {
    return;
  }

  const payload = {
    event,
    data,
    timestamp: new Date().toISOString()
  };

  try {
    if (typeof fetch === "function") {
      await fetch(WEBHOOK_URL, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify(payload)
      });
      return;
    }

    const url = new URL(WEBHOOK_URL);
    const body = JSON.stringify(payload);
    const requestOptions = {
      hostname: url.hostname,
      port: url.port || (url.protocol === "https:" ? 443 : 80),
      path: `${url.pathname}${url.search}`,
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(body)
      }
    };

    await new Promise((resolve, reject) => {
      const request = (url.protocol === "https:" ? https : http).request(requestOptions, (response) => {
        let responseBody = "";
        response.on("data", (chunk) => {
          responseBody += chunk;
        });
        response.on("end", () => {
          if (response.statusCode >= 200 && response.statusCode < 300) {
            resolve();
          } else {
            reject(new Error(`Webhook failed ${response.statusCode}: ${responseBody}`));
          }
        });
      });

      request.on("error", reject);
      request.write(body);
      request.end();
    });
  } catch (err) {
    logger.warn({ err: err.message, event, webhook: WEBHOOK_URL }, "Webhook delivery failed");
  }
}

function createMessageLog(entry) {
  messageLog.push(entry);
  if (messageLog.length > 200) {
    messageLog.shift();
  }
}

function getQueueStatus() {
  return {
    length: sendQueue.length,
    processing: isProcessingQueue
  };
}

function getHealth() {
  return {
    connected,
    lastConnection,
    lastDisconnect,
    queue: getQueueStatus(),
    webhookEnabled: Boolean(WEBHOOK_URL)
  };
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function shouldRetry(err, retries) {
  if (retries >= 2) {
    return false;
  }

  return /429|rate limit|rate-limit|retry|blocked/i.test(err?.message || "");
}

async function validateNumber(number) {
  const normalized = normalizePhoneNumber(number);

  if (!isValidPhoneNumber(normalized)) {
    throw new Error("Invalid phone number format");
  }

  const sockInstance = getSocket();

  if (!sockInstance) {
    throw new Error("WhatsApp not connected");
  }

  const result = await sockInstance.onWhatsApp(normalized);

  return Boolean(result?.[0]?.exists);
}

async function enqueueSend(jid, content, metadata = {}) {
  return new Promise((resolve, reject) => {
    const job = {
      jid,
      content,
      metadata,
      resolve,
      reject,
      retries: 0
    };

    sendQueue.push(job);
    logger.info({ jid, queueLength: sendQueue.length }, "Message queued");
    processSendQueue();
  });
}

async function processSendQueue() {
  if (isProcessingQueue) {
    return;
  }

  isProcessingQueue = true;

  while (sendQueue.length > 0) {
    const job = sendQueue.shift();

    if (!sock) {
      job.reject(new Error("WhatsApp not connected"));
      continue;
    }

    try {
      const result = await sock.sendMessage(job.jid, job.content);
      const payload = {
        jid: job.jid,
        messageId: result.key?.id || null,
        metadata: job.metadata,
        timestamp: new Date().toISOString()
      };

      logger.info({ payload }, "Message sent");
      createMessageLog({ event: "sent", ...payload });
      await sendWebhookEvent("sent", payload);
      job.resolve(result);
    } catch (err) {
      logger.error({ err: err.message, jid: job.jid, retries: job.retries }, "Send failed");

      if (shouldRetry(err, job.retries)) {
        job.retries += 1;
        sendQueue.unshift(job);
        await delay(2000);
        continue;
      }

      job.reject(err);
    }

    await delay(QUEUE_INTERVAL_MS);
  }

  isProcessingQueue = false;
}

function handleMessageStatusUpdates(updates) {
  updates.forEach((update) => {
    const fromMe = update.key?.fromMe;
    const status = update.update?.status;

    if (!fromMe || typeof status === "undefined") {
      return;
    }

    let event = null;

    if (status === proto.WebMessageInfo.Status.DELIVERY_ACK) {
      event = "delivered";
    } else if (status === proto.WebMessageInfo.Status.READ) {
      event = "read";
    }

    if (!event) {
      return;
    }

    const payload = {
      jid: update.key.remoteJid,
      messageId: update.key.id,
      status,
      timestamp: new Date().toISOString()
    };

    logger.info({ event, payload }, "Message status update");
    createMessageLog({ event, ...payload });
    sendWebhookEvent(event, payload);
  });
}

function handleReceiptUpdates(receipts) {
  receipts.forEach((receiptUpdate) => {
    const fromMe = receiptUpdate.key?.fromMe;
    const receipt = receiptUpdate.receipt;

    if (!fromMe || !receipt) {
      return;
    }

    let event = null;
    let time = null;

    if (receipt.readTimestamp) {
      event = "read";
      time = receipt.readTimestamp;
    } else if (receipt.receiptTimestamp) {
      event = "delivered";
      time = receipt.receiptTimestamp;
    }

    if (!event) {
      return;
    }

    const payload = {
      jid: receiptUpdate.key.remoteJid,
      messageId: receiptUpdate.key.id,
      userJid: receipt.userJid,
      status: event,
      timestamp: new Date(time * 1000).toISOString()
    };

    logger.info({ event, payload }, "Message receipt update");
    createMessageLog({ event, ...payload });
    sendWebhookEvent(event, payload);
  });
}

async function startWhatsApp() {
  const { state, saveCreds } = await useMultiFileAuthState("./auth");

  sock = makeWASocket({
    auth: state
  });

  sock.ev.on("creds.update", saveCreds);

  sock.ev.on("connection.update", ({ connection, qr, lastDisconnect: disconnect }) => {
    lastConnection = connection === "open" ? new Date().toISOString() : lastConnection;
    lastDisconnect = connection === "close" ? new Date().toISOString() : lastDisconnect;
    connected = connection === "open";

    if (qr) {
      console.clear();
      console.log("\n📱 Scan this QR code with WhatsApp:\n");
      qrcode.generate(qr, {
        small: true
      });
    }

    if (connection === "open") {
      logger.info("✅ WhatsApp connected");
    }

    if (connection === "close") {
      logger.warn("❌ WhatsApp connection closed");

      const shouldReconnect =
        disconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut;

      if (shouldReconnect) {
        logger.info("🔄 Reconnecting...");
        startWhatsApp();
      } else {
        logger.warn("🚪 Logged out — not reconnecting. Delete the auth/ folder and restart to re-pair.");
      }
    }
  });

  sock.ev.on("messages.update", handleMessageStatusUpdates);
  sock.ev.on("message-receipt.update", handleReceiptUpdates);
}

function getSocket() {
  return sock;
}

module.exports = {
  startWhatsApp,
  getSocket,
  enqueueSend,
  validateNumber,
  formatJid,
  getQueueStatus,
  getHealth
};