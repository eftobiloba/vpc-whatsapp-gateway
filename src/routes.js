const express = require("express");
const router = express.Router();

const {
  getSocket,
  enqueueSend,
  validateNumber,
  formatJid,
  getQueueStatus
} = require("./whatsapp");

router.get("/validate", async (req, res) => {
  try {
    const number = req.query.number;

    if (!number) {
      return res.status(400).json({ error: "number is required" });
    }

    const isValid = await validateNumber(number);

    return res.json({
      number,
      exists: isValid
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: err.message });
  }
});

router.post("/send", async (req, res) => {
  try {
    const { number, message } = req.body;

    if (!number || !message) {
      return res.status(400).json({
        error: "number and message are required"
      });
    }

    const sock = getSocket();

    if (!sock) {
      return res.status(503).json({
        error: "WhatsApp not connected"
      });
    }

    const exists = await validateNumber(number);

    if (!exists) {
      return res.status(422).json({
        error: "The phone number is not registered on WhatsApp"
      });
    }

    const result = await enqueueSend(
      formatJid(number),
      {
        text: message
      },
      {
        number
      }
    );

    res.json({
      success: true,
      messageId: result.key?.id || null,
      queue: getQueueStatus()
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: err.message
    });
  }
});

module.exports = router;