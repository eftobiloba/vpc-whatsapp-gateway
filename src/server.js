require("dotenv").config();

const express = require("express");
const cors = require("cors");

const routes = require("./routes");
const { startWhatsApp, getHealth } = require("./whatsapp");

const app = express();

app.use(cors());
app.use(express.json());

app.get("/health", (req, res) => {
  res.json(getHealth());
});

app.use((req, res, next) => {
  const apiKey = req.headers["x-api-key"];

  if (apiKey !== process.env.API_KEY) {
    return res.status(401).json({
      error: "Unauthorized"
    });
  }

  next();
});

app.use("/api", routes);

app.get("/", (req, res) => {
  res.send("WhatsApp Gateway Running");
});

const PORT = process.env.PORT || 3000;

startWhatsApp();

app.listen(PORT, () => {
  console.log(`🚀 Server running on ${PORT}`);
});