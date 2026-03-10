import express from "express";
import dotenv from "dotenv";
import {
  createAccount,
  deposit,
  withdraw,
  closeAccount,
  getAccountSummary,
  getAccountEvents,
  getBalanceAt,
  getTransactions,
} from "./accounts";
import { rebuildProjections, getProjectionStatus } from "./projections";

dotenv.config();

const app = express();
app.use(express.json());

const port = process.env.API_PORT || "8080";

app.get("/health", (_req, res) => {
  res.status(200).json({ status: "ok" });
});

// Commands
app.post("/api/accounts", async (req, res) => {
  try {
    const result = await createAccount(req.body);
    res.status(result.status).json(result.body);
  } catch (err: any) {
    res.status(500).json({ message: err.message || "Internal server error" });
  }
});

app.post("/api/accounts/:accountId/deposit", async (req, res) => {
  try {
    const result = await deposit(req.params.accountId, req.body);
    res.status(result.status).json(result.body);
  } catch (err: any) {
    if (err.message === "Invalid amount") {
      res.status(400).json({ message: err.message });
    } else {
      res.status(500).json({ message: err.message || "Internal server error" });
    }
  }
});

app.post("/api/accounts/:accountId/withdraw", async (req, res) => {
  try {
    const result = await withdraw(req.params.accountId, req.body);
    res.status(result.status).json(result.body);
  } catch (err: any) {
    if (err.message === "Invalid amount") {
      res.status(400).json({ message: err.message });
    } else {
      res.status(500).json({ message: err.message || "Internal server error" });
    }
  }
});

app.post("/api/accounts/:accountId/close", async (req, res) => {
  try {
    const result = await closeAccount(req.params.accountId, req.body);
    res.status(result.status).json(result.body);
  } catch (err: any) {
    res.status(500).json({ message: err.message || "Internal server error" });
  }
});

// Queries
app.get("/api/accounts/:accountId", async (req, res) => {
  try {
    const result = await getAccountSummary(req.params.accountId);
    res.status(result.status).json(result.body);
  } catch (err: any) {
    res.status(500).json({ message: err.message || "Internal server error" });
  }
});

app.get("/api/accounts/:accountId/events", async (req, res) => {
  try {
    const result = await getAccountEvents(req.params.accountId);
    res.status(result.status).json(result.body);
  } catch (err: any) {
    res.status(500).json({ message: err.message || "Internal server error" });
  }
});

app.get("/api/accounts/:accountId/balance-at/:timestamp", async (req, res) => {
  try {
    const { accountId, timestamp } = req.params;
    const result = await getBalanceAt(accountId, decodeURIComponent(timestamp));
    res.status(result.status).json(result.body);
  } catch (err: any) {
    res.status(500).json({ message: err.message || "Internal server error" });
  }
});

app.get("/api/accounts/:accountId/transactions", async (req, res) => {
  try {
    const { accountId } = req.params;
    const page = parseInt((req.query.page as string) || "1", 10);
    const pageSize = parseInt((req.query.pageSize as string) || "10", 10);
    const result = await getTransactions(accountId, page, pageSize);
    res.status(result.status).json(result.body);
  } catch (err: any) {
    res.status(500).json({ message: err.message || "Internal server error" });
  }
});

// Projections admin
app.post("/api/projections/rebuild", async (_req, res) => {
  try {
    await rebuildProjections();
    res.status(202).json({ message: "Projection rebuild initiated." });
  } catch (err: any) {
    res.status(500).json({ message: err.message || "Internal server error" });
  }
});

app.get("/api/projections/status", async (_req, res) => {
  try {
    const status = await getProjectionStatus();
    res.status(200).json(status);
  } catch (err: any) {
    res.status(500).json({ message: err.message || "Internal server error" });
  }
});

app.listen(port, () => {
  console.log(`API listening on port ${port}`);
});
