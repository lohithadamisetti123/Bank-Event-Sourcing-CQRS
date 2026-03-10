import { query } from "./db";
import { BankEvent } from "./types";

async function ensureProjectionStatusRow(name: string) {
  await query(
    `INSERT INTO projection_status (name, last_processed_event_number_global)
     VALUES ($1, 0)
     ON CONFLICT (name) DO NOTHING`,
    [name]
  );
}

export async function projectEvent(event: BankEvent) {
  await projectAccountSummaries(event);
  await projectTransactionHistory(event);
}

async function projectAccountSummaries(event: BankEvent) {
  const name = "AccountSummaries";
  await ensureProjectionStatusRow(name);

  if (event.eventType === "AccountCreated") {
    const data = event.eventData;
    await query(
      `INSERT INTO account_summaries
         (account_id, owner_name, balance, currency, status, version)
       VALUES ($1,$2,$3,$4,$5,$6)
       ON CONFLICT (account_id) DO NOTHING`,
      [event.aggregateId, data.ownerName, data.initialBalance, data.currency, "OPEN", 1]
    );
  } else if (event.eventType === "MoneyDeposited") {
    await query(
      `UPDATE account_summaries
       SET balance = balance + $1, version = version + 1
       WHERE account_id = $2`,
      [event.eventData.amount, event.aggregateId]
    );
  } else if (event.eventType === "MoneyWithdrawn") {
    await query(
      `UPDATE account_summaries
       SET balance = balance - $1, version = version + 1
       WHERE account_id = $2`,
      [event.eventData.amount, event.aggregateId]
    );
  } else if (event.eventType === "AccountClosed") {
    await query(
      `UPDATE account_summaries
       SET status = 'CLOSED', version = version + 1
       WHERE account_id = $1`,
      [event.aggregateId]
    );
  }

  await query(
    `UPDATE projection_status
     SET last_processed_event_number_global =
         GREATEST(last_processed_event_number_global, $1)
     WHERE name = $2`,
    [event.eventNumber, name]
  );
}

async function projectTransactionHistory(event: BankEvent) {
  const name = "TransactionHistory";
  await ensureProjectionStatusRow(name);

  if (event.eventType === "MoneyDeposited" || event.eventType === "MoneyWithdrawn") {
    await query(
      `INSERT INTO transaction_history
         (transaction_id, account_id, type, amount, description, timestamp)
       VALUES ($1,$2,$3,$4,$5,$6)
       ON CONFLICT (transaction_id) DO NOTHING`,
      [
        event.eventData.transactionId,
        event.aggregateId,
        event.eventType === "MoneyDeposited" ? "DEPOSIT" : "WITHDRAWAL",
        event.eventData.amount,
        event.eventData.description,
        event.timestamp,
      ]
    );
  }

  await query(
    `UPDATE projection_status
     SET last_processed_event_number_global =
         GREATEST(last_processed_event_number_global, $1)
     WHERE name = $2`,
    [event.eventNumber, name]
  );
}

export async function rebuildProjections() {
  await query("TRUNCATE account_summaries RESTART IDENTITY CASCADE");
  await query("TRUNCATE transaction_history RESTART IDENTITY CASCADE");
  await query("TRUNCATE projection_status RESTART IDENTITY CASCADE");

  const { rows: events } = await query(
    `SELECT event_id as "eventId",
            aggregate_id as "aggregateId",
            aggregate_type as "aggregateType",
            event_type as "eventType",
            event_data as "eventData",
            event_number as "eventNumber",
            timestamp,
            version
     FROM events
     ORDER BY event_number ASC`
  );
  for (const ev of events as BankEvent[]) {
    await projectEvent(ev);
  }
}

export async function getProjectionStatus() {
  const { rows: countRows } = await query(
    "SELECT COUNT(*)::bigint as count FROM events"
  );
  const totalEventsInStore = parseInt(countRows[0].count, 10);

  const { rows } = await query(
    "SELECT name, last_processed_event_number_global FROM projection_status"
  );

  const projections = rows.map((r: any) => ({
    name: r.name,
    lastProcessedEventNumberGlobal: parseInt(r.last_processed_event_number_global, 10),
    lag: totalEventsInStore - parseInt(r.last_processed_event_number_global, 10),
  }));

  return { totalEventsInStore, projections };
}
