import { pool, query } from "./db";
import { appendEvent, reconstructStateFromEvents, getEventsForAggregate } from "./eventStore";
import { projectEvent } from "./projections";
import { BankAccountState, BankEvent } from "./types";

function validateAmount(amount: any): number {
  const num = Number(amount);
  if (isNaN(num) || num <= 0) {
    throw new Error("Invalid amount");
  }
  return num;
}

export async function createAccount(body: any) {
  const { accountId, ownerName, initialBalance, currency } = body;

  if (!accountId || !ownerName || !currency || initialBalance == null) {
    return { status: 400, body: { message: "Missing fields" } };
  }

  const { rows } = await query("SELECT 1 FROM account_summaries WHERE account_id = $1", [
    accountId,
  ]);
  if (rows.length > 0) {
    return { status: 409, body: { message: "Account already exists" } };
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const event = await appendEvent(client, accountId, "AccountCreated", {
      ownerName,
      initialBalance: Number(initialBalance),
      currency,
      version: 1,
    });
    await client.query("COMMIT");

    await projectEvent(event as BankEvent);

    return { status: 202, body: { message: "Account creation accepted" } };
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

export async function deposit(accountId: string, body: any) {
  const { amount, description, transactionId } = body;
  if (!transactionId || description == null || amount == null) {
    return { status: 400, body: { message: "Missing fields" } };
  }

  const parsedAmount = validateAmount(amount);

  const { state } = await reconstructStateFromEvents(accountId);
  if (!state) {
    return { status: 404, body: { message: "Account not found" } };
  }
  if (state.status === "CLOSED") {
    return { status: 409, body: { message: "Account is closed" } };
  }

  const dup = await query("SELECT 1 FROM transaction_history WHERE transaction_id = $1", [
    transactionId,
  ]);
  if (dup.rows.length > 0) {
    return { status: 202, body: { message: "Duplicate transaction ignored" } };
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const event = await appendEvent(client, accountId, "MoneyDeposited", {
      amount: parsedAmount,
      description,
      transactionId,
    });
    await client.query("COMMIT");

    await projectEvent(event as BankEvent);

    return { status: 202, body: { message: "Deposit accepted" } };
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

export async function withdraw(accountId: string, body: any) {
  const { amount, description, transactionId } = body;
  if (!transactionId || description == null || amount == null) {
    return { status: 400, body: { message: "Missing fields" } };
  }

  const parsedAmount = validateAmount(amount);

  const { state } = await reconstructStateFromEvents(accountId);
  if (!state) {
    return { status: 404, body: { message: "Account not found" } };
  }
  if (state.status === "CLOSED") {
    return { status: 409, body: { message: "Account is closed" } };
  }
  if (state.balance - parsedAmount < 0) {
    return { status: 409, body: { message: "Insufficient funds" } };
  }

  const dup = await query("SELECT 1 FROM transaction_history WHERE transaction_id = $1", [
    transactionId,
  ]);
  if (dup.rows.length > 0) {
    return { status: 202, body: { message: "Duplicate transaction ignored" } };
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const event = await appendEvent(client, accountId, "MoneyWithdrawn", {
      amount: parsedAmount,
      description,
      transactionId,
    });
    await client.query("COMMIT");

    await projectEvent(event as BankEvent);

    return { status: 202, body: { message: "Withdrawal accepted" } };
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

export async function closeAccount(accountId: string, body: any) {
  const { reason } = body;
  if (!reason) {
    return { status: 400, body: { message: "Missing reason" } };
  }

  const { state } = await reconstructStateFromEvents(accountId);
  if (!state) {
    return { status: 404, body: { message: "Account not found" } };
  }
  if (state.balance !== 0) {
    return { status: 409, body: { message: "Balance must be zero to close account" } };
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const event = await appendEvent(client, accountId, "AccountClosed", {
      reason,
    });
    await client.query("COMMIT");

    await projectEvent(event as BankEvent);

    return { status: 202, body: { message: "Account close accepted" } };
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

export async function getAccountSummary(accountId: string) {
  const { rows } = await query(
    `SELECT account_id as "accountId",
            owner_name as "ownerName",
            balance,
            currency,
            status
     FROM account_summaries
     WHERE account_id = $1`,
    [accountId]
  );
  if (rows.length === 0) {
    return { status: 404, body: { message: "Account not found" } };
  }
  const row = rows[0] as any;
  return {
    status: 200,
    body: {
      accountId: row.accountId,
      ownerName: row.ownerName,
      balance: Number(row.balance),
      currency: row.currency,
      status: row.status,
    },
  };
}

export async function getAccountEvents(accountId: string) {
  const events = await getEventsForAggregate(accountId);
  if (events.length === 0) {
    return { status: 404, body: { message: "Account not found" } };
  }
  const body = events.map((e) => ({
    eventId: e.eventId,
    eventType: e.eventType,
    eventNumber: e.eventNumber,
    data: e.eventData,
    timestamp: e.timestamp,
  }));
  return { status: 200, body };
}

export async function getBalanceAt(accountId: string, timestamp: string) {
  const { rows } = await query(
    `SELECT event_id as "eventId",
            aggregate_id as "aggregateId",
            aggregate_type as "aggregateType",
            event_type as "eventType",
            event_data as "eventData",
            event_number as "eventNumber",
            timestamp,
            version
     FROM events
     WHERE aggregate_id = $1 AND timestamp <= $2
     ORDER BY event_number ASC`,
    [accountId, timestamp]
  );
  if (rows.length === 0) {
    return { status: 404, body: { message: "Account not found" } };
  }
  let state: BankAccountState | null = null;
  for (const ev of rows as BankEvent[]) {
    if (ev.eventType === "AccountCreated") {
      state = {
        accountId,
        ownerName: ev.eventData.ownerName,
        balance: ev.eventData.initialBalance,
        currency: ev.eventData.currency,
        status: "OPEN",
        version: 1,
      };
    } else {
      const { applyEvent } = await import("./eventStore");
      state = applyEvent(state, ev);
    }
  }
  return {
    status: 200,
    body: {
      accountId,
      balanceAt: state?.balance ?? 0,
      timestamp,
    },
  };
}

export async function getTransactions(accountId: string, page: number, pageSize: number) {
  const offset = (page - 1) * pageSize;

  const { rows: countRows } = await query(
    "SELECT COUNT(*)::bigint AS count FROM transaction_history WHERE account_id = $1",
    [accountId]
  );
  const totalCount = parseInt(countRows[0].count, 10);
  if (totalCount === 0) {
    return {
      status: 200,
      body: {
        currentPage: page,
        pageSize,
        totalPages: 0,
        totalCount: 0,
        items: [],
      },
    };
  }

  const { rows } = await query(
    `SELECT transaction_id as "transactionId",
            type,
            amount,
            description,
            timestamp
     FROM transaction_history
     WHERE account_id = $1
     ORDER BY timestamp ASC
     LIMIT $2 OFFSET $3`,
    [accountId, pageSize, offset]
  );

  const totalPages = Math.ceil(totalCount / pageSize);

  return {
    status: 200,
    body: {
      currentPage: page,
      pageSize,
      totalPages,
      totalCount,
      items: rows.map((r: any) => ({
        transactionId: r.transactionId,
        type: r.type,
        amount: Number(r.amount),
        description: r.description,
        timestamp: r.timestamp,
      })),
    },
  };
}
