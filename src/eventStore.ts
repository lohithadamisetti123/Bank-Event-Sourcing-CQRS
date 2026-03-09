import { v4 as uuidv4 } from "uuid";
import { query } from "./db";
import { BankEvent, BankEventType, BankAccountState } from "./types";

const AGGREGATE_TYPE = "BankAccount";
const SNAPSHOT_INTERVAL = 50;

export async function getEventsForAggregate(aggregateId: string): Promise<BankEvent[]> {
  const { rows } = await query<BankEvent>(
    "SELECT event_id as \"eventId\", aggregate_id as \"aggregateId\", aggregate_type as \"aggregateType\", event_type as \"eventType\", event_data as \"eventData\", event_number as \"eventNumber\", timestamp, version FROM events WHERE aggregate_id = $1 ORDER BY event_number ASC",
    [aggregateId]
  );
  return rows;
}

export async function getEventsAfter(aggregateId: string, lastNumber: number): Promise<BankEvent[]> {
  const { rows } = await query<BankEvent>(
    "SELECT event_id as \"eventId\", aggregate_id as \"aggregateId\", aggregate_type as \"aggregateType\", event_type as \"eventType\", event_data as \"eventData\", event_number as \"eventNumber\", timestamp, version FROM events WHERE aggregate_id = $1 AND event_number > $2 ORDER BY event_number ASC",
    [aggregateId, lastNumber]
  );
  return rows;
}

export async function getAllEvents(): Promise<BankEvent[]> {
  const { rows } = await query<BankEvent>(
    "SELECT event_id as \"eventId\", aggregate_id as \"aggregateId\", aggregate_type as \"aggregateType\", event_type as \"eventType\", event_data as \"eventData\", event_number as \"eventNumber\", timestamp, version FROM events ORDER BY event_number ASC"
  );
  return rows;
}

export async function appendEvent(
  client: any,
  aggregateId: string,
  eventType: BankEventType,
  eventData: any
): Promise<BankEvent> {
  const { rows: lastRows } = await client.query(
    "SELECT COALESCE(MAX(event_number), 0) AS last FROM events WHERE aggregate_id = $1",
    [aggregateId]
  );
  const lastNumber: number = lastRows[0].last;
  const nextNumber = lastNumber + 1;
  const eventId = uuidv4();

  const insertQuery =
    "INSERT INTO events (event_id, aggregate_id, aggregate_type, event_type, event_data, event_number) VALUES ($1,$2,$3,$4,$5,$6) RETURNING event_id as \"eventId\", aggregate_id as \"aggregateId\", aggregate_type as \"aggregateType\", event_type as \"eventType\", event_data as \"eventData\", event_number as \"eventNumber\", timestamp, version";

  const { rows } = await client.query(insertQuery, [
    eventId,
    aggregateId,
    AGGREGATE_TYPE,
    eventType,
    eventData,
    nextNumber,
  ]);

  await ensureSnapshot(client, aggregateId, nextNumber);

  const [event] = rows;
  return event;
}

export function applyEvent(state: BankAccountState | null, event: BankEvent): BankAccountState {
  switch (event.eventType) {
    case "AccountCreated": {
      return {
        accountId: event.aggregateId,
        ownerName: event.eventData.ownerName,
        balance: event.eventData.initialBalance,
        currency: event.eventData.currency,
        status: "OPEN",
        version: event.eventData.version ?? 1,
      };
    }
    case "MoneyDeposited": {
      if (!state) throw new Error("State not initialized");
      return {
        ...state,
        balance: state.balance + event.eventData.amount,
        version: state.version + 1,
      };
    }
    case "MoneyWithdrawn": {
      if (!state) throw new Error("State not initialized");
      return {
        ...state,
        balance: state.balance - event.eventData.amount,
        version: state.version + 1,
      };
    }
    case "AccountClosed": {
      if (!state) throw new Error("State not initialized");
      return {
        ...state,
        status: "CLOSED",
        version: state.version + 1,
      };
    }
    default:
      return state!;
  }
}

export async function loadLatestSnapshot(aggregateId: string): Promise<{ state: BankAccountState | null; lastEventNumber: number }> {
  const { rows } = await query(
    "SELECT snapshot_data, last_event_number FROM snapshots WHERE aggregate_id = $1",
    [aggregateId]
  );
  if (rows.length === 0) {
    return { state: null, lastEventNumber: 0 };
  }
  return {
    state: rows[0].snapshot_data as BankAccountState,
    lastEventNumber: rows[0].last_event_number as number,
  };
}

async function ensureSnapshot(client: any, aggregateId: string, currentEventNumber: number) {
  if (currentEventNumber % SNAPSHOT_INTERVAL !== 1) {
    return;
  }
  const { state } = await reconstructStateFromEvents(aggregateId);
  if (!state) return;

  const snapshotId = uuidv4();
  const { rows } = await client.query(
    "SELECT MAX(event_number) AS last FROM events WHERE aggregate_id = $1",
    [aggregateId]
  );
  const lastNumber = rows[0].last || currentEventNumber;

  await client.query(
    `INSERT INTO snapshots (snapshot_id, aggregate_id, snapshot_data, last_event_number)
     VALUES ($1,$2,$3,$4)
     ON CONFLICT (aggregate_id)
     DO UPDATE SET snapshot_data = EXCLUDED.snapshot_data, last_event_number = EXCLUDED.last_event_number, created_at = NOW()`,
    [snapshotId, aggregateId, state, lastNumber]
  );
}

export async function reconstructStateFromEvents(aggregateId: string): Promise<{ state: BankAccountState | null; events: BankEvent[] }> {
  const { state: snapshotState, lastEventNumber } = await loadLatestSnapshot(aggregateId);
  const events = await getEventsAfter(aggregateId, lastEventNumber);
  let currentState: BankAccountState | null = snapshotState;
  for (const ev of events) {
    currentState = applyEvent(currentState, ev);
  }
  return { state: currentState, events };
}
