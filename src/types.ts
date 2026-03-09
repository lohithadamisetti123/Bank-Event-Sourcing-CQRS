export type AccountStatus = "OPEN" | "CLOSED";

export interface BankAccountState {
  accountId: string;
  ownerName: string;
  balance: number;
  currency: string;
  status: AccountStatus;
  version: number; // concurrency
}

export type BankEventType = "AccountCreated" | "MoneyDeposited" | "MoneyWithdrawn" | "AccountClosed";

export interface BankEvent {
  eventId: string;
  aggregateId: string;
  aggregateType: string; // "BankAccount"
  eventType: BankEventType;
  eventData: any;
  eventNumber: number;
  timestamp: string;
  version: number;
}
