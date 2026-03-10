import { Pool } from "pg";

const databaseUrl = process.env.DATABASE_URL;
if (!databaseUrl) {
  throw new Error("DATABASE_URL is not set");
}

export const pool = new Pool({
  connectionString: databaseUrl,
});

export async function query<T = any>(text: string, params?: any[]): Promise<{ rows: T[] }> {
  return pool.query<T>(text, params);
}
