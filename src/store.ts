import { Timings, type SqlHost, type SqlKind, type Statement } from "nanocode-wasm";

type Row = Record<string, SqlStorageValue>;

type Metrics = { operations: Record<string, number>; statements: number; point_queries: number; rows_read: number; rows_written: number };

const metrics = (): Metrics => ({ operations: {}, statements: 0, point_queries: 0, rows_read: 0, rows_written: 0 });

export class Metadata {
  private values?: Map<string, string>;
  constructor(private readonly storage: DurableObjectStorage) {}

  private entries(): Map<string, string> {
    return this.values ??= new Map(this.storage.sql.exec<{ key: string; value: string }>("SELECT key, value FROM metadata").toArray().map(row => [row.key, row.value]));
  }

  get(key: string): string | undefined {
    return this.entries().get(key);
  }

  first(prefix: string): [string, string] | undefined {
    let found: [string, string] | undefined;
    for (const entry of this.entries()) if (entry[0].startsWith(prefix) && (!found || entry[0] < found[0])) found = entry;
    return found;
  }

  insert(key: string, value: string): void {
    const entries = this.entries();
    if (entries.has(key)) return;
    this.storage.sql.exec("INSERT INTO metadata VALUES (?, ?)", key, value);
    entries.set(key, value);
  }

  set(key: string, value: string): void {
    const entries = this.entries();
    this.storage.sql.exec("INSERT INTO metadata VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", key, value);
    entries.set(key, value);
  }

  delete(key: string): void {
    const entries = this.entries();
    if (!entries.has(key)) return;
    this.storage.sql.exec("DELETE FROM metadata WHERE key = ?", key);
    entries.delete(key);
  }

  transaction<T>(operation: () => T): T {
    try {
      return this.storage.transactionSync(operation);
    } catch (error) {
      this.values = undefined;
      throw error;
    }
  }

  reset(): void {
    this.values = undefined;
  }
}

export const SQLITE_VALUE_LIMIT_BYTES = 8_388_608;

export class CheckpointStore implements SqlHost {
  readonly value_limit = SQLITE_VALUE_LIMIT_BYTES;
  readonly timings = new Timings();
  readonly metrics = metrics();
  readonly log = metrics();
  constructor(readonly storage: DurableObjectStorage) {}

  row(query: string, ...values: SqlStorageValue[]): Row | undefined {
    this.metrics.point_queries++;
    return this.storage.sql.exec(query, ...values).toArray()[0];
  }

  persisted(scope: string): Row | undefined {
    const tables = new Set(this.storage.sql.exec("SELECT name FROM sqlite_schema WHERE type = 'table' AND name IN ('records', 'turns')").toArray().map(row => row.name));
    const stored = tables.has("records") ? "(SELECT COUNT(*) FROM states) AS states, (SELECT COUNT(*) FROM records) AS records" : "0 AS states, 0 AS records";
    if (!tables.has("turns")) return this.row(`SELECT ${stored}, 0 AS unfinished`);
    return this.row(`SELECT ${stored}, (SELECT COUNT(*) FROM turns WHERE scope = ? AND status IN ('queued', 'running')) AS unfinished`, scope);
  }

  transaction(kind: SqlKind, operation: string, statements: Statement[]): SqlStorageValue[][][] {
    const started = performance.now();
    const into = kind === "log" ? this.log : this.metrics;
    into.operations[operation] = (into.operations[operation] ?? 0) + 1;
    into.statements += statements.length;
    try {
      return this.storage.transactionSync(() => statements.map(([query, ...values]) => {
        const cursor = this.storage.sql.exec(query, ...values);
        const rows = [...cursor.raw()];
        into.rows_read += cursor.rowsRead;
        into.rows_written += cursor.rowsWritten;
        return rows;
      }));
    } finally {
      this.timings.record(operation, performance.now() - started);
    }
  }
}
