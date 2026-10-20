import { Timings } from "./timing";

type Row = Record<string, SqlStorageValue>;

export type Statement = [string, ...SqlStorageValue[]];

export class CheckpointStore {
  readonly timings = new Timings();
  readonly metrics = { operations: {} as Record<string, number>, statements: 0, point_queries: 0, rows_read: 0, rows_written: 0 };
  constructor(readonly storage: DurableObjectStorage) {}

  row(query: string, ...values: SqlStorageValue[]): Row | undefined {
    this.metrics.point_queries++;
    return this.storage.sql.exec(query, ...values).toArray()[0];
  }

  persisted(scope: string): Row | undefined {
    const stored = this.row("SELECT 1 FROM sqlite_schema WHERE name = 'records'") ? "(SELECT COUNT(*) FROM states) AS states, (SELECT COUNT(*) FROM records) AS records" : "0 AS states, 0 AS records";
    return this.row(`SELECT ${stored}, (SELECT COUNT(*) FROM jobs WHERE scope = ? AND status IN ('queued', 'running')) AS unfinished`, scope);
  }

  transaction(operation: string, statements: Statement[], observer?: Timings): SqlStorageValue[][][] {
    const started = performance.now();
    this.metrics.operations[operation] = (this.metrics.operations[operation] ?? 0) + 1;
    this.metrics.statements += statements.length;
    try {
      return this.storage.transactionSync(() => statements.map(([query, ...values]) => {
        const cursor = this.storage.sql.exec(query, ...values);
        const rows = [...cursor.raw()];
        this.metrics.rows_read += cursor.rowsRead;
        this.metrics.rows_written += cursor.rowsWritten;
        return rows;
      }));
    } finally {
      const elapsed = performance.now() - started;
      this.timings.record(operation, elapsed);
      observer?.record(operation, elapsed);
    }
  }
}
