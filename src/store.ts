import { Timings } from "./timing";

type Row = Record<string, SqlStorageValue>;

export class StoreFailure extends Error {
  constructor(readonly kind: string, message: string, readonly expected?: string, readonly actual?: string) {
    super(message);
  }
}

export class CheckpointStore {
  readonly timings = new Timings();
  readonly metrics = { operations: {} as Record<string, number>, point_queries: 0 };
  constructor(readonly storage: DurableObjectStorage) {
    storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS states (id TEXT PRIMARY KEY, revision TEXT NOT NULL, payload TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS owners (id TEXT PRIMARY KEY, owner TEXT NOT NULL, fence TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS records (state_id TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(state_id, key)) WITHOUT ROWID;
    `);
  }

  row(query: string, ...values: SqlStorageValue[]): Row | undefined {
    this.metrics.point_queries++;
    return this.storage.sql.exec(query, ...values).toArray()[0];
  }

  peek(id: string) {
    const row = this.row("SELECT revision, payload FROM states WHERE id = ?", id);
    return row ?? { revision: "0", payload: null };
  }

  async execute(request: any, observer?: Timings): Promise<unknown> {
    const started = performance.now();
    try { return await this.operation(request, observer); }
    finally {
      const elapsed = performance.now() - started;
      this.timings.record(request.op, elapsed);
      observer?.record(request.op, elapsed);
    }
  }

  private async sync(operation: string, observer?: Timings): Promise<void> {
    const started = performance.now();
    try { await this.storage.sync(); }
    finally {
      const elapsed = performance.now() - started;
      this.timings.record(`${operation}.sync`, elapsed);
      observer?.record(`${operation}.sync`, elapsed);
    }
  }

  private async operation(request: any, observer?: Timings): Promise<unknown> {
    this.metrics.operations[request.op] = (this.metrics.operations[request.op] ?? 0) + 1;
    const id = request.state_id;
    switch (request.op) {
      case "peek": return this.peek(id);
      case "read_record": return this.row("SELECT value FROM records WHERE state_id = ? AND key = ?", id, request.key)?.value ?? null;
      case "read_records": return this.storage.sql.exec("SELECT records.value FROM json_each(?) AS keys LEFT JOIN records ON records.state_id = ? AND records.key = keys.value ORDER BY keys.key", JSON.stringify(request.keys), id).toArray().map(row => row.value);
      case "list_states": {
        if (typeof request.limit !== "number" || request.limit < 0 || !Number.isFinite(request.limit)) throw new Error("invalid state listing limit");
        const limit = Number.isSafeInteger(request.limit) ? request.limit : -1;
        return this.storage.sql.exec("SELECT id FROM states ORDER BY rowid DESC LIMIT ?", limit).toArray().map(row => row.id);
      }
      case "acquire": {
        const value = this.storage.transactionSync(() => {
          const prior = this.row("SELECT fence FROM owners WHERE id = ?", id);
          const fence = this.increment(String(prior?.fence ?? "0"));
          this.storage.sql.exec("INSERT INTO owners VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE SET owner = excluded.owner, fence = excluded.fence", id, request.owner_id, fence);
          return { ...this.peek(id), fence };
        });
        await this.sync("acquire", observer);
        return value;
      }
      case "replace": {
        const value = this.storage.transactionSync(() => {
          const owner = this.row("SELECT owner, fence FROM owners WHERE id = ?", id);
          if (!owner || owner.owner !== request.owner_id || owner.fence !== request.fence) throw new StoreFailure("fenced", "state owner was fenced");
          const actual = String(this.row("SELECT revision FROM states WHERE id = ?", id)?.revision ?? "0");
          if (actual !== request.expected_revision) throw new StoreFailure("conflict", "state revision conflict", request.expected_revision, actual);
          const revision = this.increment(actual);
          for (const record of request.records) {
            const inserted = this.row("INSERT INTO records VALUES (?, ?, ?) ON CONFLICT DO NOTHING RETURNING key", id, record.key, record.value);
            if (!inserted && this.row("SELECT value FROM records WHERE state_id = ? AND key = ?", id, record.key)?.value !== record.value) throw new StoreFailure("not_committed", "immutable record content conflict");
          }
          this.storage.sql.exec("INSERT INTO states VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE SET revision = excluded.revision, payload = excluded.payload", id, revision, request.payload);
          return { revision };
        });
        await this.sync("replace", observer);
        return value;
      }
      default: throw new Error("unknown checkpoint operation");
    }
  }

  increment(value: string): string {
    const next = BigInt(value) + 1n;
    if (next > 18446744073709551615n) throw new StoreFailure("not_committed", "u64 counter overflow");
    return String(next);
  }
}
