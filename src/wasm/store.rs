use nanocode::durability::{Sql, SqlRow, SqlStatement, StoreError, StoreFuture};
use serde::Serialize;

use super::bridge::call_with;

pub struct WorkerStore(&'static str);

impl WorkerStore {
    pub const fn durability() -> Self {
        Self("store")
    }

    pub const fn log() -> Self {
        Self("log")
    }
}

#[derive(Serialize)]
struct Transaction<'a> {
    op: &'static str,
    operation: &'static str,
    statements: &'a [SqlStatement<'a>],
}

impl Sql for WorkerStore {
    fn transaction<'a>(
        &'a self,
        operation: &'static str,
        statements: &'a [SqlStatement<'a>],
    ) -> StoreFuture<'a, Result<Vec<Vec<SqlRow>>, StoreError>> {
        Box::pin(async move {
            let rows = call_with(&Transaction {
                op: self.0,
                operation,
                statements,
            })
            .await
            .map_err(|error| StoreError::Backend(error.to_string()))?;
            serde_json::from_value(rows).map_err(|error| StoreError::Backend(error.to_string()))
        })
    }
}
