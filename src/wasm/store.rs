use nanocode::durability::{Sql, SqlRow, SqlStatement, StoreError, StoreFuture};
use serde_json::json;

use super::bridge::call;

pub struct WorkerStore;

impl Sql for WorkerStore {
    fn transaction<'a>(
        &'a self,
        operation: &'static str,
        statements: &'a [SqlStatement<'a>],
    ) -> StoreFuture<'a, Result<Vec<Vec<SqlRow>>, StoreError>> {
        Box::pin(async move {
            let rows =
                call(json!({"op": "store", "operation": operation, "statements": statements}))
                    .await
                    .map_err(|error| StoreError::Backend(error.to_string()))?;
            serde_json::from_value(rows).map_err(|error| StoreError::Backend(error.to_string()))
        })
    }
}
