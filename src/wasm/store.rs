use nanocode::durability::{
    OwnedState, OwnerId, OwnerToken, StateStore, StoreError, StoreFuture, StoreRecord, StoredState,
};
use serde_json::{Value, json};

use super::bridge::call;

pub struct WorkerStore;

fn counter(value: &Value, key: &str) -> Result<u64, StoreError> {
    value[key]
        .as_str()
        .ok_or_else(|| StoreError::Backend(format!("missing {key}")))?
        .parse()
        .map_err(|e| StoreError::Backend(format!("invalid {key}: {e}")))
}

fn state(value: &Value) -> Result<StoredState, StoreError> {
    Ok(StoredState {
        revision: counter(value, "revision")?,
        payload: serde_json::from_value(value["payload"].clone())
            .map_err(|e| StoreError::Backend(e.to_string()))?,
    })
}

fn error(value: Value) -> StoreError {
    match value["kind"].as_str() {
        Some("fenced") => StoreError::Fenced,
        Some("conflict") => match (counter(&value, "expected"), counter(&value, "actual")) {
            (Ok(expected), Ok(actual)) => StoreError::Conflict { expected, actual },
            _ => StoreError::Backend(value.to_string()),
        },
        Some("not_committed") => StoreError::NotCommitted(value.to_string()),
        _ => StoreError::Backend(value.to_string()),
    }
}

impl StateStore for WorkerStore {
    fn read_record<'a>(
        &'a mut self,
        state_id: &'a str,
        key: &'a str,
    ) -> StoreFuture<'a, Result<Option<String>, StoreError>> {
        Box::pin(async move {
            serde_json::from_value(
                call(json!({"op": "read_record", "state_id": state_id, "key": key}))
                    .await
                    .map_err(error)?,
            )
            .map_err(|e| StoreError::Backend(e.to_string()))
        })
    }

    fn read_records<'a>(
        &'a mut self,
        state_id: &'a str,
        keys: &'a [String],
    ) -> StoreFuture<'a, Result<Vec<Option<String>>, StoreError>> {
        Box::pin(async move {
            serde_json::from_value(
                call(json!({"op": "read_records", "state_id": state_id, "keys": keys}))
                    .await
                    .map_err(error)?,
            )
            .map_err(|e| StoreError::Backend(e.to_string()))
        })
    }

    fn peek<'a>(
        &'a mut self,
        state_id: &'a str,
    ) -> StoreFuture<'a, Result<StoredState, StoreError>> {
        Box::pin(async move {
            state(
                &call(json!({"op": "peek", "state_id": state_id}))
                    .await
                    .map_err(error)?,
            )
        })
    }

    fn list_states<'a>(
        &'a mut self,
        limit: usize,
    ) -> StoreFuture<'a, Result<Vec<String>, StoreError>> {
        Box::pin(async move {
            serde_json::from_value(
                call(json!({"op": "list_states", "limit": limit}))
                    .await
                    .map_err(error)?,
            )
            .map_err(|e| StoreError::Backend(e.to_string()))
        })
    }

    fn acquire<'a>(
        &'a mut self,
        state_id: &'a str,
        owner_id: OwnerId,
    ) -> StoreFuture<'a, Result<OwnedState, StoreError>> {
        Box::pin(async move {
            let value =
                call(json!({"op": "acquire", "state_id": state_id, "owner_id": owner_id.as_str()}))
                    .await
                    .map_err(error)?;
            Ok(OwnedState {
                owner: OwnerToken::new(owner_id, counter(&value, "fence")?),
                state: state(&value)?,
            })
        })
    }

    fn replace<'a>(
        &'a mut self,
        state_id: &'a str,
        owner: &'a OwnerToken,
        expected_revision: u64,
        payload: &'a str,
        records: &'a [StoreRecord],
    ) -> StoreFuture<'a, Result<u64, StoreError>> {
        Box::pin(async move {
            let value = call(json!({"op": "replace", "state_id": state_id, "owner_id": owner.owner_id().as_str(), "fence": owner.fence().to_string(), "expected_revision": expected_revision.to_string(), "payload": payload, "records": records})).await.map_err(error)?;
            counter(&value, "revision")
        })
    }
}
