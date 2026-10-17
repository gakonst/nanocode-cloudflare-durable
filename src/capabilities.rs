use std::{future::Future, pin::Pin};

use nanocode::tool_contract::{Tool, ToolContext, ToolDefinition, ToolInput, ToolOutput, ToolResult};
use serde_json::{Value, json};

pub struct Connections;
pub struct ConnectionRequest(pub &'static str);

impl Tool for Connections {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::function(
            "list_connections",
            "List the approved account connections available to this session. Credentials are never returned.",
            json!({"type":"object","properties":{},"additionalProperties":false}),
        )
    }

    fn supports_parallel_tool_calls(&self) -> bool { true }

    fn execute<'life0, 'life1, 'async_trait>(
        &'life0 self,
        input: ToolInput,
        _context: ToolContext<'life1>,
    ) -> Pin<Box<dyn Future<Output = ToolResult> + 'async_trait>>
    where 'life0: 'async_trait, 'life1: 'async_trait, Self: 'async_trait {
        Box::pin(async move {
            let _: Value = input.decode_json()?;
            let result = super::bridge::call(json!({"op":"connection_catalog"})).await
                .map_err(|_| std::io::Error::other("connection catalog unavailable"))?;
            Ok(ToolOutput::json(&result))
        })
    }
}

impl Tool for ConnectionRequest {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::function(
            if self.0 == "vault" { "vault_request" } else { "connector_request" },
            if self.0 == "vault" {
                "Send a request to an approved vault destination. The private proxy injects the credential. Only the HTTP status is returned."
            } else {
                "Call an approved account connector. The private proxy injects credentials and returns only the operator-approved response fields."
            },
            json!({"type":"object","properties":{
                "connection_id":{"type":"string"},
                "method":{"type":"string","enum":["GET","HEAD","POST","PUT","PATCH","DELETE"]},
                "path":{"type":"string"},
                "headers":{"type":"object","additionalProperties":{"type":"string"}},
                "body":{"type":"string"}
            },"required":["connection_id"],"additionalProperties":false}),
        )
    }

    fn supports_parallel_tool_calls(&self) -> bool { true }

    fn execute<'life0, 'life1, 'async_trait>(
        &'life0 self,
        input: ToolInput,
        context: ToolContext<'life1>,
    ) -> Pin<Box<dyn Future<Output = ToolResult> + 'async_trait>>
    where 'life0: 'async_trait, 'life1: 'async_trait, Self: 'async_trait {
        Box::pin(async move {
            let input: Value = input.decode_json()?;
            let result = super::bridge::call(json!({"op":"connection_request","kind":self.0,"input":input,"call_id":context.call_id()})).await
                .map_err(|_| std::io::Error::other("connection dispatch interrupted; outcome may be uncertain"))?;
            let success = result.get("ok") != Some(&Value::Bool(false));
            Ok(ToolOutput::from_json(result, success))
        })
    }
}
