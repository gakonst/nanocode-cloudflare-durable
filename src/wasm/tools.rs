use nanocode::tool_contract::{Tool, ToolContext, ToolDefinition, ToolInput, ToolOutput, ToolResult};
use nanocode_code_mode::embedded::HostFuture;
use serde::Deserialize;
use serde_json::{Value, json};

use super::bridge::call;

#[derive(Deserialize)]
pub struct HostTool {
    name: String,
    description: String,
    parameters: Value,
}

impl Tool for HostTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::function(
            self.name.as_str(),
            self.description.as_str(),
            self.parameters.clone(),
        )
    }

    fn supports_parallel_tool_calls(&self) -> bool {
        true
    }

    fn execute<'life0, 'life1, 'async_trait>(
        &'life0 self,
        input: ToolInput,
        _context: ToolContext<'life1>,
    ) -> HostFuture<'async_trait, ToolResult>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let input: Value = input.decode_json()?;
            Ok(
                match call(json!({"op": "tool_call", "name": self.name, "input": input})).await {
                    Ok(output) => ToolOutput::json(&output).with_structured_result(output),
                    Err(error) => ToolOutput::error(
                        error
                            .get("detail")
                            .and_then(Value::as_str)
                            .map_or_else(|| error.to_string(), str::to_owned),
                    ),
                },
            )
        })
    }
}
