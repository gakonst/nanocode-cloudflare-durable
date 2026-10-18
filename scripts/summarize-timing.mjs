import { readFileSync } from "node:fs";

const files = process.argv.slice(2);
if (!files.length) throw new Error("Pass one or more measure-curl JSON report paths");
const summary = values => {
  const sorted = values.filter(value => Number.isFinite(value)).sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length ? { count: sorted.length, median: sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2, min: sorted[0], max: sorted.at(-1) } : { count: 0 };
};

for (const file of files) {
  const report = JSON.parse(readFileSync(file, "utf8"));
  const models = report.metrics?.runtime?.timing?.models ?? [];
  const calls = report.samples.flatMap(sample => sample.sdk_model_calls ?? []);
  const attempts = models.flatMap(model => model.diagnostic?.attempts ?? []);
  const perTurn = report.samples.map(sample => {
    const first = sample.first_output;
    const model = sample.sdk_model_calls?.find(call => call.call_index === first?.model_call_index);
    const modelFirstNs = first?.model_elapsed_ns ?? model?.timing?.first_output_ns ?? model?.time_to_first_output_ns;
    const modelFirstMs = modelFirstNs === undefined ? undefined : modelFirstNs / 1e6;
    return {
      request_id: sample.request_id,
      kind: sample.kind,
      status: sample.status,
      first_output_ms: sample.first_output_ms,
      answer_ttft_ms: sample.ttft_ms,
      completion_ms: sample.completion_ms,
      outside_model_to_first_output_ms: Number.isFinite(modelFirstMs) && first?.model_call_index === sample.sdk_model_calls?.[0]?.call_index ? sample.first_output_ms - modelFirstMs : undefined,
      first_model_output_ms: modelFirstMs,
      first_output_insert_to_enqueue_ms: first?.streamed_at === undefined ? undefined : first.streamed_at - first.server_at,
      first_output_sdk_queue_ms: first?.sdk_timing?.queue_ns === undefined ? undefined : first.sdk_timing.queue_ns / 1e6,
      admission_ms: sample.worker_timing?.admission_ms,
      runtime_open_ms: sample.worker_timing?.runtime_open_ms,
      checkpoint_ms: sample.worker_timing?.checkpoint_spans?.replace?.total_ms,
      checkpoint_commits: sample.worker_timing?.checkpoint_spans?.replace?.count,
      tool_durations_ms: sample.tool_calls?.filter(tool => tool.type === "tool.result").map(tool => tool.duration_ms).filter(value => value !== undefined),
    };
  });
  const timings = Object.fromEntries(["first_output_ms", "answer_ttft_ms", "completion_ms", "outside_model_to_first_output_ms", "first_output_sdk_queue_ms", "first_output_insert_to_enqueue_ms", "admission_ms", "runtime_open_ms", "checkpoint_ms"].map(key => [key, summary(perTurn.map(turn => turn[key]))]));
  console.log(JSON.stringify({ file, provider: report.provider, executor: report.executor, tools: report.tools, reasoning: report.reasoning, turns: perTurn, timing_ms: timings, dispatch_ms: { headers: summary(models.map(model => model.headers_ms)), first_byte: summary(models.map(model => model.first_byte_ms)), body_read_wait: summary(models.map(model => model.body_read_wait_ms)), credential_lookup: summary(attempts.map(attempt => attempt.auth_lookup_ms)), private_transport_headers: summary(attempts.map(attempt => attempt.fetch_header_ms)), private_total: summary(models.map(model => model.diagnostic?.total_ms)) }, credential_outcomes: Object.fromEntries([...new Set(attempts.map(attempt => attempt.credential))].map(outcome => [outcome, attempts.filter(attempt => attempt.credential === outcome).length])), sdk_calls: calls, limitations: report.limitations }, null, 2));
}
