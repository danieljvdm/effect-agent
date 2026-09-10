import { SettlementFailureDiagnostic } from "@effect-agent/thread/Records";
import type { ThreadExport } from "@effect-agent/thread/ThreadStore";
import { DateTime, Schema } from "effect";
import { Prompt } from "effect/unstable/ai";

import { PlannerInput, type PlannerActivity } from "../domain.ts";
import {
  diagnosticDetail,
  redactDiagnosticText as redactText,
  type RecordedDiagnostics,
} from "./diagnostics.ts";

type PendingDetail = { readonly label: string; readonly value: unknown };
type PendingActivity = Omit<PlannerActivity, "details"> & { readonly details: PendingDetail[] };
const detail = (label: string, value: unknown): PendingDetail => ({ label, value });
const SearchResultStatus = Schema.Struct({ status: Schema.String });

/** Serialize only retained events. Credentials and opaque provider state are not diagnostics. */
const renderDetail = ({ label, value }: PendingDetail, failure: boolean) => ({
  label,
  ...diagnosticDetail(value, failure ? 65_536 : 16_384),
});

/** Combine canonical events and private diagnostics from the selected conversation only. */
export const plannerActivity = (
  records: ReadonlyArray<{
    readonly record: ThreadExport["records"][number]["record"];
    readonly sequence: number;
  }>,
  diagnostics: typeof RecordedDiagnostics.Type = [],
): PlannerActivity[] => {
  const activity: PendingActivity[] = [];
  const runs = new Map<string, number>();
  const boundaries = new Map<string, number>();
  const submissions = new Map<string, string>();
  const calls = new Map<string, { requestedAt: number; parameters: unknown }>();
  const callEvents = new Map<string, Array<{ runId: string; eventId: string }>>();

  const trackCall = (toolCallId: string, runId: string, eventId: string) => {
    const candidates = callEvents.get(toolCallId) ?? [];

    candidates.push({ runId, eventId });
    callEvents.set(toolCallId, candidates);
  };

  for (const { record } of records)
    if (record.payload._tag === "SubmissionSettled" && record.payload.runId !== undefined)
      submissions.set(record.payload.submissionId, record.payload.runId);

  for (const { record, sequence } of records) {
    const payload = record.payload;
    const timestamp = DateTime.formatIso(record.createdAt);
    const time = DateTime.toEpochMillis(record.createdAt);
    const runId = "runId" in payload ? payload.runId : undefined;
    const started = runId === undefined ? undefined : runs.get(runId);

    const base = {
      id: String(sequence),
      timestamp,
      ...(runId === undefined ? {} : { runId }),
      ...(started === undefined ? {} : { elapsedMs: Math.max(0, time - started) }),
    };

    const recordDetails = detail("Record", {
      event: payload._tag,
      sequence,
      recordId: record.recordId,
      deploymentId: record.deploymentId,
      runId,
      ...("turn" in payload ? { turn: payload.turn } : {}),
      ...("turnId" in payload ? { turnId: payload.turnId } : {}),
      ...("toolCallId" in payload ? { toolCallId: payload.toolCallId } : {}),
      ...("submissionId" in payload ? { submissionId: payload.submissionId } : {}),
    });

    // oxlint-disable-next-line typescript/switch-exhaustiveness-check
    switch (payload._tag) {
      case "UserInputRecorded": {
        const input = Schema.decodeUnknownOption(PlannerInput)(payload.input);

        if (input._tag === "None") break;

        const inputRun =
          payload.runId ??
          (payload.submissionId === undefined ? undefined : submissions.get(payload.submissionId));

        activity.push({
          ...base,
          ...(inputRun === undefined ? {} : { runId: inputRun }),
          kind: "status",
          text: "Request received",
          details: [
            detail("Request", {
              message: input.value.message,
              settings: input.value.settings,
              selectedTripId: input.value.selectedTripId,
              publication: input.value.publication,
            }),
            recordDetails,
          ],
        });
        break;
      }
      case "RunStarted":
        runs.set(payload.runId, time);
        boundaries.set(payload.runId, time);
        activity.push({
          ...base,
          elapsedMs: 0,
          kind: "status",
          text: "Run started",
          details: [
            detail("Limits", { maxDurationMillis: payload.maxDurationMillis }),
            recordDetails,
          ],
        });
        break;
      case "ModelResponseRecorded": {
        const prompt = Schema.decodeUnknownOption(Prompt.Prompt)(payload.messages);
        const response: unknown[] = [];

        const providerResults: Array<{
          id: string;
          name: string;
          isFailure: boolean;
          result: unknown;
        }> = [];

        if (prompt._tag === "Some")
          for (const message of prompt.value.content) {
            if (message.role !== "assistant") continue;
            for (const part of message.content) {
              if (part.type === "text") response.push({ type: part.type, text: part.text });
              if (part.type === "tool-call") {
                calls.set(`${payload.runId}:${part.id}`, {
                  requestedAt: time,
                  parameters: part.params,
                });
                response.push({
                  type: part.type,
                  id: part.id,
                  name: part.name,
                  parameters: part.params,
                  providerExecuted: part.providerExecuted,
                });
              }
              if (part.type === "tool-result" && part.providerExecuted)
                providerResults.push({
                  id: part.id,
                  name: part.name,
                  isFailure: part.isFailure,
                  result: part.result,
                });
            }
          }
        const previous = boundaries.get(payload.runId);

        activity.push({
          ...base,
          kind: "usage",
          text: `Model response · turn ${payload.turn}`,
          ...(previous === undefined
            ? {}
            : {
                durationMs: Math.max(0, time - previous),
                durationLabel: "Since previous recorded step",
              }),
          details: [
            detail("Response", response),
            detail("Usage", {
              calls: payload.modelUsage,
              inputTokens: payload.inputTokens,
              outputTokens: payload.outputTokens,
              costMicrousd: payload.costMicrousd,
              unobservedModelCalls: payload.unobservedModelCalls,
            }),
            detail("Available tools", payload.toolExposure ?? null),
            recordDetails,
          ],
        });
        for (const result of providerResults) {
          trackCall(result.id, payload.runId, `${sequence}-${result.id}`);

          const search =
            result.name === "OpenAiWebSearch" || result.name === "OpenAiWebSearchPreview";

          const decoded = search
            ? Schema.decodeUnknownOption(SearchResultStatus)(result.result)
            : undefined;

          const status = decoded?._tag === "Some" ? decoded.value.status : undefined;
          // Effect AI currently marks every web-search result isFailure:false, even unfinished items.
          const failed = result.isFailure || status === "failed";

          const outcome = failed
            ? "failed"
            : status !== undefined && status !== "completed"
              ? `not completed (provider status: ${redactText(status).slice(0, 80)})`
              : search && status === undefined
                ? "result recorded by provider"
                : "completed by provider";

          activity.push({
            ...base,
            id: `${sequence}-${result.id}`,
            kind: failed ? "failure" : "tool",
            text: `${result.name}: ${outcome}`,
            details: [detail("Result", result), recordDetails],
          });
        }
        boundaries.set(payload.runId, time);
        break;
      }
      case "ToolCallPrepared": {
        const key = `${payload.runId}:${payload.toolCallId}`;

        if (!calls.has(key)) calls.set(key, { requestedAt: time, parameters: payload.parameters });
        break;
      }
      case "ToolCallSettled": {
        const call = calls.get(`${payload.runId}:${payload.toolCallId}`);

        trackCall(payload.toolCallId, payload.runId, base.id);

        activity.push({
          ...base,
          kind: payload.isFailure ? "failure" : "tool",
          text: `${payload.toolName}: ${payload.isFailure ? "failed" : "completed"}`,
          ...(call === undefined
            ? {}
            : {
                durationMs: Math.max(0, time - call.requestedAt),
                durationLabel: "Request to recorded result",
              }),
          details: [
            ...(call === undefined ? [] : [detail("Arguments", call.parameters)]),
            detail("Result", payload.result),
            detail("Tool", {
              name: payload.toolName,
              isFailure: payload.isFailure,
              budgetRejected: payload.budgetRejected,
              selection: payload.toolSelection,
            }),
            recordDetails,
          ],
        });
        boundaries.set(payload.runId, time);
        break;
      }
      case "ToolCallUnknown":
        activity.push({
          ...base,
          kind: "failure",
          text: `${payload.toolName}: outcome uncertain`,
          details: [
            detail("Diagnostic", { reason: payload.reason, replayed: false }),
            recordDetails,
          ],
        });
        break;
      case "ModelResponseInterrupted":
        activity.push({
          ...base,
          kind: "failure",
          text: "Model response interrupted",
          details: [
            detail("Interruption", {
              reason: payload.reason,
              attemptId: payload.attemptId,
              supersededEpoch: payload.supersededEpoch,
            }),
            recordDetails,
          ],
        });
        // An interrupted attempt has no reliable model interval for the replacement.
        boundaries.delete(payload.runId);
        break;
      case "CompactionCreated":
        activity.push({
          ...base,
          kind: "status",
          text: `Context compacted: ${payload.kind}`,
          details: [
            detail("Compaction", {
              kind: payload.kind,
              coversThrough: payload.coversThrough,
              summary: payload.summary,
            }),
            recordDetails,
          ],
        });
        break;
      case "RunCompleted":
        activity.push({
          ...base,
          kind: "status",
          text: "Run completed",
          details: [
            detail("Completion", {
              output: payload.output,
              finishReason: payload.finishReason,
              exhausted: payload.exhausted,
            }),
            recordDetails,
          ],
        });
        break;
      case "RunFailed":
        activity.push({
          ...base,
          kind: "failure",
          text: "Run failed",
          details: [detail("Failure", payload.failure), recordDetails],
        });
        break;
      case "SubmissionSettled": {
        const diagnostic = Schema.decodeUnknownOption(SettlementFailureDiagnostic)(payload.result);

        activity.push({
          ...base,
          kind: payload.outcome === "completed" ? "status" : "failure",
          text:
            diagnostic._tag === "Some"
              ? redactText(`${diagnostic.value.errorTag}: ${diagnostic.value.message}`).slice(
                  0,
                  1_000,
                )
              : `Request ${payload.outcome}`,
          details: [
            detail("Settlement", {
              outcome: payload.outcome,
              settlementId: payload.settlementId,
              receiptId: payload.receiptId,
              policyLimit: payload.policyLimit,
              finishReason: payload.finishReason,
              ...(diagnostic._tag === "Some" ? { diagnostic: diagnostic.value } : {}),
              usageSummary: payload.usageSummary,
              uncommittedModelUsage: payload.uncommittedModelUsage,
            }),
            recordDetails,
          ],
        });
        break;
      }
      default:
        break;
    }
  }

  const events: PlannerActivity[] = activity.map((event) => ({
    ...event,
    details: event.details.map((entry) => renderDetail(entry, event.kind === "failure")),
  }));

  for (const diagnostic of diagnostics.toReversed()) {
    const runId =
      diagnostic.runId ??
      (diagnostic.submissionId === undefined
        ? undefined
        : submissions.get(diagnostic.submissionId));

    const entry = {
      label: `${diagnostic.operation.slice(0, 160)} · ${diagnostic.timestamp.slice(11, 23)}`,
      text: diagnostic.text,
      truncated: diagnostic.truncated,
    };

    const candidates =
      diagnostic.toolCallId === undefined ? [] : (callEvents.get(diagnostic.toolCallId) ?? []);

    const matches =
      runId === undefined
        ? candidates
        : candidates.filter((candidate) => candidate.runId === runId);

    // Providers can reuse call IDs in later runs. Leave ambiguous evidence standalone.
    const eventId = matches.length === 1 ? matches[0]?.eventId : undefined;

    const index = events.findIndex((event) => event.id === eventId);
    const matched = events[index];

    if (matched !== undefined && (matched.details?.length ?? 0) < 8) {
      events[index] = { ...matched, details: [...(matched.details ?? []), entry] };
    } else {
      events.push({
        id: `diagnostic-${diagnostic.id}`,
        kind: "failure",
        text: diagnostic.operation,
        timestamp: diagnostic.timestamp,
        ...(runId === undefined ? {} : { runId }),
        ...(diagnostic.durationMs === undefined
          ? {}
          : { durationMs: diagnostic.durationMs, durationLabel: "Observed operation" }),
        details: [
          entry,
          renderDetail(
            detail("Diagnostic identity", {
              submissionId: diagnostic.submissionId,
              attemptId: diagnostic.attemptId,
              toolCallId: diagnostic.toolCallId,
              runId,
            }),
            true,
          ),
        ],
      });
    }
  }

  return events.sort((a, b) => (a.timestamp ?? "").localeCompare(b.timestamp ?? "")).slice(-100);
};
