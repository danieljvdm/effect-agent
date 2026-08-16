import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Effect, Layer, Redacted, Ref, Stream } from "effect";
import { LanguageModel, Model, type Response } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";

const usage = { inputTokens: {}, outputTokens: {} };

/**
 * The demonstration program the scripted model "writes": it queries the
 * curated invoice database, filters locally to the high-revenue customers, and
 * returns one small JSON answer. On the live profile the real model writes
 * its own program instead.
 */
const scriptedProgram = `async () => {
  const result = await invoices.query({
    sql: "SELECT customer, revenue FROM invoice_summary ORDER BY revenue DESC",
  });
  const top = result.rows.filter((row) => row.revenue > 10000);
  console.log("scanned", result.rows.length, "customers, kept", top.length);
  return {
    topCustomers: top.map((row) => row.customer),
    count: top.length,
  };
}`;

const scriptedAnswer = JSON.stringify({
  answer:
    "The customers above $10k in revenue are Stellar Freight, Vertex Robotics, and Nimbus Analytics.",
});

/**
 * A write-attempting program: it tries to mutate the invoice database and reports
 * whether the read-only database authority denied it. The write is expected
 * to be denied (query_only / SQLITE_READONLY).
 */
const writeProbeProgram = `async () => {
  let writeDenied = false;
  try {
    await invoices.query({ sql: "UPDATE invoice_summary SET revenue = 0" });
  } catch (envelope) {
    writeDenied = envelope != null && envelope._tag === "InvoiceQueryDenied";
  }
  return { writeDenied };
}`;

const writeProbeAnswer = JSON.stringify({
  answer: "The invoice database is read-only, so the write was denied.",
});

/**
 * A CTE-prefixed write: `WITH … DELETE` does not START with a write keyword,
 * so a naive denylist would let it through — the allowlist must reject it.
 */
const cteProbeProgram = `async () => {
  let writeDenied = false;
  try {
    await invoices.query({
      sql: "WITH doomed AS (SELECT customer FROM invoice_summary) DELETE FROM invoice_summary",
    });
  } catch (envelope) {
    writeDenied = envelope != null && envelope._tag === "InvoiceQueryDenied";
  }
  return { writeDenied };
}`;

/**
 * A deterministic scripted model: turn 1 emits one `run_javascript` Tool Call
 * carrying `program`; turn 2 emits `answer`. This is the default so the demo
 * runs and tests without any credential.
 */
const makeScriptedModel = (program: string, answer: string) =>
  Model.make(
    "scripted",
    "invoice-analyst",
    Layer.effect(
      LanguageModel.LanguageModel,
      Effect.gen(function* () {
        const turn = yield* Ref.make(0);
        return yield* LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () =>
            Stream.unwrap(
              Ref.getAndUpdate(turn, (value) => value + 1).pipe(
                Effect.map((value) =>
                  Stream.fromIterable<Response.StreamPartEncoded>(
                    value === 0
                      ? [
                          {
                            type: "tool-call",
                            id: "code-1",
                            name: "run_javascript",
                            params: { code: program },
                            providerExecuted: false,
                          },
                          { type: "finish", reason: "tool-calls", usage },
                        ]
                      : [
                          { type: "text-start", id: "answer" },
                          { type: "text-delta", id: "answer", delta: answer },
                          { type: "text-end", id: "answer" },
                          { type: "finish", reason: "stop", usage },
                        ],
                  ),
                ),
              ),
            ),
        });
      }),
    ),
  );

export const scriptedModel = makeScriptedModel(scriptedProgram, scriptedAnswer);
export const scriptedWriteProbeModel = makeScriptedModel(writeProbeProgram, writeProbeAnswer);
export const scriptedCteProbeModel = makeScriptedModel(cteProbeProgram, writeProbeAnswer);

/** OpenAI live profile, used when an OPENAI_API_KEY secret is present. */
export const liveModel = (apiKey: string) =>
  Model.make(
    "openai",
    "invoice-analyst",
    // Latency-tuned for the demo: low reasoning effort and the fast service
    // tier — writing one SELECT and formatting a sentence does not need
    // medium reasoning, which dominated request latency (~2s per model call).
    OpenAiLanguageModel.model("gpt-5.6-luna", {
      reasoning: { effort: "low" },
      service_tier: "fast",
    }).pipe(
      Layer.provide(
        OpenAiClient.layer({ apiKey: Redacted.make(apiKey) }).pipe(
          Layer.provide(FetchHttpClient.layer),
        ),
      ),
    ),
  );
