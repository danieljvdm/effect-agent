import { Schema } from "effect";
import { PageCaptureEngine } from "effect-agent/page-capture";
import { HttpApi, HttpApiEndpoint, HttpApiGroup } from "effect/unstable/httpapi";

export const BrowserEngine = PageCaptureEngine;
export const browserCommandTimeoutMillis = 30_000;

export const Task = Schema.Struct({
  id: Schema.Natural,
  title: Schema.NonEmptyString.check(Schema.isMaxLength(120)),
  assignee: Schema.Literals(["Alex", "Sam", "Jordan"]),
  priority: Schema.Literals(["Low", "Medium", "High"]),
  status: Schema.Literals(["Todo", "Doing", "Done"]),
});

export type Task = typeof Task.Type;
export const Board = Schema.Array(Task).check(Schema.isMaxLength(50));
export const Scenario = Schema.Literals(["create", "triage", "batch", "custom", "wikipedia"]);
export type Scenario = typeof Scenario.Type;
export const Mode = Schema.Literals(["scripted", "agent", "batched"]);
export type Mode = typeof Mode.Type;

export const ModelId = Schema.Literals([
  "gpt-6-luna",
  "gpt-6-sol",
  "@cf/meta/llama-3.3-70b-instruct-fp8-fast",
]);

export type ModelId = typeof ModelId.Type;
export const Grounding = Schema.Literals(["direct", "jev"]);
export const WikiDriver = Schema.Literals(["model", "jev"]);
export const Reasoning = Schema.Literals(["none", "low", "medium", "high", "xhigh", "max"]);
export const ServiceTier = Schema.Literals(["fast", "default"]);

// Titles, not URLs or namespaces. Keeping this boundary narrow makes the race link-only.
export const ArticleTitle = Schema.NonEmptyString.check(
  Schema.isMaxLength(180),
  Schema.isPattern(/^[^:#?[\]{}|<>\\\p{Cc}]+$/u),
);

export const WikipediaChallenge = Schema.Struct({ start: ArticleTitle, target: ArticleTitle });
export type WikipediaChallenge = typeof WikipediaChallenge.Type;
export const defaultChallenge: WikipediaChallenge = { start: "Mars", target: "Nelson Mandela" };

export const racePrompt = (challenge: WikipediaChallenge) =>
  `Starting at the Wikipedia page for ${challenge.start}, get to ${challenge.target}.`;

export const WikiHop = Schema.Struct({
  title: Schema.String,
  url: Schema.String,
  at: Schema.Number,
  via: Schema.optionalKey(Schema.Struct({ label: Schema.String, url: Schema.String })),
});

export const WikiRace = Schema.Struct({
  start: Schema.String,
  target: Schema.String,
  targetUrl: Schema.String,
  maxHops: Schema.Natural,
  path: Schema.Array(WikiHop),
});

export const modelChoices: ReadonlyArray<{ id: ModelId; label: string }> = [
  { id: "gpt-6-luna", label: "GPT-6 Luna" },
  { id: "gpt-6-sol", label: "GPT-6 Sol" },
  { id: "@cf/meta/llama-3.3-70b-instruct-fp8-fast", label: "Llama 3.3 · Workers AI" },
];

export const RunInput = Schema.Struct({
  id: Schema.String.check(Schema.isUUID()),
  scenario: Scenario,
  mode: Mode,
  // Reports before engine selection used Chromium. New runs record the choice explicitly.
  engine: Schema.optionalKey(BrowserEngine),
  // Read legacy reports; new runs always prepare a fresh browser outside flow timing.
  temperature: Schema.optionalKey(Schema.Literals(["cold", "warm"])),
  prompt: Schema.String.check(Schema.isMaxLength(2_000)),
  screenshots: Schema.Boolean,
  liveView: Schema.Boolean,
  model: Schema.optionalKey(ModelId),
  grounding: Schema.optionalKey(Grounding),
  reasoning: Schema.optionalKey(Reasoning),
  serviceTier: Schema.optionalKey(ServiceTier),
  wikipedia: Schema.optionalKey(WikipediaChallenge),
  wikiDriver: Schema.optionalKey(WikiDriver),
});

export type RunInput = typeof RunInput.Type;

export const Phase = Schema.Literals([
  "setup",
  "model",
  "decision",
  "action",
  "observation",
  "wait",
  "capture",
  "verify",
  "cleanup",
]);

export type Phase = typeof Phase.Type;

export const Span = Schema.Struct({
  id: Schema.Natural,
  phase: Phase,
  name: Schema.String,
  start: Schema.Number,
  duration: Schema.NullOr(Schema.Number),
  outcome: Schema.Literals(["running", "success", "failure", "interrupted", "defect"]),
  turn: Schema.Natural,
  inputTokens: Schema.optionalKey(Schema.Natural),
  outputTokens: Schema.optionalKey(Schema.Natural),
  reasoningTokens: Schema.optionalKey(Schema.Natural),
  reasoningSummary: Schema.optionalKey(Schema.String),
  serviceTier: Schema.optionalKey(Schema.String),
  bytes: Schema.optionalKey(Schema.Natural),
  candidateCount: Schema.optionalKey(Schema.Natural),
  questionCount: Schema.optionalKey(Schema.Natural),
  model: Schema.optionalKey(Schema.String),
  error: Schema.optionalKey(Schema.String),
  choices: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        target: Schema.String,
        ref: Schema.String,
        probability: Schema.Number,
      }),
    ),
  ),
  decisionDistributions: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        question: Schema.String,
        ref: Schema.String,
        reportedProbability: Schema.Number,
        reportedMass: Schema.Number,
      }),
    ),
  ),
});

export type Span = typeof Span.Type;

export const Report = Schema.Struct({
  version: Schema.Literal(1),
  fixture: Schema.Literals(["task-board-v1", "wikipedia-race-v1"]),
  input: RunInput,
  model: Schema.String,
  browserVersion: Schema.optionalKey(Schema.NonEmptyString.check(Schema.isMaxLength(256))),
  browserUserAgent: Schema.optionalKey(Schema.NonEmptyString.check(Schema.isMaxLength(512))),
  browserRevision: Schema.optionalKey(Schema.NonEmptyString.check(Schema.isMaxLength(256))),
  commandTimeoutMillis: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  startedAt: Schema.String,
  status: Schema.Literals(["running", "passed", "failed", "unverified", "cancelled"]),
  message: Schema.String,
  spans: Schema.Array(Span),
  elapsed: Schema.Number,
  // All timestamps share the admission-relative Worker clock. Missing timing means legacy admission timing.
  timing: Schema.optionalKey(Schema.Literal("page-ready-v1")),
  readyAt: Schema.optionalKey(Schema.NullOr(Schema.Number)),
  finishedAt: Schema.optionalKey(Schema.NullOr(Schema.Number)),
  verifiedAt: Schema.NullOr(Schema.Number),
  firstActionAt: Schema.NullOr(Schema.Number),
  cleanup: Schema.Literals(["pending", "closed", "retained", "failed"]),
  board: Board,
  race: Schema.optionalKey(WikiRace),
});

export type Report = typeof Report.Type;

export const ModelApi = Schema.Literals(["responses", "chat-completions"]);

// Viewing capabilities and images are transient: never part of exported benchmark reports.
export const Snapshot = Schema.Struct({
  ready: Schema.Boolean,
  busy: Schema.Boolean,
  canClose: Schema.Boolean,
  model: Schema.String,
  browserConfigured: Schema.Boolean,
  agentConfigured: Schema.Boolean,
  models: Schema.Array(
    Schema.Struct({ id: ModelId, label: Schema.String, configured: Schema.Boolean }),
  ),
  jevConfigured: Schema.Boolean,
  report: Schema.NullOr(Report),
  liveViewUrl: Schema.NullOr(Schema.String),
  image: Schema.NullOr(Schema.String),
  notice: Schema.NullOr(Schema.String),
});

export class LabError extends Schema.TaggedError<LabError>()(
  "LabError",
  {
    code: Schema.Literals([
      "configuration",
      "busy",
      "invalid",
      "browser",
      "expired",
      "storage",
      "interrupted",
    ]),
    message: Schema.String,
  },
  { httpApiStatus: 400 },
) {}

const headers = {
  "x-lab-session": Schema.String.check(Schema.isUUID()),
};

export const LabApi = HttpApi.make("BrowserSpeedLab").add(
  HttpApiGroup.make("lab").add(
    HttpApiEndpoint.get("snapshot", "/api/snapshot", {
      headers,
      success: Snapshot,
      error: LabError,
    }),
    HttpApiEndpoint.post("run", "/api/run", {
      headers,
      payload: RunInput,
      success: Report,
      error: LabError,
    }),
    HttpApiEndpoint.post("stop", "/api/stop", { headers, success: Schema.Void, error: LabError }),
    HttpApiEndpoint.post("close", "/api/close", { headers, success: Schema.Void, error: LabError }),
  ),
);

export const scenarios = [
  {
    id: "create",
    title: "Create a task",
    detail: "A form, two selections, one save",
    prompt:
      'Create a task called "Ship demo", assign it to Alex, set High priority and Todo status. Leave all existing tasks unchanged.',
  },
  {
    id: "triage",
    title: "Triage the backlog",
    detail: "Search, open, update, repeat",
    prompt:
      "Change every Todo task assigned to Sam to High priority and Doing status. Leave all other fields and tasks unchanged.",
  },
  {
    id: "batch",
    title: "Plan the launch",
    detail: "Three tasks, then a status change",
    prompt:
      'Create three tasks named "Write launch notes", "Record demo", and "Publish release", each assigned to Alex with High priority and Todo status. Then mark "Write launch notes" Done. Leave existing tasks unchanged.',
  },
] as const;

export const seed: ReadonlyArray<Task> = [
  { id: 1, title: "Review onboarding", assignee: "Sam", priority: "Medium", status: "Todo" },
  { id: 2, title: "Fix keyboard navigation", assignee: "Alex", priority: "High", status: "Doing" },
  { id: 3, title: "Update help center", assignee: "Sam", priority: "Low", status: "Todo" },
  { id: 4, title: "Audit empty states", assignee: "Jordan", priority: "Medium", status: "Done" },
];

/** Independent oracle compares the complete board, including unrelated fields and extra writes. */
export const verify = (scenario: Scenario, board: ReadonlyArray<Task>): boolean => {
  const expected =
    scenario === "create"
      ? [...seed, { id: 5, title: "Ship demo", assignee: "Alex", priority: "High", status: "Todo" }]
      : scenario === "triage"
        ? seed.map((task) =>
            task.assignee === "Sam" && task.status === "Todo"
              ? { ...task, priority: "High", status: "Doing" }
              : task,
          )
        : scenario === "batch"
          ? [
              ...seed,
              ...["Write launch notes", "Record demo", "Publish release"].map((title, i) => ({
                id: i + 5,
                title,
                assignee: "Alex",
                priority: "High",
                status: i === 0 ? "Done" : "Todo",
              })),
            ]
          : [];

  return (
    scenario !== "custom" &&
    scenario !== "wikipedia" &&
    expected.length === board.length &&
    expected.every((task) => {
      const actual = board.find((value) => value.id === task.id);

      return (
        actual !== undefined &&
        task.title === actual.title &&
        task.assignee === actual.assignee &&
        task.priority === actual.priority &&
        task.status === actual.status
      );
    })
  );
};
