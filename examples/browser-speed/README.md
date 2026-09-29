# Browser speed lab

Ask an agent: **“Starting at the Wikipedia page for Mars, get to Nelson Mandela.”**
Watch a real Cloudflare browser follow article links and inspect the route, hop count, and
model/action timings. The browser independently verifies arrival; claiming success cannot win.
The task-board presets remain available for repeatable form and batching measurements.

```text
React + Effect Atom → typed HTTP API → one browser owner per tab
                                        ├─ Effect Agent → observed links or controls
                                        ├─ Cloudflare Browser Run → Wikipedia or task board
                                        └─ independent verifier → route + trace + result
```

The default **Wikipedia race** accepts starting and destination article titles. It permits only
ordinary English Wikipedia article links in the current article body: no search, URL entry,
back button, namespaces, external links, or fragment shortcuts. Each observation contains an
article excerpt and up to 80 unique links in the two planner modes; the agent can page through the current article's
remaining links. Both direct selection and Jev element matching receive those same candidates. Refs expire when
another link page is read or navigation occurs, and every click rechecks its anchor.

The host resolves the destination's canonical title through Wikipedia's title API before the
race; that lookup gives the agent no route. A win requires an observed link click, a valid article,
and matching browser URL and canonical destination. Redirects count as one hop. The route records
the clicked link and actual landing page. Navigation failures with uncertain outcomes fence further
clicks. The agent has 20 hops, 40 turns, 60 tool calls, 300,000 model tokens, and three minutes;
browser preparation and cleanup fit within the owner's four-minute deadline.
Native context pruning starts at 10,000 estimated tokens and retains 4,000 recent tokens, without
an extra summarization model call. Every observation carries the route so it survives pruning.
Exhausted budgets fail explicitly rather than asking the model to claim a result.

Live pages, routes, network conditions, and link pagination affect results. Compare success rate,
verified time, and hops for the **same start and destination**; a task-board winner is not a
Wikipedia winner. Page text is untrusted input. Tools do not expose arbitrary JavaScript or URLs.

The reusable browser toolkits and model + Jev element matching come from
`effect-agent/browser-use`. `BrowserUse.make({ grounding, mode })` pairs tools with their handlers.
This example supplies the `BrowserActions` adapter and reads native Effect selection spans.
Wikipedia eligibility, route choice, verification, model
configuration, and the comparison UI remain here. [Consumer setup](../../docs/guide/browser.md#opt-into-decision-grounded-browser-tools).

The task board uses self-contained HTML and exposes observed clicks, fills, and selections.
Its preset verifier checks the complete saved board; free-form board requests are **unverified**.

## Run locally

From the repository root:

```sh
vp install
vp run -F @effect-agent/example-browser-speed build
cp examples/browser-speed/.dev.vars.example examples/browser-speed/.dev.vars
```

Fill in the ignored `.dev.vars` file:

| Variable                      | Purpose                                                                       |
| ----------------------------- | ----------------------------------------------------------------------------- |
| `CLOUDFLARE_ACCOUNT_ID`       | Account that owns the browser                                                 |
| `BROWSER_RENDERING_API_TOKEN` | Account-scoped Browser Run Write token for lifecycle and read-only Live View  |
| `OPENAI_API_KEY`              | Model access; optional for scripted and Jev-only runs                         |
| `WORKERS_AI_API_KEY`          | Optional Cloudflare Workers AI key for the Llama comparison                   |
| `TYPESAFE_API_KEY`            | Jev element selection or Jev-only routing through native Effect DecisionModel |

Wrangler also needs its normal Cloudflare login or deployment credential for the remote browser
binding. Browser Run uses the real service during local development; agent runs also call the real
model. Start these commands in separate terminals:

```sh
vp run -F @effect-agent/example-browser-speed worker
vp run -F @effect-agent/example-browser-speed dev
```

Open **http://127.0.0.1:5191** and press **Start race**. Mars → Nelson Mandela is already filled in.
A real Cloudflare browser opens Wikipedia; its route and timings appear beside the live view.
The app connects automatically; there is no login or app token. Cloudflare and model credentials
stay in the Worker. Missing configuration disables runs with an explanation.
`worker --local` disables remote bindings for API checks; it cannot run a browser benchmark.

The model picker offers **GPT-6 Luna** (default), **GPT-6 Sol**, and **Llama 3.3**. OpenAI models
use Responses; Llama uses Workers AI Chat Completions in the configured Cloudflare account.
Provider credentials stay on the server, and requests can select only this approved catalog.
Missing credentials disable the corresponding option.

OpenAI runs default to **Fast** processing and **none** reasoning. **OpenAI speed** switches
between Fast and Standard; **Reasoning** exposes none, low, medium, high, xhigh, and max.
Fast is a service tier, independent of reasoning effort, and has premium OpenAI pricing.
The Worker explicitly sends both settings and records the tier actually returned by OpenAI.
Inspect a model span for the served tier, reasoning tokens, and any provider-returned reasoning
summary (bounded to 16,000 characters). A missing summary or tier remains unavailable; neither is
inferred. Llama has no equivalent controls. OpenAI calls allow 16,384 output tokens including
reasoning, within the task-specific token budget.

## Compare runs

**Browser** selects Kitesurf (beta) or Chromium. **Compare both browsers** repeats the same
task and model settings on each engine, rotating the order each round. Chromium is the default
in the UI and for API requests without an engine. A requested engine never falls back
to the other. Reports record `input.engine`, CDP product, revision, and user agent. History keeps
engines and revisions in separate cohorts; old reports without an engine used Chromium.
Both engines use the same Puppeteer actions, link rules, verification, 30-second native-command
deadline, and page-ready clock. Navigation and decision calls retain their narrower deadlines.
Kitesurf's site and CDP compatibility can differ; preparation and flow failures remain visible.
Chromium uses a host-owned persistent session; Kitesurf uses the documented ephemeral CDP
WebSocket and closes it at scope exit. The persistent-session POST endpoint currently ignores
`browser=kitesurf`, so the lab does not use it for Kitesurf. Preparation checks the provider's
`@kitesurf` CDP revision before starting the clock. Kitesurf has screenshots but no persistent Live View or
session resumption; interrupted actions are never replayed.

**Route decisions → Jev only · no planner** gives Jev the current article, excerpt, destination,
route so far, and every eligible article link's title, label and href. Jev chooses the next hop;
the browser clicks that observed link and verifies arrival. No language model, OpenAI key, or
planner fallback is used. Jev returns a choice distribution, not a prose reasoning summary.

Large pages use balanced groups of at most 255 links, batched in one request, followed
by a choice among group winners. Every eligible link participates; probabilities across groups
are never compared. The run fails explicitly above 5,000 unique eligible links or 10,000 source
anchors. Jev chooses the best exploratory hop even when the connection is indirect; there is no
abstention option. A page with just one eligible link follows it without a decision request.
Jev-only stops on invalid output, timeout, 20 hops, three minutes, or 300,000
reported decision tokens. Each decision request has a 15-second deadline and no retry. Route
choices have no probability cutoff: several routes may be useful, and probability does not
guarantee eventual success. The independent browser verifier remains authoritative.
Jev-only accepts two-decimal distribution rounding within 0.02 of total mass 1,
rescales those probabilities for native DecisionModel validation, and records the raw selected
probability and total in each trace. It never changes the chosen link or confidence. Missing,
out-of-range, nonfinite, or more divergent probabilities still fail. Planner + Jev element
matching retains its strict distributions and 0.6 threshold.

Compare these three strategies: **model route + model element**, **model route + Jev element**,
and **Jev-only route + element**. Jev-only sees all links at once; planner modes see 80 per page.
Both modes stop explicitly above 10,000 source anchors instead of returning incomplete observations.
This compares complete navigation strategies, not model latency on identical observations.
The trace shows candidate/question counts, selected refs, probabilities, tokens, and separate
planner/Jev call counts. History keeps the route driver in its cohort settings.

Select a model, or **Compare configured models** to run the same task with each available model.
Each repetition rotates the starting model/browser configuration; runs execute sequentially and return to the starting article or reset the board.
The comparison table separates model, element selection, reasoning, and requested tier,
includes failures in the flow success count, lists preparation failures separately, and shows
median ready-to-verified latency and successful race hop counts. Served tiers are reported
across the entire configuration, including `unknown` when no tier was returned. A few runs
do not establish a speed advantage.

**Element selection → Jev** keeps the selected language model as the planner. It describes
targets in plain language; native Effect `DecisionModel` asks `jev-latest` to select from visible,
action-compatible controls. All targets in a batch share one decision request. An abstention,
invalid answer, or selected probability below the experimental 0.6 threshold stops the whole
batch before dispatch. The existing browser checks still guard visibility and partial execution.
This threshold is not a correctness guarantee; the independent task verifier remains authoritative.
Decision calls have a 15-second deadline and no automatic retries. Their time, reported tokens,
selected controls, and probabilities appear in separate **Jev selection** spans; decision tokens
are separate from the language model's token budget. Jev receives the observed page and targets.

- **Scripted** runs a fixed UI sequence through the same action/observation implementation. It is
  a diagnostic baseline, not a claim about the fastest possible sequence.
- **Individual** lets the model choose one action per tool call.
- **Batched** allows up to eight sequential actions on already observed controls. A batch stops at
  its first failure and reports how many actions completed; completed actions must not be replayed.

Live View is provider-enforced read-only. Its URL is only sent to the current tab's UI and
excluded from reports. A final screenshot is always attempted after verification; optionally
capture one after every tool action/batch. Screenshot failures remain visible as failed spans.
The idle pane stays empty until the remote browser supplies a live view or screenshot.

The primary **Flow time** runs from the starting page and initial observation being ready to
independently verified success. Browser launch, Live View connection, initial navigation, and
destination resolution are preparation, outside this clock. Every run prepares a fresh browser
automatically; there are no cold/warm controls. A transient browser failure during preparation
gets at most one fresh-browser retry after confirmed closure. Both attempts remain in the trace;
invalid article requests are not retried. After readiness, actions and decisions are never retried
by the owner. Destination lookup has an eight-second request timeout and distinguishes unavailable
Wikipedia responses from missing articles.

**First action** uses the same ready boundary and ends when the first browser interaction completes.
**Preparation** is shown separately. A failed preparation has no flow time and stays in history as
**preparation failed**. A running or unsuccessful flow shows elapsed time, never a verified result.
The complete trace and exported timestamps remain admission-relative on one Worker clock:
`timing: "page-ready-v1"`, `readyAt`, `verifiedAt`, and `finishedAt`. Flow latency is
`verifiedAt - readyAt`; final capture and cleanup are excluded. Client request duration remains
in JSON for transport diagnostics and is a different clock domain. Model spans cover complete model
responses and include reported tokens and resolved model IDs; time to first token and CDP command
counts are not measured. Worker clocks can coarsen synchronous work. Overlapping spans are not
additive, and uninstrumented gaps remain visible rather than assigned to a component.

Repeat 1, 3, or 10 times. History retains failures. A single-model failure stops repetitions;
comparison runs continue to the next sample after an ordinary failure. Cancellation or failed
browser cleanup stops either sequence. Statistics compare the same preset, mode, timing protocol, model,
element selector, reasoning, requested tier, and capture settings. Older reports with
unspecified model settings stay in separate provider-default cohorts. Latency
percentiles use verified successes, with flow failures retained in the started-flow denominator.
Actual served tiers do not split that denominator; mixed tiers remain visible in the table and
individual model spans, so configuration medians may include different served tiers.
Preparation failures are counted separately; older admission-timed reports stay in separate cohorts. p95 appears
after 20 successes. Export JSON before reloading: history lives in the tab, while the owner retains
only its latest report. Record the tested commit and environment alongside exports for comparisons.
No performance improvement is established by the deterministic tests.

To link an independently published comparison from the UI, set
`VITE_BROWSER_BENCHMARK_REPORT_URL` when building. This optional link is hidden by default;
the example does not bundle historical benchmark reports.

## Ownership and limits

One tab owns one browser and admits one run at a time. Task-board runs allow 30 model turns,
100 tool calls, 100 browser actions, and 60,000 tokens. Wikipedia limits are listed above.
Every run has a four-minute overall deadline. Every tab is bounded to 100 admissions.
The remote browser has a ten-minute maximum lifetime and a durable cleanup alarm. Use **Stop run**
to interrupt active work and **Close browser** to retry failed cleanup or release a recovered browser.
Lost requests stay fenced; closing them never replays browser input. Leaving the tab does not
guarantee an immediate server-side cancellation; the run deadline and owner alarm still apply.

Persisted owner state uses a versioned Schema and atomic SQLite writes. Failpoints surround each
state transition and alarm mutation. Running spans are in-memory; a process loss can lose partial
timing data while the persisted admission prevents replay. Reports never claim complete telemetry
for a lost request. Unsupported owner state fails decoding without resetting stored data.

## Validate and deploy

```sh
vp run -F @effect-agent/example-browser-speed check
vp run -F @effect-agent/example-browser-speed test
vp run -F @effect-agent/example-browser-speed build
```

The ordinary suite covers verification and lifecycle behavior without credentials. To exercise
task-board presets and Wikipedia navigation in real local Chromium, set `BROWSER_TEST_EXECUTABLE`:

```sh
BROWSER_TEST_EXECUTABLE="/path/to/chrome" vp run -F @effect-agent/example-browser-speed test
```

That test substitutes only model HTTP responses; it retains native Effect AI decoding, tool
execution, Chromium, and independent verification. It does not establish hosted performance.

For a hosted lab, configure the same values as Worker secrets using the repository's credential
workflow, then run `vp run -F @effect-agent/example-browser-speed deploy`. The Worker is named
`effect-agent-browser-speed`; its assets and browser-owner Durable Object are declared in
`wrangler.jsonc`. This configuration serves the lab on its `workers.dev` hostname: protect that
entire hostname with Cloudflare Access before deploying, including `/api/*`. The demo has no
application authentication. If using a custom domain instead, set `workers_dev: false` and
protect the custom hostname; an Access policy on a custom domain does not protect `workers.dev`.
Preview URLs are disabled so they cannot bypass that policy.
