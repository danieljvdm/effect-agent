# effect-agent

## 0.1.0-beta.142

### Patch Changes

- [#597](https://github.com/danieljvdm/effect-agent/pull/597) [`161aab3`](https://github.com/danieljvdm/effect-agent/commit/161aab335dbbb9bf704d005bf95015be2e04858b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Share SQL persistence implementations through `@effect-agent/storage-sql` while preserving SQLite storage formats and adapter APIs. BEHAVIOR CHANGE: import SQL subscription, message-delivery, native-read, and upgrade helpers from `@effect-agent/storage-sql` instead of `effect-agent`, and pass custom transactions through the factory options.

## 0.1.0-beta.141

### Patch Changes

- [#661](https://github.com/danieljvdm/effect-agent/pull/661) [`e6127e4`](https://github.com/danieljvdm/effect-agent/commit/e6127e44d10f7103929abe65217f0ad837ce0d9f) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Preserve OpenAI implicit prompt-cache boundaries when `runStatus: "appended"` is enabled.
  Keep discarded status and reference context out of later requests when native response-ID tracking is enabled.

## 0.1.0-beta.140

### Patch Changes

- [#655](https://github.com/danieljvdm/effect-agent/pull/655) [`6d16773`](https://github.com/danieljvdm/effect-agent/commit/6d1677383d3377a0a399baeaec4c661d51b15878) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Preserve reusable prompt prefixes across Runs by keeping system instructions and the output contract before conversation history. Retain native cache controls and distinct instruction precedence without rewriting stored history.

## 0.1.0-beta.139

## 0.1.0-beta.138

### Patch Changes

- [#647](https://github.com/danieljvdm/effect-agent/pull/647) [`00355c1`](https://github.com/danieljvdm/effect-agent/commit/00355c1871e8fdab22ae1dbb1f03c1f35171f357) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Send MCP requests and notifications as strict JSON-RPC messages so servers that reject tracing extensions or null notification parameters can connect. Expose discovered MCP tools in non-strict model schema mode so providers can accept valid MCP input schemas outside their strict subset.

## 0.1.0-beta.137

## 0.1.0-beta.136

### Patch Changes

- [#639](https://github.com/danieljvdm/effect-agent/pull/639) [`a8c32dc`](https://github.com/danieljvdm/effect-agent/commit/a8c32dcc652192d81afbebf4f5940bf26fcc332c) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Resume each Run with its original input and its own continuation, preserving late results for prior calls. Keep other interleaved exchanges visible in Thread history when compacting a Run's context.

## 0.1.0-beta.135

### Patch Changes

- [#637](https://github.com/danieljvdm/effect-agent/pull/637) [`8fc53ad`](https://github.com/danieljvdm/effect-agent/commit/8fc53ad9eb6b110ca6faaaebbb6dbba08e3c292f) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Allow hosts to hand off at completed Turn boundaries to the next independent input while retaining each Run's authority, receipts and obligations. Install matching runtime and storage packages before enabling `SubmissionScheduling.yieldTo`.

## 0.1.0-beta.134

### Patch Changes

- [#631](https://github.com/danieljvdm/effect-agent/pull/631) [`d210027`](https://github.com/danieljvdm/effect-agent/commit/d210027cd1103cb5a13da03e7054e504c0159e2d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Require Effect 4.0.0-rc.117 and update the model examples to GPT-6.

## 0.1.0-beta.133

### Patch Changes

- [#629](https://github.com/danieljvdm/effect-agent/pull/629) [`450bac0`](https://github.com/danieljvdm/effect-agent/commit/450bac01e8e4fa937961e53f2231cfaa525167a4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Wake progress observers when a durable update is accepted, including updates omitted from parent reports.

- [#624](https://github.com/danieljvdm/effect-agent/pull/624) [`c5a487b`](https://github.com/danieljvdm/effect-agent/commit/c5a487beef98a5dfa6adb9a3e2edf542fccea90a) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Read canonical worker identity and its producer fence in one bounded owner snapshot instead of four serial remote reads. Custom ThreadStore adapters must implement `readIdentity`; deploy matching Cloudflare client and owner packages for the new read-only operation.

- [#621](https://github.com/danieljvdm/effect-agent/pull/621) [`e336239`](https://github.com/danieljvdm/effect-agent/commit/e336239226540001e6e4876c6f5dffc57b785769) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Reduce worker origin and stop-request latency without weakening producer or history fencing.

## 0.1.0-beta.132

## 0.1.0-beta.131

## 0.1.0-beta.130

## 0.1.0-beta.129

## 0.1.0-beta.128

## 0.1.0-beta.127

## 0.1.0-beta.126

### Patch Changes

- [#603](https://github.com/danieljvdm/effect-agent/pull/603) [`bf955bb`](https://github.com/danieljvdm/effect-agent/commit/bf955bbf275901e560d93cf0a054cfbf51aa9420) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Allow an authorized `Subagent.start` with `continuationOf` to create a new worker from a successfully completed assignment while preserving its seal, lineage and policy bounds. Retain explicit worker stops in native worker-state reads.

- [#604](https://github.com/danieljvdm/effect-agent/pull/604) [`81a78cd`](https://github.com/danieljvdm/effect-agent/commit/81a78cd2bbd932b939b942eedea53c8e2894480e) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Allow background workers to filter automatic completion reports while retaining durable decisions and independent final reports.

## 0.1.0-beta.125

### Patch Changes

- [#600](https://github.com/danieljvdm/effect-agent/pull/600) [`34d7c5f`](https://github.com/danieljvdm/effect-agent/commit/34d7c5ff9392fd6fb1db348257fd22dea58a337c) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add `reportUpdate` to background reporting so applications can retain routine progress for observers without starting a parent run. Selected updates and completion reports keep their existing durable delivery guarantees.

## 0.1.0-beta.124

### Patch Changes

- [#593](https://github.com/danieljvdm/effect-agent/pull/593) [`d8bd6db`](https://github.com/danieljvdm/effect-agent/commit/d8bd6db4d21dbb0ae53132d52db7fa3fa6ef9f76) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Allow Code Mode tools with strict empty-object parameter and result schemas.

## 0.1.0-beta.123

### Minor Changes

- [#590](https://github.com/danieljvdm/effect-agent/pull/590) [`72e07a3`](https://github.com/danieljvdm/effect-agent/commit/72e07a35010564acb411845e250fa5d552edef0d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Replace protected browser passes with application-owned Cloudflare sessions, native Puppeteer actions, and authorized credential filling.

  BEHAVIOR CHANGE: Migrate removed `protected-browser` APIs to `browser-session` and `browser-credentials`; ordinary page observations may expose filled values, and application owners must retain session references and close browsers on completion or expiry.

## 0.1.0-beta.122

### Patch Changes

- [#587](https://github.com/danieljvdm/effect-agent/pull/587) [`83fb830`](https://github.com/danieljvdm/effect-agent/commit/83fb83078a95a5fb60fffa0ea818dca98d4e88bd) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Allow callers to set finite browser pass allowances beyond one hour and keep retained protected sessions active with `BrowserRunProtectedHost.keepAlive(sessionId)`. Preserve unrestricted service-worker handling and distinguish failed-resume attachment retirement from uncertain local cleanup.

## 0.1.0-beta.121

## 0.1.0-beta.120

### Patch Changes

- [#583](https://github.com/danieljvdm/effect-agent/pull/583) [`037d29a`](https://github.com/danieljvdm/effect-agent/commit/037d29a754034551520c8df9cb41bfb7660cde40) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Retain attempt resources before durable approval suspension with `DurableApprovalSuspension`. Resume approvals that arrive during retention with fresh attempt services and the same pending tool batch.

## 0.1.0-beta.119

### Patch Changes

- [#581](https://github.com/danieljvdm/effect-agent/pull/581) [`5c11bea`](https://github.com/danieljvdm/effect-agent/commit/5c11bea7ec185136b3453d317a0fea20f015a3a8) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Expose visible choices and selection state for ordinary protected selects, and preserve prior dispatch evidence after acknowledged no-write refusals.

  BEHAVIOR CHANGE: Fill ordinary selects with the exact observed option label; raw option values are no longer a fallback, while credential selects continue to use private values.

## 0.1.0-beta.118

### Patch Changes

- [#579](https://github.com/danieljvdm/effect-agent/pull/579) [`ff29420`](https://github.com/danieljvdm/effect-agent/commit/ff2942050eae59ad3ccc9731caaf809e13d957f1) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Allow credential access hooks to report recoverable `busy` while preserving known protected browser dispatch evidence and the same usable session.
  Complete protected controller Return independently of observation authority, requiring a fresh authorized observation before subsequent operations.

## 0.1.0-beta.117

## 0.1.0-beta.116

### Patch Changes

- [#575](https://github.com/danieljvdm/effect-agent/pull/575) [`c29c38c`](https://github.com/danieljvdm/effect-agent/commit/c29c38cc4ebaf81c700911b83a57073005c6bdfa) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Reuse the original prepared input when a public worker follow-up repeats its command key and parameters across Runs. Preserve current authorization and reject changed parameters.

## 0.1.0-beta.115

### Minor Changes

- [#572](https://github.com/danieljvdm/effect-agent/pull/572) [`d1313aa`](https://github.com/danieljvdm/effect-agent/commit/d1313aaf2a1be18b34e5ebfa680ed12f4cef31bc) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Remove custom worker-report APIs and registration mappers. **BEHAVIOR CHANGE:** replace `Subagent.reporting` / `reportingToWorker` with `reportToParent: true` and map typed reports on receipt; drain unprepared custom-report work with its original release before upgrading.

### Patch Changes

- [#568](https://github.com/danieljvdm/effect-agent/pull/568) [`1c33f81`](https://github.com/danieljvdm/effect-agent/commit/1c33f812e4339f1b5757d2721aa8318c8119aa51) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add durable worker-wide stop and indexed summaries with exact accepted and applied input identities. Replay retained starts before public input preparation and drain up to 32 accepted worker inputs at each safe steering boundary.

- [#571](https://github.com/danieljvdm/effect-agent/pull/571) [`432036c`](https://github.com/danieljvdm/effect-agent/commit/432036cedbe59e8ecbdcd4c71417b730d5b781df) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add opt-in terminal worker assignments that remain steerable while waiting and permanently reject new work after completion, failure, or cancellation. Preserve existing reusable workers and upgrade native storage seals without resetting retained data.

## 0.1.0-beta.114

## 0.1.0-beta.113

## 0.1.0-beta.112

### Patch Changes

- [#556](https://github.com/danieljvdm/effect-agent/pull/556) [`ab5030d`](https://github.com/danieljvdm/effect-agent/commit/ab5030d9814a5c47f6facfdf89fe5799bdba6b00) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Isolate Thread recovery faults with history-independent status and bounded retries, let Cloudflare dispatch fresh Threads while old cleanup is pending, and keep Node startup closed on blocked recovery. Preserve content-free storage diagnostics.

  BEHAVIOR CHANGE: Call `runtime.runRecovery()` instead of yielding `runtime.runRecovery`; its result contains ordinary Submission `reports` and one `blocked` fault per failed Thread. Blocked Threads remain ineligible for claims; pass `{ threadId }` to recover only a selected Thread.

  `SubmissionLedger.scanNonterminal` now emits control-only `SubmissionWorkItem` entries. Use `lookup` or `loadRecoverySnapshot` for selected execution payloads.

- [#558](https://github.com/danieljvdm/effect-agent/pull/558) [`6716f8c`](https://github.com/danieljvdm/effect-agent/commit/6716f8c5915fee466c89d9d82159fd8f2b67ece4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Require Effect rc.116 and replace the local decision and TypeSafe APIs with native `Decision`, `DecisionModel`, and `@effect/ai-typesafe`, retaining `AutoModel` for thread selection.

  BEHAVIOR CHANGE: Import decisions from `effect/unstable/ai` and configure TypeSafe with `TypeSafeClient.layerConfig()`; AutoModel requires at least two profiles, writes version 2 selection records, and rejects version 1 records without reselection or mutation. Retain the previous runtime for active version 1 threads or explicitly upgrade their records in your storage adapter; native probability sums must be within `1e-6` of 1.

## 0.1.0-beta.111

### Patch Changes

- [#554](https://github.com/danieljvdm/effect-agent/pull/554) [`b2cf08c`](https://github.com/danieljvdm/effect-agent/commit/b2cf08c14d3c455990724fb30062bdd5544dcabb) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Preserve browser dispatch evidence and add exact-page checkpoint recovery with pending-input fences. Expose structured page observations and refuse stale click/fill targets before dispatch.

## 0.1.0-beta.110

### Patch Changes

- [#551](https://github.com/danieljvdm/effect-agent/pull/551) [`c2ae9e7`](https://github.com/danieljvdm/effect-agent/commit/c2ae9e777766fba0e14e8a472bc833d2122c2b10) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Acknowledge native `emit_update` success with `{ emitted: true }` after the Run accepts the update. Represent `Schema.Void` success encodings as JSON `null` in model history and programmatic broker results.

- [#549](https://github.com/danieljvdm/effect-agent/pull/549) [`a1957c4`](https://github.com/danieljvdm/effect-agent/commit/a1957c457777e7f8eeb7b51ab8833f41593c3ecf) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Return retained worker delivery states with stable message references and inspect the same operation through destination acceptance and settlement.

  BEHAVIOR CHANGE: Read `Subagent.start(...).delivery` and the `MessageStatus` returned by `followUp`; inspect their `message` instead of resending pending input, and use an accepted `receipt` for execution results, waiting, or cancellation.

- [#548](https://github.com/danieljvdm/effect-agent/pull/548) [`2582969`](https://github.com/danieljvdm/effect-agent/commit/25829699c09a4cc862b650e4e30e5edc0fbb4fc0) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Keep standard worker progress and completion independent of parent runtime reads, with live
  permission checks at destination admission. Distinguish tool calls that never reached required
  durable preparation from genuinely uncertain outcomes in historical model context.

  BEHAVIOR CHANGE: Retire pre-production standard-report worker families lacking the captured
  return address before upgrading: their execution journals, native admissions, and queued
  deliveries. Reconcile uncertain external operations before retiring receipts; source resource
  data is outside this reset scope.

## 0.1.0-beta.109

### Patch Changes

- [#545](https://github.com/danieljvdm/effect-agent/pull/545) [`cdbe786`](https://github.com/danieljvdm/effect-agent/commit/cdbe786861e9ba10ecb1dccf3b26f47170a8245e) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Exclude native model-call parameter rejections from the tool failure observer while retaining failed tool results and warning telemetry. Continue reporting failures returned by started handlers.

## 0.1.0-beta.108

### Minor Changes

- [#539](https://github.com/danieljvdm/effect-agent/pull/539) [`92bd9e2`](https://github.com/danieljvdm/effect-agent/commit/92bd9e26c181c07f84371a372d8885cd4db4667a) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Provide AutoModel as a native Effect model Layer to select through DecisionModel automatically on each parent or subagent thread's first turn. Retain selections across follow-ups through a shared SelectionStore, with an in-memory Layer and schema-backed records for host-owned persistence.

## 0.1.0-beta.107

### Patch Changes

- [#540](https://github.com/danieljvdm/effect-agent/pull/540) [`cfb6e1e`](https://github.com/danieljvdm/effect-agent/commit/cfb6e1e04b9e80d276f918f29c369cddec5b917c) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add bounded byte-backed file selection to interactive browsers, including Cloudflare Browser Run file inputs and dynamic choosers. Distinguish confirmed selection from website upload or submission receipts.

## 0.1.0-beta.106

### Patch Changes

- [#533](https://github.com/danieljvdm/effect-agent/pull/533) [`992d062`](https://github.com/danieljvdm/effect-agent/commit/992d062a095995bd8f328a01cc784b6a9a7ffc72) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Distinguish authorization check failures from policy denials and preserve original local causes with structured private diagnostics across durable settlements, Worker admissions, retained message deliveries, and programmatic Worker observation. Add reusable diagnostic codecs and bounded diagnostic context copies while keeping generated tool results and completion reports free of private causal detail.

  BEHAVIOR CHANGE: Project Worker and validation errors into a safe failure schema before exposing them through custom tools with `failureMode: "return"`; their new causal fields are private diagnostics.

## 0.1.0-beta.105

## 0.1.0-beta.104

### Patch Changes

- [#530](https://github.com/danieljvdm/effect-agent/pull/530) [`caf7e7e`](https://github.com/danieljvdm/effect-agent/commit/caf7e7ea69448fb820f9e95cffe480cbb458d500) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Preserve each invocation's tracing context and sampling decision when running registered attempts or preparing reports. Remove per-poll, digest, and response-part helper spans while retaining operation boundaries and errors, and allow hosts to use effect-cf 0.45.

## 0.1.0-beta.103

## 0.1.0-beta.102

### Patch Changes

- [#521](https://github.com/danieljvdm/effect-agent/pull/521) [`be0dcaf`](https://github.com/danieljvdm/effect-agent/commit/be0dcafb69e0641d8b82ff174fee53a53e367f18) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Settle cancelled durable runs as aborted when tool authorization observes their abort intent before the cancellation watcher.

## 0.1.0-beta.101

### Patch Changes

- [#517](https://github.com/danieljvdm/effect-agent/pull/517) [`6a4f4f8`](https://github.com/danieljvdm/effect-agent/commit/6a4f4f870fe87ebb0d3cc76905dcadd77c9a29ef) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Expose bounded outstanding-operation and pending-delivery reads, exact canonical record locators, and native worker/peer admission lookups. Retain uncertain external outcomes after abort and retire worker inputs only after their effects are resolved.

## 0.1.0-beta.100

## 0.1.0-beta.99

### Patch Changes

- [#513](https://github.com/danieljvdm/effect-agent/pull/513) [`e1f06bb`](https://github.com/danieljvdm/effect-agent/commit/e1f06bbd3f66478c9223c5888696cd8c6e75fc37) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Preserve registered tool replay contracts when an attempt copies its Agent Definition to restore accepted execution limits. Keep interrupted idempotent operations recoverable without repeating their committed durable steps.

## 0.1.0-beta.98

### Patch Changes

- [#507](https://github.com/danieljvdm/effect-agent/pull/507) [`95c962f`](https://github.com/danieljvdm/effect-agent/commit/95c962f8ee45c35f877d0bb21f82d4f6bac6759c) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Continue accepted requests with current Agent bindings, retaining original operations and outcomes while allowing later input around unknown work under one Thread lease. BEHAVIOR CHANGE: replace historical binding manifests with per-operation replay versions and deploy matching runtime and storage packages together.

## 0.1.0-beta.97

### Patch Changes

- [#504](https://github.com/danieljvdm/effect-agent/pull/504) [`385f119`](https://github.com/danieljvdm/effect-agent/commit/385f1197eb41e8114c5daf5b6763824450095cf5) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Resume retained requests across compatible input changes and widened JSON Schema object declarations using their original payloads and receipts. Keep changed semantics and incompatible operations pending until compatible code is available.

## 0.1.0-beta.96

### Minor Changes

- [#490](https://github.com/danieljvdm/effect-agent/pull/490) [`771498b`](https://github.com/danieljvdm/effect-agent/commit/771498b1952794b8f2f19d1e35b604937bffcc3c) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Resume retained requests using explicit replay contracts and back off failed and blocked Cloudflare maintenance without abandoning child obligations or changing receipts. BEHAVIOR CHANGE: bound durable execution duration per active Attempt, retain actual duration exhaustion, and deploy matching runtime and storage packages before writing the new record.

## 0.1.0-beta.95

## 0.1.0-beta.94

### Patch Changes

- [#494](https://github.com/danieljvdm/effect-agent/pull/494) [`373d188`](https://github.com/danieljvdm/effect-agent/commit/373d18828f2fc2851614cf2612c5e71e91075c88) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Return invalid native tool arguments to the model when the tool uses `failureMode: "return"`, before approval or handler execution. Preserve rejection evidence through durable recovery and allow corrected Code Mode arguments in the same run.

  BEHAVIOR CHANGE: Custom durability hooks must persist `RunTurnResponseCommit.toolParameterRejections` and restore it through `RunTurnResume.toolParameterRejections`.

- [#497](https://github.com/danieljvdm/effect-agent/pull/497) [`bbb709c`](https://github.com/danieljvdm/effect-agent/commit/bbb709c9beff0b8f2e6b67d05e0f8223a7cb6f93) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Return complete tool discovery matches within the configured UTF-8 byte budget, with an actionable notice when documentation is omitted. Keep agent runs able to continue when no match fits, using an empty tool selection instead of a limit-exceeded failure.

## 0.1.0-beta.93

### Patch Changes

- [#491](https://github.com/danieljvdm/effect-agent/pull/491) [`319c156`](https://github.com/danieljvdm/effect-agent/commit/319c156be5a85a2d490cf79531f94591881436f8) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Rename the in-memory runtime setup to `InMemory.layer` and clarify that conversations can span Runs for the lifetime of the application Scope.

  BEHAVIOR CHANGE: Replace the `Ephemeral` root import with `InMemory` and the `effect-agent/ephemeral` module path with `effect-agent/in-memory`.

## 0.1.0-beta.92

### Patch Changes

- [#487](https://github.com/danieljvdm/effect-agent/pull/487) [`054b1c3`](https://github.com/danieljvdm/effect-agent/commit/054b1c3a7e7a6571fc82caedc4ae8835c5aacfb4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Require Effect rc.115 across the packages and effect-cf 0.43.0 for Cloudflare hosts.

## 0.1.0-beta.91

### Patch Changes

- [#485](https://github.com/danieljvdm/effect-agent/pull/485) [`b60b07e`](https://github.com/danieljvdm/effect-agent/commit/b60b07e307dc366637f5247fb788b24b17c554eb) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Reduce CPU work for large SQL memory writes and receipt replay while preserving storage limits and receipt validation.

## 0.1.0-beta.90

## 0.1.0-beta.89

### Minor Changes

- [#479](https://github.com/danieljvdm/effect-agent/pull/479) [`983a558`](https://github.com/danieljvdm/effect-agent/commit/983a558703a187285ff9c900792defc8f15984a1) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add host-owned protected browser suspension, exact-page resume, and authorized human takeover while preserving credential exposure and browser budgets. Require fresh observation after Return and keep transferred sessions independent of the previous Attempt’s scope.

## 0.1.0-beta.88

### Patch Changes

- [#476](https://github.com/danieljvdm/effect-agent/pull/476) [`5e24e87`](https://github.com/danieljvdm/effect-agent/commit/5e24e8782203aef836c8b4ba49e72468d7d510b1) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Label agent and model spans with GenAI operation names and agent, conversation, and model identity for agent dashboards. Update span-name filters from `AgentRuntime.run` to `invoke_agent <agent ID>` and from `LanguageModel.streamText` to `chat <model>`.

## 0.1.0-beta.87

### Patch Changes

- [#471](https://github.com/danieljvdm/effect-agent/pull/471) [`0be6edf`](https://github.com/danieljvdm/effect-agent/commit/0be6edfa8c73822f59184e6177a265c56c3ac1cd) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Persist completed Tool results before draining new inputs. An admission read failure after a Tool returns no longer loses its outcome, turns it into an unknown call, or blocks later submissions behind it. Preserve atomic no-tool and completion-Tool terminal commits.

## 0.1.0-beta.86

### Minor Changes

- [#466](https://github.com/danieljvdm/effect-agent/pull/466) [`6560df2`](https://github.com/danieljvdm/effect-agent/commit/6560df20d900627d28d745ea11e9759774bb9cf0) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Consolidate agent definitions, execution, capabilities, and sandbox contracts into `effect-agent`, and use kebab-case public module paths across framework packages.

  BEHAVIOR CHANGE: Replace `@effect-agent/core`, `@effect-agent/engine`, `@effect-agent/capabilities`, and `@effect-agent/sandbox` dependencies with `effect-agent`; migrate direct imports such as `effect-agent/AgentRuntime` to `effect-agent/agent-runtime` and upgrade framework packages together.

- [#466](https://github.com/danieljvdm/effect-agent/pull/466) [`6560df2`](https://github.com/danieljvdm/effect-agent/commit/6560df20d900627d28d745ea11e9759774bb9cf0) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Fold `@effect-agent/thread` into `effect-agent` and expose the in-memory conversation model through the `Thread` namespace. Keep database drivers and platform hosts in their adapter packages.

  BEHAVIOR CHANGE: Replace `@effect-agent/thread/*` imports with `effect-agent/*` and remove the old dependency. Use `Thread.Store`, `Thread.Thread`, `Thread.layerMemory`, and `Thread.toPrompt` in place of the `EphemeralThreads` module; import `PersistentHistory` from the package root for `PersistentHistory.layer`. Stored formats and service identities are unchanged.

### Patch Changes

- [#467](https://github.com/danieljvdm/effect-agent/pull/467) [`1112b1b`](https://github.com/danieljvdm/effect-agent/commit/1112b1bfb388be600c9326737d10608660698ef3) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Reduce the time required to certify durable adapters and run subscription-store conformance.

- [#466](https://github.com/danieljvdm/effect-agent/pull/466) [`6560df2`](https://github.com/danieljvdm/effect-agent/commit/6560df20d900627d28d745ea11e9759774bb9cf0) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Run agents and attached subagents with `Ephemeral.layer` for in-memory conversation history, default IDs, and module-level `Subagent.layer` and `ThreadHistory.layer` APIs.

  BEHAVIOR CHANGE: Replace `SubagentRuntime.layer` with `Subagent.layer`.

  BEHAVIOR CHANGE: Remove `IdGenerator` from service requirement unions and omit routine ID Layer provisions; custom generator overrides still work, and explicitly selecting the default uses the module-level `layer` export from `effect-agent/id-generator`.

  BEHAVIOR CHANGE: Replace `ThreadHistory.layerTransient` with `ThreadHistory.layer`; share one application Layer and reuse Thread IDs to retain conversations between Runs. Complete history updates remain after a failed or interrupted Run and are released when the application Scope closes. Custom history adapters must declare `retention` as `"incremental"` or `"on-success"` and return a history owner from `open`.

## 0.1.0-beta.85

### Minor Changes

- [#459](https://github.com/danieljvdm/effect-agent/pull/459) [`eab5b7c`](https://github.com/danieljvdm/effect-agent/commit/eab5b7c0ea2c52bf81e947253a8387a4b1e78c9d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Opt into durable typed parent completion messages with `Subagent.background(Research, { start: true, followUp: true, reportToParent: true })`, without an application input union, mapper, or reporting registration. Pass a custom reporting descriptor as `reportToParent` when an application-specific input format is needed.

- [#459](https://github.com/danieljvdm/effect-agent/pull/459) [`eab5b7c`](https://github.com/danieljvdm/effect-agent/commit/eab5b7c0ea2c52bf81e947253a8387a4b1e78c9d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Accept Agent definitions directly in background subagent tools and declare typed intermediate updates on Agents. Deliver opted-in worker findings to the parent before completion, with durable retention, ordering, and bounded backpressure.

### Patch Changes

- Updated dependencies [[`eab5b7c`](https://github.com/danieljvdm/effect-agent/commit/eab5b7c0ea2c52bf81e947253a8387a4b1e78c9d), [`eab5b7c`](https://github.com/danieljvdm/effect-agent/commit/eab5b7c0ea2c52bf81e947253a8387a4b1e78c9d)]:
  - @effect-agent/capabilities@0.1.0-beta.85
  - @effect-agent/core@0.1.0-beta.85
  - @effect-agent/engine@0.1.0-beta.85

## 0.1.0-beta.84

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.84
  - @effect-agent/core@0.1.0-beta.84
  - @effect-agent/engine@0.1.0-beta.84

## 0.1.0-beta.83

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.83
  - @effect-agent/core@0.1.0-beta.83
  - @effect-agent/engine@0.1.0-beta.83

## 0.1.0-beta.82

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.82
  - @effect-agent/core@0.1.0-beta.82
  - @effect-agent/engine@0.1.0-beta.82

## 0.1.0-beta.81

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.81
  - @effect-agent/core@0.1.0-beta.81
  - @effect-agent/engine@0.1.0-beta.81

## 0.1.0-beta.80

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.80
  - @effect-agent/core@0.1.0-beta.80
  - @effect-agent/engine@0.1.0-beta.80

## 0.1.0-beta.79

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.79
  - @effect-agent/core@0.1.0-beta.79
  - @effect-agent/engine@0.1.0-beta.79

## 0.1.0-beta.78

### Patch Changes

- Updated dependencies [[`84fe655`](https://github.com/danieljvdm/effect-agent/commit/84fe65580c35a91707eda809b7e47d90402179a9)]:
  - @effect-agent/capabilities@0.1.0-beta.78
  - @effect-agent/engine@0.1.0-beta.78
  - @effect-agent/core@0.1.0-beta.78

## 0.1.0-beta.77

### Minor Changes

- [#421](https://github.com/danieljvdm/effect-agent/pull/421) [`38bc092`](https://github.com/danieljvdm/effect-agent/commit/38bc092ba87e631416b75d0ed4871330c2c40489) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add a WebSearch tool with interchangeable native search backends and bounded, cited results. Route upstream Effect clients through Cloudflare AI Gateway with a pipeable Layer helper for search, model calls, streaming, and supported provider APIs.

### Patch Changes

- Updated dependencies [[`84d6684`](https://github.com/danieljvdm/effect-agent/commit/84d66844e24e7fbdc5dc3f54a5d3a7a6127cdd99), [`38bc092`](https://github.com/danieljvdm/effect-agent/commit/38bc092ba87e631416b75d0ed4871330c2c40489)]:
  - @effect-agent/core@0.1.0-beta.77
  - @effect-agent/engine@0.1.0-beta.77
  - @effect-agent/capabilities@0.1.0-beta.77

## 0.1.0-beta.76

### Patch Changes

- Updated dependencies [[`3eef297`](https://github.com/danieljvdm/effect-agent/commit/3eef297d2343989a830d5d2b88e0b863b54c91fd)]:
  - @effect-agent/core@0.1.0-beta.76
  - @effect-agent/engine@0.1.0-beta.76
  - @effect-agent/capabilities@0.1.0-beta.76

## 0.1.0-beta.75

### Patch Changes

- Updated dependencies [[`103a755`](https://github.com/danieljvdm/effect-agent/commit/103a755f002b3de6108e2ccaa7116caaac32d83b), [`230c18a`](https://github.com/danieljvdm/effect-agent/commit/230c18a79fa3941615a6116f5678a1a3bd4b169c), [`1208f7e`](https://github.com/danieljvdm/effect-agent/commit/1208f7e77a348ffd4a9dc0bcb90954b1095e1d4b), [`71afa3d`](https://github.com/danieljvdm/effect-agent/commit/71afa3d64f1cef889b46bea6a352d4e6f8446e32)]:
  - @effect-agent/capabilities@0.1.0-beta.75
  - @effect-agent/engine@0.1.0-beta.75
  - @effect-agent/core@0.1.0-beta.75

## 0.1.0-beta.74

### Patch Changes

- Updated dependencies [[`cf10ec3`](https://github.com/danieljvdm/effect-agent/commit/cf10ec32e2d94402d417b05358bf96715e8c5401)]:
  - @effect-agent/core@0.1.0-beta.74
  - @effect-agent/engine@0.1.0-beta.74
  - @effect-agent/capabilities@0.1.0-beta.74

## 0.1.0-beta.73

### Patch Changes

- Updated dependencies [[`da6971d`](https://github.com/danieljvdm/effect-agent/commit/da6971d450c7ed73b88c8ae74ac8376aee6c1254)]:
  - @effect-agent/core@0.1.0-beta.73
  - @effect-agent/capabilities@0.1.0-beta.73
  - @effect-agent/engine@0.1.0-beta.73

## 0.1.0-beta.72

### Minor Changes

- [#401](https://github.com/danieljvdm/effect-agent/pull/401) [`08571ea`](https://github.com/danieljvdm/effect-agent/commit/08571eacf1483fbc0008106e6753138ad75eb011) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add opt-in run-scoped tool exposure and `ToolDiscovery.make` with bounded namespace search and durable selection recovery. Add selective Code Mode documentation; set `includeDeclarations: false` when host visibility or subagent grants hide any allowlisted method.

### Patch Changes

- Updated dependencies [[`08571ea`](https://github.com/danieljvdm/effect-agent/commit/08571eacf1483fbc0008106e6753138ad75eb011)]:
  - @effect-agent/core@0.1.0-beta.72
  - @effect-agent/engine@0.1.0-beta.72
  - @effect-agent/capabilities@0.1.0-beta.72

## 0.1.0-beta.71

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.71
  - @effect-agent/core@0.1.0-beta.71
  - @effect-agent/engine@0.1.0-beta.71

## 0.1.0-beta.70

### Patch Changes

- Updated dependencies [[`3230353`](https://github.com/danieljvdm/effect-agent/commit/323035380f8296fc731a224f79f2717724b7f889)]:
  - @effect-agent/capabilities@0.1.0-beta.70
  - @effect-agent/core@0.1.0-beta.70
  - @effect-agent/engine@0.1.0-beta.70

## 0.1.0-beta.69

### Patch Changes

- Updated dependencies [[`e37a126`](https://github.com/danieljvdm/effect-agent/commit/e37a12613f25225c3ae8544dc384f4f7da4adc03), [`9c98161`](https://github.com/danieljvdm/effect-agent/commit/9c98161d5a1026f2dc3d0fb395ea4a0bf6323fdd), [`318b442`](https://github.com/danieljvdm/effect-agent/commit/318b4420c5dcd14cbcd36bdfa9dce5abf53b40ad)]:
  - @effect-agent/core@0.1.0-beta.69
  - @effect-agent/engine@0.1.0-beta.69
  - @effect-agent/capabilities@0.1.0-beta.69

## 0.1.0-beta.68

### Patch Changes

- Updated dependencies [[`4ca6361`](https://github.com/danieljvdm/effect-agent/commit/4ca6361c2085b5b77d1835c2b61ca1e67d2f8e6c)]:
  - @effect-agent/engine@0.1.0-beta.68
  - @effect-agent/capabilities@0.1.0-beta.68
  - @effect-agent/core@0.1.0-beta.68

## 0.1.0-beta.67

### Patch Changes

- Updated dependencies [[`8c0fe3b`](https://github.com/danieljvdm/effect-agent/commit/8c0fe3bf4f5a2ff84bd3ae6a44abd18b89f6bc1f)]:
  - @effect-agent/engine@0.1.0-beta.67
  - @effect-agent/capabilities@0.1.0-beta.67
  - @effect-agent/core@0.1.0-beta.67

## 0.1.0-beta.66

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.66
  - @effect-agent/core@0.1.0-beta.66
  - @effect-agent/engine@0.1.0-beta.66

## 0.1.0-beta.65

### Patch Changes

- Updated dependencies [[`41cf0df`](https://github.com/danieljvdm/effect-agent/commit/41cf0df75f483ea99ba494db01cc3247ed5d00b9)]:
  - @effect-agent/capabilities@0.1.0-beta.65
  - @effect-agent/core@0.1.0-beta.65
  - @effect-agent/engine@0.1.0-beta.65

## 0.1.0-beta.64

### Patch Changes

- Updated dependencies [[`620d7d3`](https://github.com/danieljvdm/effect-agent/commit/620d7d38dd94b95c29d2e07a79b445a6fbccd648)]:
  - @effect-agent/core@0.1.0-beta.64
  - @effect-agent/capabilities@0.1.0-beta.64
  - @effect-agent/engine@0.1.0-beta.64

## 0.1.0-beta.63

### Patch Changes

- [#376](https://github.com/danieljvdm/effect-agent/pull/376) [`d0f36bf`](https://github.com/danieljvdm/effect-agent/commit/d0f36bfc21e821fcc34caacf3c39f1e904a5d1c9) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add bounded older-match pagination with optional `beforeRecordId` to context history search and explain literal queries in the native tool. BEHAVIOR CHANGE: update custom `ContextHistory` adapters to honor the exclusive canonical anchor or explicitly reject anchored requests before adopting the updated tool.

- Updated dependencies [[`d0f36bf`](https://github.com/danieljvdm/effect-agent/commit/d0f36bfc21e821fcc34caacf3c39f1e904a5d1c9)]:
  - @effect-agent/engine@0.1.0-beta.63
  - @effect-agent/capabilities@0.1.0-beta.63
  - @effect-agent/core@0.1.0-beta.63

## 0.1.0-beta.62

### Patch Changes

- Updated dependencies [[`46e8ad2`](https://github.com/danieljvdm/effect-agent/commit/46e8ad22fa9f436ce155a6696ddf8e11cec2931c)]:
  - @effect-agent/engine@0.1.0-beta.62
  - @effect-agent/capabilities@0.1.0-beta.62
  - @effect-agent/core@0.1.0-beta.62

## 0.1.0-beta.61

### Patch Changes

- Updated dependencies [[`21431ae`](https://github.com/danieljvdm/effect-agent/commit/21431ae6cacd78e6330b1017c2768f4f9c347b7a)]:
  - @effect-agent/core@0.1.0-beta.61
  - @effect-agent/engine@0.1.0-beta.61
  - @effect-agent/capabilities@0.1.0-beta.61

## 0.1.0-beta.60

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.60
  - @effect-agent/core@0.1.0-beta.60
  - @effect-agent/engine@0.1.0-beta.60

## 0.1.0-beta.59

### Patch Changes

- Updated dependencies [[`cb1d297`](https://github.com/danieljvdm/effect-agent/commit/cb1d297d3464850b5e4645a0d3b3a5062a1ba71b)]:
  - @effect-agent/core@0.1.0-beta.59
  - @effect-agent/engine@0.1.0-beta.59
  - @effect-agent/capabilities@0.1.0-beta.59

## 0.1.0-beta.58

### Patch Changes

- Updated dependencies [[`daca525`](https://github.com/danieljvdm/effect-agent/commit/daca52585983bb90b6c43a29e4a44a28c8de1743)]:
  - @effect-agent/engine@0.1.0-beta.58
  - @effect-agent/capabilities@0.1.0-beta.58
  - @effect-agent/core@0.1.0-beta.58

## 0.1.0-beta.57

### Minor Changes

- [#358](https://github.com/danieljvdm/effect-agent/pull/358) [`4ff21e2`](https://github.com/danieljvdm/effect-agent/commit/4ff21e2a4c3735be34955e7d5caf64f623d33f81) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Construct subagent capabilities with `Subagent.make`, application-chosen names, and exact host registration resolution. Start and control durable background workers with typed follow-up, completion reporting, authorized history, and bounded nested delegation.

### Patch Changes

- Updated dependencies [[`4ff21e2`](https://github.com/danieljvdm/effect-agent/commit/4ff21e2a4c3735be34955e7d5caf64f623d33f81)]:
  - @effect-agent/capabilities@0.1.0-beta.57
  - @effect-agent/core@0.1.0-beta.57
  - @effect-agent/engine@0.1.0-beta.57

## 0.1.0-beta.56

### Patch Changes

- Updated dependencies [[`fdde35f`](https://github.com/danieljvdm/effect-agent/commit/fdde35f4b837be8acef0dc1badca69bef1a2dd05)]:
  - @effect-agent/engine@0.1.0-beta.56
  - @effect-agent/capabilities@0.1.0-beta.56
  - @effect-agent/core@0.1.0-beta.56

## 0.1.0-beta.55

### Patch Changes

- Updated dependencies [[`2259fc0`](https://github.com/danieljvdm/effect-agent/commit/2259fc05eec3bfac2a92a8d055953f3482e54735), [`2259fc0`](https://github.com/danieljvdm/effect-agent/commit/2259fc05eec3bfac2a92a8d055953f3482e54735)]:
  - @effect-agent/capabilities@0.1.0-beta.55
  - @effect-agent/engine@0.1.0-beta.55
  - @effect-agent/core@0.1.0-beta.55

## 0.1.0-beta.54

### Patch Changes

- Updated dependencies [[`e9c7591`](https://github.com/danieljvdm/effect-agent/commit/e9c75913024f09b74cdbb1bd4f25b15c87acf06e), [`e9c7591`](https://github.com/danieljvdm/effect-agent/commit/e9c75913024f09b74cdbb1bd4f25b15c87acf06e)]:
  - @effect-agent/capabilities@0.1.0-beta.54
  - @effect-agent/core@0.1.0-beta.54
  - @effect-agent/engine@0.1.0-beta.54

## 0.1.0-beta.53

### Patch Changes

- Updated dependencies [[`d93903e`](https://github.com/danieljvdm/effect-agent/commit/d93903ec923da7a9841b5ab1a72bba5c0a0fb34b)]:
  - @effect-agent/engine@0.1.0-beta.53
  - @effect-agent/capabilities@0.1.0-beta.53
  - @effect-agent/core@0.1.0-beta.53

## 0.1.0-beta.52

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.52
  - @effect-agent/core@0.1.0-beta.52
  - @effect-agent/engine@0.1.0-beta.52

## 0.1.0-beta.51

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.51
  - @effect-agent/core@0.1.0-beta.51
  - @effect-agent/engine@0.1.0-beta.51

## 0.1.0-beta.50

### Minor Changes

- [#335](https://github.com/danieljvdm/effect-agent/pull/335) [`207e910`](https://github.com/danieljvdm/effect-agent/commit/207e9108d25f25a204324b0f6217fea89de569cf) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Inject compaction strategies directly through `ContextCompactor`, and supply Effect AI's `IdGenerator` when constructing `MemoryNotes.layer`. BEHAVIOR CHANGE: Replace `RunContextPreparation.compactor` and `contextCompactorRunContextLayer` with `Layer.provide(CompactorLive)` at the runtime Layer.

- [#336](https://github.com/danieljvdm/effect-agent/pull/336) [`0438a7b`](https://github.com/danieljvdm/effect-agent/commit/0438a7b9c58869a91870d3df44dc163ec790a929) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Compose service-backed callbacks and reconciler Layers through Effect requirements, and finalize subscription and redaction resources per invocation. Preserve the caller's clock during late browser cleanup.

  BEHAVIOR CHANGE: call `toRunThreadOptions(threadId, runId)` with `EphemeralThreads` provided; ephemeral runs now honor a provided `RunToolAuthorization` unless a per-run hook overrides it.

- [#335](https://github.com/danieljvdm/effect-agent/pull/335) [`207e910`](https://github.com/danieljvdm/effect-agent/commit/207e9108d25f25a204324b0f6217fea89de569cf) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add native context-window rollover with durable recovery, model-directed context tools, retained transcript lookup, and revision-checked working notes. BEHAVIOR CHANGE: Update custom compactors to use `trigger`, `modelCallAllowed`, and `state.replacement` instead of the summary-specific flags and state fields.

### Patch Changes

- Updated dependencies [[`207e910`](https://github.com/danieljvdm/effect-agent/commit/207e9108d25f25a204324b0f6217fea89de569cf), [`0438a7b`](https://github.com/danieljvdm/effect-agent/commit/0438a7b9c58869a91870d3df44dc163ec790a929), [`207e910`](https://github.com/danieljvdm/effect-agent/commit/207e9108d25f25a204324b0f6217fea89de569cf)]:
  - @effect-agent/engine@0.1.0-beta.50
  - @effect-agent/capabilities@0.1.0-beta.50
  - @effect-agent/core@0.1.0-beta.50

## 0.1.0-beta.49

### Patch Changes

- Updated dependencies [[`91ac3bf`](https://github.com/danieljvdm/effect-agent/commit/91ac3bf8cabe1cd7d7851995a3fd714b02db58a0), [`b54eea8`](https://github.com/danieljvdm/effect-agent/commit/b54eea8ce9973a1ef2a58ddd6eb87bcc912bec75), [`e3024c0`](https://github.com/danieljvdm/effect-agent/commit/e3024c00673a12b0df79127bcf68176742c51294), [`b54eea8`](https://github.com/danieljvdm/effect-agent/commit/b54eea8ce9973a1ef2a58ddd6eb87bcc912bec75)]:
  - @effect-agent/engine@0.1.0-beta.49
  - @effect-agent/capabilities@0.1.0-beta.49
  - @effect-agent/core@0.1.0-beta.49

## 0.1.0-beta.48

### Patch Changes

- Updated dependencies [[`e640747`](https://github.com/danieljvdm/effect-agent/commit/e6407479ae233527685928bead040dbfe5153a22), [`8899bdb`](https://github.com/danieljvdm/effect-agent/commit/8899bdbcbbd16c5b7f9981564939f64729b73015)]:
  - @effect-agent/engine@0.1.0-beta.48
  - @effect-agent/core@0.1.0-beta.48
  - @effect-agent/capabilities@0.1.0-beta.48

## 0.1.0-beta.47

### Minor Changes

- [#316](https://github.com/danieljvdm/effect-agent/pull/316) [`e6ff3bc`](https://github.com/danieljvdm/effect-agent/commit/e6ff3bcd1b5ce0f2348de668853482ba9d5e126b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Admit remembering durably and process it in a separate host-owned worker with saved proposals, exact command retries, conflict rebase, and source invalidation. Bind the portable checkpoint contract to existing host jobs and retain source references for later cleanup.

### Patch Changes

- Updated dependencies [[`e6ff3bc`](https://github.com/danieljvdm/effect-agent/commit/e6ff3bcd1b5ce0f2348de668853482ba9d5e126b)]:
  - @effect-agent/core@0.1.0-beta.47
  - @effect-agent/capabilities@0.1.0-beta.47
  - @effect-agent/engine@0.1.0-beta.47

## 0.1.0-beta.46

### Minor Changes

- [#313](https://github.com/danieljvdm/effect-agent/pull/313) [`c1a6e6a`](https://github.com/danieljvdm/effect-agent/commit/c1a6e6a915be73a49b2c266e2df74256f44c25e2) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Import module namespaces from package roots, or import declarations from their explicit PascalCase module paths, following the package map's migration examples. Discard unused modules from audited packages when bundling consumers.
  BEHAVIOR CHANGE: Replace flat declaration imports, lowercase aggregate paths, cross-package aliases, and internal helper imports with their documented owning modules; use `MemoryThreadStoreLive` instead of `MemoryStorageLive`.

### Patch Changes

- Updated dependencies [[`c1a6e6a`](https://github.com/danieljvdm/effect-agent/commit/c1a6e6a915be73a49b2c266e2df74256f44c25e2)]:
  - @effect-agent/capabilities@0.1.0-beta.46
  - @effect-agent/core@0.1.0-beta.46
  - @effect-agent/engine@0.1.0-beta.46

## 0.1.0-beta.45

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.45
  - @effect-agent/core@0.1.0-beta.45
  - @effect-agent/engine@0.1.0-beta.45

## 0.1.0-beta.44

### Patch Changes

- [#307](https://github.com/danieljvdm/effect-agent/pull/307) [`f8365ee`](https://github.com/danieljvdm/effect-agent/commit/f8365eee4048076ced0a79b9149efc29297b7c41) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Upgrade to Effect rc.112 and `effect-cf` 0.40.0 while preserving MCP transports and Cloudflare host behavior.

  BEHAVIOR CHANGE: Upgrade Effect and its provider/platform/SQL packages to rc.112 or a compatible version. In Cloudflare hosts, provide `effect-cf@^0.40.0` and enable `nodejs_compat` for its async context support.

- Updated dependencies [[`f8365ee`](https://github.com/danieljvdm/effect-agent/commit/f8365eee4048076ced0a79b9149efc29297b7c41)]:
  - @effect-agent/core@0.1.0-beta.44
  - @effect-agent/engine@0.1.0-beta.44
  - @effect-agent/capabilities@0.1.0-beta.44

## 0.1.0-beta.43

### Patch Changes

- Updated dependencies [[`4532f8d`](https://github.com/danieljvdm/effect-agent/commit/4532f8d5c65b0d41532e9ebe0212c64a0a63f678)]:
  - @effect-agent/capabilities@0.1.0-beta.43
  - @effect-agent/core@0.1.0-beta.43
  - @effect-agent/engine@0.1.0-beta.43

## 0.1.0-beta.42

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.42
  - @effect-agent/core@0.1.0-beta.42
  - @effect-agent/engine@0.1.0-beta.42

## 0.1.0-beta.41

### Patch Changes

- Updated dependencies [[`e21d6da`](https://github.com/danieljvdm/effect-agent/commit/e21d6da596b97c98ace533c3fa42fe9767d127e1), [`edfa7dc`](https://github.com/danieljvdm/effect-agent/commit/edfa7dc6693dea2a84366f5053826ffa87f7c587)]:
  - @effect-agent/core@0.1.0-beta.41
  - @effect-agent/capabilities@0.1.0-beta.41
  - @effect-agent/engine@0.1.0-beta.41

## 0.1.0-beta.40

### Patch Changes

- Updated dependencies [[`1432833`](https://github.com/danieljvdm/effect-agent/commit/14328336cd3480c5ddda8447f522591eb99eaaeb), [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7), [`1432833`](https://github.com/danieljvdm/effect-agent/commit/14328336cd3480c5ddda8447f522591eb99eaaeb), [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7), [`1432833`](https://github.com/danieljvdm/effect-agent/commit/14328336cd3480c5ddda8447f522591eb99eaaeb), [`c36fe73`](https://github.com/danieljvdm/effect-agent/commit/c36fe73d2d226f9271c6dd60071159b0d82862ae), [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7), [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7), [`c36fe73`](https://github.com/danieljvdm/effect-agent/commit/c36fe73d2d226f9271c6dd60071159b0d82862ae), [`0fbcbbf`](https://github.com/danieljvdm/effect-agent/commit/0fbcbbf3c8c2ca7595543e545baddb0c6f965436)]:
  - @effect-agent/capabilities@0.1.0-beta.40
  - @effect-agent/core@0.1.0-beta.40
  - @effect-agent/engine@0.1.0-beta.40

## 0.1.0-beta.39

### Minor Changes

- [#263](https://github.com/danieljvdm/effect-agent/pull/263) [`95865d7`](https://github.com/danieljvdm/effect-agent/commit/95865d78f55546d42f562f2f13509bbfc198c091) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Rename `@effect-agent/session` to `@effect-agent/thread` and rename the Conversation framework API to Thread.

  BEHAVIOR CHANGE: Rename Conversation identifiers, fields, record families and tags, and the durable-admin `--conversation` selector to their Thread equivalents. Reset incompatible alpha storage before upgrading.

### Patch Changes

- [#256](https://github.com/danieljvdm/effect-agent/pull/256) [`ac70e21`](https://github.com/danieljvdm/effect-agent/commit/ac70e212c7d9741ce48bd9b2a4dbd355f9dac72e) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Declare `effect` as a required `^4.0.0-rc.111` peer across all public packages so they share the application's runtime and accept compatible upgrades. Keep `effect` in application dependencies at a version satisfying the framework's and providers' peer ranges.

- Updated dependencies [[`34ca82e`](https://github.com/danieljvdm/effect-agent/commit/34ca82e86191bc85229bd32886b8cfaf9a2edce9), [`e0aa7d9`](https://github.com/danieljvdm/effect-agent/commit/e0aa7d9442ca2ec62df8195a2f9cce7b52af5257), [`f4f37c3`](https://github.com/danieljvdm/effect-agent/commit/f4f37c37fa1b650341c6e18ee3a22cd6f518bfd2), [`e0aa7d9`](https://github.com/danieljvdm/effect-agent/commit/e0aa7d9442ca2ec62df8195a2f9cce7b52af5257), [`7bab6c0`](https://github.com/danieljvdm/effect-agent/commit/7bab6c053b01398a0f1898374103997da6550268), [`0d88d90`](https://github.com/danieljvdm/effect-agent/commit/0d88d90443e7d35e34799f4458d274fde99e0859), [`79fbd8b`](https://github.com/danieljvdm/effect-agent/commit/79fbd8b755434a162629a534478e188636d186fe), [`4c458e4`](https://github.com/danieljvdm/effect-agent/commit/4c458e43738bb243d1e343c97ecfd49e3b41ca9f), [`95865d7`](https://github.com/danieljvdm/effect-agent/commit/95865d78f55546d42f562f2f13509bbfc198c091), [`655bf5f`](https://github.com/danieljvdm/effect-agent/commit/655bf5f217dce1865c97ce613246c27846bfaf8a), [`d004a36`](https://github.com/danieljvdm/effect-agent/commit/d004a361518c23cdc81f1768e5ab31560e014935), [`ac70e21`](https://github.com/danieljvdm/effect-agent/commit/ac70e212c7d9741ce48bd9b2a4dbd355f9dac72e), [`511c852`](https://github.com/danieljvdm/effect-agent/commit/511c85212a564ff2729de401620fcbdeddcb4748)]:
  - @effect-agent/capabilities@0.1.0-beta.39
  - @effect-agent/engine@0.1.0-beta.39
  - @effect-agent/core@0.1.0-beta.39

## 0.1.0-beta.38

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.38
  - @effect-agent/core@0.1.0-beta.38
  - @effect-agent/engine@0.1.0-beta.38

## 0.1.0-beta.37

### Patch Changes

- Updated dependencies [[`bd48a7b`](https://github.com/danieljvdm/effect-agent/commit/bd48a7b200fb71335b19edd7941be331b6ede9ea), [`bd48a7b`](https://github.com/danieljvdm/effect-agent/commit/bd48a7b200fb71335b19edd7941be331b6ede9ea)]:
  - @effect-agent/engine@0.1.0-beta.37
  - @effect-agent/core@0.1.0-beta.37
  - @effect-agent/capabilities@0.1.0-beta.37

## 0.1.0-beta.36

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.36
  - @effect-agent/core@0.1.0-beta.36
  - @effect-agent/engine@0.1.0-beta.36

## 0.1.0-beta.35

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.35
  - @effect-agent/core@0.1.0-beta.35
  - @effect-agent/engine@0.1.0-beta.35

## 0.1.0-beta.34

### Patch Changes

- [#202](https://github.com/danieljvdm/effect-agent/pull/202) [`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align the Effect family with rc.111 to decode nested OpenAI error events, and preserve transformed Tool parameters under its encoded response contract.

- Updated dependencies [[`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee), [`baecd08`](https://github.com/danieljvdm/effect-agent/commit/baecd08f1d6f2c0698e16487cdcccf2f6ffcebca), [`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee), [`baecd08`](https://github.com/danieljvdm/effect-agent/commit/baecd08f1d6f2c0698e16487cdcccf2f6ffcebca), [`aa3ebfb`](https://github.com/danieljvdm/effect-agent/commit/aa3ebfb4fd1e69be77c433a881ddecb3567c36c2)]:
  - @effect-agent/engine@0.1.0-beta.34
  - @effect-agent/core@0.1.0-beta.34
  - @effect-agent/capabilities@0.1.0-beta.34

## 0.1.0-beta.33

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.33
  - @effect-agent/core@0.1.0-beta.33
  - @effect-agent/engine@0.1.0-beta.33

## 0.1.0-beta.32

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.32
  - @effect-agent/core@0.1.0-beta.32
  - @effect-agent/engine@0.1.0-beta.32

## 0.1.0-beta.31

### Patch Changes

- Updated dependencies [[`d3c42d4`](https://github.com/danieljvdm/effect-agent/commit/d3c42d4e34f27610845863ec29908cd3fce95188)]:
  - @effect-agent/capabilities@0.1.0-beta.31
  - @effect-agent/core@0.1.0-beta.31
  - @effect-agent/engine@0.1.0-beta.31

## 0.1.0-beta.30

### Patch Changes

- Updated dependencies [[`34d05cd`](https://github.com/danieljvdm/effect-agent/commit/34d05cd1ce06f57f890b18b5ba1bce8af85db3e3)]:
  - @effect-agent/capabilities@0.1.0-beta.30
  - @effect-agent/core@0.1.0-beta.30
  - @effect-agent/engine@0.1.0-beta.30

## 0.1.0-beta.29

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.29
  - @effect-agent/core@0.1.0-beta.29
  - @effect-agent/engine@0.1.0-beta.29

## 0.1.0-beta.28

### Patch Changes

- Updated dependencies [[`374771d`](https://github.com/danieljvdm/effect-agent/commit/374771d90afa26ce7e1832f76715aa7b9eea3741)]:
  - @effect-agent/engine@0.1.0-beta.28
  - @effect-agent/capabilities@0.1.0-beta.28
  - @effect-agent/core@0.1.0-beta.28

## 0.1.0-beta.27

### Patch Changes

- Updated dependencies [[`47e9a53`](https://github.com/danieljvdm/effect-agent/commit/47e9a53d99555af3b0ac993b5c9c55ad266e327b), [`773264b`](https://github.com/danieljvdm/effect-agent/commit/773264b75759c4456e1e549d2172bbe39610a8c1)]:
  - @effect-agent/capabilities@0.1.0-beta.27
  - @effect-agent/core@0.1.0-beta.27
  - @effect-agent/engine@0.1.0-beta.27

## 0.1.0-beta.26

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.26
  - @effect-agent/core@0.1.0-beta.26
  - @effect-agent/engine@0.1.0-beta.26

## 0.1.0-beta.25

### Patch Changes

- Updated dependencies [[`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d), [`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d)]:
  - @effect-agent/engine@0.1.0-beta.25
  - @effect-agent/capabilities@0.1.0-beta.25
  - @effect-agent/core@0.1.0-beta.25

## 0.1.0-beta.24

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.24
  - @effect-agent/core@0.1.0-beta.24
  - @effect-agent/engine@0.1.0-beta.24

## 0.1.0-beta.23

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.23
  - @effect-agent/core@0.1.0-beta.23
  - @effect-agent/engine@0.1.0-beta.23

## 0.1.0-beta.22

### Patch Changes

- Updated dependencies [[`ce8b39c`](https://github.com/danieljvdm/effect-agent/commit/ce8b39ce8f716c0a11c6394d136b67cb9be84588)]:
  - @effect-agent/capabilities@0.1.0-beta.22
  - @effect-agent/core@0.1.0-beta.22
  - @effect-agent/engine@0.1.0-beta.22

## 0.1.0-beta.21

### Patch Changes

- Updated dependencies [[`27618dc`](https://github.com/danieljvdm/effect-agent/commit/27618dc03b0703fc784dc7abc4280fc74bb95045)]:
  - @effect-agent/capabilities@0.1.0-beta.21
  - @effect-agent/core@0.1.0-beta.21
  - @effect-agent/engine@0.1.0-beta.21

## 0.1.0-beta.20

### Patch Changes

- [#111](https://github.com/danieljvdm/effect-agent/pull/111) [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align every public package with the Effect 4.0.0-rc.110 family.

- [#111](https://github.com/danieljvdm/effect-agent/pull/111) [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Fix `validateMcpDiscovery` reporting a permanent schema drift for MCP tools whose parameters or success type is a named, refined Schema (a branded ID, a bounded string, a `Schema.Class`) — both schema derivations now resolve a top-level `$ref` before comparison.

- Updated dependencies [[`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4), [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4)]:
  - @effect-agent/core@0.1.0-beta.20
  - @effect-agent/engine@0.1.0-beta.20
  - @effect-agent/capabilities@0.1.0-beta.20

## 0.1.0-beta.19

### Patch Changes

- Updated dependencies [[`9e31de4`](https://github.com/danieljvdm/effect-agent/commit/9e31de4c5f63ebc7eefbce33d3e0ed2052538f26)]:
  - @effect-agent/engine@0.1.0-beta.19
  - @effect-agent/capabilities@0.1.0-beta.19
  - @effect-agent/core@0.1.0-beta.19

## 0.1.0-beta.18

### Patch Changes

- Updated dependencies [[`f36fd40`](https://github.com/danieljvdm/effect-agent/commit/f36fd409f8a34e13c87646fd857a4060ac89e89d)]:
  - @effect-agent/engine@0.1.0-beta.18
  - @effect-agent/capabilities@0.1.0-beta.18
  - @effect-agent/core@0.1.0-beta.18

## 0.1.0-beta.17

### Patch Changes

- Updated dependencies [[`016df57`](https://github.com/danieljvdm/effect-agent/commit/016df574fa8c0f362468d848ae830d72532cbcaf)]:
  - @effect-agent/core@0.1.0-beta.17
  - @effect-agent/engine@0.1.0-beta.17
  - @effect-agent/capabilities@0.1.0-beta.17

## 0.1.0-beta.16

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.16
  - @effect-agent/core@0.1.0-beta.16
  - @effect-agent/engine@0.1.0-beta.16

## 0.1.0-beta.15

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.15
  - @effect-agent/core@0.1.0-beta.15
  - @effect-agent/engine@0.1.0-beta.15

## 0.1.0-beta.14

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.14
  - @effect-agent/core@0.1.0-beta.14
  - @effect-agent/engine@0.1.0-beta.14

## 0.1.0-beta.13

### Patch Changes

- Updated dependencies [[`68b48c9`](https://github.com/danieljvdm/effect-agent/commit/68b48c932b6a76d2c8ed0f04cc87c123a9fd11e4)]:
  - @effect-agent/core@0.1.0-beta.13
  - @effect-agent/capabilities@0.1.0-beta.13
  - @effect-agent/engine@0.1.0-beta.13

## 0.1.0-beta.12

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.12
  - @effect-agent/core@0.1.0-beta.12
  - @effect-agent/engine@0.1.0-beta.12

## 0.1.0-beta.11

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.11
  - @effect-agent/core@0.1.0-beta.11
  - @effect-agent/engine@0.1.0-beta.11

## 0.1.0-beta.10

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.10
  - @effect-agent/core@0.1.0-beta.10
  - @effect-agent/engine@0.1.0-beta.10

## 0.1.0-beta.9

### Patch Changes

- Updated dependencies [[`91ff50d`](https://github.com/danieljvdm/effect-agent/commit/91ff50df5480a0ccdfb8e0a00db39a1576e6c34b)]:
  - @effect-agent/core@0.1.0-beta.9
  - @effect-agent/engine@0.1.0-beta.9
  - @effect-agent/capabilities@0.1.0-beta.9

## 0.1.0-beta.8

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.8
  - @effect-agent/core@0.1.0-beta.8
  - @effect-agent/engine@0.1.0-beta.8

## 0.1.0-beta.7

### Patch Changes

- Updated dependencies [[`5c49b78`](https://github.com/danieljvdm/effect-agent/commit/5c49b786604b3e8389cdc2c54d4f5cb284eac2b7), [`afe755a`](https://github.com/danieljvdm/effect-agent/commit/afe755a331172ffca9ceee7dd82bb452c6ccbb8a), [`b44ed77`](https://github.com/danieljvdm/effect-agent/commit/b44ed7771c3e1ace2516507b0b54d11e662f036c), [`3a44b5f`](https://github.com/danieljvdm/effect-agent/commit/3a44b5f6595f4070abb61c79d5b756a9f7ed20af)]:
  - @effect-agent/engine@0.1.0-beta.7
  - @effect-agent/capabilities@0.1.0-beta.7
  - @effect-agent/core@0.1.0-beta.7

## 0.1.0-beta.6

### Patch Changes

- [#30](https://github.com/danieljvdm/effect-agent/pull/30) [`94c169a`](https://github.com/danieljvdm/effect-agent/commit/94c169a44a248972158ca955e33fb02dd5e55463) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Export privacy-safe canonical Tool spans and bounded terminal logs from the engine, including
  model-declared and programmatic broker calls, value-level failures, and delayed terminal event/
  trace commit, while isolating complete span-lifecycle defects through Effect's error reporter.
  Build Cloudflare Conversation Objects on `effect-cf`'s native `DurableObject.make` boundary so it
  owns the cached runtime, event-scoped Layers, native RPC methods, `waitUntil`, and post-RPC OTLP
  flush isolation. Upgrade to `effect-cf` 0.25.3 so the same upstream boundary flushes alarm
  telemetry. Remove Effect Agent's duplicate telemetry service, flush coordinator, timeout
  configuration, and lifecycle fixture matrix.
- Updated dependencies [[`e13ee6e`](https://github.com/danieljvdm/effect-agent/commit/e13ee6e7817549e99837d06e86caf2dea8656aa8), [`94c169a`](https://github.com/danieljvdm/effect-agent/commit/94c169a44a248972158ca955e33fb02dd5e55463)]:
  - @effect-agent/core@0.1.0-beta.6
  - @effect-agent/engine@0.1.0-beta.6
  - @effect-agent/capabilities@0.1.0-beta.6

## 0.0.1-beta.5

### Patch Changes

- [#19](https://github.com/danieljvdm/effect-agent/pull/19) [`a063031`](https://github.com/danieljvdm/effect-agent/commit/a063031c6b1f1637d947ae193a410b6bb9e8a9fc) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align every public package with Effect 4.0.0-beta.107. Also expose per-incarnation Cloudflare
  Binding capture with live Durable Object context and derived identities, and prevent incomplete
  application Tool batches from a failed or aborted Run from poisoning prompts for later Runs.
- Updated dependencies [[`a063031`](https://github.com/danieljvdm/effect-agent/commit/a063031c6b1f1637d947ae193a410b6bb9e8a9fc)]:
  - @effect-agent/core@0.0.1-beta.5
  - @effect-agent/engine@0.0.1-beta.5
  - @effect-agent/capabilities@0.0.1-beta.5

## 0.0.1-beta.4

### Patch Changes

- [#13](https://github.com/danieljvdm/effect-agent/pull/13) [`f4e3786`](https://github.com/danieljvdm/effect-agent/commit/f4e378635a794d4c17192ee3de011697ccec3a3b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Introduce the `effect-agent` umbrella package: the framework's complete pure
  surface — schema-first authoring (core), the bounded interpreter (engine),
  and operational capabilities — as one dependency-clean root package,
  mirroring how `effect` fronts the `@effect/*` satellites. Platform adapters
  remain scoped. The umbrella is version-fixed to its three constituents.
- Updated dependencies [[`f4e3786`](https://github.com/danieljvdm/effect-agent/commit/f4e378635a794d4c17192ee3de011697ccec3a3b)]:
  - @effect-agent/core@0.0.1-beta.4
  - @effect-agent/engine@0.0.1-beta.4
  - @effect-agent/capabilities@0.0.1-beta.4
