# @effect-agent/platform-cloudflare

## 0.1.0-beta.142

### Patch Changes

- Updated dependencies [[`161aab3`](https://github.com/danieljvdm/effect-agent/commit/161aab335dbbb9bf704d005bf95015be2e04858b)]:
  - @effect-agent/storage-cloudflare@0.1.0-beta.142
  - effect-agent@0.1.0-beta.142

## 0.1.0-beta.141

### Patch Changes

- Updated dependencies [[`e6127e4`](https://github.com/danieljvdm/effect-agent/commit/e6127e44d10f7103929abe65217f0ad837ce0d9f)]:
  - effect-agent@0.1.0-beta.141
  - @effect-agent/storage-cloudflare@0.1.0-beta.141

## 0.1.0-beta.140

### Patch Changes

- Updated dependencies [[`6d16773`](https://github.com/danieljvdm/effect-agent/commit/6d1677383d3377a0a399baeaec4c661d51b15878)]:
  - effect-agent@0.1.0-beta.140
  - @effect-agent/storage-cloudflare@0.1.0-beta.140

## 0.1.0-beta.139

### Minor Changes

- [#648](https://github.com/danieljvdm/effect-agent/pull/648) [`46001cc`](https://github.com/danieljvdm/effect-agent/commit/46001cc629c61fdd009c4081d6f1a0f97a95f4a9) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add `BrowserSessions.createAttached` to retain a new browser and use its initial scoped attachment without reconnecting before the first command. Preserve durable ownership, per-command authorization and timeouts, and exact-session cleanup.

### Patch Changes

- Updated dependencies []:
  - @effect-agent/storage-cloudflare@0.1.0-beta.139
  - effect-agent@0.1.0-beta.139

## 0.1.0-beta.138

### Patch Changes

- Updated dependencies [[`00355c1`](https://github.com/danieljvdm/effect-agent/commit/00355c1871e8fdab22ae1dbb1f03c1f35171f357)]:
  - effect-agent@0.1.0-beta.138
  - @effect-agent/storage-cloudflare@0.1.0-beta.138

## 0.1.0-beta.137

### Patch Changes

- Updated dependencies []:
  - @effect-agent/storage-cloudflare@0.1.0-beta.137
  - effect-agent@0.1.0-beta.137

## 0.1.0-beta.136

### Patch Changes

- Updated dependencies [[`a8c32dc`](https://github.com/danieljvdm/effect-agent/commit/a8c32dcc652192d81afbebf4f5940bf26fcc332c)]:
  - effect-agent@0.1.0-beta.136
  - @effect-agent/storage-cloudflare@0.1.0-beta.136

## 0.1.0-beta.135

### Patch Changes

- Updated dependencies [[`8fc53ad`](https://github.com/danieljvdm/effect-agent/commit/8fc53ad9eb6b110ca6faaaebbb6dbba08e3c292f)]:
  - effect-agent@0.1.0-beta.135
  - @effect-agent/storage-cloudflare@0.1.0-beta.135

## 0.1.0-beta.134

### Patch Changes

- [#631](https://github.com/danieljvdm/effect-agent/pull/631) [`d210027`](https://github.com/danieljvdm/effect-agent/commit/d210027cd1103cb5a13da03e7054e504c0159e2d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Require Effect 4.0.0-rc.117 and update the model examples to GPT-6.

- Updated dependencies [[`d210027`](https://github.com/danieljvdm/effect-agent/commit/d210027cd1103cb5a13da03e7054e504c0159e2d)]:
  - effect-agent@0.1.0-beta.134
  - @effect-agent/storage-cloudflare@0.1.0-beta.134

## 0.1.0-beta.133

### Patch Changes

- [#623](https://github.com/danieljvdm/effect-agent/pull/623) [`62665f5`](https://github.com/danieljvdm/effect-agent/commit/62665f5f6b9b55aa9d1bdf057e8fbb4b2f93dab6) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Propagate opt-in native RPC trace context across routed Thread port calls. Preserve the encoded request and existing behavior when tracing is disabled.

- [#625](https://github.com/danieljvdm/effect-agent/pull/625) [`9420943`](https://github.com/danieljvdm/effect-agent/commit/942094390475ef980d2edba50ed50c6e26af000d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Reject foreign submission identities and misplaced stored rows in local-only Thread Object lookups while preserving colocated threads and local absence.

- [#622](https://github.com/danieljvdm/effect-agent/pull/622) [`2eb83ea`](https://github.com/danieljvdm/effect-agent/commit/2eb83eab3bb2a609ed497dc9099e95a97a1ea62f) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Stop repeated empty native-maintenance scans after an overlapping admission completes while an unrelated host lane remains active.

- [#624](https://github.com/danieljvdm/effect-agent/pull/624) [`c5a487b`](https://github.com/danieljvdm/effect-agent/commit/c5a487beef98a5dfa6adb9a3e2edf542fccea90a) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Read canonical worker identity and its producer fence in one bounded owner snapshot instead of four serial remote reads. Custom ThreadStore adapters must implement `readIdentity`; deploy matching Cloudflare client and owner packages for the new read-only operation.

- Updated dependencies [[`9420943`](https://github.com/danieljvdm/effect-agent/commit/942094390475ef980d2edba50ed50c6e26af000d), [`450bac0`](https://github.com/danieljvdm/effect-agent/commit/450bac01e8e4fa937961e53f2231cfaa525167a4), [`c5a487b`](https://github.com/danieljvdm/effect-agent/commit/c5a487beef98a5dfa6adb9a3e2edf542fccea90a), [`e336239`](https://github.com/danieljvdm/effect-agent/commit/e336239226540001e6e4876c6f5dffc57b785769)]:
  - @effect-agent/storage-cloudflare@0.1.0-beta.133
  - effect-agent@0.1.0-beta.133

## 0.1.0-beta.132

### Patch Changes

- [#619](https://github.com/danieljvdm/effect-agent/pull/619) [`b548e6f`](https://github.com/danieljvdm/effect-agent/commit/b548e6f747f216a092703231440b73dd972c592e) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Acknowledge fresh empty native maintenance snapshots while independent host work remains active, avoiding repeated empty ledger scans without losing overlapping admissions.

- Updated dependencies []:
  - @effect-agent/storage-cloudflare@0.1.0-beta.132
  - effect-agent@0.1.0-beta.132

## 0.1.0-beta.131

### Patch Changes

- [#617](https://github.com/danieljvdm/effect-agent/pull/617) [`7fd0bab`](https://github.com/danieljvdm/effect-agent/commit/7fd0babeb0404c59034a804c0bc0925412101dbe) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add `BrowserSession.getReadOnlyLiveView` for provider-enforced read-only viewing of the retained page. Reject URLs unless Cloudflare confirms the requested read-only guardrail and exact target.

- Updated dependencies []:
  - @effect-agent/storage-cloudflare@0.1.0-beta.131
  - effect-agent@0.1.0-beta.131

## 0.1.0-beta.130

### Patch Changes

- [#615](https://github.com/danieljvdm/effect-agent/pull/615) [`31da959`](https://github.com/danieljvdm/effect-agent/commit/31da95903d8e943161e429e18311555717203505) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Keep healthy admission, delivery, and native execution running when an independent maintenance lane fails. Join bounded auxiliary waves before reporting their failure and retaining recovery.

- Updated dependencies []:
  - @effect-agent/storage-cloudflare@0.1.0-beta.130
  - effect-agent@0.1.0-beta.130

## 0.1.0-beta.129

### Patch Changes

- [#613](https://github.com/danieljvdm/effect-agent/pull/613) [`23e08c3`](https://github.com/danieljvdm/effect-agent/commit/23e08c33abaec27925b5856310e7cd94cdb58339) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Complete peer-initiated browser connection closure on Workers runtimes that require an explicit close reply. Preserve local attachment cleanup without closing the remote browser session.

- Updated dependencies []:
  - @effect-agent/storage-cloudflare@0.1.0-beta.129
  - effect-agent@0.1.0-beta.129

## 0.1.0-beta.128

### Patch Changes

- [#611](https://github.com/danieljvdm/effect-agent/pull/611) [`82b35f9`](https://github.com/danieljvdm/effect-agent/commit/82b35f99b45df6d9dd53edde26f54b50f2a6de8e) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Preserve the host's browser viewport when attaching to an existing session.

- Updated dependencies []:
  - @effect-agent/storage-cloudflare@0.1.0-beta.128
  - effect-agent@0.1.0-beta.128

## 0.1.0-beta.127

### Patch Changes

- [#608](https://github.com/danieljvdm/effect-agent/pull/608) [`7f0724f`](https://github.com/danieljvdm/effect-agent/commit/7f0724f4a58cd3ced56b77f811391831781212c2) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Allow retained browser attachments up to ten seconds to acknowledge disconnection, preserving completed commands when the close handshake takes longer than one second. Continue fencing dispatch immediately and fail cleanup when closure remains unconfirmed.

- Updated dependencies []:
  - @effect-agent/storage-cloudflare@0.1.0-beta.127
  - effect-agent@0.1.0-beta.127

## 0.1.0-beta.126

### Patch Changes

- [#602](https://github.com/danieljvdm/effect-agent/pull/602) [`8fe0316`](https://github.com/danieljvdm/effect-agent/commit/8fe03169c5eef38f1cb01bfe8be363436b094ea2) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Dispatch up to two independent Threads concurrently in a Cloudflare maintenance event so newly ready input can start while another Thread is busy. Preserve per-Thread FIFO, scoped claim cleanup, and durable alarm recovery.

- Updated dependencies [[`bf955bb`](https://github.com/danieljvdm/effect-agent/commit/bf955bbf275901e560d93cf0a054cfbf51aa9420), [`81a78cd`](https://github.com/danieljvdm/effect-agent/commit/81a78cd2bbd932b939b942eedea53c8e2894480e)]:
  - effect-agent@0.1.0-beta.126
  - @effect-agent/storage-cloudflare@0.1.0-beta.126

## 0.1.0-beta.125

### Patch Changes

- Updated dependencies [[`34d7c5f`](https://github.com/danieljvdm/effect-agent/commit/34d7c5ff9392fd6fb1db348257fd22dea58a337c)]:
  - effect-agent@0.1.0-beta.125
  - @effect-agent/storage-cloudflare@0.1.0-beta.125

## 0.1.0-beta.124

### Patch Changes

- Updated dependencies [[`d8bd6db`](https://github.com/danieljvdm/effect-agent/commit/d8bd6db4d21dbb0ae53132d52db7fa3fa6ef9f76)]:
  - effect-agent@0.1.0-beta.124
  - @effect-agent/storage-cloudflare@0.1.0-beta.124

## 0.1.0-beta.123

### Minor Changes

- [#590](https://github.com/danieljvdm/effect-agent/pull/590) [`72e07a3`](https://github.com/danieljvdm/effect-agent/commit/72e07a35010564acb411845e250fa5d552edef0d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Replace protected browser passes with application-owned Cloudflare sessions, native Puppeteer actions, and authorized credential filling.

  BEHAVIOR CHANGE: Migrate removed `protected-browser` APIs to `browser-session` and `browser-credentials`; ordinary page observations may expose filled values, and application owners must retain session references and close browsers on completion or expiry.

### Patch Changes

- [#590](https://github.com/danieljvdm/effect-agent/pull/590) [`72e07a3`](https://github.com/danieljvdm/effect-agent/commit/72e07a35010564acb411845e250fa5d552edef0d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Preserve acknowledged credential writes, dispatch evidence, and browser cleanup status in credential timeout failures.

  BEHAVIOR CHANGE: Call `session.fillCredential(request)` instead of the standalone `fillCredential(page, request)` helper.

- Updated dependencies [[`72e07a3`](https://github.com/danieljvdm/effect-agent/commit/72e07a35010564acb411845e250fa5d552edef0d)]:
  - effect-agent@0.1.0-beta.123
  - @effect-agent/storage-cloudflare@0.1.0-beta.123

## 0.1.0-beta.122

### Patch Changes

- [#587](https://github.com/danieljvdm/effect-agent/pull/587) [`83fb830`](https://github.com/danieljvdm/effect-agent/commit/83fb83078a95a5fb60fffa0ea818dca98d4e88bd) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Allow callers to set finite browser pass allowances beyond one hour and keep retained protected sessions active with `BrowserRunProtectedHost.keepAlive(sessionId)`. Preserve unrestricted service-worker handling and distinguish failed-resume attachment retirement from uncertain local cleanup.

- Updated dependencies [[`83fb830`](https://github.com/danieljvdm/effect-agent/commit/83fb83078a95a5fb60fffa0ea818dca98d4e88bd)]:
  - effect-agent@0.1.0-beta.122
  - @effect-agent/storage-cloudflare@0.1.0-beta.122

## 0.1.0-beta.121

### Patch Changes

- [#585](https://github.com/danieljvdm/effect-agent/pull/585) [`0459848`](https://github.com/danieljvdm/effect-agent/commit/0459848d055649855419c948371480f9f85a5e68) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Reconnect retained browser pages after navigation leaves cached cross-origin frames. Preserve the host-owned browser when attachment acquisition fails, and include the browser client as a runtime dependency.

- Updated dependencies []:
  - @effect-agent/storage-cloudflare@0.1.0-beta.121
  - effect-agent@0.1.0-beta.121

## 0.1.0-beta.120

### Patch Changes

- [#583](https://github.com/danieljvdm/effect-agent/pull/583) [`037d29a`](https://github.com/danieljvdm/effect-agent/commit/037d29a754034551520c8df9cb41bfb7660cde40) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Retain attempt resources before durable approval suspension with `DurableApprovalSuspension`. Resume approvals that arrive during retention with fresh attempt services and the same pending tool batch.

- Updated dependencies [[`037d29a`](https://github.com/danieljvdm/effect-agent/commit/037d29a754034551520c8df9cb41bfb7660cde40)]:
  - effect-agent@0.1.0-beta.120
  - @effect-agent/storage-cloudflare@0.1.0-beta.120

## 0.1.0-beta.119

### Patch Changes

- [#581](https://github.com/danieljvdm/effect-agent/pull/581) [`5c11bea`](https://github.com/danieljvdm/effect-agent/commit/5c11bea7ec185136b3453d317a0fea20f015a3a8) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Expose visible choices and selection state for ordinary protected selects, and preserve prior dispatch evidence after acknowledged no-write refusals.

  BEHAVIOR CHANGE: Fill ordinary selects with the exact observed option label; raw option values are no longer a fallback, while credential selects continue to use private values.

- Updated dependencies [[`5c11bea`](https://github.com/danieljvdm/effect-agent/commit/5c11bea7ec185136b3453d317a0fea20f015a3a8)]:
  - effect-agent@0.1.0-beta.119
  - @effect-agent/storage-cloudflare@0.1.0-beta.119

## 0.1.0-beta.118

### Patch Changes

- [#579](https://github.com/danieljvdm/effect-agent/pull/579) [`ff29420`](https://github.com/danieljvdm/effect-agent/commit/ff2942050eae59ad3ccc9731caaf809e13d957f1) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Allow credential access hooks to report recoverable `busy` while preserving known protected browser dispatch evidence and the same usable session.
  Complete protected controller Return independently of observation authority, requiring a fresh authorized observation before subsequent operations.
- Updated dependencies [[`ff29420`](https://github.com/danieljvdm/effect-agent/commit/ff2942050eae59ad3ccc9731caaf809e13d957f1)]:
  - effect-agent@0.1.0-beta.118
  - @effect-agent/storage-cloudflare@0.1.0-beta.118

## 0.1.0-beta.117

### Patch Changes

- [#577](https://github.com/danieljvdm/effect-agent/pull/577) [`8b6130e`](https://github.com/danieljvdm/effect-agent/commit/8b6130e21881edaa0928ec2ff30c4d6129b9c5b9) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Notify recovery-status observers when a fault is created, updated or cleared, including while unrelated maintenance remains active.

- Updated dependencies []:
  - @effect-agent/storage-cloudflare@0.1.0-beta.117
  - effect-agent@0.1.0-beta.117

## 0.1.0-beta.116

### Patch Changes

- Updated dependencies [[`c29c38c`](https://github.com/danieljvdm/effect-agent/commit/c29c38cc4ebaf81c700911b83a57073005c6bdfa)]:
  - effect-agent@0.1.0-beta.116
  - @effect-agent/storage-cloudflare@0.1.0-beta.116

## 0.1.0-beta.115

### Minor Changes

- [#570](https://github.com/danieljvdm/effect-agent/pull/570) [`8948e5d`](https://github.com/danieljvdm/effect-agent/commit/8948e5d7d149c94ca82699b0519601a165702bb5) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Let the Cloudflare alarm schedule independent finite host lanes and native message waves. BEHAVIOR CHANGE: replace `ThreadHostMaintenance.drainUntil` and `ThreadMaintenanceActivity` with `lanes` of `run`, `pendingDeadline` and `dispatchTimeoutMillis`; route producer hints through `WakeScheduler`, and replace `ThreadMessageDelivery.drainUntil` with `prepare`.

### Patch Changes

- [#568](https://github.com/danieljvdm/effect-agent/pull/568) [`1c33f81`](https://github.com/danieljvdm/effect-agent/commit/1c33f812e4339f1b5757d2721aa8318c8119aa51) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add durable worker-wide stop and indexed summaries with exact accepted and applied input identities. Replay retained starts before public input preparation and drain up to 32 accepted worker inputs at each safe steering boundary.

- [#569](https://github.com/danieljvdm/effect-agent/pull/569) [`0f2c67c`](https://github.com/danieljvdm/effect-agent/commit/0f2c67c1eb459d0192d0d7cc7f752e087c8807c7) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Allow ordinary protected browser filling for checkout and contact email fields. BEHAVIOR CHANGE: Treat email-only login forms marked solely `autocomplete="email"` as ordinary text without saved-login offers; retain offers for explicit `autocomplete="username"` and preserve password-form and card protections.

- Updated dependencies [[`1c33f81`](https://github.com/danieljvdm/effect-agent/commit/1c33f812e4339f1b5757d2721aa8318c8119aa51), [`d1313aa`](https://github.com/danieljvdm/effect-agent/commit/d1313aaf2a1be18b34e5ebfa680ed12f4cef31bc), [`432036c`](https://github.com/danieljvdm/effect-agent/commit/432036cedbe59e8ecbdcd4c71417b730d5b781df)]:
  - effect-agent@0.1.0-beta.115
  - @effect-agent/storage-cloudflare@0.1.0-beta.115

## 0.1.0-beta.114

### Patch Changes

- [#564](https://github.com/danieljvdm/effect-agent/pull/564) [`93a95e9`](https://github.com/danieljvdm/effect-agent/commit/93a95e945440e8cdbe165c91da0cb7598470336c) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Keep newly ready admissions eligible when maintenance overlaps their mutation, without inheriting another thread's recovery retry deadline.

- Updated dependencies []:
  - @effect-agent/storage-cloudflare@0.1.0-beta.114
  - effect-agent@0.1.0-beta.114

## 0.1.0-beta.113

### Patch Changes

- [#561](https://github.com/danieljvdm/effect-agent/pull/561) [`f90854e`](https://github.com/danieljvdm/effect-agent/commit/f90854ee893134df8022043a97544946ca4f1e25) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Keep native dispatch and host abort/reply waves open together while unrelated alarm work retires. BEHAVIOR CHANGE: yield the event-scoped `ThreadMaintenanceActivity` service in `drainUntil`, acquire `subscribeChanges` before initial setup, register finite waves with `run`, and acknowledge readiness with `ready`; use `ThreadMaintenanceActivity.all` to compose independent pumps.

- Updated dependencies []:
  - @effect-agent/storage-cloudflare@0.1.0-beta.113
  - effect-agent@0.1.0-beta.113

## 0.1.0-beta.112

### Patch Changes

- [#556](https://github.com/danieljvdm/effect-agent/pull/556) [`ab5030d`](https://github.com/danieljvdm/effect-agent/commit/ab5030d9814a5c47f6facfdf89fe5799bdba6b00) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Isolate Thread recovery faults with history-independent status and bounded retries, let Cloudflare dispatch fresh Threads while old cleanup is pending, and keep Node startup closed on blocked recovery. Preserve content-free storage diagnostics.

  BEHAVIOR CHANGE: Call `runtime.runRecovery()` instead of yielding `runtime.runRecovery`; its result contains ordinary Submission `reports` and one `blocked` fault per failed Thread. Blocked Threads remain ineligible for claims; pass `{ threadId }` to recover only a selected Thread.

  `SubmissionLedger.scanNonterminal` now emits control-only `SubmissionWorkItem` entries. Use `lookup` or `loadRecoverySnapshot` for selected execution payloads.

- [#558](https://github.com/danieljvdm/effect-agent/pull/558) [`6716f8c`](https://github.com/danieljvdm/effect-agent/commit/6716f8c5915fee466c89d9d82159fd8f2b67ece4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Require Effect rc.116 and replace the local decision and TypeSafe APIs with native `Decision`, `DecisionModel`, and `@effect/ai-typesafe`, retaining `AutoModel` for thread selection.

  BEHAVIOR CHANGE: Import decisions from `effect/unstable/ai` and configure TypeSafe with `TypeSafeClient.layerConfig()`; AutoModel requires at least two profiles, writes version 2 selection records, and rejects version 1 records without reselection or mutation. Retain the previous runtime for active version 1 threads or explicitly upgrade their records in your storage adapter; native probability sums must be within `1e-6` of 1.

- Updated dependencies [[`ab5030d`](https://github.com/danieljvdm/effect-agent/commit/ab5030d9814a5c47f6facfdf89fe5799bdba6b00), [`6716f8c`](https://github.com/danieljvdm/effect-agent/commit/6716f8c5915fee466c89d9d82159fd8f2b67ece4)]:
  - effect-agent@0.1.0-beta.112
  - @effect-agent/storage-cloudflare@0.1.0-beta.112

## 0.1.0-beta.111

### Patch Changes

- [#554](https://github.com/danieljvdm/effect-agent/pull/554) [`b2cf08c`](https://github.com/danieljvdm/effect-agent/commit/b2cf08c14d3c455990724fb30062bdd5544dcabb) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Preserve browser dispatch evidence and add exact-page checkpoint recovery with pending-input fences. Expose structured page observations and refuse stale click/fill targets before dispatch.

- Updated dependencies [[`b2cf08c`](https://github.com/danieljvdm/effect-agent/commit/b2cf08c14d3c455990724fb30062bdd5544dcabb)]:
  - effect-agent@0.1.0-beta.111
  - @effect-agent/storage-cloudflare@0.1.0-beta.111

## 0.1.0-beta.110

### Patch Changes

- [#547](https://github.com/danieljvdm/effect-agent/pull/547) [`bf246f9`](https://github.com/danieljvdm/effect-agent/commit/bf246f9f103557ee8e64687a3b20275b697f8753) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Allow protected browser hosts to resume committed workflow pauses without a human handoff while preserving fresh observation, credential grants, and cumulative session limits.

- Updated dependencies [[`c2ae9e7`](https://github.com/danieljvdm/effect-agent/commit/c2ae9e777766fba0e14e8a472bc833d2122c2b10), [`a1957c4`](https://github.com/danieljvdm/effect-agent/commit/a1957c457777e7f8eeb7b51ab8833f41593c3ecf), [`2582969`](https://github.com/danieljvdm/effect-agent/commit/25829699c09a4cc862b650e4e30e5edc0fbb4fc0)]:
  - effect-agent@0.1.0-beta.110
  - @effect-agent/storage-cloudflare@0.1.0-beta.110

## 0.1.0-beta.109

### Patch Changes

- Updated dependencies [[`cdbe786`](https://github.com/danieljvdm/effect-agent/commit/cdbe786861e9ba10ecb1dccf3b26f47170a8245e)]:
  - effect-agent@0.1.0-beta.109
  - @effect-agent/storage-cloudflare@0.1.0-beta.109

## 0.1.0-beta.108

### Patch Changes

- [#543](https://github.com/danieljvdm/effect-agent/pull/543) [`3b4e238`](https://github.com/danieljvdm/effect-agent/commit/3b4e238f9cda40a782919b7d1b8316489b7cb675) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Report safe browser acquisition and cleanup diagnostics through Effect ErrorReporter without exposing provider content. Preserve mixed defects through protected policy recovery and use one recording-disabled acquisition path for interactive and protected sessions.

- Updated dependencies [[`92bd9e2`](https://github.com/danieljvdm/effect-agent/commit/92bd9e26c181c07f84371a372d8885cd4db4667a)]:
  - effect-agent@0.1.0-beta.108
  - @effect-agent/storage-cloudflare@0.1.0-beta.108

## 0.1.0-beta.107

### Patch Changes

- [#540](https://github.com/danieljvdm/effect-agent/pull/540) [`cfb6e1e`](https://github.com/danieljvdm/effect-agent/commit/cfb6e1e04b9e80d276f918f29c369cddec5b917c) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add bounded byte-backed file selection to interactive browsers, including Cloudflare Browser Run file inputs and dynamic choosers. Distinguish confirmed selection from website upload or submission receipts.

- Updated dependencies [[`cfb6e1e`](https://github.com/danieljvdm/effect-agent/commit/cfb6e1e04b9e80d276f918f29c369cddec5b917c)]:
  - effect-agent@0.1.0-beta.107
  - @effect-agent/storage-cloudflare@0.1.0-beta.107

## 0.1.0-beta.106

### Patch Changes

- Updated dependencies [[`992d062`](https://github.com/danieljvdm/effect-agent/commit/992d062a095995bd8f328a01cc784b6a9a7ffc72)]:
  - effect-agent@0.1.0-beta.106
  - @effect-agent/storage-cloudflare@0.1.0-beta.106

## 0.1.0-beta.105

### Patch Changes

- [#534](https://github.com/danieljvdm/effect-agent/pull/534) [`8491671`](https://github.com/danieljvdm/effect-agent/commit/84916713273694ae717095c5ec5aaae108300520) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Keep ready Thread work running while alarm-owned delivery finishes, preserving independent retry deadlines and one shared event budget. Treat `ThreadHostMaintenance`'s `dispatchClosed` signal as the end of new delivery waves; keep local admission listeners alive until their event Scope closes.

- Updated dependencies []:
  - @effect-agent/storage-cloudflare@0.1.0-beta.105
  - effect-agent@0.1.0-beta.105

## 0.1.0-beta.104

### Patch Changes

- [#530](https://github.com/danieljvdm/effect-agent/pull/530) [`caf7e7e`](https://github.com/danieljvdm/effect-agent/commit/caf7e7ea69448fb820f9e95cffe480cbb458d500) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Preserve each invocation's tracing context and sampling decision when running registered attempts or preparing reports. Remove per-poll, digest, and response-part helper spans while retaining operation boundaries and errors, and allow hosts to use effect-cf 0.45.

- Updated dependencies [[`caf7e7e`](https://github.com/danieljvdm/effect-agent/commit/caf7e7ea69448fb820f9e95cffe480cbb458d500)]:
  - effect-agent@0.1.0-beta.104
  - @effect-agent/storage-cloudflare@0.1.0-beta.104

## 0.1.0-beta.103

### Patch Changes

- Updated dependencies []:
  - @effect-agent/storage-cloudflare@0.1.0-beta.103
  - effect-agent@0.1.0-beta.103

## 0.1.0-beta.102

### Patch Changes

- Updated dependencies [[`be0dcaf`](https://github.com/danieljvdm/effect-agent/commit/be0dcafb69e0641d8b82ff174fee53a53e367f18)]:
  - effect-agent@0.1.0-beta.102
  - @effect-agent/storage-cloudflare@0.1.0-beta.102

## 0.1.0-beta.101

### Patch Changes

- [#517](https://github.com/danieljvdm/effect-agent/pull/517) [`6a4f4f8`](https://github.com/danieljvdm/effect-agent/commit/6a4f4f870fe87ebb0d3cc76905dcadd77c9a29ef) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Expose bounded outstanding-operation and pending-delivery reads, exact canonical record locators, and native worker/peer admission lookups. Retain uncertain external outcomes after abort and retire worker inputs only after their effects are resolved.

- Updated dependencies [[`6a4f4f8`](https://github.com/danieljvdm/effect-agent/commit/6a4f4f870fe87ebb0d3cc76905dcadd77c9a29ef)]:
  - effect-agent@0.1.0-beta.101
  - @effect-agent/storage-cloudflare@0.1.0-beta.101

## 0.1.0-beta.100

### Patch Changes

- Updated dependencies []:
  - @effect-agent/storage-cloudflare@0.1.0-beta.100
  - effect-agent@0.1.0-beta.100

## 0.1.0-beta.99

### Patch Changes

- Updated dependencies [[`e1f06bb`](https://github.com/danieljvdm/effect-agent/commit/e1f06bbd3f66478c9223c5888696cd8c6e75fc37)]:
  - effect-agent@0.1.0-beta.99
  - @effect-agent/storage-cloudflare@0.1.0-beta.99

## 0.1.0-beta.98

### Patch Changes

- [#507](https://github.com/danieljvdm/effect-agent/pull/507) [`95c962f`](https://github.com/danieljvdm/effect-agent/commit/95c962f8ee45c35f877d0bb21f82d4f6bac6759c) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Continue accepted requests with current Agent bindings, retaining original operations and outcomes while allowing later input around unknown work under one Thread lease. BEHAVIOR CHANGE: replace historical binding manifests with per-operation replay versions and deploy matching runtime and storage packages together.

- Updated dependencies [[`95c962f`](https://github.com/danieljvdm/effect-agent/commit/95c962f8ee45c35f877d0bb21f82d4f6bac6759c)]:
  - effect-agent@0.1.0-beta.98
  - @effect-agent/storage-cloudflare@0.1.0-beta.98

## 0.1.0-beta.97

### Patch Changes

- Updated dependencies [[`385f119`](https://github.com/danieljvdm/effect-agent/commit/385f1197eb41e8114c5daf5b6763824450095cf5)]:
  - effect-agent@0.1.0-beta.97
  - @effect-agent/storage-cloudflare@0.1.0-beta.97

## 0.1.0-beta.96

### Minor Changes

- [#490](https://github.com/danieljvdm/effect-agent/pull/490) [`771498b`](https://github.com/danieljvdm/effect-agent/commit/771498b1952794b8f2f19d1e35b604937bffcc3c) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Resume retained requests using explicit replay contracts and back off failed and blocked Cloudflare maintenance without abandoning child obligations or changing receipts. BEHAVIOR CHANGE: bound durable execution duration per active Attempt, retain actual duration exhaustion, and deploy matching runtime and storage packages before writing the new record.

### Patch Changes

- [#502](https://github.com/danieljvdm/effect-agent/pull/502) [`c9e1045`](https://github.com/danieljvdm/effect-agent/commit/c9e1045b2c563c6779f40d5cb9dccea14410c654) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Require effect-cf ^0.44.1 so native callback completion retains its callback scheduler.

- Updated dependencies [[`771498b`](https://github.com/danieljvdm/effect-agent/commit/771498b1952794b8f2f19d1e35b604937bffcc3c)]:
  - effect-agent@0.1.0-beta.96
  - @effect-agent/storage-cloudflare@0.1.0-beta.96

## 0.1.0-beta.95

### Patch Changes

- [#500](https://github.com/danieljvdm/effect-agent/pull/500) [`0fe79ac`](https://github.com/danieljvdm/effect-agent/commit/0fe79ac5b432ce3a824b6c7aa49853829b92d0b4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Retire alarm-owned host delivery and disposable projection work after finite dispatch opportunities while preserving native delivery timeout/retry commits and wake-driven overlap. BEHAVIOR CHANGE: Declare `ThreadHostMaintenance.dispatchTimeoutMillis`, keep admission listeners in the event Scope, and use the single `drainUntil(sourceFinished, dispatchUntil)` hook instead of the native `drain` fallback.

- Updated dependencies []:
  - @effect-agent/storage-cloudflare@0.1.0-beta.95
  - effect-agent@0.1.0-beta.95

## 0.1.0-beta.94

### Patch Changes

- Updated dependencies [[`373d188`](https://github.com/danieljvdm/effect-agent/commit/373d18828f2fc2851614cf2612c5e71e91075c88), [`bbb709c`](https://github.com/danieljvdm/effect-agent/commit/bbb709c9beff0b8f2e6b67d05e0f8223a7cb6f93)]:
  - effect-agent@0.1.0-beta.94
  - @effect-agent/storage-cloudflare@0.1.0-beta.94

## 0.1.0-beta.93

### Patch Changes

- [#495](https://github.com/danieljvdm/effect-agent/pull/495) [`723642c`](https://github.com/danieljvdm/effect-agent/commit/723642c2ed7a12eb41f19dcaa84c5f2c08ae9b76) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Expose scoped browser acquisition so hosts can persist the cleanup identity before connection and page setup. Keep interrupted allocation cleanup bound to the exact provider session, and apply the configured initial viewport to the new page.

- Updated dependencies [[`319c156`](https://github.com/danieljvdm/effect-agent/commit/319c156be5a85a2d490cf79531f94591881436f8)]:
  - effect-agent@0.1.0-beta.93
  - @effect-agent/storage-cloudflare@0.1.0-beta.93

## 0.1.0-beta.92

### Patch Changes

- [#487](https://github.com/danieljvdm/effect-agent/pull/487) [`054b1c3`](https://github.com/danieljvdm/effect-agent/commit/054b1c3a7e7a6571fc82caedc4ae8835c5aacfb4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Require Effect rc.115 across the packages and effect-cf 0.43.0 for Cloudflare hosts.

- Updated dependencies [[`054b1c3`](https://github.com/danieljvdm/effect-agent/commit/054b1c3a7e7a6571fc82caedc4ae8835c5aacfb4)]:
  - @effect-agent/storage-cloudflare@0.1.0-beta.92
  - effect-agent@0.1.0-beta.92

## 0.1.0-beta.91

### Patch Changes

- [#484](https://github.com/danieljvdm/effect-agent/pull/484) [`f5ee0ad`](https://github.com/danieljvdm/effect-agent/commit/f5ee0ad2b369f0f218df7076a8600291b137778a) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Require effect-cf 0.42.1 for correct Durable Object initialization ordering and native callback scheduling.

- Updated dependencies [[`b60b07e`](https://github.com/danieljvdm/effect-agent/commit/b60b07e307dc366637f5247fb788b24b17c554eb)]:
  - effect-agent@0.1.0-beta.91
  - @effect-agent/storage-cloudflare@0.1.0-beta.91

## 0.1.0-beta.90

### Patch Changes

- [#482](https://github.com/danieljvdm/effect-agent/pull/482) [`1effa15`](https://github.com/danieljvdm/effect-agent/commit/1effa15fa381dbc78bf75e1eb7e5efd135c3b266) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Expose the provider-verified current origin after protected human handoff completes, without resuming agent tools or reading page content.

- Updated dependencies []:
  - @effect-agent/storage-cloudflare@0.1.0-beta.90
  - effect-agent@0.1.0-beta.90

## 0.1.0-beta.89

### Minor Changes

- [#479](https://github.com/danieljvdm/effect-agent/pull/479) [`983a558`](https://github.com/danieljvdm/effect-agent/commit/983a558703a187285ff9c900792defc8f15984a1) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add host-owned protected browser suspension, exact-page resume, and authorized human takeover while preserving credential exposure and browser budgets. Require fresh observation after Return and keep transferred sessions independent of the previous Attempt’s scope.

### Patch Changes

- Updated dependencies [[`983a558`](https://github.com/danieljvdm/effect-agent/commit/983a558703a187285ff9c900792defc8f15984a1)]:
  - effect-agent@0.1.0-beta.89
  - @effect-agent/storage-cloudflare@0.1.0-beta.89

## 0.1.0-beta.88

### Patch Changes

- Updated dependencies [[`5e24e87`](https://github.com/danieljvdm/effect-agent/commit/5e24e8782203aef836c8b4ba49e72468d7d510b1)]:
  - effect-agent@0.1.0-beta.88
  - @effect-agent/storage-cloudflare@0.1.0-beta.88

## 0.1.0-beta.87

### Patch Changes

- [#471](https://github.com/danieljvdm/effect-agent/pull/471) [`0be6edf`](https://github.com/danieljvdm/effect-agent/commit/0be6edfa8c73822f59184e6177a265c56c3ac1cd) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Schedule message delivery maintenance from its durable due index without waking source execution recovery for delivery bookkeeping. Preserve prearmed recovery across eviction and wake retained deliveries when they become due.

- Updated dependencies [[`0be6edf`](https://github.com/danieljvdm/effect-agent/commit/0be6edfa8c73822f59184e6177a265c56c3ac1cd)]:
  - effect-agent@0.1.0-beta.87
  - @effect-agent/storage-cloudflare@0.1.0-beta.87

## 0.1.0-beta.86

### Minor Changes

- [#466](https://github.com/danieljvdm/effect-agent/pull/466) [`6560df2`](https://github.com/danieljvdm/effect-agent/commit/6560df20d900627d28d745ea11e9759774bb9cf0) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Consolidate agent definitions, execution, capabilities, and sandbox contracts into `effect-agent`, and use kebab-case public module paths across framework packages.

  BEHAVIOR CHANGE: Replace `@effect-agent/core`, `@effect-agent/engine`, `@effect-agent/capabilities`, and `@effect-agent/sandbox` dependencies with `effect-agent`; migrate direct imports such as `effect-agent/AgentRuntime` to `effect-agent/agent-runtime` and upgrade framework packages together.

### Patch Changes

- Updated dependencies [[`1112b1b`](https://github.com/danieljvdm/effect-agent/commit/1112b1bfb388be600c9326737d10608660698ef3), [`6560df2`](https://github.com/danieljvdm/effect-agent/commit/6560df20d900627d28d745ea11e9759774bb9cf0), [`6560df2`](https://github.com/danieljvdm/effect-agent/commit/6560df20d900627d28d745ea11e9759774bb9cf0), [`6560df2`](https://github.com/danieljvdm/effect-agent/commit/6560df20d900627d28d745ea11e9759774bb9cf0)]:
  - effect-agent@0.1.0-beta.86
  - @effect-agent/storage-cloudflare@0.1.0-beta.86

## 0.1.0-beta.85

### Minor Changes

- [#459](https://github.com/danieljvdm/effect-agent/pull/459) [`eab5b7c`](https://github.com/danieljvdm/effect-agent/commit/eab5b7c0ea2c52bf81e947253a8387a4b1e78c9d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Opt into durable typed parent completion messages with `Subagent.background(Research, { start: true, followUp: true, reportToParent: true })`, without an application input union, mapper, or reporting registration. Pass a custom reporting descriptor as `reportToParent` when an application-specific input format is needed.

- [#459](https://github.com/danieljvdm/effect-agent/pull/459) [`eab5b7c`](https://github.com/danieljvdm/effect-agent/commit/eab5b7c0ea2c52bf81e947253a8387a4b1e78c9d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Accept Agent definitions directly in background subagent tools and declare typed intermediate updates on Agents. Deliver opted-in worker findings to the parent before completion, with durable retention, ordering, and bounded backpressure.

### Patch Changes

- Updated dependencies [[`eab5b7c`](https://github.com/danieljvdm/effect-agent/commit/eab5b7c0ea2c52bf81e947253a8387a4b1e78c9d), [`eab5b7c`](https://github.com/danieljvdm/effect-agent/commit/eab5b7c0ea2c52bf81e947253a8387a4b1e78c9d)]:
  - @effect-agent/core@0.1.0-beta.85
  - @effect-agent/engine@0.1.0-beta.85
  - @effect-agent/thread@0.1.0-beta.85
  - @effect-agent/storage-cloudflare@0.1.0-beta.85
  - @effect-agent/sandbox@0.1.0-beta.85

## 0.1.0-beta.84

### Patch Changes

- [#457](https://github.com/danieljvdm/effect-agent/pull/457) [`7298014`](https://github.com/danieljvdm/effect-agent/commit/7298014b83979d44716082f1c52cfc151c6edeaa) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Reuse native RPC targets within each Cloudflare invocation so delegated work, progress observation, memory and scheduling callbacks do not exhaust subrequest depth. Replace failed channels and start fresh target scopes for incoming requests and durable retries.

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.84
  - @effect-agent/engine@0.1.0-beta.84
  - @effect-agent/sandbox@0.1.0-beta.84
  - @effect-agent/thread@0.1.0-beta.84
  - @effect-agent/storage-cloudflare@0.1.0-beta.84

## 0.1.0-beta.83

### Patch Changes

- Updated dependencies [[`d349fa1`](https://github.com/danieljvdm/effect-agent/commit/d349fa181bb3ecc88823aeef4ae9075a12d21f1f)]:
  - @effect-agent/thread@0.1.0-beta.83
  - @effect-agent/storage-cloudflare@0.1.0-beta.83
  - @effect-agent/core@0.1.0-beta.83
  - @effect-agent/engine@0.1.0-beta.83
  - @effect-agent/sandbox@0.1.0-beta.83

## 0.1.0-beta.82

### Patch Changes

- Updated dependencies [[`c7fb67c`](https://github.com/danieljvdm/effect-agent/commit/c7fb67c0607f9a7e7a31e4f90ba3b20c7e6079aa)]:
  - @effect-agent/thread@0.1.0-beta.82
  - @effect-agent/storage-cloudflare@0.1.0-beta.82
  - @effect-agent/core@0.1.0-beta.82
  - @effect-agent/engine@0.1.0-beta.82
  - @effect-agent/sandbox@0.1.0-beta.82

## 0.1.0-beta.81

### Patch Changes

- [#442](https://github.com/danieljvdm/effect-agent/pull/442) [`e43996e`](https://github.com/danieljvdm/effect-agent/commit/e43996e78959c64fca1e44baa4ce15934f50d466) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Keep alarm updates independent of concurrent SQLite transactions, and retain the earliest requested recovery deadline atomically. Provide the owner's SqlClient when building standalone alarm or maintenance layers.

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.81
  - @effect-agent/engine@0.1.0-beta.81
  - @effect-agent/sandbox@0.1.0-beta.81
  - @effect-agent/thread@0.1.0-beta.81
  - @effect-agent/storage-cloudflare@0.1.0-beta.81

## 0.1.0-beta.80

### Patch Changes

- [#440](https://github.com/danieljvdm/effect-agent/pull/440) [`227b5e8`](https://github.com/danieljvdm/effect-agent/commit/227b5e8a98ce4d1b303bacbeaf35c15ff45f6c75) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Host multiple logical Threads inside an application Durable Object using its existing SQL client and one shared alarm, with addressed controls, recovery across local lanes, and isolated native migration history. **BEHAVIOR CHANGE:** Define custom `ThreadObjectNamespace` services with `get(threadId)` to resolve each logical endpoint.

- Updated dependencies [[`227b5e8`](https://github.com/danieljvdm/effect-agent/commit/227b5e8a98ce4d1b303bacbeaf35c15ff45f6c75)]:
  - @effect-agent/storage-cloudflare@0.1.0-beta.80
  - @effect-agent/core@0.1.0-beta.80
  - @effect-agent/engine@0.1.0-beta.80
  - @effect-agent/sandbox@0.1.0-beta.80
  - @effect-agent/thread@0.1.0-beta.80

## 0.1.0-beta.79

### Patch Changes

- [#436](https://github.com/danieljvdm/effect-agent/pull/436) [`cf8306d`](https://github.com/danieljvdm/effect-agent/commit/cf8306d6c9de9d561ceb3ff94edf181b4e8adad4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Report recognized Browser Run navigation timeouts with their provider limit and distinguish Quick Action API statuses from destination page statuses.

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.79
  - @effect-agent/engine@0.1.0-beta.79
  - @effect-agent/sandbox@0.1.0-beta.79
  - @effect-agent/thread@0.1.0-beta.79
  - @effect-agent/storage-cloudflare@0.1.0-beta.79

## 0.1.0-beta.78

### Patch Changes

- [#426](https://github.com/danieljvdm/effect-agent/pull/426) [`b8bae94`](https://github.com/danieljvdm/effect-agent/commit/b8bae94a03ffa5bec9e01a4ea779993512ca19ac) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add authorized, bounded exact-key current document reads through `CloudflareMemoryClient.get`, returning explicit absence or withdrawal tombstones while preserving typed access, storage, and transport failures.

- [#431](https://github.com/danieljvdm/effect-agent/pull/431) [`16ec320`](https://github.com/danieljvdm/effect-agent/commit/16ec320ab3565c9429d62b60b19e024218283bb0) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Retain Browser Run response status, request identifiers, and body truncation metadata in host-only failure causes.

- Updated dependencies [[`b8bae94`](https://github.com/danieljvdm/effect-agent/commit/b8bae94a03ffa5bec9e01a4ea779993512ca19ac), [`301ead3`](https://github.com/danieljvdm/effect-agent/commit/301ead3c88dbd5b6fc40e31e53f11745235d3977), [`84fe655`](https://github.com/danieljvdm/effect-agent/commit/84fe65580c35a91707eda809b7e47d90402179a9), [`c8163c1`](https://github.com/danieljvdm/effect-agent/commit/c8163c14194b256653db98821c58c493bdebe21a)]:
  - @effect-agent/storage-cloudflare@0.1.0-beta.78
  - @effect-agent/thread@0.1.0-beta.78
  - @effect-agent/engine@0.1.0-beta.78
  - @effect-agent/core@0.1.0-beta.78
  - @effect-agent/sandbox@0.1.0-beta.78

## 0.1.0-beta.77

### Minor Changes

- [#421](https://github.com/danieljvdm/effect-agent/pull/421) [`38bc092`](https://github.com/danieljvdm/effect-agent/commit/38bc092ba87e631416b75d0ed4871330c2c40489) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add a WebSearch tool with interchangeable native search backends and bounded, cited results. Route upstream Effect clients through Cloudflare AI Gateway with a pipeable Layer helper for search, model calls, streaming, and supported provider APIs.

### Patch Changes

- Updated dependencies [[`84d6684`](https://github.com/danieljvdm/effect-agent/commit/84d66844e24e7fbdc5dc3f54a5d3a7a6127cdd99)]:
  - @effect-agent/core@0.1.0-beta.77
  - @effect-agent/engine@0.1.0-beta.77
  - @effect-agent/storage-cloudflare@0.1.0-beta.77
  - @effect-agent/thread@0.1.0-beta.77
  - @effect-agent/sandbox@0.1.0-beta.77

## 0.1.0-beta.76

### Patch Changes

- Updated dependencies [[`3eef297`](https://github.com/danieljvdm/effect-agent/commit/3eef297d2343989a830d5d2b88e0b863b54c91fd)]:
  - @effect-agent/core@0.1.0-beta.76
  - @effect-agent/engine@0.1.0-beta.76
  - @effect-agent/thread@0.1.0-beta.76
  - @effect-agent/storage-cloudflare@0.1.0-beta.76
  - @effect-agent/sandbox@0.1.0-beta.76

## 0.1.0-beta.75

### Patch Changes

- [#417](https://github.com/danieljvdm/effect-agent/pull/417) [`230c18a`](https://github.com/danieljvdm/effect-agent/commit/230c18a79fa3941615a6116f5678a1a3bd4b169c) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Allow authorized writes and bounded concurrent tool calls in Code Mode, with individual outcome reports after partial failure or interruption. Classify generated programs as uncertain to prevent automatic replay after ownership loss.

  BEHAVIOR CHANGE: Host Tool authorization now checks inner calls; policies must allow the selected Tools explicitly.

- Updated dependencies [[`230c18a`](https://github.com/danieljvdm/effect-agent/commit/230c18a79fa3941615a6116f5678a1a3bd4b169c), [`1208f7e`](https://github.com/danieljvdm/effect-agent/commit/1208f7e77a348ffd4a9dc0bcb90954b1095e1d4b), [`71afa3d`](https://github.com/danieljvdm/effect-agent/commit/71afa3d64f1cef889b46bea6a352d4e6f8446e32)]:
  - @effect-agent/engine@0.1.0-beta.75
  - @effect-agent/sandbox@0.1.0-beta.75
  - @effect-agent/core@0.1.0-beta.75
  - @effect-agent/thread@0.1.0-beta.75
  - @effect-agent/storage-cloudflare@0.1.0-beta.75

## 0.1.0-beta.74

### Patch Changes

- Updated dependencies [[`cf10ec3`](https://github.com/danieljvdm/effect-agent/commit/cf10ec32e2d94402d417b05358bf96715e8c5401)]:
  - @effect-agent/core@0.1.0-beta.74
  - @effect-agent/engine@0.1.0-beta.74
  - @effect-agent/storage-cloudflare@0.1.0-beta.74
  - @effect-agent/thread@0.1.0-beta.74
  - @effect-agent/sandbox@0.1.0-beta.74

## 0.1.0-beta.73

### Patch Changes

- [#405](https://github.com/danieljvdm/effect-agent/pull/405) [`da6971d`](https://github.com/danieljvdm/effect-agent/commit/da6971d450c7ed73b88c8ae74ac8376aee6c1254) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Wake outgoing delivery immediately when messages are inserted during an active Cloudflare Run. Preserve bounded delivery, durable recovery, and periodic scans when wake notifications are lost.

- Updated dependencies [[`da6971d`](https://github.com/danieljvdm/effect-agent/commit/da6971d450c7ed73b88c8ae74ac8376aee6c1254)]:
  - @effect-agent/core@0.1.0-beta.73
  - @effect-agent/engine@0.1.0-beta.73
  - @effect-agent/storage-cloudflare@0.1.0-beta.73
  - @effect-agent/thread@0.1.0-beta.73
  - @effect-agent/sandbox@0.1.0-beta.73

## 0.1.0-beta.72

### Patch Changes

- Updated dependencies [[`08571ea`](https://github.com/danieljvdm/effect-agent/commit/08571eacf1483fbc0008106e6753138ad75eb011)]:
  - @effect-agent/core@0.1.0-beta.72
  - @effect-agent/engine@0.1.0-beta.72
  - @effect-agent/thread@0.1.0-beta.72
  - @effect-agent/storage-cloudflare@0.1.0-beta.72
  - @effect-agent/sandbox@0.1.0-beta.72

## 0.1.0-beta.71

### Patch Changes

- [#402](https://github.com/danieljvdm/effect-agent/pull/402) [`5ad2c51`](https://github.com/danieljvdm/effect-agent/commit/5ad2c513ec12be198f569c3fe82fdcb48a07c009) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Deliver messages created during an active Cloudflare source Run without waiting for that Run to finish.

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.71
  - @effect-agent/engine@0.1.0-beta.71
  - @effect-agent/sandbox@0.1.0-beta.71
  - @effect-agent/thread@0.1.0-beta.71
  - @effect-agent/storage-cloudflare@0.1.0-beta.71

## 0.1.0-beta.70

### Patch Changes

- Updated dependencies [[`3230353`](https://github.com/danieljvdm/effect-agent/commit/323035380f8296fc731a224f79f2717724b7f889)]:
  - @effect-agent/storage-cloudflare@0.1.0-beta.70
  - @effect-agent/core@0.1.0-beta.70
  - @effect-agent/engine@0.1.0-beta.70
  - @effect-agent/sandbox@0.1.0-beta.70
  - @effect-agent/thread@0.1.0-beta.70

## 0.1.0-beta.69

### Patch Changes

- Updated dependencies [[`e37a126`](https://github.com/danieljvdm/effect-agent/commit/e37a12613f25225c3ae8544dc384f4f7da4adc03), [`f497de2`](https://github.com/danieljvdm/effect-agent/commit/f497de24ad24ba12b2eebf29f4473b4b89f90158), [`9c98161`](https://github.com/danieljvdm/effect-agent/commit/9c98161d5a1026f2dc3d0fb395ea4a0bf6323fdd), [`318b442`](https://github.com/danieljvdm/effect-agent/commit/318b4420c5dcd14cbcd36bdfa9dce5abf53b40ad)]:
  - @effect-agent/core@0.1.0-beta.69
  - @effect-agent/engine@0.1.0-beta.69
  - @effect-agent/thread@0.1.0-beta.69
  - @effect-agent/storage-cloudflare@0.1.0-beta.69
  - @effect-agent/sandbox@0.1.0-beta.69

## 0.1.0-beta.68

### Patch Changes

- Updated dependencies [[`4ca6361`](https://github.com/danieljvdm/effect-agent/commit/4ca6361c2085b5b77d1835c2b61ca1e67d2f8e6c)]:
  - @effect-agent/engine@0.1.0-beta.68
  - @effect-agent/thread@0.1.0-beta.68
  - @effect-agent/storage-cloudflare@0.1.0-beta.68
  - @effect-agent/core@0.1.0-beta.68
  - @effect-agent/sandbox@0.1.0-beta.68

## 0.1.0-beta.67

### Patch Changes

- Updated dependencies [[`8c0fe3b`](https://github.com/danieljvdm/effect-agent/commit/8c0fe3bf4f5a2ff84bd3ae6a44abd18b89f6bc1f)]:
  - @effect-agent/engine@0.1.0-beta.67
  - @effect-agent/thread@0.1.0-beta.67
  - @effect-agent/storage-cloudflare@0.1.0-beta.67
  - @effect-agent/core@0.1.0-beta.67
  - @effect-agent/sandbox@0.1.0-beta.67

## 0.1.0-beta.66

### Patch Changes

- Updated dependencies [[`05f105d`](https://github.com/danieljvdm/effect-agent/commit/05f105dba7ee6ea5d605ef41bb39db913dc08254)]:
  - @effect-agent/thread@0.1.0-beta.66
  - @effect-agent/storage-cloudflare@0.1.0-beta.66
  - @effect-agent/core@0.1.0-beta.66
  - @effect-agent/engine@0.1.0-beta.66
  - @effect-agent/sandbox@0.1.0-beta.66

## 0.1.0-beta.65

### Patch Changes

- Updated dependencies [[`4d8a33a`](https://github.com/danieljvdm/effect-agent/commit/4d8a33a82ebae221bcb62e9cec4d53a0e76f6bd8)]:
  - @effect-agent/thread@0.1.0-beta.65
  - @effect-agent/storage-cloudflare@0.1.0-beta.65
  - @effect-agent/core@0.1.0-beta.65
  - @effect-agent/engine@0.1.0-beta.65
  - @effect-agent/sandbox@0.1.0-beta.65

## 0.1.0-beta.64

### Patch Changes

- [#379](https://github.com/danieljvdm/effect-agent/pull/379) [`a5bcce2`](https://github.com/danieljvdm/effect-agent/commit/a5bcce2bcb8683735284b24dd026391237cf70d9) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Resolve source-authorized background concurrency at the canonical reservation boundary, preserving active steering, retained origins, and retryable authority failures. Include the original owner submission in terminal report authorization and preserve native message recovery when applications rebuild Cloudflare runtime maintenance.

- Updated dependencies [[`620d7d3`](https://github.com/danieljvdm/effect-agent/commit/620d7d38dd94b95c29d2e07a79b445a6fbccd648), [`a5bcce2`](https://github.com/danieljvdm/effect-agent/commit/a5bcce2bcb8683735284b24dd026391237cf70d9)]:
  - @effect-agent/core@0.1.0-beta.64
  - @effect-agent/thread@0.1.0-beta.64
  - @effect-agent/storage-cloudflare@0.1.0-beta.64
  - @effect-agent/engine@0.1.0-beta.64
  - @effect-agent/sandbox@0.1.0-beta.64

## 0.1.0-beta.63

### Patch Changes

- Updated dependencies [[`d0f36bf`](https://github.com/danieljvdm/effect-agent/commit/d0f36bfc21e821fcc34caacf3c39f1e904a5d1c9)]:
  - @effect-agent/engine@0.1.0-beta.63
  - @effect-agent/thread@0.1.0-beta.63
  - @effect-agent/storage-cloudflare@0.1.0-beta.63
  - @effect-agent/core@0.1.0-beta.63
  - @effect-agent/sandbox@0.1.0-beta.63

## 0.1.0-beta.62

### Patch Changes

- Updated dependencies [[`46e8ad2`](https://github.com/danieljvdm/effect-agent/commit/46e8ad22fa9f436ce155a6696ddf8e11cec2931c)]:
  - @effect-agent/engine@0.1.0-beta.62
  - @effect-agent/thread@0.1.0-beta.62
  - @effect-agent/storage-cloudflare@0.1.0-beta.62
  - @effect-agent/core@0.1.0-beta.62
  - @effect-agent/sandbox@0.1.0-beta.62

## 0.1.0-beta.61

### Patch Changes

- Updated dependencies [[`21431ae`](https://github.com/danieljvdm/effect-agent/commit/21431ae6cacd78e6330b1017c2768f4f9c347b7a)]:
  - @effect-agent/core@0.1.0-beta.61
  - @effect-agent/engine@0.1.0-beta.61
  - @effect-agent/thread@0.1.0-beta.61
  - @effect-agent/storage-cloudflare@0.1.0-beta.61
  - @effect-agent/sandbox@0.1.0-beta.61

## 0.1.0-beta.60

### Patch Changes

- [#370](https://github.com/danieljvdm/effect-agent/pull/370) [`bed4e71`](https://github.com/danieljvdm/effect-agent/commit/bed4e7170ab2d9b2ef3fbf3c7c3a8fa16e0d803d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add typed Thread projection maintenance with live committed-batch hooks and bounded background backfill. Share host index services with Cloudflare Tools while preserving canonical commits and independent approval publication.

- Updated dependencies [[`bed4e71`](https://github.com/danieljvdm/effect-agent/commit/bed4e7170ab2d9b2ef3fbf3c7c3a8fa16e0d803d)]:
  - @effect-agent/thread@0.1.0-beta.60
  - @effect-agent/storage-cloudflare@0.1.0-beta.60
  - @effect-agent/core@0.1.0-beta.60
  - @effect-agent/engine@0.1.0-beta.60
  - @effect-agent/sandbox@0.1.0-beta.60

## 0.1.0-beta.59

### Patch Changes

- Updated dependencies [[`cb1d297`](https://github.com/danieljvdm/effect-agent/commit/cb1d297d3464850b5e4645a0d3b3a5062a1ba71b)]:
  - @effect-agent/core@0.1.0-beta.59
  - @effect-agent/engine@0.1.0-beta.59
  - @effect-agent/thread@0.1.0-beta.59
  - @effect-agent/storage-cloudflare@0.1.0-beta.59
  - @effect-agent/sandbox@0.1.0-beta.59

## 0.1.0-beta.58

### Patch Changes

- Updated dependencies [[`daca525`](https://github.com/danieljvdm/effect-agent/commit/daca52585983bb90b6c43a29e4a44a28c8de1743)]:
  - @effect-agent/engine@0.1.0-beta.58
  - @effect-agent/thread@0.1.0-beta.58
  - @effect-agent/storage-cloudflare@0.1.0-beta.58
  - @effect-agent/core@0.1.0-beta.58
  - @effect-agent/sandbox@0.1.0-beta.58

## 0.1.0-beta.57

### Minor Changes

- [#358](https://github.com/danieljvdm/effect-agent/pull/358) [`4ff21e2`](https://github.com/danieljvdm/effect-agent/commit/4ff21e2a4c3735be34955e7d5caf64f623d33f81) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Deliver typed peer messages and worker reports independently of active agent runs, with frozen input, bounded retries, and separate acceptance and processing status. Recover delivery through scoped Node polling and Cloudflare alarms while preserving existing thread data on supported storage upgrades.

### Patch Changes

- Updated dependencies [[`4ff21e2`](https://github.com/danieljvdm/effect-agent/commit/4ff21e2a4c3735be34955e7d5caf64f623d33f81), [`4ff21e2`](https://github.com/danieljvdm/effect-agent/commit/4ff21e2a4c3735be34955e7d5caf64f623d33f81)]:
  - @effect-agent/core@0.1.0-beta.57
  - @effect-agent/engine@0.1.0-beta.57
  - @effect-agent/thread@0.1.0-beta.57
  - @effect-agent/storage-cloudflare@0.1.0-beta.57
  - @effect-agent/sandbox@0.1.0-beta.57

## 0.1.0-beta.56

### Patch Changes

- Updated dependencies [[`fdde35f`](https://github.com/danieljvdm/effect-agent/commit/fdde35f4b837be8acef0dc1badca69bef1a2dd05)]:
  - @effect-agent/engine@0.1.0-beta.56
  - @effect-agent/thread@0.1.0-beta.56
  - @effect-agent/storage-cloudflare@0.1.0-beta.56
  - @effect-agent/core@0.1.0-beta.56
  - @effect-agent/sandbox@0.1.0-beta.56

## 0.1.0-beta.55

### Minor Changes

- [#357](https://github.com/danieljvdm/effect-agent/pull/357) [`2259fc0`](https://github.com/danieljvdm/effect-agent/commit/2259fc05eec3bfac2a92a8d055953f3482e54735) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Resolve model routing and context admission from the same per-turn configuration, and allow trusted hosts to request native rollover. Expose retained-history projections and lexical matching for authorized indexed history adapters, and the existing Thread Object SQL client for optional owner-local repositories.

### Patch Changes

- Updated dependencies [[`2259fc0`](https://github.com/danieljvdm/effect-agent/commit/2259fc05eec3bfac2a92a8d055953f3482e54735)]:
  - @effect-agent/engine@0.1.0-beta.55
  - @effect-agent/thread@0.1.0-beta.55
  - @effect-agent/storage-cloudflare@0.1.0-beta.55
  - @effect-agent/core@0.1.0-beta.55
  - @effect-agent/sandbox@0.1.0-beta.55

## 0.1.0-beta.54

### Patch Changes

- [#353](https://github.com/danieljvdm/effect-agent/pull/353) [`e9c7591`](https://github.com/danieljvdm/effect-agent/commit/e9c75913024f09b74cdbb1bd4f25b15c87acf06e) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Bound Cloudflare progress-wait cancellation and Quick Action response cleanup, and retain cancellation hints for late progress retries. Apply sandbox wall-time limits to configuration, process startup, and execution together.

- Updated dependencies [[`e9c7591`](https://github.com/danieljvdm/effect-agent/commit/e9c75913024f09b74cdbb1bd4f25b15c87acf06e), [`e9c7591`](https://github.com/danieljvdm/effect-agent/commit/e9c75913024f09b74cdbb1bd4f25b15c87acf06e), [`e9c7591`](https://github.com/danieljvdm/effect-agent/commit/e9c75913024f09b74cdbb1bd4f25b15c87acf06e)]:
  - @effect-agent/storage-cloudflare@0.1.0-beta.54
  - @effect-agent/core@0.1.0-beta.54
  - @effect-agent/engine@0.1.0-beta.54
  - @effect-agent/thread@0.1.0-beta.54
  - @effect-agent/sandbox@0.1.0-beta.54

## 0.1.0-beta.53

### Patch Changes

- [#349](https://github.com/danieljvdm/effect-agent/pull/349) [`9ffca15`](https://github.com/danieljvdm/effect-agent/commit/9ffca15f5e2b6d3461fb25bc45f798a8c259ebbf) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add scoped host publication hooks to Thread Object Layers, with durable producer generations and publication deadlines sharing the native maintenance alarm. Drain pending host publications before runtime recovery and Agent work.

- [#350](https://github.com/danieljvdm/effect-agent/pull/350) [`1398ce5`](https://github.com/danieljvdm/effect-agent/commit/1398ce52ba6828a3ae17f1808c545c64b2fc566a) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add protected non-secret form filling, native choice state, billing-address metadata, and explicit host authorization for checkout submission and observation origins. Allow finite interactive browser passes up to one hour while preserving credential gates and private session cleanup.

- [#352](https://github.com/danieljvdm/effect-agent/pull/352) [`1a86ca1`](https://github.com/danieljvdm/effect-agent/commit/1a86ca13b53885e30c6070c3527fb3f0b60f1690) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Expose the native producer gate through Thread Object services so rebuilt runtime and maintenance Layers share in-flight publication activity.

- Updated dependencies [[`d93903e`](https://github.com/danieljvdm/effect-agent/commit/d93903ec923da7a9841b5ab1a72bba5c0a0fb34b), [`6b4839f`](https://github.com/danieljvdm/effect-agent/commit/6b4839f6ab14adcf82c72159152ab5fe2a946f97), [`1398ce5`](https://github.com/danieljvdm/effect-agent/commit/1398ce52ba6828a3ae17f1808c545c64b2fc566a)]:
  - @effect-agent/engine@0.1.0-beta.53
  - @effect-agent/thread@0.1.0-beta.53
  - @effect-agent/storage-cloudflare@0.1.0-beta.53
  - @effect-agent/sandbox@0.1.0-beta.53
  - @effect-agent/core@0.1.0-beta.53

## 0.1.0-beta.52

### Patch Changes

- [#344](https://github.com/danieljvdm/effect-agent/pull/344) [`afe14c9`](https://github.com/danieljvdm/effect-agent/commit/afe14c9839a084af8c8d8f3046d757a157278643) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Allow ancillary alarm callbacks and payload decoders to use native partition subscription services while capturing host dependencies during assembly.

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.52
  - @effect-agent/engine@0.1.0-beta.52
  - @effect-agent/sandbox@0.1.0-beta.52
  - @effect-agent/thread@0.1.0-beta.52
  - @effect-agent/storage-cloudflare@0.1.0-beta.52

## 0.1.0-beta.51

### Patch Changes

- [#341](https://github.com/danieljvdm/effect-agent/pull/341) [`75898ae`](https://github.com/danieljvdm/effect-agent/commit/75898aef60b09945d90bfe5674b5153edb0717eb) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add revisioned subscription management, bounded event retention, and explicit recovery of parked admissions. Fence fresh destination admission by host policy and retain one unsettled submission per optional admission group until canonical settlement.

  BEHAVIOR CHANGE: Reset incompatible development storage and update custom stores for required configuration revisions and retry generations.

- Updated dependencies [[`75898ae`](https://github.com/danieljvdm/effect-agent/commit/75898aef60b09945d90bfe5674b5153edb0717eb)]:
  - @effect-agent/thread@0.1.0-beta.51
  - @effect-agent/storage-cloudflare@0.1.0-beta.51
  - @effect-agent/core@0.1.0-beta.51
  - @effect-agent/engine@0.1.0-beta.51
  - @effect-agent/sandbox@0.1.0-beta.51

## 0.1.0-beta.50

### Patch Changes

- [#336](https://github.com/danieljvdm/effect-agent/pull/336) [`0438a7b`](https://github.com/danieljvdm/effect-agent/commit/0438a7b9c58869a91870d3df44dc163ec790a929) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Compose service-backed callbacks and reconciler Layers through Effect requirements, and finalize subscription and redaction resources per invocation. Preserve the caller's clock during late browser cleanup.

  BEHAVIOR CHANGE: call `toRunThreadOptions(threadId, runId)` with `EphemeralThreads` provided; ephemeral runs now honor a provided `RunToolAuthorization` unless a per-run hook overrides it.

- Updated dependencies [[`207e910`](https://github.com/danieljvdm/effect-agent/commit/207e9108d25f25a204324b0f6217fea89de569cf), [`0438a7b`](https://github.com/danieljvdm/effect-agent/commit/0438a7b9c58869a91870d3df44dc163ec790a929), [`207e910`](https://github.com/danieljvdm/effect-agent/commit/207e9108d25f25a204324b0f6217fea89de569cf)]:
  - @effect-agent/engine@0.1.0-beta.50
  - @effect-agent/thread@0.1.0-beta.50
  - @effect-agent/core@0.1.0-beta.50
  - @effect-agent/storage-cloudflare@0.1.0-beta.50
  - @effect-agent/sandbox@0.1.0-beta.50

## 0.1.0-beta.49

### Patch Changes

- Updated dependencies [[`91ac3bf`](https://github.com/danieljvdm/effect-agent/commit/91ac3bf8cabe1cd7d7851995a3fd714b02db58a0), [`b285e5b`](https://github.com/danieljvdm/effect-agent/commit/b285e5b06a52ac7fc4e3c7fc0ff232650e33857f), [`b285e5b`](https://github.com/danieljvdm/effect-agent/commit/b285e5b06a52ac7fc4e3c7fc0ff232650e33857f), [`b54eea8`](https://github.com/danieljvdm/effect-agent/commit/b54eea8ce9973a1ef2a58ddd6eb87bcc912bec75), [`e3024c0`](https://github.com/danieljvdm/effect-agent/commit/e3024c00673a12b0df79127bcf68176742c51294), [`b54eea8`](https://github.com/danieljvdm/effect-agent/commit/b54eea8ce9973a1ef2a58ddd6eb87bcc912bec75), [`b285e5b`](https://github.com/danieljvdm/effect-agent/commit/b285e5b06a52ac7fc4e3c7fc0ff232650e33857f)]:
  - @effect-agent/engine@0.1.0-beta.49
  - @effect-agent/thread@0.1.0-beta.49
  - @effect-agent/storage-cloudflare@0.1.0-beta.49
  - @effect-agent/core@0.1.0-beta.49
  - @effect-agent/sandbox@0.1.0-beta.49

## 0.1.0-beta.48

### Patch Changes

- [#321](https://github.com/danieljvdm/effect-agent/pull/321) [`e640747`](https://github.com/danieljvdm/effect-agent/commit/e6407479ae233527685928bead040dbfe5153a22) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Stream durable history and bound Cloudflare journal read payloads while removing unused startup modules and temporary encodings. Bound alarm execution time and let durable runs yield between committed turns while preserving recovery and original run budgets.

- Updated dependencies [[`e640747`](https://github.com/danieljvdm/effect-agent/commit/e6407479ae233527685928bead040dbfe5153a22), [`8899bdb`](https://github.com/danieljvdm/effect-agent/commit/8899bdbcbbd16c5b7f9981564939f64729b73015)]:
  - @effect-agent/engine@0.1.0-beta.48
  - @effect-agent/thread@0.1.0-beta.48
  - @effect-agent/storage-cloudflare@0.1.0-beta.48
  - @effect-agent/core@0.1.0-beta.48
  - @effect-agent/sandbox@0.1.0-beta.48

## 0.1.0-beta.47

### Minor Changes

- [#316](https://github.com/danieljvdm/effect-agent/pull/316) [`e6ff3bc`](https://github.com/danieljvdm/effect-agent/commit/e6ff3bcd1b5ce0f2348de668853482ba9d5e126b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Admit remembering durably and process it in a separate host-owned worker with saved proposals, exact command retries, conflict rebase, and source invalidation. Bind the portable checkpoint contract to existing host jobs and retain source references for later cleanup.

### Patch Changes

- Updated dependencies [[`e6ff3bc`](https://github.com/danieljvdm/effect-agent/commit/e6ff3bcd1b5ce0f2348de668853482ba9d5e126b)]:
  - @effect-agent/core@0.1.0-beta.47
  - @effect-agent/engine@0.1.0-beta.47
  - @effect-agent/storage-cloudflare@0.1.0-beta.47
  - @effect-agent/thread@0.1.0-beta.47
  - @effect-agent/sandbox@0.1.0-beta.47

## 0.1.0-beta.46

### Minor Changes

- [#313](https://github.com/danieljvdm/effect-agent/pull/313) [`c1a6e6a`](https://github.com/danieljvdm/effect-agent/commit/c1a6e6a915be73a49b2c266e2df74256f44c25e2) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Import module namespaces from package roots, or import declarations from their explicit PascalCase module paths, following the package map's migration examples. Discard unused modules from audited packages when bundling consumers.
  BEHAVIOR CHANGE: Replace flat declaration imports, lowercase aggregate paths, cross-package aliases, and internal helper imports with their documented owning modules; use `MemoryThreadStoreLive` instead of `MemoryStorageLive`.

### Patch Changes

- Updated dependencies [[`c1a6e6a`](https://github.com/danieljvdm/effect-agent/commit/c1a6e6a915be73a49b2c266e2df74256f44c25e2), [`cebe728`](https://github.com/danieljvdm/effect-agent/commit/cebe728685cf9f45c1d9579273222a865bb8109d)]:
  - @effect-agent/core@0.1.0-beta.46
  - @effect-agent/engine@0.1.0-beta.46
  - @effect-agent/storage-cloudflare@0.1.0-beta.46
  - @effect-agent/sandbox@0.1.0-beta.46
  - @effect-agent/thread@0.1.0-beta.46

## 0.1.0-beta.45

### Minor Changes

- [#309](https://github.com/danieljvdm/effect-agent/pull/309) [`c8812c2`](https://github.com/danieljvdm/effect-agent/commit/c8812c221004bfbeded7a56a03f13102e282f4e0) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Run existing registered agents through an application-supplied Effect Workflow engine with durable dispatch repair. Add SQLite dispatch storage, a scoped Node repair trigger, and bounded durable processing with attempt-scoped ownership release.

  BEHAVIOR CHANGE: Replace `NodeDurableRuntime` and its `Options`, `Config`, `ConfigValue`, `Services`, and `InitializationError` exports with the corresponding `NodeDurableAgentRuntime` names.

  BEHAVIOR CHANGE: Register agents with `DurableAgentRuntime.layerRegistered` or `NodeDurableAgentRuntime.layerRegistered`, and use `layerWithBindings` for precompiled bindings. Call `processThreadResolved(threadId)` and run the `runResolvedWorker` Effect without binding arguments; use `NodeDurableHost.layer` and `ThreadMaintenance.layer` as Layer values over an already-assembled runtime.

  BEHAVIOR CHANGE: Supply `WakeScheduler`, `ToolReconciler`, and `DurableRuntimeFailpoint` when calling `runChaosPlan`, which constructs registered runtimes for its delegation fixtures.

### Patch Changes

- Updated dependencies [[`c8812c2`](https://github.com/danieljvdm/effect-agent/commit/c8812c221004bfbeded7a56a03f13102e282f4e0)]:
  - @effect-agent/thread@0.1.0-beta.45
  - @effect-agent/storage-cloudflare@0.1.0-beta.45
  - @effect-agent/core@0.1.0-beta.45
  - @effect-agent/engine@0.1.0-beta.45
  - @effect-agent/sandbox@0.1.0-beta.45

## 0.1.0-beta.44

### Patch Changes

- [#307](https://github.com/danieljvdm/effect-agent/pull/307) [`f8365ee`](https://github.com/danieljvdm/effect-agent/commit/f8365eee4048076ced0a79b9149efc29297b7c41) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Upgrade to Effect rc.112 and `effect-cf` 0.40.0 while preserving MCP transports and Cloudflare host behavior.

  BEHAVIOR CHANGE: Upgrade Effect and its provider/platform/SQL packages to rc.112 or a compatible version. In Cloudflare hosts, provide `effect-cf@^0.40.0` and enable `nodejs_compat` for its async context support.

- Updated dependencies [[`f8365ee`](https://github.com/danieljvdm/effect-agent/commit/f8365eee4048076ced0a79b9149efc29297b7c41)]:
  - @effect-agent/core@0.1.0-beta.44
  - @effect-agent/engine@0.1.0-beta.44
  - @effect-agent/sandbox@0.1.0-beta.44
  - @effect-agent/thread@0.1.0-beta.44
  - @effect-agent/storage-cloudflare@0.1.0-beta.44

## 0.1.0-beta.43

### Minor Changes

- [#287](https://github.com/danieljvdm/effect-agent/pull/287) [`361c643`](https://github.com/danieljvdm/effect-agent/commit/361c643bfd1ac40095bc1d63d4d84c5a0afbf3d0) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add private login and card autofill with host-owned credential grants, opaque browser references, and explicit post-use recipient trust. Allow Agent Registrations to supply fresh scoped services for each durable Attempt.

### Patch Changes

- Updated dependencies [[`361c643`](https://github.com/danieljvdm/effect-agent/commit/361c643bfd1ac40095bc1d63d4d84c5a0afbf3d0)]:
  - @effect-agent/sandbox@0.1.0-beta.43
  - @effect-agent/thread@0.1.0-beta.43
  - @effect-agent/storage-cloudflare@0.1.0-beta.43
  - @effect-agent/core@0.1.0-beta.43
  - @effect-agent/engine@0.1.0-beta.43

## 0.1.0-beta.42

### Patch Changes

- [#290](https://github.com/danieljvdm/effect-agent/pull/290) [`8323d0d`](https://github.com/danieljvdm/effect-agent/commit/8323d0db6fe882e4c2a84e4a14c20ff871257072) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Export `layerFromBindings` from the package root to assemble a Cloudflare durable runtime from resolved Agent Bindings.

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.42
  - @effect-agent/engine@0.1.0-beta.42
  - @effect-agent/sandbox@0.1.0-beta.42
  - @effect-agent/thread@0.1.0-beta.42
  - @effect-agent/storage-cloudflare@0.1.0-beta.42

## 0.1.0-beta.41

### Minor Changes

- [#284](https://github.com/danieljvdm/effect-agent/pull/284) [`e21d6da`](https://github.com/danieljvdm/effect-agent/commit/e21d6da596b97c98ace533c3fa42fe9767d127e1) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add optional namespace-owned Cloudflare memory with bounded batch recall, authoritative semantic-candidate validation, and durable conditional writes shared across Threads. Limit semantic recall output with `maxOutputBytes`, counting repeated attribution and metadata.

  BEHAVIOR CHANGE: Construct access and document scopes with `MemoryScope.make` or decode them with its Schema; Cloudflare memory clients require the existing branded `Principal`, capped at 256 characters.

  BEHAVIOR CHANGE: Replace `recallMemory` with `Memory.recall` for multi-source composition, or use `client.recall(candidates, limits)` for a bound Cloudflare memory client. The old function is removed without an alias.

### Patch Changes

- Updated dependencies [[`e21d6da`](https://github.com/danieljvdm/effect-agent/commit/e21d6da596b97c98ace533c3fa42fe9767d127e1), [`edfa7dc`](https://github.com/danieljvdm/effect-agent/commit/edfa7dc6693dea2a84366f5053826ffa87f7c587)]:
  - @effect-agent/core@0.1.0-beta.41
  - @effect-agent/thread@0.1.0-beta.41
  - @effect-agent/storage-cloudflare@0.1.0-beta.41
  - @effect-agent/engine@0.1.0-beta.41
  - @effect-agent/sandbox@0.1.0-beta.41

## 0.1.0-beta.40

### Patch Changes

- [#280](https://github.com/danieljvdm/effect-agent/pull/280) [`614a81d`](https://github.com/danieljvdm/effect-agent/commit/614a81db4f9d121ac209cc56fc6d420f43f4ab1b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Compose WebCapture tool handlers and a Worker browser binding with `CloudflareBrowser.layer(ReadPage, { browser: env.BROWSER })`. Pass an explicit `workersAi` authorization and accounting policy to enable structured extraction.

- [#280](https://github.com/danieljvdm/effect-agent/pull/280) [`614a81d`](https://github.com/danieljvdm/effect-agent/commit/614a81db4f9d121ac209cc56fc6d420f43f4ab1b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Assemble Cloudflare Code Mode, REST capture tools, interactive browsers, and thread clients with platform-owned Layer constructors. Register Node agents through `NodeDurableHost.layerRegistered(registrations, options)` while preserving application service requirements and host-scoped cleanup.

- Updated dependencies [[`720e6d9`](https://github.com/danieljvdm/effect-agent/commit/720e6d952cf14cf61a6550c01473938fd46a1e74), [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7), [`720e6d9`](https://github.com/danieljvdm/effect-agent/commit/720e6d952cf14cf61a6550c01473938fd46a1e74), [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7), [`1432833`](https://github.com/danieljvdm/effect-agent/commit/14328336cd3480c5ddda8447f522591eb99eaaeb), [`c36fe73`](https://github.com/danieljvdm/effect-agent/commit/c36fe73d2d226f9271c6dd60071159b0d82862ae), [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7), [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7), [`0fbcbbf`](https://github.com/danieljvdm/effect-agent/commit/0fbcbbf3c8c2ca7595543e545baddb0c6f965436)]:
  - @effect-agent/thread@0.1.0-beta.40
  - @effect-agent/core@0.1.0-beta.40
  - @effect-agent/engine@0.1.0-beta.40
  - @effect-agent/storage-cloudflare@0.1.0-beta.40
  - @effect-agent/sandbox@0.1.0-beta.40

## 0.1.0-beta.39

### Minor Changes

- [#241](https://github.com/danieljvdm/effect-agent/pull/241) [`dd85dc0`](https://github.com/danieljvdm/effect-agent/commit/dd85dc07e2513e2ec56316fd7609e137d6c3f6fa) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add durable once and continuous event subscriptions that deliver Schema-defined input through ordinary Thread admission. Provide owner-scoped management Tools and a GitHub workflow run completion source with missed-webhook reconciliation.

  BEHAVIOR CHANGE: Reset incompatible private-development SQLite databases before opening them with storage version 6.

- [#249](https://github.com/danieljvdm/effect-agent/pull/249) [`f8de2d8`](https://github.com/danieljvdm/effect-agent/commit/f8de2d8a022e81eac9c357b361dd567fb65ac239) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Import specialized testing utilities and fixtures from their documented subpaths, and use failpoint controls from `/testing` with `TestControl.layer` in place of `Failpoint.layerTest`; keep migration loaders internal.
  Import Browser Run adapters from their dedicated Cloudflare subpaths and install `@cloudflare/puppeteer` explicitly when using `/interactive-browser`.

- [#263](https://github.com/danieljvdm/effect-agent/pull/263) [`95865d7`](https://github.com/danieljvdm/effect-agent/commit/95865d78f55546d42f562f2f13509bbfc198c091) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Rename `@effect-agent/session` to `@effect-agent/thread` and rename the Conversation framework API to Thread.

  BEHAVIOR CHANGE: Rename Conversation identifiers, fields, record families and tags, and the durable-admin `--conversation` selector to their Thread equivalents. Reset incompatible alpha storage before upgrading.

- [#241](https://github.com/danieljvdm/effect-agent/pull/241) [`dd85dc0`](https://github.com/danieljvdm/effect-agent/commit/dd85dc07e2513e2ec56316fd7609e137d6c3f6fa) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Bind subscription input preparation to each destination Agent's retained definition version, authorize reconciliation through explicit host policy, and preserve newer delivery retry state.

  BEHAVIOR CHANGE: Provide `SubscriptionInputBindings` and `SubscriptionAuthorizer.reconcile` in subscription hosts, and import GitHub integration from `@effect-agent/thread/github`.

### Patch Changes

- [#260](https://github.com/danieljvdm/effect-agent/pull/260) [`e6d05f5`](https://github.com/danieljvdm/effect-agent/commit/e6d05f51783035cec4f99247de2f064e730770ca) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Compose Cloudflare Thread Objects from application Layers and typed Agent version declarations, preserving initialization failures and scoped dependencies. Resolve durable work from explicit exact-version bindings and reject digest-transparent registrations.

  BEHAVIOR CHANGE: Replace `makeConversationObjectClass` with `ThreadObject.make`. Pass a composed `ThreadObject.layer(registrations)` to `ThreadObject.make`, move preparation and Tool authorization into Layers, and use `options.eventLayer` for observability. Pass bindings directly to resolved worker methods and `NodeDurableHost.layer(bindings)` instead of providing `AgentBindingResolver`.

- [#262](https://github.com/danieljvdm/effect-agent/pull/262) [`34ca82e`](https://github.com/danieljvdm/effect-agent/commit/34ca82e86191bc85229bd32886b8cfaf9a2edce9) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Enforce durable child tool-call allowances across recovery and distinguish passing checks from complete adapter certification. Rename the custom durable assembly to `layerWithServices` and preserve Node extension-layer construction errors and dependencies.

  BEHAVIOR CHANGE: Replace `DurableAgentRuntime.layerWithContext` with `layerWithServices`, still supplying both separate services. Regenerate certification reports with the `effect-agent/certification@2` schema and use `fullyCertified` for gates requiring executed real-loss checks; `ok` retains its executed-check meaning. Existing child records without an allowance keep their original definition policy; start a new delegation to apply a limit.

- [#248](https://github.com/danieljvdm/effect-agent/pull/248) [`f4f37c3`](https://github.com/danieljvdm/effect-agent/commit/f4f37c37fa1b650341c6e18ee3a22cd6f518bfd2) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Compose prompt preparation and Tool authorization independently in durable hosts, preserving both across recovery.

  BEHAVIOR CHANGE: move `RunContextPreparation.toolAuthorization` to a separate `RunToolAuthorization` Layer and provide both services to `DurableAgentRuntime.layerWithServices`.

- [#243](https://github.com/danieljvdm/effect-agent/pull/243) [`e0aa7d9`](https://github.com/danieljvdm/effect-agent/commit/e0aa7d9442ca2ec62df8195a2f9cce7b52af5257) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Preserve Run limits across durable recovery, require explicit delegation replay authority, and reject unusable compaction summaries. Authorize settlement waits and aborts through the runtime authorizer and reject settlement Receipts whose Submission belongs to another Thread.

  BEHAVIOR CHANGE: Reset private-development histories whose RunStarted records predate policy accounting version 1 before resuming them.

- [#256](https://github.com/danieljvdm/effect-agent/pull/256) [`ac70e21`](https://github.com/danieljvdm/effect-agent/commit/ac70e212c7d9741ce48bd9b2a4dbd355f9dac72e) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Declare `effect` as a required `^4.0.0-rc.111` peer across all public packages so they share the application's runtime and accept compatible upgrades. Keep `effect` in application dependencies at a version satisfying the framework's and providers' peer ranges.

- Updated dependencies [[`e6d05f5`](https://github.com/danieljvdm/effect-agent/commit/e6d05f51783035cec4f99247de2f064e730770ca), [`34ca82e`](https://github.com/danieljvdm/effect-agent/commit/34ca82e86191bc85229bd32886b8cfaf9a2edce9), [`dd85dc0`](https://github.com/danieljvdm/effect-agent/commit/dd85dc07e2513e2ec56316fd7609e137d6c3f6fa), [`e0aa7d9`](https://github.com/danieljvdm/effect-agent/commit/e0aa7d9442ca2ec62df8195a2f9cce7b52af5257), [`f4f37c3`](https://github.com/danieljvdm/effect-agent/commit/f4f37c37fa1b650341c6e18ee3a22cd6f518bfd2), [`e0aa7d9`](https://github.com/danieljvdm/effect-agent/commit/e0aa7d9442ca2ec62df8195a2f9cce7b52af5257), [`7bab6c0`](https://github.com/danieljvdm/effect-agent/commit/7bab6c053b01398a0f1898374103997da6550268), [`f8de2d8`](https://github.com/danieljvdm/effect-agent/commit/f8de2d8a022e81eac9c357b361dd567fb65ac239), [`0d88d90`](https://github.com/danieljvdm/effect-agent/commit/0d88d90443e7d35e34799f4458d274fde99e0859), [`79fbd8b`](https://github.com/danieljvdm/effect-agent/commit/79fbd8b755434a162629a534478e188636d186fe), [`4c458e4`](https://github.com/danieljvdm/effect-agent/commit/4c458e43738bb243d1e343c97ecfd49e3b41ca9f), [`95865d7`](https://github.com/danieljvdm/effect-agent/commit/95865d78f55546d42f562f2f13509bbfc198c091), [`655bf5f`](https://github.com/danieljvdm/effect-agent/commit/655bf5f217dce1865c97ce613246c27846bfaf8a), [`d004a36`](https://github.com/danieljvdm/effect-agent/commit/d004a361518c23cdc81f1768e5ab31560e014935), [`ac70e21`](https://github.com/danieljvdm/effect-agent/commit/ac70e212c7d9741ce48bd9b2a4dbd355f9dac72e), [`dd85dc0`](https://github.com/danieljvdm/effect-agent/commit/dd85dc07e2513e2ec56316fd7609e137d6c3f6fa), [`dd85dc0`](https://github.com/danieljvdm/effect-agent/commit/dd85dc07e2513e2ec56316fd7609e137d6c3f6fa), [`511c852`](https://github.com/danieljvdm/effect-agent/commit/511c85212a564ff2729de401620fcbdeddcb4748)]:
  - @effect-agent/thread@0.1.0-beta.39
  - @effect-agent/engine@0.1.0-beta.39
  - @effect-agent/storage-cloudflare@0.1.0-beta.39
  - @effect-agent/core@0.1.0-beta.39
  - @effect-agent/sandbox@0.1.0-beta.39

## 0.1.0-beta.38

### Patch Changes

- [#222](https://github.com/danieljvdm/effect-agent/pull/222) [`22ee09d`](https://github.com/danieljvdm/effect-agent/commit/22ee09d279bf0561ef7eb96e3f80f4b29481a71c) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Confirm exact Browser Run session termination before reporting cleanup success, including already-absent sessions. Provide `BrowserRunSessionLifecycle.layer({ accountId, apiToken })` with an account-scoped Browser Rendering Write token when constructing the interactive binding.

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.38
  - @effect-agent/engine@0.1.0-beta.38
  - @effect-agent/sandbox@0.1.0-beta.38
  - @effect-agent/session@0.1.0-beta.38
  - @effect-agent/storage-cloudflare@0.1.0-beta.38

## 0.1.0-beta.37

### Minor Changes

- [#212](https://github.com/danieljvdm/effect-agent/pull/212) [`242b601`](https://github.com/danieljvdm/effect-agent/commit/242b601c6d14c3448c2a3acdc28b97b48e27cf92) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add durable schedules for typed Agent input with owner authorization, one-shot, interval and cron timing, and recovery through ordinary Submission admission on Node and Cloudflare.

  BEHAVIOR CHANGE: Reset older private-development SQLite databases for storage version 5, and provide `effect-cf ^0.37.0` to Cloudflare hosts.

- [#219](https://github.com/danieljvdm/effect-agent/pull/219) [`d713c28`](https://github.com/danieljvdm/effect-agent/commit/d713c28f5b7537e133e063e5df0207ecd4046856) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Expose a validated launch viewport and host-only session resizing without spending the agent action budget.

  BEHAVIOR CHANGE: Handle `InteractiveBrowserPolicyDeniedError` when building `BrowserRunInteractiveBinding.layer` with invalid viewport configuration.

- [#218](https://github.com/danieljvdm/effect-agent/pull/218) [`b43cf38`](https://github.com/danieljvdm/effect-agent/commit/b43cf38093f716cefc998241183ca2059ee83fe0) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Separate scheduling management from driver authority, expose explicit public status, and fix DST delivery, failed-record starvation, and repeated resume. Allow positive host interval minimums and release operational capacity when schedules finish while retaining creation replay guarantees.

  BEHAVIOR CHANGE: Cloudflare consumers yield `Scheduling` from `CloudflareSchedulingClient.layer`; local drivers use `ScheduleDriver.layer`. Status omits persisted input and admission internals, and `dueBatchSize` bounds a query page within a sweep.

### Patch Changes

- [#220](https://github.com/danieljvdm/effect-agent/pull/220) [`5f83df4`](https://github.com/danieljvdm/effect-agent/commit/5f83df46d392b1d61e39cb2c74d9eebf36c52415) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Expose bounded JSON page text and form-control observations, including native option selection, and wait for DOM-ready navigation with a 30-second timeout. Require unique click/fill targets, export `isBrowserRunUndispatchedActionError`, and observe bounded fetch/XHR settlement while preserving uncertainty and cleanup on interruption.

- Updated dependencies [[`242b601`](https://github.com/danieljvdm/effect-agent/commit/242b601c6d14c3448c2a3acdc28b97b48e27cf92), [`bd48a7b`](https://github.com/danieljvdm/effect-agent/commit/bd48a7b200fb71335b19edd7941be331b6ede9ea), [`bd48a7b`](https://github.com/danieljvdm/effect-agent/commit/bd48a7b200fb71335b19edd7941be331b6ede9ea), [`b43cf38`](https://github.com/danieljvdm/effect-agent/commit/b43cf38093f716cefc998241183ca2059ee83fe0)]:
  - @effect-agent/session@0.1.0-beta.37
  - @effect-agent/storage-cloudflare@0.1.0-beta.37
  - @effect-agent/engine@0.1.0-beta.37
  - @effect-agent/core@0.1.0-beta.37
  - @effect-agent/sandbox@0.1.0-beta.37

## 0.1.0-beta.36

### Patch Changes

- [#214](https://github.com/danieljvdm/effect-agent/pull/214) [`082c258`](https://github.com/danieljvdm/effect-agent/commit/082c2584573c1ffbfa7d5b7166f4243e996816eb) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Allow durably authorized aborts to settle unknown submissions and release queued followers without replaying uncertain tools. Quiesce Cloudflare maintenance for ready followers behind an unresolved external wait.

- Updated dependencies [[`082c258`](https://github.com/danieljvdm/effect-agent/commit/082c2584573c1ffbfa7d5b7166f4243e996816eb)]:
  - @effect-agent/session@0.1.0-beta.36
  - @effect-agent/storage-cloudflare@0.1.0-beta.36
  - @effect-agent/core@0.1.0-beta.36
  - @effect-agent/engine@0.1.0-beta.36
  - @effect-agent/sandbox@0.1.0-beta.36

## 0.1.0-beta.35

### Patch Changes

- [#208](https://github.com/danieljvdm/effect-agent/pull/208) [`065c455`](https://github.com/danieljvdm/effect-agent/commit/065c455d1277f73157f610429de283f41ec83d9c) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add an explicit interactive browser network policy and reject `PublicWeb` with a typed unsupported error before Cloudflare launches a browser.

  BEHAVIOR CHANGE: Move `allowedHosts` into `network: { _tag: "ExactHosts", allowedHosts }` for existing page-request allowlist workflows; `PublicWeb` remains unsupported on Cloudflare.

- [#210](https://github.com/danieljvdm/effect-agent/pull/210) [`06d4f88`](https://github.com/danieljvdm/effect-agent/commit/06d4f88c78ad175bb7e4106d53e01a2c6076ebdc) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add an explicit `Unrestricted` interactive browser policy for arbitrary-site browsing without URL/host or private-network containment guarantees, retaining session limits and host controls. Admit credential-free HTTP and HTTPS interactive navigation and URL observations without changing PageCapture contracts.

- Updated dependencies [[`065c455`](https://github.com/danieljvdm/effect-agent/commit/065c455d1277f73157f610429de283f41ec83d9c), [`06d4f88`](https://github.com/danieljvdm/effect-agent/commit/06d4f88c78ad175bb7e4106d53e01a2c6076ebdc)]:
  - @effect-agent/sandbox@0.1.0-beta.35
  - @effect-agent/storage-cloudflare@0.1.0-beta.35
  - @effect-agent/core@0.1.0-beta.35
  - @effect-agent/engine@0.1.0-beta.35
  - @effect-agent/session@0.1.0-beta.35

## 0.1.0-beta.34

### Minor Changes

- [#205](https://github.com/danieljvdm/effect-agent/pull/205) [`baecd08`](https://github.com/danieljvdm/effect-agent/commit/baecd08f1d6f2c0698e16487cdcccf2f6ffcebca) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add opt-in native Conversation RPC tracing with binding/method client spans, transient current-span propagation, and typed receiver invocation hooks. Remove routine storage codec, failpoint-wrapper, and engine identifier-helper spans while preserving validation, failures, and I/O tracing.

  BEHAVIOR CHANGE: Upgrade the host's `effect-cf` dependency to `^0.34.0` for the native tracing contract.

- [#206](https://github.com/danieljvdm/effect-agent/pull/206) [`aa3ebfb`](https://github.com/danieljvdm/effect-agent/commit/aa3ebfb4fd1e69be77c433a881ddecb3567c36c2) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Expose non-propagating Tool failures to an opt-in trusted local observer, preserving live Causes without automatic export. Install the same observer through durable Node and Cloudflare runtime options while excluding settled-call replay.

### Patch Changes

- [#202](https://github.com/danieljvdm/effect-agent/pull/202) [`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align the Effect family with rc.111 to decode nested OpenAI error events, and preserve transformed Tool parameters under its encoded response contract.

- Updated dependencies [[`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee), [`baecd08`](https://github.com/danieljvdm/effect-agent/commit/baecd08f1d6f2c0698e16487cdcccf2f6ffcebca), [`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee), [`baecd08`](https://github.com/danieljvdm/effect-agent/commit/baecd08f1d6f2c0698e16487cdcccf2f6ffcebca), [`aa3ebfb`](https://github.com/danieljvdm/effect-agent/commit/aa3ebfb4fd1e69be77c433a881ddecb3567c36c2)]:
  - @effect-agent/engine@0.1.0-beta.34
  - @effect-agent/session@0.1.0-beta.34
  - @effect-agent/core@0.1.0-beta.34
  - @effect-agent/sandbox@0.1.0-beta.34
  - @effect-agent/storage-cloudflare@0.1.0-beta.34

## 0.1.0-beta.33

### Patch Changes

- [#196](https://github.com/danieljvdm/effect-agent/pull/196) [`2aa8713`](https://github.com/danieljvdm/effect-agent/commit/2aa8713d943e20faedfae029551b6faa2f8b08d4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Update controlled React form state when filling interactive browser fields.

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.33
  - @effect-agent/engine@0.1.0-beta.33
  - @effect-agent/sandbox@0.1.0-beta.33
  - @effect-agent/session@0.1.0-beta.33
  - @effect-agent/storage-cloudflare@0.1.0-beta.33

## 0.1.0-beta.32

### Minor Changes

- [#194](https://github.com/danieljvdm/effect-agent/pull/194) [`7592ded`](https://github.com/danieljvdm/effect-agent/commit/7592deda757e0eeb0243f86bae9c2b15623e3c76) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add same-session PNG screenshots, viewport scrolling, and explicit closure to interactive browser handles. Expose host-only Cloudflare Live View, handoff, and cleanup through redacted session identities.

  BEHAVIOR CHANGE: Custom browser adapters must implement `screenshot`, `scroll`, and the `close` Effect.

### Patch Changes

- Updated dependencies [[`7592ded`](https://github.com/danieljvdm/effect-agent/commit/7592deda757e0eeb0243f86bae9c2b15623e3c76)]:
  - @effect-agent/sandbox@0.1.0-beta.32
  - @effect-agent/storage-cloudflare@0.1.0-beta.32
  - @effect-agent/core@0.1.0-beta.32
  - @effect-agent/engine@0.1.0-beta.32
  - @effect-agent/session@0.1.0-beta.32

## 0.1.0-beta.31

### Patch Changes

- [#183](https://github.com/danieljvdm/effect-agent/pull/183) [`d3c42d4`](https://github.com/danieljvdm/effect-agent/commit/d3c42d4e34f27610845863ec29908cd3fce95188) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add bounded selector scrape to `PageCapture`, `WebCapture.makeScrape`, and the Cloudflare binding and REST adapters.

- Updated dependencies [[`d3c42d4`](https://github.com/danieljvdm/effect-agent/commit/d3c42d4e34f27610845863ec29908cd3fce95188)]:
  - @effect-agent/sandbox@0.1.0-beta.31
  - @effect-agent/storage-cloudflare@0.1.0-beta.31
  - @effect-agent/core@0.1.0-beta.31
  - @effect-agent/engine@0.1.0-beta.31
  - @effect-agent/session@0.1.0-beta.31

## 0.1.0-beta.30

### Patch Changes

- [#172](https://github.com/danieljvdm/effect-agent/pull/172) [`d123424`](https://github.com/danieljvdm/effect-agent/commit/d123424be7679cfe1b8d133d0d2aa1497e087590) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add a bounded `PageScreenshot` PNG port and the native Cloudflare Browser Run Quick Action Layer.
  Screenshot bytes remain caller-owned and are never persisted or projected by the framework.

- [#172](https://github.com/danieljvdm/effect-agent/pull/172) [`d123424`](https://github.com/danieljvdm/effect-agent/commit/d123424be7679cfe1b8d133d0d2aa1497e087590) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add a Node-safe Cloudflare Browser Run REST PageCapture Layer with explicit Chromium and Kitesurf selection.

- [#172](https://github.com/danieljvdm/effect-agent/pull/172) [`d123424`](https://github.com/danieljvdm/effect-agent/commit/d123424be7679cfe1b8d133d0d2aa1497e087590) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add a bounded same-host `PageCrawl` stream and a Cloudflare Browser Run REST adapter with scoped
  remote-job cleanup.

- [#172](https://github.com/danieljvdm/effect-agent/pull/172) [`d123424`](https://github.com/danieljvdm/effect-agent/commit/d123424be7679cfe1b8d133d0d2aa1497e087590) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add a scoped, provider-neutral `InteractiveBrowser` contract for bounded navigation and interaction, with typed busy, limit, capacity, expiry, and uncertain-execution semantics.

  Document the Cloudflare Browser Run Puppeteer adapter boundary and opt-in Worker proof requirements.

- Updated dependencies [[`d123424`](https://github.com/danieljvdm/effect-agent/commit/d123424be7679cfe1b8d133d0d2aa1497e087590), [`d123424`](https://github.com/danieljvdm/effect-agent/commit/d123424be7679cfe1b8d133d0d2aa1497e087590), [`d123424`](https://github.com/danieljvdm/effect-agent/commit/d123424be7679cfe1b8d133d0d2aa1497e087590)]:
  - @effect-agent/sandbox@0.1.0-beta.30
  - @effect-agent/storage-cloudflare@0.1.0-beta.30
  - @effect-agent/core@0.1.0-beta.30
  - @effect-agent/engine@0.1.0-beta.30
  - @effect-agent/session@0.1.0-beta.30

## 0.1.0-beta.29

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.29
  - @effect-agent/engine@0.1.0-beta.29
  - @effect-agent/sandbox@0.1.0-beta.29
  - @effect-agent/session@0.1.0-beta.29
  - @effect-agent/storage-cloudflare@0.1.0-beta.29

## 0.1.0-beta.28

### Patch Changes

- Updated dependencies [[`374771d`](https://github.com/danieljvdm/effect-agent/commit/374771d90afa26ce7e1832f76715aa7b9eea3741)]:
  - @effect-agent/engine@0.1.0-beta.28
  - @effect-agent/session@0.1.0-beta.28
  - @effect-agent/storage-cloudflare@0.1.0-beta.28
  - @effect-agent/core@0.1.0-beta.28
  - @effect-agent/sandbox@0.1.0-beta.28

## 0.1.0-beta.27

### Minor Changes

- [#155](https://github.com/danieljvdm/effect-agent/pull/155) [`773264b`](https://github.com/danieljvdm/effect-agent/commit/773264b75759c4456e1e549d2172bbe39610a8c1) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add crash-safe terminal delivery Tools and final model responses, completion-capacity reservation, Run-scoped prompt provenance, and target-aware compaction.
  Persist priced per-call model usage in the DN and DC assemblies and expose aggregate usage on Run settlements.

### Patch Changes

- [#148](https://github.com/danieljvdm/effect-agent/pull/148) [`47e9a53`](https://github.com/danieljvdm/effect-agent/commit/47e9a53d99555af3b0ac993b5c9c55ad266e327b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add the schema-first `PageCapture` port and conservative `WebCapture.make`/`WebCapture.makeExtract` Tools over an immutable, deny-by-default browser-request allowlist. Add native `BrowserRun` Quick Action Layers with bounded response streaming and a typed Workers AI authorization and accounting failure for structured extraction.

  ```ts
  const readDocs = WebCapture.make("read_webpage", {
    description: "Read documentation pages.",
    urls: ["docs.example.com", "*.effect.website"],
  });
  // worker: browserQuickActionCaptureLayer().pipe(
  //   Layer.provide(BrowserQuickActionBrowserBinding.layer({ browser: env.BROWSER })),
  // )
  ```

- Updated dependencies [[`47e9a53`](https://github.com/danieljvdm/effect-agent/commit/47e9a53d99555af3b0ac993b5c9c55ad266e327b), [`773264b`](https://github.com/danieljvdm/effect-agent/commit/773264b75759c4456e1e549d2172bbe39610a8c1)]:
  - @effect-agent/sandbox@0.1.0-beta.27
  - @effect-agent/core@0.1.0-beta.27
  - @effect-agent/engine@0.1.0-beta.27
  - @effect-agent/session@0.1.0-beta.27
  - @effect-agent/storage-cloudflare@0.1.0-beta.27

## 0.1.0-beta.26

### Patch Changes

- [#146](https://github.com/danieljvdm/effect-agent/pull/146) [`02311ad`](https://github.com/danieljvdm/effect-agent/commit/02311ad49b6982a15525b8be3f9252536a77be8a) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Route Dynamic Worker host calls through a pass-scoped RPC target owned by the caller's event context.
  Remove the application-provided Code Mode host entrypoint binding.
- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.26
  - @effect-agent/engine@0.1.0-beta.26
  - @effect-agent/sandbox@0.1.0-beta.26
  - @effect-agent/session@0.1.0-beta.26
  - @effect-agent/storage-cloudflare@0.1.0-beta.26

## 0.1.0-beta.25

### Minor Changes

- [#140](https://github.com/danieljvdm/effect-agent/pull/140) [`eb9c5fd`](https://github.com/danieljvdm/effect-agent/commit/eb9c5fd4683a63807b131f8c8d94e9c1205bd36d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Remove deprecated pull-request review outputs and aliases, legacy review-state decoding, and unused
  Travel Planner fixtures. Require Cloudflare worker bindings to use the per-incarnation callback.

### Patch Changes

- [#142](https://github.com/danieljvdm/effect-agent/pull/142) [`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Validate Node runtime configuration before opening SQLite. Keep Cloudflare RPC failures typed when foreign diagnostics are hostile, and close both Dynamic Worker RPC handles when a pass ends.

- Updated dependencies [[`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d), [`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d), [`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d), [`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d), [`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d)]:
  - @effect-agent/engine@0.1.0-beta.25
  - @effect-agent/sandbox@0.1.0-beta.25
  - @effect-agent/session@0.1.0-beta.25
  - @effect-agent/storage-cloudflare@0.1.0-beta.25
  - @effect-agent/core@0.1.0-beta.25

## 0.1.0-beta.24

### Patch Changes

- Updated dependencies [[`6e3f56f`](https://github.com/danieljvdm/effect-agent/commit/6e3f56fbadd831372124578b027ea2bd5ff8f008)]:
  - @effect-agent/session@0.1.0-beta.24
  - @effect-agent/storage-cloudflare@0.1.0-beta.24
  - @effect-agent/core@0.1.0-beta.24
  - @effect-agent/engine@0.1.0-beta.24
  - @effect-agent/sandbox@0.1.0-beta.24

## 0.1.0-beta.23

### Patch Changes

- [#133](https://github.com/danieljvdm/effect-agent/pull/133) [`b130876`](https://github.com/danieljvdm/effect-agent/commit/b130876477ff39349e0d8249298cc0589d284540) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Run Dynamic Worker Code Mode host calls on a Scope-owned pass fiber so they inherit the `execute` Context and die with the pass.

  BEHAVIOR CHANGE: `CodeExecutionHost.call` now sees services provided to `execute` instead of Effect defaults from a `runFork` root.

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.23
  - @effect-agent/engine@0.1.0-beta.23
  - @effect-agent/sandbox@0.1.0-beta.23
  - @effect-agent/session@0.1.0-beta.23
  - @effect-agent/storage-cloudflare@0.1.0-beta.23

## 0.1.0-beta.22

### Minor Changes

- [#124](https://github.com/danieljvdm/effect-agent/pull/124) [`ce8b39c`](https://github.com/danieljvdm/effect-agent/commit/ce8b39ce8f716c0a11c6394d136b67cb9be84588) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Authorize every still-executable model-declared call in a fresh or resumed application Tool batch
  through a host-supplied Run option before durable preparation or Handler execution. Settle denied
  accepted work with a typed failure while preserving canonical Run, Turn, input, and Tool Call
  identity across recovery.

### Patch Changes

- Updated dependencies [[`ce8b39c`](https://github.com/danieljvdm/effect-agent/commit/ce8b39ce8f716c0a11c6394d136b67cb9be84588)]:
  - @effect-agent/core@0.1.0-beta.22
  - @effect-agent/engine@0.1.0-beta.22
  - @effect-agent/session@0.1.0-beta.22
  - @effect-agent/storage-cloudflare@0.1.0-beta.22
  - @effect-agent/sandbox@0.1.0-beta.22

## 0.1.0-beta.21

### Patch Changes

- Updated dependencies []:
  - @effect-agent/storage-cloudflare@0.1.0-beta.21
  - @effect-agent/core@0.1.0-beta.21
  - @effect-agent/engine@0.1.0-beta.21
  - @effect-agent/sandbox@0.1.0-beta.21
  - @effect-agent/session@0.1.0-beta.21

## 0.1.0-beta.20

### Patch Changes

- [#116](https://github.com/danieljvdm/effect-agent/pull/116) [`11a6562`](https://github.com/danieljvdm/effect-agent/commit/11a65620f330736e92931f100618c797437b0ca4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Stop requesting the `allowExperimental` Worker Loader option for Code Mode dynamic workers — it made every pass fail to load unless the calling worker had the `experimental` compatibility flag, which deployed Workers cannot set.

- [#111](https://github.com/danieljvdm/effect-agent/pull/111) [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align every public package with the Effect 4.0.0-rc.110 family.

- [#111](https://github.com/danieljvdm/effect-agent/pull/111) [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Fix `validateMcpDiscovery` reporting a permanent schema drift for MCP tools whose parameters or success type is a named, refined Schema (a branded ID, a bounded string, a `Schema.Class`) — both schema derivations now resolve a top-level `$ref` before comparison.

- Updated dependencies [[`7c093ec`](https://github.com/danieljvdm/effect-agent/commit/7c093ecfd900a0c55163fce76b0609d04434fa73), [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4), [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4)]:
  - @effect-agent/session@0.1.0-beta.20
  - @effect-agent/core@0.1.0-beta.20
  - @effect-agent/engine@0.1.0-beta.20
  - @effect-agent/sandbox@0.1.0-beta.20
  - @effect-agent/storage-cloudflare@0.1.0-beta.20

## 0.1.0-beta.19

### Minor Changes

- [#106](https://github.com/danieljvdm/effect-agent/pull/106) [`9e31de4`](https://github.com/danieljvdm/effect-agent/commit/9e31de4c5f63ebc7eefbce33d3e0ed2052538f26) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Expose host-supplied model-context preparation through Cloudflare Conversation Object options
  ([#49](https://github.com/danieljvdm/effect-agent/issues/49)). A generic scoped `RunContextPreparation` service now composes after canonical durable
  resume reconstruction, `contextCompactorRunContextLayer` adapts the digest-bound
  `ContextCompactor` capability with typed failures, and `CloudflareDurableRuntimeOptions.runContext`
  accepts a closed Layer or per-incarnation Layer factory. Compaction changes only model-visible
  context; canonical history remains recoverable across Durable Object eviction and retries.

### Patch Changes

- Updated dependencies [[`9e31de4`](https://github.com/danieljvdm/effect-agent/commit/9e31de4c5f63ebc7eefbce33d3e0ed2052538f26), [`b8beef5`](https://github.com/danieljvdm/effect-agent/commit/b8beef5624f6704b0e52b5023babd1272d6b0603)]:
  - @effect-agent/engine@0.1.0-beta.19
  - @effect-agent/session@0.1.0-beta.19
  - @effect-agent/storage-cloudflare@0.1.0-beta.19
  - @effect-agent/core@0.1.0-beta.19
  - @effect-agent/sandbox@0.1.0-beta.19

## 0.1.0-beta.18

### Patch Changes

- Updated dependencies [[`f36fd40`](https://github.com/danieljvdm/effect-agent/commit/f36fd409f8a34e13c87646fd857a4060ac89e89d)]:
  - @effect-agent/session@0.1.0-beta.18
  - @effect-agent/storage-cloudflare@0.1.0-beta.18
  - @effect-agent/core@0.1.0-beta.18
  - @effect-agent/sandbox@0.1.0-beta.18

## 0.1.0-beta.17

### Patch Changes

- Updated dependencies [[`016df57`](https://github.com/danieljvdm/effect-agent/commit/016df574fa8c0f362468d848ae830d72532cbcaf)]:
  - @effect-agent/core@0.1.0-beta.17
  - @effect-agent/session@0.1.0-beta.17
  - @effect-agent/storage-cloudflare@0.1.0-beta.17
  - @effect-agent/sandbox@0.1.0-beta.17

## 0.1.0-beta.16

### Minor Changes

- [#99](https://github.com/danieljvdm/effect-agent/pull/99) [`e4b32b5`](https://github.com/danieljvdm/effect-agent/commit/e4b32b54061e58de57d5c27f06f8ef2a821ccb38) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add the Effect-native durable progress wait from [#94](https://github.com/danieljvdm/effect-agent/issues/94). Runtime and Cloudflare callers now subscribe
  before an authoritative canonical read, wake from post-commit hints without polling, broadcast to
  concurrent same-conversation waiters, clean up on interruption, and reconnect safely after Durable
  Object eviction. Cloudflare observation and resolution calls also preserve typed authorization
  denials, and the client Layer now requires an explicit `Crypto.Crypto` provider for cancellation
  identities.

### Patch Changes

- Updated dependencies [[`e4b32b5`](https://github.com/danieljvdm/effect-agent/commit/e4b32b54061e58de57d5c27f06f8ef2a821ccb38)]:
  - @effect-agent/session@0.1.0-beta.16
  - @effect-agent/storage-cloudflare@0.1.0-beta.16
  - @effect-agent/core@0.1.0-beta.16
  - @effect-agent/sandbox@0.1.0-beta.16

## 0.1.0-beta.15

### Patch Changes

- [#97](https://github.com/danieljvdm/effect-agent/pull/97) [`38ac06e`](https://github.com/danieljvdm/effect-agent/commit/38ac06eea0956d7bef4576c5e527c6053f5a86f0) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Make Cloudflare Conversation maintenance durably incremental and quiescent ([#93](https://github.com/danieljvdm/effect-agent/issues/93)). Stable
  externally-driven waits now clear their alarm after acknowledging the observed maintenance
  generation, while pre-armed public and routed mutations, restart recovery, and bounded autonomous
  rearming preserve liveness. A caught-up forced alarm takes an O(1) maintenance-record path without
  recovery, ledger scans, or canonical-history reads. Child settlements also commit the parent's
  durable wake before child ledger finalization, preventing eviction from losing a quiescent join.
- Updated dependencies [[`38ac06e`](https://github.com/danieljvdm/effect-agent/commit/38ac06eea0956d7bef4576c5e527c6053f5a86f0)]:
  - @effect-agent/session@0.1.0-beta.15
  - @effect-agent/storage-cloudflare@0.1.0-beta.15
  - @effect-agent/core@0.1.0-beta.15
  - @effect-agent/sandbox@0.1.0-beta.15

## 0.1.0-beta.14

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.14
  - @effect-agent/sandbox@0.1.0-beta.14
  - @effect-agent/session@0.1.0-beta.14
  - @effect-agent/storage-cloudflare@0.1.0-beta.14

## 0.1.0-beta.13

### Patch Changes

- [#84](https://github.com/danieljvdm/effect-agent/pull/84) [`dd0e5c3`](https://github.com/danieljvdm/effect-agent/commit/dd0e5c38a462abe341063842521530c0d484e54a) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Run Dynamic Worker Code Mode host callbacks on retained independent Effect fibers so guest RPC
  callbacks can complete without deadlocking the in-flight worker RPC. Bound callback execution by
  the pass deadline and host-call limits, and close, interrupt, and settle callback work on teardown.
- Updated dependencies [[`68b48c9`](https://github.com/danieljvdm/effect-agent/commit/68b48c932b6a76d2c8ed0f04cc87c123a9fd11e4)]:
  - @effect-agent/core@0.1.0-beta.13
  - @effect-agent/session@0.1.0-beta.13
  - @effect-agent/storage-cloudflare@0.1.0-beta.13
  - @effect-agent/sandbox@0.1.0-beta.13

## 0.1.0-beta.12

### Patch Changes

- [#81](https://github.com/danieljvdm/effect-agent/pull/81) [`51bc32b`](https://github.com/danieljvdm/effect-agent/commit/51bc32b982abc21412c55d61064be5ec6fa1664f) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Publish `effect-cf` as a compatible host-owned peer and update the workspace integration to 0.27.0, avoiding consumer overrides and duplicate Effect service identities.

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.12
  - @effect-agent/sandbox@0.1.0-beta.12
  - @effect-agent/session@0.1.0-beta.12
  - @effect-agent/storage-cloudflare@0.1.0-beta.12

## 0.1.0-beta.11

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.11
  - @effect-agent/sandbox@0.1.0-beta.11
  - @effect-agent/session@0.1.0-beta.11
  - @effect-agent/storage-cloudflare@0.1.0-beta.11

## 0.1.0-beta.10

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.10
  - @effect-agent/sandbox@0.1.0-beta.10
  - @effect-agent/session@0.1.0-beta.10
  - @effect-agent/storage-cloudflare@0.1.0-beta.10

## 0.1.0-beta.9

### Patch Changes

- Updated dependencies [[`91ff50d`](https://github.com/danieljvdm/effect-agent/commit/91ff50df5480a0ccdfb8e0a00db39a1576e6c34b)]:
  - @effect-agent/core@0.1.0-beta.9
  - @effect-agent/session@0.1.0-beta.9
  - @effect-agent/storage-cloudflare@0.1.0-beta.9
  - @effect-agent/sandbox@0.1.0-beta.9

## 0.1.0-beta.8

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.8
  - @effect-agent/sandbox@0.1.0-beta.8
  - @effect-agent/session@0.1.0-beta.8
  - @effect-agent/storage-cloudflare@0.1.0-beta.8

## 0.1.0-beta.7

### Patch Changes

- Updated dependencies [[`5c49b78`](https://github.com/danieljvdm/effect-agent/commit/5c49b786604b3e8389cdc2c54d4f5cb284eac2b7), [`afe755a`](https://github.com/danieljvdm/effect-agent/commit/afe755a331172ffca9ceee7dd82bb452c6ccbb8a)]:
  - @effect-agent/session@0.1.0-beta.7
  - @effect-agent/core@0.1.0-beta.7
  - @effect-agent/storage-cloudflare@0.1.0-beta.7
  - @effect-agent/sandbox@0.1.0-beta.7

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
- Updated dependencies [[`e13ee6e`](https://github.com/danieljvdm/effect-agent/commit/e13ee6e7817549e99837d06e86caf2dea8656aa8)]:
  - @effect-agent/core@0.1.0-beta.6
  - @effect-agent/session@0.1.0-beta.6
  - @effect-agent/storage-cloudflare@0.1.0-beta.6
  - @effect-agent/sandbox@0.1.0-beta.6

## 0.0.1-beta.5

### Patch Changes

- [#19](https://github.com/danieljvdm/effect-agent/pull/19) [`a063031`](https://github.com/danieljvdm/effect-agent/commit/a063031c6b1f1637d947ae193a410b6bb9e8a9fc) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align every public package with Effect 4.0.0-beta.107. Also expose per-incarnation Cloudflare
  Binding capture with live Durable Object context and derived identities, and prevent incomplete
  application Tool batches from a failed or aborted Run from poisoning prompts for later Runs.
- Updated dependencies [[`a063031`](https://github.com/danieljvdm/effect-agent/commit/a063031c6b1f1637d947ae193a410b6bb9e8a9fc)]:
  - @effect-agent/core@0.0.1-beta.5
  - @effect-agent/session@0.0.1-beta.5
  - @effect-agent/storage-cloudflare@0.0.1-beta.5

## 0.0.1-beta.4

### Patch Changes

- [#13](https://github.com/danieljvdm/effect-agent/pull/13) [`f4e3786`](https://github.com/danieljvdm/effect-agent/commit/f4e378635a794d4c17192ee3de011697ccec3a3b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Introduce the `effect-agent` umbrella package: the framework's complete pure
  surface — schema-first authoring (core), the bounded interpreter (engine),
  and operational capabilities — as one dependency-clean root package,
  mirroring how `effect` fronts the `@effect/*` satellites. Platform adapters
  remain scoped. The umbrella is version-fixed to its three constituents.
- Updated dependencies [[`f4e3786`](https://github.com/danieljvdm/effect-agent/commit/f4e378635a794d4c17192ee3de011697ccec3a3b)]:
  - @effect-agent/core@0.0.1-beta.4
  - @effect-agent/session@0.0.1-beta.4
  - @effect-agent/storage-cloudflare@0.0.1-beta.4

## 0.0.1-beta.3

### Patch Changes

- Adopt the MIT license across every published package, and ship the Cloudflare
  packages with type declarations for the first time: their Durable Object
  class factory now carries an explicit `ConversationObjectClass` return type,
  which unblocks TypeScript declaration emit (TS4094). Supersedes the
  0.0.1-beta.2 round (and the Cloudflare pair's 0.0.1-beta.0), which was
  published out of band from an uncommitted tree, still UNLICENSED, and without
  `.d.mts` for the Cloudflare packages.
- Updated dependencies []:
  - @effect-agent/core@0.0.1-beta.3
  - @effect-agent/session@0.0.1-beta.3
  - @effect-agent/storage-cloudflare@0.0.1-beta.3
