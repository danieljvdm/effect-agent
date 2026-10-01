# @effect-agent/storage-sql

## 0.1.0-beta.162

### Patch Changes

- [#737](https://github.com/danieljvdm/effect-agent/pull/737) [`aa50237`](https://github.com/danieljvdm/effect-agent/commit/aa502375beb6e45a0979cb46a8324dccf6c22304) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Batch lifecycle publication acknowledgements, and reduce recovery queries and redundant maintenance for exclusive SQLite owners. Preserve exact identities, persisted retry budgets, and compatibility with custom publication storage.

- Updated dependencies [[`aa50237`](https://github.com/danieljvdm/effect-agent/commit/aa502375beb6e45a0979cb46a8324dccf6c22304)]:
  - effect-agent@0.1.0-beta.162

## 0.1.0-beta.161

### Patch Changes

- Updated dependencies [[`ff7f6c3`](https://github.com/danieljvdm/effect-agent/commit/ff7f6c30988b3c714f8cf4f4d26748c6baea1ccb)]:
  - effect-agent@0.1.0-beta.161

## 0.1.0-beta.160

### Patch Changes

- [#731](https://github.com/danieljvdm/effect-agent/pull/731) [`9a0b5bf`](https://github.com/danieljvdm/effect-agent/commit/9a0b5bf0f8f7a862f805b889a7305664e82ccaf5) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Reuse eligible compacted Thread context across fresh durable Runs, refreshing it from new canonical records while preserving full replay for incompatible histories. Validate stored checkpoints through indexed canonical batch lookups.

- [#733](https://github.com/danieljvdm/effect-agent/pull/733) [`8c25dfd`](https://github.com/danieljvdm/effect-agent/commit/8c25dfdd2a16271df44bdfae4258b0d5c7114c38) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Coalesce Cloudflare maintenance scheduling writes within each transaction and reuse its queue view without changing retry, publication, or recovery behavior.

- Updated dependencies [[`9a0b5bf`](https://github.com/danieljvdm/effect-agent/commit/9a0b5bf0f8f7a862f805b889a7305664e82ccaf5), [`8c25dfd`](https://github.com/danieljvdm/effect-agent/commit/8c25dfdd2a16271df44bdfae4258b0d5c7114c38)]:
  - effect-agent@0.1.0-beta.160

## 0.1.0-beta.159

### Patch Changes

- [#724](https://github.com/danieljvdm/effect-agent/pull/724) [`8a839e6`](https://github.com/danieljvdm/effect-agent/commit/8a839e685444bff89ba236e0de316adf40ee010f) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Remove obsolete runtime aliases, frozen context tools, and unused storage failpoint controls. Use indexed canonical reads for selected Thread records instead of decoding a cached full history.

  BEHAVIOR CHANGE: Use `Subagent.make`, `ContextTools.toolkit` with `ContextTools.layer`, and the registered `runResolvedWorker` in place of `Subagent.define`, legacy context tools, and `runWorker`; classify delegation with `DelegationTool` metadata instead of name helpers. Replace the removed `DoStorageFailpointTestControl` and SQLite testing module with the corresponding storage failpoint service Layers.

- Updated dependencies [[`4e78cc4`](https://github.com/danieljvdm/effect-agent/commit/4e78cc4c0011b9d60146397fa6e0143c70b8c9ca), [`04889a9`](https://github.com/danieljvdm/effect-agent/commit/04889a95338a4f786bdd7fdf45f23236e4dbb1ef), [`b6d171d`](https://github.com/danieljvdm/effect-agent/commit/b6d171de3d385960a062ce7f8e3415ffe22bbb93), [`8a839e6`](https://github.com/danieljvdm/effect-agent/commit/8a839e685444bff89ba236e0de316adf40ee010f), [`976f337`](https://github.com/danieljvdm/effect-agent/commit/976f3371a63b7a9f1a92b26f88371350042735b3)]:
  - effect-agent@0.1.0-beta.159

## 0.1.0-beta.158

### Patch Changes

- [#720](https://github.com/danieljvdm/effect-agent/pull/720) [`9a7d358`](https://github.com/danieljvdm/effect-agent/commit/9a7d3581930b1a36fed318d13c966ff8efc4baa5) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Park accepted message deliveries without status polling and acknowledge native sources on terminal settlement. Bound no-progress Cloudflare maintenance and reuse canonical hydration codecs and a bounded multi-page cache.

  BEHAVIOR CHANGE: generic host envelopes need an exact terminal `Complete` acknowledgement or explicit receipt recovery. Draining a host lane resets its scheduling budget; eight unchanged self-rearming passes park pending work for hourly recovery and report once through the installed error reporter. New source-transaction `recordProgress` or a strictly increasing retained commit cursor resumes it immediately. Canonical hydration retention is bounded by one shared eight-MiB serialized-byte budget per isolate.

- Updated dependencies [[`9a7d358`](https://github.com/danieljvdm/effect-agent/commit/9a7d3581930b1a36fed318d13c966ff8efc4baa5), [`a52c77f`](https://github.com/danieljvdm/effect-agent/commit/a52c77fe67ac7f729f3ea1c637474f4b8036b2dd)]:
  - effect-agent@0.1.0-beta.158

## 0.1.0-beta.157

### Patch Changes

- [#715](https://github.com/danieljvdm/effect-agent/pull/715) [`b6ec526`](https://github.com/danieljvdm/effect-agent/commit/b6ec526daf05a71d318fec0b31c5b31db54fed35) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Publish committed Run and Subagent start progress while native execution continues. Reduce SQLite statements for warm Durable Object turns while preserving recovery and ownership fencing.

- Updated dependencies [[`b6ec526`](https://github.com/danieljvdm/effect-agent/commit/b6ec526daf05a71d318fec0b31c5b31db54fed35), [`ee41eb7`](https://github.com/danieljvdm/effect-agent/commit/ee41eb7ef96a9edfa7e8d2dd4b2a50b8944f7af3)]:
  - effect-agent@0.1.0-beta.157

## 0.1.0-beta.156

### Patch Changes

- [#713](https://github.com/danieljvdm/effect-agent/pull/713) [`d2d83a2`](https://github.com/danieljvdm/effect-agent/commit/d2d83a2fa815fe11224421e64135145ed58e7bca) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Reduce Durable Object SQLite work with shared write-through reads and asynchronous lifecycle batches after native execution, preserving durable receipts across eviction. Use the SQL Memory Layer's `SqlMemoryBatchWriter.changeMany` to commit up to 128 ordered commands atomically and combine their writes.

- Updated dependencies [[`d2d83a2`](https://github.com/danieljvdm/effect-agent/commit/d2d83a2fa815fe11224421e64135145ed58e7bca)]:
  - effect-agent@0.1.0-beta.156

## 0.1.0-beta.155

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.155

## 0.1.0-beta.154

### Patch Changes

- Updated dependencies [[`826fb91`](https://github.com/danieljvdm/effect-agent/commit/826fb911a7dfd6548182e02f103c747f1a9a567f)]:
  - effect-agent@0.1.0-beta.154

## 0.1.0-beta.153

### Patch Changes

- Updated dependencies [[`4e16d81`](https://github.com/danieljvdm/effect-agent/commit/4e16d81311ad26f1666fdedd23bb409f2100a669), [`6f9e913`](https://github.com/danieljvdm/effect-agent/commit/6f9e91364cd03174bb8cf0223d0e7b468c3d7c1e), [`8085bda`](https://github.com/danieljvdm/effect-agent/commit/8085bda3355ef97bc3f7ed82612a105581648440)]:
  - effect-agent@0.1.0-beta.153

## 0.1.0-beta.152

### Patch Changes

- Updated dependencies [[`cff65f6`](https://github.com/danieljvdm/effect-agent/commit/cff65f61b7b46fb00ebecd1a6242c1d04cf0bd24)]:
  - effect-agent@0.1.0-beta.152

## 0.1.0-beta.151

### Patch Changes

- Updated dependencies [[`61cb29f`](https://github.com/danieljvdm/effect-agent/commit/61cb29f025fedd14483e013d7b33d099439b8074)]:
  - effect-agent@0.1.0-beta.151

## 0.1.0-beta.150

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.150

## 0.1.0-beta.149

### Patch Changes

- Updated dependencies [[`08e4acf`](https://github.com/danieljvdm/effect-agent/commit/08e4acf1cd791b0a615f5ba751b698e915c3b8be)]:
  - effect-agent@0.1.0-beta.149

## 0.1.0-beta.148

### Patch Changes

- Updated dependencies [[`f2726bb`](https://github.com/danieljvdm/effect-agent/commit/f2726bb4f48848a7cbaa0878a9911f68220b8255)]:
  - effect-agent@0.1.0-beta.148

## 0.1.0-beta.147

### Patch Changes

- Updated dependencies [[`27877c8`](https://github.com/danieljvdm/effect-agent/commit/27877c820b42cbffcbeecca42dc7c4b6f4a382cc), [`af24505`](https://github.com/danieljvdm/effect-agent/commit/af2450560f185e75d725a425349e9f611741645c)]:
  - effect-agent@0.1.0-beta.147

## 0.1.0-beta.146

### Minor Changes

- [#681](https://github.com/danieljvdm/effect-agent/pull/681) [`9252c1a`](https://github.com/danieljvdm/effect-agent/commit/9252c1ad4707035308ff72527ed303a685027a28) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Publish retained lifecycle facts asynchronously in ordered owner batches without delaying model attempts, with atomic receipts and bounded retries that park exhausted work.

  BEHAVIOR CHANGE: Implement `LifecyclePublicationHandler.publish(batch)` for a nonempty array of at most eight facts and commit the entire batch idempotently in one host transaction; custom lifecycle storage implementations must return bounded owner batches, replace `defer` with `claim`, and implement `retryParked` instead of `pendingDeadlineFor`.

### Patch Changes

- Updated dependencies [[`9252c1a`](https://github.com/danieljvdm/effect-agent/commit/9252c1ad4707035308ff72527ed303a685027a28), [`c2fc81a`](https://github.com/danieljvdm/effect-agent/commit/c2fc81a2882deec908868955d1325fdec400b979)]:
  - effect-agent@0.1.0-beta.146

## 0.1.0-beta.145

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.145

## 0.1.0-beta.144

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.144

## 0.1.0-beta.143

### Patch Changes

- [#669](https://github.com/danieljvdm/effect-agent/pull/669) [`c68edc7`](https://github.com/danieljvdm/effect-agent/commit/c68edc7aa1cdedb92ad6f31b63ba048bb718b2c6) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add an optional typed lifecycle publication handler with atomically retained native obligations and acknowledgement retries through existing Cloudflare maintenance. Publish application records from exact native admissions and transitions without scanning execution history.

- Updated dependencies [[`c68edc7`](https://github.com/danieljvdm/effect-agent/commit/c68edc7aa1cdedb92ad6f31b63ba048bb718b2c6)]:
  - effect-agent@0.1.0-beta.143

## 0.1.0-beta.142

### Patch Changes

- [#665](https://github.com/danieljvdm/effect-agent/pull/665) [`03831c5`](https://github.com/danieljvdm/effect-agent/commit/03831c5554b568bbf87ba79dcf1f030444d35e90) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Compose PostgreSQL storage with an application-provided Effect SQL client and Crypto layer, preserving native connection pooling, codecs, and schema defaults.

  BEHAVIOR CHANGE: Replace the `client` option and `PostgresStorageClient` with `PostgresStorage.layer` (or `layerWith(options)`) and native Layers. Install the service values returned by shared `makeSqlThreadStore` and `makeSqlSubmissionLedger` factories with `Layer.effect` for their corresponding ports; yield `makeSqlQuery(namespace?)` and call shared schema and index creation helpers as functions, optionally supplying a namespace.

- [#597](https://github.com/danieljvdm/effect-agent/pull/597) [`161aab3`](https://github.com/danieljvdm/effect-agent/commit/161aab335dbbb9bf704d005bf95015be2e04858b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Share SQL persistence implementations through `@effect-agent/storage-sql` while preserving SQLite storage formats and adapter APIs. BEHAVIOR CHANGE: import SQL subscription, message-delivery, native-read, and upgrade helpers from `@effect-agent/storage-sql` instead of `effect-agent`, and pass custom transactions through the factory options.

- Updated dependencies [[`161aab3`](https://github.com/danieljvdm/effect-agent/commit/161aab335dbbb9bf704d005bf95015be2e04858b)]:
  - effect-agent@0.1.0-beta.142
