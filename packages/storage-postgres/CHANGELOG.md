# @effect-agent/storage-postgres

## 0.1.0-beta.142

### Patch Changes

- [#665](https://github.com/danieljvdm/effect-agent/pull/665) [`03831c5`](https://github.com/danieljvdm/effect-agent/commit/03831c5554b568bbf87ba79dcf1f030444d35e90) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Compose PostgreSQL storage with an application-provided Effect SQL client and Crypto layer, preserving native connection pooling, codecs, and schema defaults.

  BEHAVIOR CHANGE: Replace the `client` option and `PostgresStorageClient` with `PostgresStorage.layer` (or `layerWith(options)`) and native Layers. Install the service values returned by shared `makeSqlThreadStore` and `makeSqlSubmissionLedger` factories with `Layer.effect` for their corresponding ports; yield `makeSqlQuery(namespace?)` and call shared schema and index creation helpers as functions, optionally supplying a namespace.

- [#597](https://github.com/danieljvdm/effect-agent/pull/597) [`161aab3`](https://github.com/danieljvdm/effect-agent/commit/161aab335dbbb9bf704d005bf95015be2e04858b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add Postgres storage for durable thread history, submissions, schedules, subscriptions, messages, and activity progress.

- Updated dependencies [[`03831c5`](https://github.com/danieljvdm/effect-agent/commit/03831c5554b568bbf87ba79dcf1f030444d35e90), [`161aab3`](https://github.com/danieljvdm/effect-agent/commit/161aab335dbbb9bf704d005bf95015be2e04858b)]:
  - @effect-agent/storage-sql@0.1.0-beta.142
  - effect-agent@0.1.0-beta.142
