# @effect-agent/storage-postgres

## 0.1.0-beta.149

### Patch Changes

- Updated dependencies [[`08e4acf`](https://github.com/danieljvdm/effect-agent/commit/08e4acf1cd791b0a615f5ba751b698e915c3b8be)]:
  - effect-agent@0.1.0-beta.149
  - @effect-agent/storage-sql@0.1.0-beta.149

## 0.1.0-beta.148

### Patch Changes

- Updated dependencies [[`f2726bb`](https://github.com/danieljvdm/effect-agent/commit/f2726bb4f48848a7cbaa0878a9911f68220b8255)]:
  - effect-agent@0.1.0-beta.148
  - @effect-agent/storage-sql@0.1.0-beta.148

## 0.1.0-beta.147

### Patch Changes

- Updated dependencies [[`27877c8`](https://github.com/danieljvdm/effect-agent/commit/27877c820b42cbffcbeecca42dc7c4b6f4a382cc), [`af24505`](https://github.com/danieljvdm/effect-agent/commit/af2450560f185e75d725a425349e9f611741645c)]:
  - effect-agent@0.1.0-beta.147
  - @effect-agent/storage-sql@0.1.0-beta.147

## 0.1.0-beta.146

### Patch Changes

- Updated dependencies [[`9252c1a`](https://github.com/danieljvdm/effect-agent/commit/9252c1ad4707035308ff72527ed303a685027a28), [`c2fc81a`](https://github.com/danieljvdm/effect-agent/commit/c2fc81a2882deec908868955d1325fdec400b979)]:
  - effect-agent@0.1.0-beta.146
  - @effect-agent/storage-sql@0.1.0-beta.146

## 0.1.0-beta.145

### Patch Changes

- Updated dependencies []:
  - @effect-agent/storage-sql@0.1.0-beta.145
  - effect-agent@0.1.0-beta.145

## 0.1.0-beta.144

### Patch Changes

- Updated dependencies []:
  - @effect-agent/storage-sql@0.1.0-beta.144
  - effect-agent@0.1.0-beta.144

## 0.1.0-beta.143

### Patch Changes

- Updated dependencies [[`c68edc7`](https://github.com/danieljvdm/effect-agent/commit/c68edc7aa1cdedb92ad6f31b63ba048bb718b2c6)]:
  - effect-agent@0.1.0-beta.143
  - @effect-agent/storage-sql@0.1.0-beta.143

## 0.1.0-beta.142

### Patch Changes

- [#665](https://github.com/danieljvdm/effect-agent/pull/665) [`03831c5`](https://github.com/danieljvdm/effect-agent/commit/03831c5554b568bbf87ba79dcf1f030444d35e90) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Compose PostgreSQL storage with an application-provided Effect SQL client and Crypto layer, preserving native connection pooling, codecs, and schema defaults.

  BEHAVIOR CHANGE: Replace the `client` option and `PostgresStorageClient` with `PostgresStorage.layer` (or `layerWith(options)`) and native Layers. Install the service values returned by shared `makeSqlThreadStore` and `makeSqlSubmissionLedger` factories with `Layer.effect` for their corresponding ports; yield `makeSqlQuery(namespace?)` and call shared schema and index creation helpers as functions, optionally supplying a namespace.

- [#597](https://github.com/danieljvdm/effect-agent/pull/597) [`161aab3`](https://github.com/danieljvdm/effect-agent/commit/161aab335dbbb9bf704d005bf95015be2e04858b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add Postgres storage for durable thread history, submissions, schedules, subscriptions, messages, and activity progress.

- Updated dependencies [[`03831c5`](https://github.com/danieljvdm/effect-agent/commit/03831c5554b568bbf87ba79dcf1f030444d35e90), [`161aab3`](https://github.com/danieljvdm/effect-agent/commit/161aab335dbbb9bf704d005bf95015be2e04858b)]:
  - @effect-agent/storage-sql@0.1.0-beta.142
  - effect-agent@0.1.0-beta.142
