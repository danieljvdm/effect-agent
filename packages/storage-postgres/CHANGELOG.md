# @yielded/agent-storage-postgres

## 0.1.0-beta.167

### Patch Changes

- Updated dependencies [[`776aaca`](https://github.com/yielded-dev/agent/commit/776aaca3809ca5959327ebff5623d525503e8e34)]:
  - @yielded/agent@0.1.0-beta.167
  - @yielded/agent-storage-sql@0.1.0-beta.167

## 0.1.0-beta.166

### Minor Changes

- [#766](https://github.com/yielded-dev/agent/pull/766) [`a1fb42a`](https://github.com/yielded-dev/agent/commit/a1fb42a651eccef46b8775fe4373d3f04d85e8de) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Move Effect Agent to `@yielded/agent` and its `@yielded/agent-*` adapters. Update dependencies and import prefixes together; APIs, service identities, and stored formats remain unchanged.

### Patch Changes

- [#749](https://github.com/yielded-dev/agent/pull/749) [`08d1384`](https://github.com/yielded-dev/agent/commit/08d1384e625091d83a6cb6eeb9c95e28cff2cc69) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Require Effect 4.0.0 and use its current module paths and encoding APIs. Require `effect-cf@^0.53.0` for the Cloudflare adapter.

  BEHAVIOR CHANGE: upgrade Effect and matching provider, platform, SQL, and Atom packages to 4.0.0; replace `effect/unstable/*` imports with `effect/*` and use `effect/http-api` for HTTP APIs. Cloudflare logical alarms now back off from one second and park for hourly recovery after eight attempts without reported source progress.

- Updated dependencies [[`00995dd`](https://github.com/yielded-dev/agent/commit/00995dd9049e11190588b143a32ba5c520686e7f), [`2cfa8f7`](https://github.com/yielded-dev/agent/commit/2cfa8f75258edd32898958e264e549a737368c9e), [`08d1384`](https://github.com/yielded-dev/agent/commit/08d1384e625091d83a6cb6eeb9c95e28cff2cc69), [`a1fb42a`](https://github.com/yielded-dev/agent/commit/a1fb42a651eccef46b8775fe4373d3f04d85e8de)]:
  - @yielded/agent@0.1.0-beta.166
  - @yielded/agent-storage-sql@0.1.0-beta.166

## 0.1.0-beta.165

### Patch Changes

- Updated dependencies [[`c426ed7`](https://github.com/danieljvdm/effect-agent/commit/c426ed78de30b9b186b4967e2e42bd19870ddc84), [`9d5f4d7`](https://github.com/danieljvdm/effect-agent/commit/9d5f4d7baac8e7311efa07b088f074406264bbc3), [`4ac924a`](https://github.com/danieljvdm/effect-agent/commit/4ac924aa14e3cfbfdac9ffaa7026164115943f11), [`656f964`](https://github.com/danieljvdm/effect-agent/commit/656f9643beecce1b6233e02672987ba413b5c5fc), [`de31c1e`](https://github.com/danieljvdm/effect-agent/commit/de31c1ece882beed14880628636c6bbd04f2ba52)]:
  - effect-agent@0.1.0-beta.165
  - @effect-agent/storage-sql@0.1.0-beta.165

## 0.1.0-beta.164

### Patch Changes

- Updated dependencies [[`d3ef7ea`](https://github.com/danieljvdm/effect-agent/commit/d3ef7ea8d24e6d7a5d4d057d497badfc72d3f5de)]:
  - effect-agent@0.1.0-beta.164
  - @effect-agent/storage-sql@0.1.0-beta.164

## 0.1.0-beta.163

### Patch Changes

- Updated dependencies [[`58f80d2`](https://github.com/danieljvdm/effect-agent/commit/58f80d2c1d2a832eba47176e24cc05010ee056a8)]:
  - effect-agent@0.1.0-beta.163
  - @effect-agent/storage-sql@0.1.0-beta.163

## 0.1.0-beta.162

### Patch Changes

- Updated dependencies [[`aa50237`](https://github.com/danieljvdm/effect-agent/commit/aa502375beb6e45a0979cb46a8324dccf6c22304)]:
  - effect-agent@0.1.0-beta.162
  - @effect-agent/storage-sql@0.1.0-beta.162

## 0.1.0-beta.161

### Patch Changes

- Updated dependencies [[`ff7f6c3`](https://github.com/danieljvdm/effect-agent/commit/ff7f6c30988b3c714f8cf4f4d26748c6baea1ccb)]:
  - effect-agent@0.1.0-beta.161
  - @effect-agent/storage-sql@0.1.0-beta.161

## 0.1.0-beta.160

### Patch Changes

- Updated dependencies [[`9a0b5bf`](https://github.com/danieljvdm/effect-agent/commit/9a0b5bf0f8f7a862f805b889a7305664e82ccaf5), [`8c25dfd`](https://github.com/danieljvdm/effect-agent/commit/8c25dfdd2a16271df44bdfae4258b0d5c7114c38)]:
  - effect-agent@0.1.0-beta.160
  - @effect-agent/storage-sql@0.1.0-beta.160

## 0.1.0-beta.159

### Patch Changes

- Updated dependencies [[`4e78cc4`](https://github.com/danieljvdm/effect-agent/commit/4e78cc4c0011b9d60146397fa6e0143c70b8c9ca), [`04889a9`](https://github.com/danieljvdm/effect-agent/commit/04889a95338a4f786bdd7fdf45f23236e4dbb1ef), [`b6d171d`](https://github.com/danieljvdm/effect-agent/commit/b6d171de3d385960a062ce7f8e3415ffe22bbb93), [`8a839e6`](https://github.com/danieljvdm/effect-agent/commit/8a839e685444bff89ba236e0de316adf40ee010f), [`976f337`](https://github.com/danieljvdm/effect-agent/commit/976f3371a63b7a9f1a92b26f88371350042735b3)]:
  - effect-agent@0.1.0-beta.159
  - @effect-agent/storage-sql@0.1.0-beta.159

## 0.1.0-beta.158

### Patch Changes

- Updated dependencies [[`9a7d358`](https://github.com/danieljvdm/effect-agent/commit/9a7d3581930b1a36fed318d13c966ff8efc4baa5), [`a52c77f`](https://github.com/danieljvdm/effect-agent/commit/a52c77fe67ac7f729f3ea1c637474f4b8036b2dd)]:
  - effect-agent@0.1.0-beta.158
  - @effect-agent/storage-sql@0.1.0-beta.158

## 0.1.0-beta.157

### Patch Changes

- Updated dependencies [[`b6ec526`](https://github.com/danieljvdm/effect-agent/commit/b6ec526daf05a71d318fec0b31c5b31db54fed35), [`ee41eb7`](https://github.com/danieljvdm/effect-agent/commit/ee41eb7ef96a9edfa7e8d2dd4b2a50b8944f7af3)]:
  - effect-agent@0.1.0-beta.157
  - @effect-agent/storage-sql@0.1.0-beta.157

## 0.1.0-beta.156

### Patch Changes

- Updated dependencies [[`d2d83a2`](https://github.com/danieljvdm/effect-agent/commit/d2d83a2fa815fe11224421e64135145ed58e7bca)]:
  - effect-agent@0.1.0-beta.156
  - @effect-agent/storage-sql@0.1.0-beta.156

## 0.1.0-beta.155

### Patch Changes

- Updated dependencies []:
  - @effect-agent/storage-sql@0.1.0-beta.155
  - effect-agent@0.1.0-beta.155

## 0.1.0-beta.154

### Patch Changes

- Updated dependencies [[`826fb91`](https://github.com/danieljvdm/effect-agent/commit/826fb911a7dfd6548182e02f103c747f1a9a567f)]:
  - effect-agent@0.1.0-beta.154
  - @effect-agent/storage-sql@0.1.0-beta.154

## 0.1.0-beta.153

### Patch Changes

- Updated dependencies [[`4e16d81`](https://github.com/danieljvdm/effect-agent/commit/4e16d81311ad26f1666fdedd23bb409f2100a669), [`6f9e913`](https://github.com/danieljvdm/effect-agent/commit/6f9e91364cd03174bb8cf0223d0e7b468c3d7c1e), [`8085bda`](https://github.com/danieljvdm/effect-agent/commit/8085bda3355ef97bc3f7ed82612a105581648440)]:
  - effect-agent@0.1.0-beta.153
  - @effect-agent/storage-sql@0.1.0-beta.153

## 0.1.0-beta.152

### Patch Changes

- Updated dependencies [[`cff65f6`](https://github.com/danieljvdm/effect-agent/commit/cff65f61b7b46fb00ebecd1a6242c1d04cf0bd24)]:
  - effect-agent@0.1.0-beta.152
  - @effect-agent/storage-sql@0.1.0-beta.152

## 0.1.0-beta.151

### Patch Changes

- Updated dependencies [[`61cb29f`](https://github.com/danieljvdm/effect-agent/commit/61cb29f025fedd14483e013d7b33d099439b8074)]:
  - effect-agent@0.1.0-beta.151
  - @effect-agent/storage-sql@0.1.0-beta.151

## 0.1.0-beta.150

### Patch Changes

- Updated dependencies []:
  - @effect-agent/storage-sql@0.1.0-beta.150
  - effect-agent@0.1.0-beta.150

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
