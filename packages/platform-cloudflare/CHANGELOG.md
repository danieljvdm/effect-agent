# @effect-agent/platform-cloudflare

## 0.0.1-beta.1

### Patch Changes

- Exclude incomplete application Tool turns from later Run history while preserving
  the owning Run's canonical declaration for durable recovery. Republish the
  Cloudflare storage and host packages so their exact internal dependency pins
  select the corrected session runtime.
- Updated dependencies []:
  - @effect-agent/session@0.0.1-beta.4
  - @effect-agent/storage-cloudflare@0.0.1-beta.1

## 0.0.1-beta.0

### Patch Changes

- [`63c9646`](https://github.com/danieljvdm/effect-agent/commit/63c9646574e621269e0ccf105104eeb98c6ab530) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add the Cloudflare Durable Object host packages to the beta channel, align the
  runtime packages with Effect 4.0.0-beta.105 through singleton peer contracts,
  and support per-incarnation binding resolution for host application tools.
- Updated dependencies [[`63c9646`](https://github.com/danieljvdm/effect-agent/commit/63c9646574e621269e0ccf105104eeb98c6ab530)]:
  - @effect-agent/core@0.0.1-beta.2
  - @effect-agent/session@0.0.1-beta.2
  - @effect-agent/storage-cloudflare@0.0.1-beta.0
