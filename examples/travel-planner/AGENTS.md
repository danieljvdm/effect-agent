# Improve the library through this example

This canonical app is also a real-world testbed for improving Effect Agent. While
building or debugging it, speak up whenever you find a suspected library bug or an
API that could be safer, simpler, or harder to misuse.

- Distinguish application mistakes, Effect Agent defects, and upstream Effect or
  provider behavior. State uncertainty when the cause is not yet confirmed.
- Explain the concrete failure, the relevant library boundary, and a possible
  improvement. Do not silently hide a library problem behind an app workaround.
- Fix reusable framework defects in separate library PRs with sufficient verification
  under the root testing policy and changesets. Keep library source changes out of the demo PR.
- Consume Effect Agent through explicit `workspace:*` dependencies so the demo
  validates the current framework. Use the root catalog for Effect-family and
  `effect-cf` versions. Do not add source aliases or patch framework packages
  inside the demo. Keep application policy here.
- Flag usability issues even when existing configuration fixes the app. Do not
  change public library semantics just to accommodate this example without
  considering other consumers.
