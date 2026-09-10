# Improve the library through this example

This canonical app is also a real-world testbed for improving Effect Agent. While
building or debugging it, speak up whenever you find a suspected library bug or an
API that could be safer, simpler, or harder to misuse.

- Distinguish application mistakes, Effect Agent defects, and upstream Effect or
  provider behavior. State uncertainty when the cause is not yet confirmed.
- Explain the concrete failure, the relevant library boundary, and a possible
  improvement. Do not silently hide a library problem behind an app workaround.
- Fix reusable framework defects in separate library PRs with focused regression
  coverage and changesets. Keep library source changes out of the demo PR.
- Consume exact published npm versions of Effect Agent. Do not use workspace
  links, source aliases, or local library patches to make the demo pass. If a
  required fix is unreleased, identify the library PR and wait for its release
  before upgrading or deploying the demo. Keep application policy here.
- Flag usability issues even when existing configuration fixes the app. Do not
  change public library semantics just to accommodate this example without
  considering other consumers.
