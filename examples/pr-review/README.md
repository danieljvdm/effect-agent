# GitHub review channel

This private leaf workspace adapts GitHub to `@effect-agent/pr-review`. It owns
event selection, REST calls, OpenAI binding, diff admission, and publication.
The package itself remains provider- and transport-neutral.

Automatic events admit at most two model attempts, including failed attempts.
Further pushes require `@effect-agent review` or `@effect-agent full review`
from a repository collaborator. Review bodies carry only a tiny terminal
marker; no model conversation or signed continuity state is persisted.
