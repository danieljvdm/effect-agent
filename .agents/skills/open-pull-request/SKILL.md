---
name: open-pull-request
description: Prepare, open, update, or land pull requests with concise explanations and relevant review evidence.
---

# Pull requests

Lead with the concrete problem and resulting behavior. Describe the final
aggregate diff, omitting intermediate commits, abandoned approaches, and work
session history unless they explain a relevant tradeoff. Simple changes need
only a brief summary; complex or high-risk changes can justify more context.

Keep test and validation reporting out of PR bodies: no dedicated sections,
checklists, or lists of commands run. Still perform required checks and disclose
material risks or limitations; include validation details only when required by
higher-priority instructions.

This is a public library. Keep descriptions self-contained and follow any
required PR template. Omit private tracker links and issue IDs, including Linear
references; link to public code or guides when useful. Keep company/customer
names, account identifiers, and private workspace links out of PR text and linked
evidence unless the user explicitly requests them.

- Ownership, data flow, or API changes: [diagrams and examples](references/explanation.md).
- UI or other visible behavior: [capture and publish evidence](references/evidence.md).
- Performance claims: [baseline and candidate comparisons](references/explanation.md#performance-claims).
- Commits, publication, readiness, or landing: [PR workflow](references/publication.md).

Reuse verification and captures for unchanged inputs. Include limitations or
manual steps when they affect review. Follow repository merge policy and
existing authorization; opening a PR does not authorize merging it.
