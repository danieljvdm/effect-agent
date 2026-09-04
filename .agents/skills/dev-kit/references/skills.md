# Repository skills

The transient CLI copies skills directly into `.agents/skills`. Each copied
skill is repository-owned. A `.dev-kit-origin.json` file inside that skill records
only its selector, approved source, and base digest; it is not desired state and
runs no lifecycle.

## Discover and add

Inspect repository capabilities before selecting skills. Search descriptions and
prefer the narrowest matching skills:

```bash
bunx @danieljvdm/dev-kit@latest skills search cloudflare
bunx @danieljvdm/dev-kit@latest skills info cloudflare-workers
bunx @danieljvdm/dev-kit@latest skills add cloudflare-workers
```

Treat source families as broad selections: add one only when every member applies.

## Refresh

Run `skills status` before updates. `skills update` fast-forwards a skill only
when its repository copy still matches the recorded base. For a locally modified
skill whose upstream also changed, inspect `skills diff`, merge the relevant
upstream intent into the repository copy, and preserve local policy deliberately.

After a manual merge, preserve the merged content and advance its base with
`skills update <name> --accept-local`. A future CLI may automate three-way
merges; current conflicts remain agent-owned rather than being overwritten.

## Detach

Run `skills detach <name>` to remove only the origin receipt. The skill content
stays in place as an ordinary local skill and future Dev Kit updates ignore it.
