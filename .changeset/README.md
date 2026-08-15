# Changesets

This directory contains release notes for published `@effect-agent/*` workspaces.

All public framework workspaces belong to one Changesets fixed group. A changeset names only the
packages whose behavior changed, while versioning advances every public workspace to the same
version and publishes unchanged packages on the same release train.

Create a changeset, review the generated version plan, and merge the automated version PR to
publish. The repository-specific publisher must be used instead of `changeset publish`; see the
release runbook in `docs/TOOLCHAIN.md` for the automated path and manual fallback.
