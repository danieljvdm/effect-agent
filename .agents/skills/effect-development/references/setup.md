# Set up Effect

Inspect the package boundaries and existing Effect version first. Use the
repository's package manager and version policy to add Effect where it is
needed. In a monorepo, make its source and instructions available at the root
when that is how agents inspect installed APIs. Avoid an unrelated upgrade or
copying a package-manager command from a different project.

Ensure repository agent instructions direct Effect work to the installed
`node_modules/effect/AGENTS.md`, require reading it completely before writing
Effect code, and follow its relevant links. For APIs not covered there, consult
`node_modules/effect/src`.

Preserve existing agent guidance and keep one pointer to the installed source
instead of copying library instructions into the repository.
