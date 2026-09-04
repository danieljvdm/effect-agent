# Typed Schema codecs

Match the codec to the type known at the call site. Use the typed decode variant
when the input is the schema's Encoded type, and the typed encode variant when
it is the schema's Type. Consult installed declarations for the Effect, Sync,
Exit, Option, Result, or Promise form needed by that boundary.

Reserve unknown codecs for values whose declared type is actually unknown,
such as untyped JSON, external messages, or persistence results. A static
mismatch with a Schema.Class is not a reason to bypass type checking; construct
or map the correct schema value first.

Follow the repository's policy for lint suppressions at justified untyped
boundaries. Keep the explanation with the boundary rather than duplicating it
across contract consumers.
