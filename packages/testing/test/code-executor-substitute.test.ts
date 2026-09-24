import { codeExecutorConformanceCases } from "@effect-agent/testing/code-executor-conformance";
import {
  inProcessCodeExecutorImplementation,
  inProcessCodeExecutorLayer,
} from "@effect-agent/testing/code-executor-substitute";
import { layer } from "@effect/vitest";

// The wall-clock conformance case needs the live Clock, so the suite opts out
// of the injected test services the same way the sandbox-local suite does.
layer(inProcessCodeExecutorLayer, { excludeTestServices: true })(
  "CAP-015 in-process CodeExecutor substitute",
  (it) => {
    for (const conformanceCase of codeExecutorConformanceCases({
      implementation: inProcessCodeExecutorImplementation,
    })) {
      it.effect(conformanceCase.name, () => conformanceCase.run);
    }
  },
);
