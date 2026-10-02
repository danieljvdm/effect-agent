import { layer } from "@effect/vitest";
import { codeExecutorConformanceCases } from "@yielded/agent-testing/code-executor-conformance";
import {
  inProcessCodeExecutorImplementation,
  inProcessCodeExecutorLayer,
} from "@yielded/agent-testing/code-executor-substitute";

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
