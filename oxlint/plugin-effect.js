const noUntypedThrow = {
  meta: {
    type: "problem",
    docs: { description: "Disallow throw statements from Effect-owned application code." },
    messages: {
      noUntypedThrow:
        "Do not throw from Effect-owned code. Fail with a tagged error in the Effect error channel.",
    },
  },
  create(context) {
    return {
      ThrowStatement(node) {
        context.report({ node, messageId: "noUntypedThrow" });
      },
    };
  },
};

const noPromiseAtomMode = {
  meta: {
    type: "problem",
    docs: { description: "Require Effect Atom AsyncResult mode for React mutations." },
    messages: {
      noPromiseAtomMode:
        'Do not use Effect Atom mode: "promise". Compose the workflow with Atom.fn and render its AsyncResult.',
    },
  },
  create(context) {
    return {
      Property(node) {
        const key = node.key;
        const value = node.value;
        const isMode =
          (key.type === "Identifier" && key.name === "mode") ||
          (key.type === "Literal" && key.value === "mode");

        if (isMode && value.type === "Literal" && value.value === "promise") {
          context.report({ node, messageId: "noPromiseAtomMode" });
        }
      },
    };
  },
};

const noEffectRun = {
  meta: {
    type: "problem",
    docs: { description: "Keep Effect runtime execution at explicit host entry points." },
    messages: {
      noEffectRun:
        "Do not execute Effect with Effect.run* inside application code. Return or compose the Effect instead.",
    },
  },
  create(context) {
    return {
      MemberExpression(node) {
        if (
          node.object.type === "Identifier" &&
          node.object.name === "Effect" &&
          !node.computed &&
          node.property.type === "Identifier" &&
          node.property.name.startsWith("run")
        ) {
          context.report({ node, messageId: "noEffectRun" });
        }
      },
    };
  },
};

const noUnsafePromise = {
  meta: {
    type: "problem",
    docs: { description: "Require rejecting Promise boundaries to preserve typed failures." },
    messages: {
      noEffectPromise:
        "Effect.promise turns rejection into a defect. Use Effect.tryPromise and map a tagged boundary error.",
      noPromiseConstructor:
        "Do not construct workflow Promises directly. Use Effect async/scheduling/concurrency operators.",
    },
  },
  create(context) {
    return {
      MemberExpression(node) {
        if (
          node.object.type === "Identifier" &&
          node.object.name === "Effect" &&
          !node.computed &&
          node.property.type === "Identifier" &&
          node.property.name === "promise"
        ) {
          context.report({ node, messageId: "noEffectPromise" });
        }
      },
      NewExpression(node) {
        if (node.callee.type === "Identifier" && node.callee.name === "Promise") {
          context.report({ node, messageId: "noPromiseConstructor" });
        }
      },
    };
  },
};

const noAsyncWorkflow = {
  meta: {
    type: "problem",
    docs: { description: "Keep business workflows in Effect instead of async functions." },
    messages: {
      noAsyncWorkflow:
        "Do not implement application workflows as async functions. Return an Effect and capture foreign Promises with Effect.tryPromise.",
    },
  },
  create(context) {
    const check = (node) => {
      if (!node.async) return;
      const parent = node.parent;
      const isCapturedTryThunk =
        parent?.type === "Property" &&
        ((parent.key.type === "Identifier" && parent.key.name === "try") ||
          (parent.key.type === "Literal" && parent.key.value === "try"));

      if (!isCapturedTryThunk) context.report({ node, messageId: "noAsyncWorkflow" });
    };

    return {
      ArrowFunctionExpression: check,
      FunctionDeclaration: check,
      FunctionExpression: check,
    };
  },
};

const noSyncBoundaryDecode = {
  meta: {
    type: "problem",
    docs: { description: "Prevent external values from becoming synchronous schema defects." },
    messages: {
      noSyncBoundaryDecode:
        "Do not synchronously decode route, persisted, or native input. Use Schema.decodeUnknownEffect/Option and handle the typed failure.",
    },
  },
  create(context) {
    return {
      MemberExpression(node) {
        if (
          node.object.type === "Identifier" &&
          node.object.name === "Schema" &&
          !node.computed &&
          node.property.type === "Identifier" &&
          node.property.name === "decodeUnknownSync"
        ) {
          context.report({ node, messageId: "noSyncBoundaryDecode" });
        }
      },
    };
  },
};

export default {
  meta: { name: "dev-kit-effect" },
  rules: {
    "no-async-workflow": noAsyncWorkflow,
    "no-effect-run": noEffectRun,
    "no-promise-atom-mode": noPromiseAtomMode,
    "no-sync-boundary-decode": noSyncBoundaryDecode,
    "no-untyped-throw": noUntypedThrow,
    "no-unsafe-promise": noUnsafePromise,
  },
};
