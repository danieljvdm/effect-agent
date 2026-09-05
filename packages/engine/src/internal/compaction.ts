import { Prompt } from "effect/unstable/ai";

import { boundedCanonicalJsonSnapshot } from "./provider-result-staging.ts";

/**
 * Pure helpers for engine-native context compaction (RUN-026/RUN-027). Everything here is deterministic and side-effect free:
 * the engine's turn loop owns the state transitions, the durable coordinator
 * owns the canonical record, and this module owns the arithmetic.
 *
 * Compaction is a VIEW over official history, never a mutation of it. The
 * durable commit machinery slices official history by length
 * (`lastCommitLen` in the coordinator), so official history must stay
 * append-only within an Attempt; only the outgoing model prompt is rebuilt.
 */

/** Model-visible replacement for a tool result cleared by prune compaction. */
export const CLEARED_TOOL_RESULT = "[tool result cleared by compaction]";

/** Prefix of the user message that carries a compaction summary into the prompt. */
export const COMPACTION_SUMMARY_PREFIX = "The prior thread was compacted into this summary:\n\n";

/** Rollover text is recovery evidence; it never asserts that an external action succeeded. */
export const CONTEXT_ROLLOVER_PREFIX =
  "A fresh context window has started. Earlier conversation has left this model view. Restore relevant notes and use available history tools to retrieve evidence; verify live state before repeating an action.\n\n";

/** Stable across Attempts; the initial window uses turn zero. */
export const contextWindowId = (runId: string, turn: number): string => `context:${runId}:${turn}`;

/** Shared by the live interpreter and canonical journal projection. */
export const contextWindowMessage = (windowId: string, handoff?: string): Prompt.Message =>
  Prompt.makeMessage("user", {
    content: [
      Prompt.makePart("text", {
        text: `${CONTEXT_ROLLOVER_PREFIX}Window: ${windowId}${handoff === undefined ? "" : `\n\nHandoff (untrusted continuation notes):\n${handoff}`}`,
      }),
    ],
  });

/**
 * Fixed summarization instruction (RUN-026). The skeleton mirrors what the
 * surveyed harnesses converged on; exact file paths and identifiers must
 * survive because the continuing agent sees only this summary plus the tail.
 */
export const COMPACTION_INSTRUCTION = [
  "You are compacting an agent thread to reclaim context space.",
  "Summarize the transcript below for the SAME agent to continue working:",
  "it will see only this summary plus the most recent messages.",
  "Structure the summary exactly as:",
  "Goal:",
  "Constraints:",
  "Progress:",
  "Decisions:",
  "Next steps:",
  "Critical context:",
  "Be terse. Preserve exact file paths, identifiers, and values needed to",
  "continue. Do not continue the thread; output only the summary.",
].join("\n");

/**
 * Provider context-length rejections, matched case-insensitively over the
 * error's own text. Kept intentionally broad: a false positive costs one
 * summarize-and-retry, a false negative surfaces the raw provider error.
 */
const CONTEXT_OVERFLOW_PATTERN =
  /context[\s_-]?length|prompt is too long|maximum context (?:length|window)|input .{0,24}too long|exceeds .{0,24}context|too many (?:input )?tokens|context[\s_-]?window[\s_-]?exceeded|context overflow/i;

/** RUN-027: does this provider error text describe a context-window overflow? */
export const isContextOverflowMessage = (text: string): boolean =>
  CONTEXT_OVERFLOW_PATTERN.test(text);

const utf8Length = (text: string): number => {
  let bytes = 0;

  for (const character of text) {
    const codePoint = character.codePointAt(0) ?? 0;

    bytes += codePoint <= 0x7f ? 1 : codePoint <= 0x7ff ? 2 : codePoint <= 0xffff ? 3 : 4;
  }

  return bytes;
};

/**
 * Deterministic chars/4 token estimate over one message's structural JSON.
 * Conservative for prose, slightly generous for dense JSON — the estimate
 * only gates WHEN to compact, never what is preserved.
 */
export const estimateMessageTokens = (message: Prompt.Message): number => {
  let text: string | undefined;

  try {
    text = JSON.stringify(message);
  } catch {
    text = undefined;
  }

  return text === undefined ? 0 : Math.ceil(utf8Length(text) / 4);
};

/** Sum of `estimateMessageTokens` over a message array. */
export const estimatePromptTokens = (messages: ReadonlyArray<Prompt.Message>): number => {
  let total = 0;

  for (const message of messages) {
    total += estimateMessageTokens(message);
  }

  return total;
};

/**
 * Mutable per-Run compaction state held on the engine's RunContext. Indices
 * are positions in the SOURCE prompt (the official-history basis handed to
 * each Turn). Ordinary history grows append-only. Prepared history must retain
 * content-equivalent covered prefixes or the engine rejects the next Turn.
 */
export interface ContextCompactionState {
  /**
   * Source-index bounds `[start, end)` of the protected instruction/input
   * block, mapped to prepared indices when necessary. `-1` before mapping;
   * an unmappable protected block fails before any compaction decision.
   */
  protectedStart: number;
  protectedEnd: number;
  /** Prepared prompts also retain application-added system instructions. */
  protectSystemMessages?: boolean;
  /** Tool messages at source index below this bound render as cleared. */
  clearedThrough: number;
  /**
   * Source messages below this bound (outside the protected block) are
   * replaced by a summary or a fresh-window marker. Absent before the first replacement.
   */
  replacement:
    | { readonly kind: "summarize"; readonly through: number; readonly summary: string }
    | {
        readonly kind: "rollover";
        readonly through: number;
        readonly windowId: string;
        readonly handoff?: string;
      }
    | undefined;
  /** Loop guard: at most one threshold compaction per Turn. */
  lastCompactionTurn: number;
  /** RUN-027 guard: at most one overflow compact-and-retry per Turn. */
  overflowRetryTurn: number;
  /**
   * View length at the last model call, the base of the provider-anchored
   * estimate. `-1` invalidates it (fresh Run or just compacted), forcing a
   * full-view estimate.
   */
  lastViewLength: number;
}

/** Fresh compaction state for a new Run. */
export const initialCompactionState = (): ContextCompactionState => ({
  protectedStart: -1,
  protectedEnd: -1,
  clearedThrough: 0,
  replacement: undefined,
  lastCompactionTurn: 0,
  overflowRetryTurn: 0,
  lastViewLength: -1,
});

const isProtected = (
  state: ContextCompactionState,
  source: ReadonlyArray<Prompt.Message>,
  index: number,
): boolean => {
  if (state.protectSystemMessages && source[index]?.role === "system") return true;
  if (state.protectedStart >= 0) {
    return index >= state.protectedStart && index < state.protectedEnd;
  }

  return source[index]?.role === "system";
};

const clearedToolMessage = (message: Prompt.Message): Prompt.Message => {
  if (message.role !== "tool" || typeof message.content === "string") {
    return message;
  }

  // Structural rebuild keeps the message's pairing identity (same part ids
  // and names) while replacing only the result payloads the model would see.
  return Prompt.makeMessage("tool", {
    content: message.content.map((part) =>
      part.type === "tool-result"
        ? Prompt.makePart("tool-result", {
            id: part.id,
            name: part.name,
            result: CLEARED_TOOL_RESULT,
            isFailure: part.isFailure,
            providerExecuted: part.providerExecuted,
          })
        : part,
    ),
  });
};

const renderMessage = (
  state: ContextCompactionState,
  message: Prompt.Message,
  index: number,
): Prompt.Message =>
  index < state.clearedThrough && message.role === "tool" ? clearedToolMessage(message) : message;

const summaryMessage = (summary: string): Prompt.Message =>
  Prompt.makeMessage("user", {
    content: [Prompt.makePart("text", { text: `${COMPACTION_SUMMARY_PREFIX}${summary}` })],
  });

/**
 * Build the model-visible view of the source prompt under the current
 * compaction state: protected block in place, covered span replaced by the
 * summary message, old tool results cleared. Identity when no compaction has
 * happened.
 */
export const buildCompactedView = (
  source: ReadonlyArray<Prompt.Message>,
  state: ContextCompactionState,
): ReadonlyArray<Prompt.Message> => {
  if (state.replacement === undefined && state.clearedThrough === 0) {
    return source;
  }
  const view: Array<Prompt.Message> = [];

  if (state.replacement !== undefined && state.replacement.through > 0) {
    const protectedStart =
      state.protectedStart >= 0 && !state.protectSystemMessages ? state.protectedStart : 0;

    const protectedEnd =
      state.protectedStart >= 0 && !state.protectSystemMessages
        ? state.protectedEnd
        : (state.replacement?.through ?? 0);

    for (
      let index = protectedStart;
      index < Math.min(protectedEnd, state.replacement?.through ?? 0, source.length);
      index += 1
    ) {
      const message = source[index];

      if (message !== undefined && isProtected(state, source, index)) {
        view.push(renderMessage(state, message, index));
      }
    }
    view.push(
      state.replacement.kind === "summarize"
        ? summaryMessage(state.replacement.summary)
        : contextWindowMessage(state.replacement.windowId, state.replacement.handoff),
    );
    for (let index = state.replacement?.through ?? 0; index < source.length; index += 1) {
      const message = source[index];

      if (message !== undefined) {
        view.push(renderMessage(state, message, index));
      }
    }

    return view;
  }
  for (let index = 0; index < source.length; index += 1) {
    const message = source[index];

    if (message !== undefined) {
      view.push(renderMessage(state, message, index));
    }
  }

  return view;
};

/**
 * Prune selection: the new `clearedThrough` bound. Walks tool messages
 * newest→oldest, always protecting the most recent tool message (the model
 * has not reacted to it yet), then protecting older ones while their
 * estimates fit inside `keepRecentTokens`. Never decreases the bound.
 */
export const choosePruneBound = (
  source: ReadonlyArray<Prompt.Message>,
  state: ContextCompactionState,
  keepRecentTokens: number,
): number => {
  const toolIndices: Array<number> = [];

  for (let index = 0; index < source.length; index += 1) {
    if (source[index]?.role === "tool" && !isProtected(state, source, index)) {
      toolIndices.push(index);
    }
  }
  if (toolIndices.length <= 1) {
    return state.clearedThrough;
  }
  let budget = keepRecentTokens;
  let newestCleared = -1;

  for (let position = toolIndices.length - 1; position >= 0; position -= 1) {
    const index = toolIndices[position];

    if (index === undefined) continue;
    const message = source[index];

    if (message === undefined) continue;
    if (position === toolIndices.length - 1) {
      budget -= estimateMessageTokens(message);
      continue;
    }
    const cost = estimateMessageTokens(message);

    if (budget - cost >= 0 && index >= state.clearedThrough) {
      budget -= cost;
      continue;
    }
    newestCleared = index;
    break;
  }

  return newestCleared === -1
    ? state.clearedThrough
    : Math.max(state.clearedThrough, newestCleared + 1);
};

/**
 * Summarize selection: the source cut index `C` — messages `[0, C)` outside
 * the protected block fold into the summary; `[C, …)` is the kept tail.
 * Walks newest→oldest accumulating view estimates until `keepRecentTokens`
 * is retained, then moves the cut down so no tool message is separated from
 * the assistant message that declared its calls.
 */
export const chooseSummarizeCut = (
  source: ReadonlyArray<Prompt.Message>,
  state: ContextCompactionState,
  keepRecentTokens: number,
): number => {
  let kept = 0;
  let cut = 0;

  for (let index = source.length - 1; index >= 0; index -= 1) {
    const message = source[index];

    if (message === undefined) continue;
    kept += estimateMessageTokens(renderMessage(state, message, index));
    if (kept >= keepRecentTokens) {
      cut = index;
      break;
    }
  }
  // A tool message must stay with its declaring assistant message: walk the
  // cut down across any tool messages so pairing is never split.
  while (cut > 0 && source[cut]?.role === "tool") {
    cut -= 1;
  }

  return Math.max(cut, state.replacement?.through ?? 0);
};

/**
 * The messages a cut NEWLY covers — from the prior replacement boundary to the
 * cut, protected block excluded, in order. Starting at the replacement boundary
 * keeps repeated compactions incremental: the previously covered span lives
 * on only through the previous summary, never as resubmitted raw history.
 */
export const collectCoveredMessages = (
  source: ReadonlyArray<Prompt.Message>,
  state: ContextCompactionState,
  cut: number,
): ReadonlyArray<Prompt.Message> => {
  const covered: Array<Prompt.Message> = [];

  for (
    let index = Math.max(0, state.replacement?.through ?? 0);
    index < cut && index < source.length;
    index += 1
  ) {
    const message = source[index];

    if (message !== undefined && !isProtected(state, source, index)) {
      covered.push(message);
    }
  }

  return covered;
};

const SUMMARY_TOOL_RESULT_CLIP = 2_000;

const jsonPreview = (value: unknown, limit: number): string => {
  if (typeof value === "string") return JSON.stringify(value.slice(0, limit)).slice(0, limit);
  const snapshot = boundedCanonicalJsonSnapshot(value, limit);

  return snapshot === undefined
    ? "[omitted oversized or unsupported JSON]"
    : JSON.stringify(snapshot.value);
};

const partText = (part: Prompt.Message["content"][number] | string): string => {
  if (typeof part === "string") return part;
  switch (part.type) {
    case "text": {
      return part.text;
    }
    case "reasoning": {
      return "";
    }
    case "tool-call": {
      return `[tool call ${part.name.slice(0, 500)} ${jsonPreview(part.params, 500)}]`;
    }
    case "tool-result": {
      return `[tool result ${part.name.slice(0, 500)}: ${jsonPreview(part.result, SUMMARY_TOOL_RESULT_CLIP)}]`;
    }
    default: {
      return `[${part.type}]`;
    }
  }
};

/**
 * Character budget for the complete default summarizer request, including
 * its instruction, transcript delimiters, previous summary, and elision marker.
 */
export const SUMMARY_INPUT_BUDGET = 80_000;
export const SUMMARY_MAX_LENGTH = 65_536;
export const SUMMARY_REQUEST_PREFIX = `${COMPACTION_INSTRUCTION}\n\n<transcript>\n`;
export const SUMMARY_REQUEST_SUFFIX = "\n</transcript>";

const SUMMARY_MESSAGE_CLIP = 2_000;
const SUMMARY_ELISION_RESERVE = 128;

const summaryBlock = (message: Prompt.Message): string => {
  let text = "";

  if (typeof message.content === "string") {
    text = message.content.slice(0, SUMMARY_MESSAGE_CLIP + 1);
  } else {
    for (const part of message.content) {
      const piece = partText(part);

      if (piece.length === 0) continue;
      if (text.length > 0) text += "\n";
      text += piece.slice(0, SUMMARY_MESSAGE_CLIP + 1 - text.length);
      if (text.length > SUMMARY_MESSAGE_CLIP) break;
    }
  }
  if (text.length === 0) return "";

  return `[${message.role}]\n${text.length > SUMMARY_MESSAGE_CLIP ? `${text.slice(0, SUMMARY_MESSAGE_CLIP)}…` : text}`;
};

/**
 * Deterministic emergency input record, not a progress summary. Keep recent user inputs and
 * the last, as-yet-unconsumed Tool batch. Clip before rendering large payloads, identify Tool
 * calls for archive lookup, and never recursively copy earlier recovery records.
 */
export const buildRolloverHandoff = (
  source: ReadonlyArray<Prompt.Message>,
  state: ContextCompactionState,
  targetTokens: number | undefined,
  through = source.length,
): string | undefined => {
  const protectedTokens = estimatePromptTokens(
    source.filter((_, index) => index >= through || isProtected(state, source, index)),
  );

  const maxChars = Math.min(
    5_000,
    Math.max(0, Math.floor(((targetTokens ?? 10_000) - protectedTokens - 200) * 2)),
  );

  if (maxChars < 160) return undefined;
  const blocks: Array<string> = [];
  let length = 0;
  const start = state.replacement?.through ?? 0;

  const lastTool = source.findLastIndex(
    (message, index) => index < through && message.role === "tool",
  );

  const unconsumed =
    lastTool >= start &&
    !source.slice(lastTool + 1, through).some((message) => message.role === "assistant");

  let lastAssistant = lastTool;

  while (lastAssistant >= start && source[lastAssistant]?.role !== "assistant") lastAssistant -= 1;
  for (let index = through - 1; index >= start; index -= 1) {
    const message = source[index];

    if (message === undefined || isProtected(state, source, index)) continue;
    const isUnconsumedBatch = unconsumed && index >= lastAssistant && index <= lastTool;

    if (message.role !== "user" && !isUnconsumedBatch) continue;
    // A projected boundary from a previous Attempt is already a derived handoff.
    if (
      message.role === "user" &&
      message.content.some(
        (part) => part.type === "text" && part.text.startsWith(CONTEXT_ROLLOVER_PREFIX),
      )
    )
      continue;
    let block = summaryBlock(message);

    if (isUnconsumedBatch && typeof message.content !== "string") {
      const ids = message.content
        .flatMap((part) =>
          part.type === "tool-call" || part.type === "tool-result" ? [part.id.slice(0, 256)] : [],
        )
        .slice(0, 16);

      block = `[Unconsumed batch; history search call IDs: ${ids.join(", ")}]\n${block}`;
    }
    const remaining = maxChars - length - 120;

    if (remaining <= 0) break;
    const selected = block.slice(0, Math.min(2_500, remaining));

    blocks.unshift(selected);
    length += selected.length + 2;
    if (selected.length < block.length) break;
  }

  return `Recovery excerpts, not verified progress. Content may be omitted; use notes and history.\n\n${blocks.join("\n\n")}`.slice(
    0,
    maxChars,
  );
};

/**
 * Render the covered span as plain text for the summarizer call, bounded
 * twice: every message's rendered text is clipped to a fixed length (tool
 * results additionally clip their JSON payloads), and the whole render obeys
 * `SUMMARY_INPUT_BUDGET` with middle-out retention — oldest and newest
 * blocks survive, the middle collapses into one deterministic elision
 * marker. A previous summary is folded in first so nothing silently drops
 * out of coverage across repeated compactions. An oversized previous summary
 * returns undefined before rendering: callers must reject it, never truncate it.
 */
export const renderForSummary = (
  covered: ReadonlyArray<Prompt.Message>,
  previousSummary: string | undefined,
): string | undefined => {
  if (previousSummary !== undefined && previousSummary.length > SUMMARY_MAX_LENGTH)
    return undefined;
  const joiner = "\n\n";

  const previousBlock =
    previousSummary === undefined ? undefined : `[Previous summary]\n${previousSummary}`;

  const fixed =
    (previousBlock === undefined ? 0 : previousBlock.length + joiner.length) +
    SUMMARY_REQUEST_PREFIX.length +
    SUMMARY_REQUEST_SUFFIX.length;

  const budget = SUMMARY_INPUT_BUDGET - fixed - SUMMARY_ELISION_RESERVE;
  const head: Array<string> = [];
  const tail: Array<string> = [];
  let length = 0;
  let start = 0;
  let end = covered.length - 1;
  let boundaryBlock: string | undefined;

  while (start <= end) {
    const message = covered[start];
    const block = message === undefined ? "" : summaryBlock(message);

    if (block.length > 0 && length + block.length + joiner.length > Math.floor(budget / 2)) {
      boundaryBlock = block;
      break;
    }
    if (block.length > 0) {
      head.push(block);
      length += block.length + joiner.length;
    }
    start += 1;
  }
  while (end >= start) {
    const message = covered[end];

    const block =
      end === start && boundaryBlock !== undefined
        ? boundaryBlock
        : message === undefined
          ? ""
          : summaryBlock(message);

    if (block.length > 0 && length + block.length + joiner.length > budget) break;
    if (block.length > 0) {
      tail.push(block);
      length += block.length + joiner.length;
    }
    end -= 1;
  }
  const omitted = end - start + 1;
  const lines: Array<string> = [];

  if (previousBlock !== undefined) {
    lines.push(previousBlock);
  }
  lines.push(...head);
  if (omitted > 0) lines.push(`[… ${omitted} messages omitted from summary input …]`);
  lines.push(...tail.reverse());

  return lines.join(joiner);
};
