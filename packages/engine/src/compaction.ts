import { Prompt } from "effect/unstable/ai";

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
 * each Turn), which grows append-only within an Attempt, so recorded indices
 * stay valid across Turns.
 */
export interface ContextCompactionState {
  /**
   * Source-index bounds `[start, end)` of the protected instruction/input
   * block. `-1` when the source is hook-prepared (durable resume): the block
   * cannot be located by index there, so protection falls back to
   * system-role messages.
   */
  protectedStart: number;
  protectedEnd: number;
  /** Tool messages at source index below this bound render as cleared. */
  clearedThrough: number;
  /**
   * Source messages below this bound (outside the protected block) are
   * replaced by the summary message. `0` means no summarize compaction yet.
   */
  summarizedThrough: number;
  summary: string | undefined;
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
  summarizedThrough: 0,
  summary: undefined,
  lastCompactionTurn: 0,
  overflowRetryTurn: 0,
  lastViewLength: -1,
});

const isProtected = (
  state: ContextCompactionState,
  source: ReadonlyArray<Prompt.Message>,
  index: number,
): boolean => {
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
  if (state.summary === undefined && state.clearedThrough === 0) {
    return source;
  }
  const view: Array<Prompt.Message> = [];
  if (state.summary !== undefined && state.summarizedThrough > 0) {
    for (let index = 0; index < state.summarizedThrough && index < source.length; index += 1) {
      const message = source[index];
      if (message !== undefined && isProtected(state, source, index)) {
        view.push(renderMessage(state, message, index));
      }
    }
    view.push(summaryMessage(state.summary));
    for (let index = state.summarizedThrough; index < source.length; index += 1) {
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
  return Math.max(cut, state.summarizedThrough);
};

/**
 * The messages a cut NEWLY covers — from the prior summarize watermark to the
 * cut, protected block excluded, in order. Starting at `summarizedThrough`
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
    let index = Math.max(0, state.summarizedThrough);
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
      let params: string;
      try {
        params = JSON.stringify(part.params) ?? "";
      } catch {
        params = "";
      }
      return `[tool call ${part.name} ${params.slice(0, 500)}]`;
    }
    case "tool-result": {
      let result: string;
      try {
        result = JSON.stringify(part.result) ?? "";
      } catch {
        result = "";
      }
      return `[tool result ${part.name}: ${result.slice(0, SUMMARY_TOOL_RESULT_CLIP)}]`;
    }
    default: {
      return `[${part.type}]`;
    }
  }
};

/**
 * Total character budget for one summarizer request, the fixed instruction
 * and previous summary counted first. Deterministic and provider-agnostic:
 * ~80k chars is ~20k tokens, far inside every supported window, so the
 * overflow-recovery summarization can never itself overflow.
 */
export const SUMMARY_INPUT_BUDGET = 80_000;

const SUMMARY_MESSAGE_CLIP = 2_000;
const SUMMARY_ELISION_RESERVE = 128;

/**
 * Render the covered span as plain text for the summarizer call, bounded
 * twice: every message's rendered text is clipped to a fixed length (tool
 * results additionally clip their JSON payloads), and the whole render obeys
 * `SUMMARY_INPUT_BUDGET` with middle-out retention — oldest and newest
 * blocks survive, the middle collapses into one deterministic elision
 * marker. A previous summary is folded in first so nothing silently drops
 * out of coverage across repeated compactions.
 */
export const renderForSummary = (
  covered: ReadonlyArray<Prompt.Message>,
  previousSummary: string | undefined,
): string => {
  const joiner = "\n\n";
  const blocks: Array<string> = [];
  for (const message of covered) {
    const text =
      typeof message.content === "string"
        ? message.content
        : message.content
            .map((part) => partText(part))
            .filter((piece) => piece.length > 0)
            .join("\n");
    if (text.length === 0) continue;
    const clipped =
      text.length > SUMMARY_MESSAGE_CLIP ? `${text.slice(0, SUMMARY_MESSAGE_CLIP)}…` : text;
    blocks.push(`[${message.role}]\n${clipped}`);
  }
  const previousBlock =
    previousSummary === undefined ? undefined : `[Previous summary]\n${previousSummary}`;
  const fixed =
    (previousBlock === undefined ? 0 : previousBlock.length + joiner.length) +
    COMPACTION_INSTRUCTION.length;
  const budget = SUMMARY_INPUT_BUDGET - fixed - SUMMARY_ELISION_RESERVE;
  let selected: ReadonlyArray<string> = blocks;
  let total = 0;
  for (const block of blocks) {
    total += block.length + joiner.length;
  }
  if (total > budget) {
    const head: Array<string> = [];
    const tail: Array<string> = [];
    let headLength = 0;
    let tailLength = 0;
    let start = 0;
    let end = blocks.length - 1;
    const half = Math.floor(budget / 2);
    while (start <= end) {
      const candidate = blocks[start];
      if (candidate === undefined || headLength + candidate.length + joiner.length > half) break;
      head.push(candidate);
      headLength += candidate.length + joiner.length;
      start += 1;
    }
    while (end >= start) {
      const candidate = blocks[end];
      if (
        candidate === undefined ||
        headLength + tailLength + candidate.length + joiner.length > budget
      ) {
        break;
      }
      tail.unshift(candidate);
      tailLength += candidate.length + joiner.length;
      end -= 1;
    }
    const omitted = end - start + 1;
    selected =
      omitted > 0
        ? [...head, `[… ${omitted} messages omitted from summary input …]`, ...tail]
        : [...head, ...tail];
  }
  const lines: Array<string> = [];
  if (previousBlock !== undefined) {
    lines.push(previousBlock);
  }
  lines.push(...selected);
  return lines.join(joiner);
};
