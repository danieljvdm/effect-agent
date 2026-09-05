/** Public Compaction API. Implementation helpers remain private. */
export {
  CLEARED_TOOL_RESULT,
  COMPACTION_SUMMARY_PREFIX,
  CONTEXT_ROLLOVER_PREFIX,
  contextWindowId,
  contextWindowMessage,
  estimateMessageTokens,
  estimatePromptTokens,
  type ContextCompactionState,
} from "./internal/compaction.ts";
