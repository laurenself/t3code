import { ThreadId } from "@t3tools/contracts";

const PREFIX = "import:";

/**
 * Threads that mirror a Claude Code, Codex or Pi session started outside T3.
 * The native session stays the source of truth: T3 resumes it, never replaces it,
 * and follows its transcript for turns added elsewhere.
 */
export const nativeSessionThreadId = (input: {
  readonly providerInstanceId: string;
  readonly providerSessionId: string;
}): ThreadId => ThreadId.make(`${PREFIX}${input.providerInstanceId}:${input.providerSessionId}`);

export const isNativeSessionThreadId = (threadId: string): boolean => threadId.startsWith(PREFIX);
