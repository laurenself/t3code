/**
 * Claude Code keeps one file per running process under `<config dir>/sessions`,
 * naming the session it holds and whether it is mid-turn. T3 reads them before
 * it takes a turn on a session, because two processes writing one session each
 * continue from their own last message and split the conversation.
 */
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import { isProcessAlive } from "../../serverRuntimeState.ts";

const ClaudeProcessRecord = Schema.Struct({
  pid: Schema.Number,
  sessionId: Schema.String,
  status: Schema.optional(Schema.String),
  entrypoint: Schema.optional(Schema.String),
});
const decodeRecord = Schema.decodeUnknownOption(Schema.fromJsonString(ClaudeProcessRecord));

/** The entrypoint T3 gives the Claude processes it starts; those are T3's own to manage. */
const T3_ENTRYPOINT = "t3-code";
const EXIT_POLL = Duration.millis(100);
const EXIT_POLLS = 50;

export type ClaudeTerminalClaim =
  | { readonly _tag: "Free" }
  /** A terminal is running a turn on the session; taking it would cut that turn off. */
  | { readonly _tag: "Busy"; readonly pid: number }
  /** An idle terminal did not exit when asked. */
  | { readonly _tag: "Stuck"; readonly pid: number };

/**
 * Makes T3 the only process on a Claude session. A terminal sitting idle on it
 * is closed (`claude --resume` reopens it with everything T3 adds); one that is
 * mid-turn is left alone and reported.
 */
export const claimClaudeSession = Effect.fn("claimClaudeSession")(function* (input: {
  readonly configDir: string;
  readonly sessionId: string;
}) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = path.join(input.configDir, "sessions");
  const names = yield* fileSystem.readDirectory(directory).pipe(Effect.orElseSucceed(() => []));
  for (const name of names) {
    if (!name.endsWith(".json")) continue;
    const record = yield* fileSystem.readFileString(path.join(directory, name)).pipe(
      Effect.map(decodeRecord),
      Effect.orElseSucceed(() => Option.none()),
    );
    if (
      Option.isNone(record) ||
      record.value.sessionId !== input.sessionId ||
      record.value.entrypoint === T3_ENTRYPOINT ||
      !isProcessAlive(record.value.pid)
    ) {
      continue;
    }
    const pid = record.value.pid;
    if (record.value.status !== "idle") {
      return { _tag: "Busy", pid } satisfies ClaudeTerminalClaim;
    }
    yield* Effect.sync(() => {
      try {
        process.kill(pid, "SIGTERM");
      } catch {
        // It exited on its own between the read and the signal.
      }
    });
    let exited = !isProcessAlive(pid);
    for (let poll = 0; !exited && poll < EXIT_POLLS; poll++) {
      yield* Effect.sleep(EXIT_POLL);
      exited = !isProcessAlive(pid);
    }
    if (!exited) return { _tag: "Stuck", pid } satisfies ClaudeTerminalClaim;
  }
  return { _tag: "Free" } satisfies ClaudeTerminalClaim;
});
