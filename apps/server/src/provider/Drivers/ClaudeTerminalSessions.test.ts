// @effect-diagnostics nodeBuiltinImport:off -- the terminal being closed is a real process.
import * as NodeChildProcess from "node:child_process";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { isProcessAlive } from "../../serverRuntimeState.ts";
import { claimClaudeSession } from "./ClaudeTerminalSessions.ts";

const sessionId = "0b0f2a52-7c1c-4a53-9d4f-2f6f5f7f9a10";

const withSessions = <A, E>(
  records: ReadonlyArray<Record<string, unknown>>,
  use: (configDir: string) => Effect.Effect<A, E, FileSystem.FileSystem | Path.Path>,
) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const configDir = yield* fileSystem.makeTempDirectoryScoped();
    yield* fileSystem.makeDirectory(path.join(configDir, "sessions"));
    for (const [index, record] of records.entries()) {
      yield* fileSystem.writeFileString(
        path.join(configDir, "sessions", `${index}.json`),
        JSON.stringify(record),
      );
    }
    return yield* use(configDir);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer));

it.effect("a session no live terminal holds is free", () =>
  withSessions(
    [
      { pid: 2 ** 30, sessionId, status: "idle", entrypoint: "cli" },
      { pid: process.pid, sessionId: "another-session", status: "busy", entrypoint: "cli" },
      // T3's own Claude processes are not terminals.
      { pid: process.pid, sessionId, status: "busy", entrypoint: "t3-code" },
    ],
    (configDir) =>
      Effect.gen(function* () {
        expect(yield* claimClaudeSession({ configDir, sessionId })).toEqual({ _tag: "Free" });
      }),
  ),
);

it.effect("a terminal that is mid-turn keeps the session", () =>
  withSessions([{ pid: process.pid, sessionId, status: "busy", entrypoint: "cli" }], (configDir) =>
    Effect.gen(function* () {
      expect(yield* claimClaudeSession({ configDir, sessionId })).toEqual({
        _tag: "Busy",
        pid: process.pid,
      });
    }),
  ),
);

it.live("an idle terminal is closed so T3 is the only writer", () =>
  Effect.gen(function* () {
    const child = yield* Effect.acquireRelease(
      Effect.sync(() => NodeChildProcess.spawn("sleep", ["60"], { stdio: "ignore" })),
      (spawned) => Effect.sync(() => spawned.kill("SIGKILL")),
    );
    const pid = child.pid ?? 0;
    expect(isProcessAlive(pid)).toBe(true);
    yield* withSessions([{ pid, sessionId, status: "idle", entrypoint: "cli" }], (configDir) =>
      Effect.gen(function* () {
        expect(yield* claimClaudeSession({ configDir, sessionId })).toEqual({ _tag: "Free" });
      }),
    );
    expect(isProcessAlive(pid)).toBe(false);
  }).pipe(Effect.scoped),
);
