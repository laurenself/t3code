import {
  DEFAULT_MODEL,
  DEFAULT_MODEL_BY_PROVIDER,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_RUNTIME_MODE,
  isRuntimeModeAbove,
  AgentSessionImportProjectChangedError,
  AgentSessionImportProjectNotFoundError,
  AgentSessionScanError,
  AgentSessionSource,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ThreadId,
  TurnItemId,
  type AgentSessionImportInput,
  type AgentSessionImportSource,
  type AgentSessionImportResult,
  type OrchestrationV2AppThread,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import { normalizeProjectPathForComparison } from "@t3tools/shared/path";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import * as Clock from "effect/Clock";
import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ThreadCommandExecutor from "../orchestration-v2/ThreadCommandExecutor.ts";
import * as ProviderSessionManager from "../orchestration-v2/ProviderSessionManager.ts";
import * as TurnItemPositionStore from "../orchestration-v2/TurnItemPositionStore.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";
import * as ServerSettings from "../serverSettings.ts";
import { busyClaudeTerminalSessions } from "../provider/Drivers/ClaudeTerminalSessions.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";
import { nativeSessionThreadId } from "./nativeSessionThread.ts";
import * as ProjectService from "./ProjectService.ts";

const IMPORT_EVENT_PREFIX = "agent-session-import:v2";
/**
 * How often the server re-imports Claude Code / Codex / Pi sessions started
 * outside T3 (in a terminal, an IDE) into every project. "0" turns it off.
 */
const SYNC_INTERVAL_ENV = "T3CODE_AGENT_SESSION_SYNC_INTERVAL";
const DEFAULT_SYNC_INTERVAL = Duration.minutes(2);
const SYNC_STARTUP_DELAY = Duration.seconds(20);
const SYNC_PROJECT_PAGE_SIZE = 100;
const MAX_IGNORED_TRANSCRIPTS = 10_000;
/** A turn writes its records in a burst; one sync after it settles reads them all. */
const TRANSCRIPT_CHANGE_DEBOUNCE = Duration.seconds(1);
/** A changed file no project claims is looked for again only after this long. */
const UNPLACED_TRANSCRIPT_RETRY_MS = 15_000;
/** A run that can still write to its native session. */
const OPEN_RUN_STATUSES: ReadonlySet<string> = new Set([
  "preparing",
  "queued",
  "starting",
  "running",
  "waiting",
]);
const BUSY_SESSIONS_CACHE_MS = 500;
const PROCESS_CHANGE_DEBOUNCE = Duration.millis(300);
/** An imported session this recent is still in play, so it is not filed under Settled. */
const RECENT_SESSION_MS = 24 * 60 * 60 * 1000;
/** A Codex turn with no ending and no writes for this long was cut off. */
const STALLED_TURN_MS = 15 * 60 * 1000;
/** Transcript records land slightly outside the run that wrote them. */
const RUN_WINDOW_SLACK_MS = 2_000;
const CLAUDE_SESSION_ID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

class AgentSessionUnresumableSessionError extends Schema.TaggedError<AgentSessionUnresumableSessionError>()(
  "AgentSessionUnresumableSessionError",
  {
    source: AgentSessionSource,
    providerSessionId: Schema.String,
  },
) {
  override get message(): string {
    return `Session '${this.providerSessionId}' from '${this.source}' cannot be resumed.`;
  }
}

class AgentSessionThreadProjectConflictError extends Schema.TaggedError<AgentSessionThreadProjectConflictError>()(
  "AgentSessionThreadProjectConflictError",
  {
    threadId: ThreadId,
    expectedProjectId: ProjectId,
    actualProjectId: ProjectId,
  },
) {
  override get message(): string {
    return `Imported thread '${this.threadId}' belongs to project '${this.actualProjectId}', not '${this.expectedProjectId}'.`;
  }
}

class AgentSessionThreadModifiedError extends Schema.TaggedError<AgentSessionThreadModifiedError>()(
  "AgentSessionThreadModifiedError",
  { threadId: ThreadId },
) {
  override get message(): string {
    return `Imported thread '${this.threadId}' already contains non-imported activity.`;
  }
}

// ACP imports read the history while holding the session-mutation lock, and an agent can
// leave session/resume or initialize unanswered. Longer than session/load's own deadline.
const NATIVE_HISTORY_READ_TIMEOUT = "2 minutes";

/**
 * Whether a thread bound to a native session still lacks its native history.
 * Once a turn has run, the native session also holds that turn's messages, so
 * its history can no longer be imported as run-less items.
 */
export function isNativeHistoryPending(input: {
  readonly historyOrigin: OrchestrationV2AppThread["historyOrigin"];
  readonly hasRun: boolean;
}): boolean {
  return input.historyOrigin !== "v1_import" && !input.hasRun;
}

const TITLE_SEPARATOR = " · ";
/** The thread list shows no folder, so a mirrored session's title leads with its project. */
const nativeSessionTitle = (project: string, session: string) =>
  `${project}${TITLE_SEPARATOR}${session.trim() === "" ? "Untitled thread" : session}`;

function dateTime(value: string): DateTime.Utc {
  return DateTime.makeUnsafe(value);
}

type HistoryMessage = Omit<AgentSessionScanner.AgentSessionThreadMessage, "role"> & {
  readonly role: AgentSessionScanner.AgentSessionThreadMessage["role"] | "reasoning";
};

/**
 * Names a turn the transcript gained after the import. The transcript is
 * append-only, so a record keeps its timestamp and role on every read.
 */
function syncedMessageKey(message: AgentSessionScanner.AgentSessionThreadMessage): string {
  return `sync:${message.createdAt}:${message.role}`;
}

function messageEvents(
  input: {
    readonly threadId: ThreadId;
    readonly message: HistoryMessage;
  } & ({ readonly index: number } | { readonly key: string }),
): ReadonlyArray<OrchestrationV2DomainEvent> {
  const ordinal = "index" in input ? input.index + 1 : 1;
  const suffix = "index" in input ? String(input.index).padStart(6, "0") : input.key;
  const messageId = MessageId.make(`${input.threadId}:${suffix}`);
  const turnItemId = TurnItemId.make(
    `${IMPORT_EVENT_PREFIX}:turn-item:${input.threadId}:${suffix}`,
  );
  const at = dateTime(input.message.createdAt);
  const common = {
    id: turnItemId,
    threadId: input.threadId,
    runId: null,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal,
    status: "completed" as const,
    title: null,
    startedAt: at,
    completedAt: at,
    updatedAt: at,
  };
  const turnItemEvent = (payload: OrchestrationV2TurnItem): OrchestrationV2DomainEvent => ({
    id: EventId.make(turnItemId),
    type: "turn-item.updated",
    threadId: input.threadId,
    occurredAt: at,
    payload,
  });
  if (input.message.role === "reasoning") {
    return [
      turnItemEvent({ ...common, type: "reasoning", text: input.message.text, streaming: false }),
    ];
  }
  const message: OrchestrationV2ConversationMessage = {
    createdBy: input.message.role === "user" ? "user" : "agent",
    creationSource: "server",
    id: messageId,
    threadId: input.threadId,
    runId: null,
    nodeId: null,
    role: input.message.role,
    text: input.message.text,
    attachments: [],
    streaming: false,
    createdAt: at,
    updatedAt: at,
  };
  const turnItem: OrchestrationV2TurnItem =
    input.message.role === "user"
      ? {
          ...common,
          createdBy: "user",
          creationSource: "server",
          type: "user_message",
          messageId,
          inputIntent: "turn_start",
          text: input.message.text,
          attachments: [],
        }
      : {
          ...common,
          type: "assistant_message",
          messageId,
          text: input.message.text,
          streaming: false,
        };
  return [
    {
      id: EventId.make(`${IMPORT_EVENT_PREFIX}:message:${input.threadId}:${suffix}`),
      type: "message.updated",
      threadId: input.threadId,
      occurredAt: at,
      payload: message,
    },
    turnItemEvent(turnItem),
  ];
}

const make = Effect.gen(function* () {
  const scanner = yield* AgentSessionScanner.AgentSessionScanner;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const projects = yield* ProjectService.ProjectService;
  const eventSink = yield* EventSink.EventSinkV2;
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const providerSessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const turnItemPositions = yield* TurnItemPositionStore.TurnItemPositionStoreV2;
  const sql = yield* SqlClient.SqlClient;
  const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
  const registry = yield* ProviderAdapterRegistry.ProviderAdapterRegistryV2;
  const threadCommands = yield* ThreadCommandExecutor.ThreadCommandExecutor;
  const settingsService = yield* ServerSettings.ServerSettingsService;
  const ceiling = settingsService.managedPolicy?.limits?.maxRuntimeMode;
  // Transcripts of threads T3 itself runs. Their turns rewrite the file constantly, so
  // without this every sync tick would re-read them only to find them owned again.
  const nativeTranscripts = new Map<string, ThreadId | null>();
  // Native transcripts that changed while T3 had no run on them: a terminal may have
  // continued the session, so the next pass reads them once.
  const changedNativeTranscripts = new Set<string>();
  const markNative = (filePath: string, owner: ThreadId | null) => {
    if (nativeTranscripts.size >= MAX_IGNORED_TRANSCRIPTS) nativeTranscripts.clear();
    nativeTranscripts.set(filePath, owner);
    changedNativeTranscripts.delete(filePath);
  };
  const skipTranscript = (transcript: { readonly filePath: string }) =>
    nativeTranscripts.has(transcript.filePath) &&
    !changedNativeTranscripts.has(transcript.filePath);
  /** The thread T3 itself started on this native session, if any. */
  const findNativeOwner = (thread: AgentSessionScanner.AgentSessionThread, nativeId: string) =>
    sql<{ readonly threadId: string }>`
      SELECT thread_id AS "threadId"
      FROM orchestration_v2_projection_provider_threads
      WHERE driver = ${thread.source}
        AND provider_instance_id = ${thread.providerInstanceId}
        AND thread_id IS NOT NULL
        AND json_extract(payload_json, '$.nativeThreadRef.nativeId') = ${nativeId}
      LIMIT 1
    `.pipe(Effect.map((rows) => rows[0]?.threadId));
  const claudeConfigDirs = new Set<string>();
  // Claude's process files are read once per burst of work, not once per thread.
  const busyClaudeCache = new Map<string, { readonly at: number; readonly busy: Set<string> }>();
  const busyClaudeSessions = Effect.fn("agentSessionBusyClaudeSessions")(function* (
    configDir: string,
  ) {
    const now = yield* Clock.currentTimeMillis;
    const cached = busyClaudeCache.get(configDir);
    if (cached !== undefined && now - cached.at < BUSY_SESSIONS_CACHE_MS) return cached.busy;
    const busy = yield* busyClaudeTerminalSessions(configDir).pipe(
      Effect.provideService(FileSystem.FileSystem, fileSystem),
      Effect.provideService(Path.Path, path),
    );
    busyClaudeCache.set(configDir, { at: now, busy });
    return busy;
  });
  // Which project each transcript belongs to, so a change to one file syncs one project.
  const transcriptProjects = new Map<string, ProjectId>();
  /**
   * Appends the turns a native session gained outside T3 (in a terminal) to the
   * thread that mirrors it. Turns T3 ran itself are already in the thread as
   * runs, so records written inside a run are left out. Returns how many it added,
   * or null while a run is open: its records are still arriving, and the next pass reads them.
   */
  const appendOutsideTurns = Effect.fn("appendOutsideAgentTurnsV2")(function* (input: {
    readonly threadId: ThreadId;
    readonly thread: AgentSessionScanner.AgentSessionThread;
    /**
     * Take only records whose writer the transcript names. A thread T3 started
     * has no imported history to tell its own early records from a terminal's.
     */
    readonly namedWritersOnly?: boolean;
  }) {
    const records = yield* orchestrator.getThreadRecords(input.threadId, [
      "messages",
      "runs",
      "providerSessions",
    ]);
    if (records.runs.some((run) => OPEN_RUN_STATUSES.has(run.status))) return null;
    const millis = (value: DateTime.Utc) => DateTime.toEpochMillis(value);
    const runWindows = records.runs.map((run) => ({
      from: millis(run.requestedAt) - RUN_WINDOW_SLACK_MS,
      to: millis(run.completedAt ?? run.startedAt ?? run.requestedAt) + RUN_WINDOW_SLACK_MS,
    }));
    // Everything up to the newest mirrored message is already in the thread.
    let mirroredThrough = 0;
    for (const message of records.messages) {
      if (message.runId === null) {
        mirroredThrough = Math.max(mirroredThrough, millis(message.createdAt));
      }
    }
    const seen = new Set<string>();
    const outside = input.thread.messages.filter((message) => {
      // A record with no timestamp of its own takes the file's, which moves on every read.
      if (message.writtenByT3 === true || message.createdAt === input.thread.updatedAt) {
        return false;
      }
      if (input.namedWritersOnly === true && message.writtenByT3 === undefined) return false;
      const at = millis(dateTime(message.createdAt));
      if (at <= mirroredThrough) return false;
      // Without the writer's name, a record written while T3 ran a turn is taken as T3's.
      if (
        message.writtenByT3 === undefined &&
        runWindows.some((window) => at >= window.from && at <= window.to)
      ) {
        return false;
      }
      const key = syncedMessageKey(message);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
    if (outside.length === 0) return 0;

    const events = outside.flatMap((message) =>
      messageEvents({ threadId: input.threadId, key: syncedMessageKey(message), message }),
    );
    // Items with no run sort ahead of every run. These turns happened after the
    // thread's runs, so they take the end of the latest run's range instead.
    const latestRunOrdinal = records.runs.reduce((latest, run) => Math.max(latest, run.ordinal), 0);
    if (latestRunOrdinal > 0) {
      for (const event of events) {
        if (event.type !== "turn-item.updated") continue;
        yield* turnItemPositions.allocate({
          threadId: input.threadId,
          turnItemId: event.payload.id,
          runId: null,
          runOrdinal: latestRunOrdinal,
        });
      }
    }
    yield* eventSink.write({ events });
    // An agent process T3 still holds for this thread loaded the session before
    // these turns. Letting it go makes the next message resume from the transcript.
    for (const session of records.providerSessions) {
      yield* providerSessions
        .detach({
          providerSessionId: session.id,
          threadId: input.threadId,
          detail: "The session continued outside T3 Code.",
        })
        .pipe(Effect.ignore);
    }
    return outside.length;
  });

  /** Whether a terminal is running a turn on the session this transcript belongs to. */
  const outsideTurnOpen = Effect.fn("agentSessionOutsideTurnOpen")(function* (input: {
    readonly thread: AgentSessionScanner.AgentSessionThread;
    readonly source: AgentSessionImportSource;
  }) {
    if (input.thread.source === "claudeAgent") {
      // <config dir>/projects/<project>/<session>.jsonl
      const configDir = path.resolve(path.dirname(input.source.filePath), "..", "..");
      claudeConfigDirs.add(configDir);
      return (yield* busyClaudeSessions(configDir)).has(input.thread.providerSessionId);
    }
    if (input.thread.turnOpen !== true || input.source.mtimeMs === null) return false;
    return (yield* Clock.currentTimeMillis) - input.source.mtimeMs < STALLED_TURN_MS;
  });

  /**
   * Keeps a mirrored thread's place in the thread list true to its session:
   * working while a terminal runs a turn on it, back in the list when the
   * terminal adds to it, and titled after its project and the session's own title.
   */
  const presentNativeSession = Effect.fn("presentNativeSessionV2")(function* (input: {
    readonly threadId: ThreadId;
    readonly working: boolean;
    readonly continuedOutside: boolean;
    readonly title?: { readonly project: string; readonly session: string };
    readonly lastActiveAt?: DateTime.Utc;
  }) {
    const { thread, runs } = yield* orchestrator.getThreadRecords(input.threadId, ["runs"]);
    const now = yield* DateTime.now;
    const startedAt = thread.outsideTurnStartedAt ?? null;
    const settled = thread.settledOverride === "settled" || thread.settledAt !== null;
    // Filed under Settled by the import itself, not by the user, and recent.
    const filedByImport =
      thread.settledOverride === "settled" &&
      thread.settledAt !== null &&
      DateTime.toEpochMillis(thread.settledAt) === DateTime.toEpochMillis(thread.updatedAt) &&
      input.lastActiveAt !== undefined &&
      DateTime.toEpochMillis(now) - DateTime.toEpochMillis(input.lastActiveAt) < RECENT_SESSION_MS;
    const unsettle =
      settled && (input.continuedOutside || (input.working && startedAt === null) || filedByImport);
    const title =
      input.title !== undefined &&
      // A title the user gave the thread in T3 Code stays.
      (runs.length === 0 ||
        thread.title === input.title.session ||
        thread.title.startsWith("<") ||
        thread.title.startsWith(`${input.title.project}${TITLE_SEPARATOR}`))
        ? nativeSessionTitle(input.title.project, input.title.session)
        : thread.title;
    if (!unsettle && title === thread.title && input.working === (startedAt !== null)) return;
    yield* eventSink.write({
      events: [
        {
          id: EventId.make(
            `${IMPORT_EVENT_PREFIX}:presented:${input.threadId}:${DateTime.toEpochMillis(now)}`,
          ),
          type: unsettle ? "thread.unsettled" : "thread.metadata-updated",
          threadId: input.threadId,
          providerInstanceId: thread.providerInstanceId,
          occurredAt: now,
          payload: {
            ...thread,
            title,
            outsideTurnStartedAt: input.working ? (startedAt ?? now) : null,
            ...(unsettle ? { settledOverride: null, settledAt: null, unsettledAt: now } : {}),
            updatedAt: now,
          },
        },
      ],
    });
  });

  /**
   * Ends the working state of threads whose terminal turn is over. A turn's end
   * does not always touch the transcript, so this also runs when Claude's
   * process files change and on every full pass.
   */
  const settleFinishedOutsideTurns: Effect.Effect<void> = Effect.gen(function* () {
    const marked = yield* sql<{
      readonly threadId: string;
      readonly driver: string | null;
      readonly nativeId: string | null;
    }>`
      SELECT
        thread.thread_id AS "threadId",
        provider_thread.driver AS "driver",
        json_extract(provider_thread.payload_json, '$.nativeThreadRef.nativeId') AS "nativeId"
      FROM orchestration_v2_projection_threads AS thread
      LEFT JOIN orchestration_v2_projection_provider_threads AS provider_thread
        ON provider_thread.provider_thread_id =
          json_extract(thread.payload_json, '$.activeProviderThreadId')
      WHERE json_extract(thread.payload_json, '$.outsideTurnStartedAt') IS NOT NULL
    `;
    if (marked.length === 0) return;
    busyClaudeCache.clear();
    const busyClaude = new Set<string>();
    for (const configDir of claudeConfigDirs) {
      for (const sessionId of yield* busyClaudeSessions(configDir)) busyClaude.add(sessionId);
    }
    const now = yield* Clock.currentTimeMillis;
    for (const row of marked) {
      const threadId = ThreadId.make(row.threadId);
      let working = false;
      if (row.driver === "claudeAgent") {
        working = row.nativeId !== null && busyClaude.has(row.nativeId);
      } else if (row.driver === "codex") {
        // Codex ends a turn in the transcript, which the sync reads. Only a turn
        // that was cut off needs ending here.
        const runtime = yield* runtimes.getByThreadId({ threadId });
        const filePath = Option.isNone(runtime)
          ? undefined
          : Option.getOrUndefined(
              AgentSessionScanner.decodeImportedTranscriptPayload(runtime.value.runtimePayload),
            )?.importedTranscripts?.[0]?.filePath;
        const modifiedAt =
          filePath === undefined
            ? null
            : yield* fileSystem.stat(filePath).pipe(
                Effect.map((info) => Option.getOrNull(info.mtime)?.getTime() ?? null),
                Effect.orElseSucceed(() => null),
              );
        working = modifiedAt !== null && now - modifiedAt < STALLED_TURN_MS;
      }
      if (!working) {
        yield* presentNativeSession({ threadId, working: false, continuedOutside: false });
      }
    }
  }).pipe(
    Effect.catchCause((cause) =>
      Effect.logDebug("Could not settle finished terminal turns", { cause }),
    ),
  );

  const importRecentAgentThreads = Effect.fn("importRecentAgentThreadsV2")(function* (
    input: AgentSessionImportInput,
  ) {
    const project = yield* projects.getById(input.projectId).pipe(
      Effect.mapError((cause) => new AgentSessionScanError({ operation: "read-projects", cause })),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(new AgentSessionImportProjectNotFoundError({ projectId: input.projectId })),
          onSome: Effect.succeed,
        }),
      ),
    );
    if (
      input.expectedWorkspaceRoot !== undefined &&
      normalizeProjectPathForComparison(project.workspaceRoot) !==
        normalizeProjectPathForComparison(input.expectedWorkspaceRoot)
    ) {
      return yield* new AgentSessionImportProjectChangedError({ projectId: input.projectId });
    }
    // Imports run in full access, except under a policy ceiling: then in the project's default,
    // which the policy keeps under it, so imported threads never sit above the ceiling.
    const runtimeMode =
      ceiling === undefined
        ? DEFAULT_RUNTIME_MODE
        : yield* settingsService.getSettings.pipe(
            Effect.mapError(
              (cause) => new AgentSessionScanError({ operation: "read-settings", cause }),
            ),
            Effect.map((settings) => {
              const mode = resolveProjectSettings(settings, input.projectId, project).settings
                .defaultRuntimeMode;
              return isRuntimeModeAbove(mode, ceiling) ? ceiling : mode;
            }),
          );
    const runtimeRows = yield* runtimes
      .list()
      .pipe(
        Effect.mapError(
          (cause) => new AgentSessionScanError({ operation: "read-projects", cause }),
        ),
      );
    // Threads mirrored before titles led with the project are read once more to be retitled.
    const untitled = new Set(
      (yield* sql<{ readonly threadId: string }>`
        SELECT thread.thread_id AS "threadId"
        FROM orchestration_v2_projection_threads AS thread
        WHERE thread.project_id = ${input.projectId}
          AND thread.thread_id LIKE 'import:%'
          AND instr(thread.title, ${TITLE_SEPARATOR}) = 0
          AND NOT EXISTS (
            SELECT 1 FROM orchestration_v2_projection_runs AS run
            WHERE run.thread_id = thread.thread_id
          )
      `.pipe(
        Effect.mapError(
          (cause) => new AgentSessionScanError({ operation: "read-projects", cause }),
        ),
      )).map((row) => row.threadId),
    );
    const completedSources = runtimeRows.flatMap((runtime) => {
      const payload = AgentSessionScanner.decodeImportedTranscriptPayload(runtime.runtimePayload);
      if (
        untitled.has(runtime.threadId) ||
        Option.isNone(payload) ||
        payload.value.cwd === undefined ||
        normalizeProjectPathForComparison(payload.value.cwd) !==
          normalizeProjectPathForComparison(project.workspaceRoot)
      ) {
        return [];
      }
      return payload.value.importedTranscripts ?? [];
    });
    const outcomes = scanner.recentThreads(project.workspaceRoot, completedSources, {
      skipTranscript,
    });
    const importedThreadIds = new Set<ThreadId>();
    let importedCount = 0;
    let skippedCount = 0;

    yield* Stream.runForEach(outcomes, (outcome) =>
      Effect.gen(function* () {
        if (outcome._tag === "Skipped") {
          skippedCount += 1;
          return;
        }
        const source = outcome.source;
        if (transcriptProjects.size >= MAX_IGNORED_TRANSCRIPTS) transcriptProjects.clear();
        transcriptProjects.set(source.filePath, input.projectId);
        const threadId = nativeSessionThreadId(source);
        if (outcome._tag === "AlreadyImported") {
          importedThreadIds.add(threadId);
          importedCount += 1;
          return;
        }
        if (outcome._tag === "Duplicate") {
          if (importedThreadIds.has(threadId)) {
            yield* runtimes.recordImportedTranscript({ threadId, source }).pipe(Effect.ignore);
          }
          return;
        }

        const imported = yield* Effect.gen(function* () {
          const thread = outcome.thread;
          if (
            thread.source === "claudeAgent" &&
            !CLAUDE_SESSION_ID_PATTERN.test(thread.providerSessionId)
          ) {
            return yield* new AgentSessionUnresumableSessionError({
              source: thread.source,
              providerSessionId: thread.providerSessionId,
            });
          }
          const driver = ProviderDriverKind.make(thread.source);
          // Pi resumes by session file path; its session UUID only names the thread.
          const nativeThreadId =
            thread.source === "pi" ? source.filePath : thread.providerSessionId;
          const persistRuntime = runtimes.upsert(
            {
              threadId,
              providerName: driver,
              providerInstanceId: thread.providerInstanceId,
              adapterKey: driver,
              runtimeMode,
              status: "stopped",
              lastSeenAt: thread.updatedAt,
              resumeCursor:
                thread.source === "claudeAgent"
                  ? { threadId, resume: thread.providerSessionId }
                  : { threadId: nativeThreadId },
              runtimePayload: { cwd: outcome.worktree?.path ?? project.workspaceRoot },
            },
            { onConflict: "ignore" },
          );
          const existing = yield* Effect.option(orchestrator.getThreadRecords(threadId, []));
          if (Option.isSome(existing)) {
            if (existing.value.thread.projectId !== input.projectId) {
              return yield* new AgentSessionThreadProjectConflictError({
                threadId,
                expectedProjectId: input.projectId,
                actualProjectId: existing.value.thread.projectId,
              });
            }
            if (existing.value.thread.historyOrigin !== "v1_import") {
              return yield* new AgentSessionThreadModifiedError({ threadId });
            }
            // A transcript that grew after the import gets its new turns appended below.
            yield* persistRuntime;
            // Leaving the transcript unrecorded makes the next pass read it again.
            const appended = yield* appendOutsideTurns({ threadId, thread });
            if (appended === null) return true;
            yield* presentNativeSession({
              threadId,
              working: yield* outsideTurnOpen({ thread, source }),
              continuedOutside: appended > 0,
              title: { project: project.title, session: thread.title },
              lastActiveAt: dateTime(thread.updatedAt),
            });
            yield* runtimes.recordImportedTranscript({ threadId, source });
            return true;
          }

          const model = thread.model ?? DEFAULT_MODEL_BY_PROVIDER[driver] ?? DEFAULT_MODEL;
          const providerThreadId = idAllocator.derive.providerThread({
            driver,
            providerInstanceId: thread.providerInstanceId,
            nativeThreadId,
          });
          const createdAt = dateTime(thread.createdAt);
          const updatedAt = dateTime(thread.updatedAt);
          const working = yield* outsideTurnOpen({ thread, source });
          const recent =
            working ||
            (yield* Clock.currentTimeMillis) - DateTime.toEpochMillis(updatedAt) <
              RECENT_SESSION_MS;
          const appThread: OrchestrationV2AppThread = {
            createdBy: "system",
            creationSource: "server",
            id: threadId,
            projectId: input.projectId,
            title: nativeSessionTitle(project.title, thread.title),
            providerInstanceId: thread.providerInstanceId,
            modelSelection: { instanceId: thread.providerInstanceId, model },
            runtimeMode,
            interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
            branch: outcome.worktree?.branch ?? null,
            worktreePath: outcome.worktree?.path ?? null,
            linkedPullRequest: null,
            branchPullRequest: null,
            activeProviderThreadId: providerThreadId,
            historyOrigin: "v1_import",
            lineage: {
              parentThreadId: null,
              relationshipToParent: null,
              rootThreadId: threadId,
            },
            forkedFrom: null,
            createdAt,
            updatedAt,
            archivedAt: null,
            ...(recent
              ? { settledOverride: null, settledAt: null, unsettledAt: updatedAt }
              : { settledOverride: "settled" as const, settledAt: updatedAt, unsettledAt: null }),
            ...(working ? { outsideTurnStartedAt: updatedAt } : {}),
            snoozedUntil: null,
            snoozedAt: null,
            pinnedAt: null,
            pinOrderKey: null,
            activeOrderKey: null,
            lastVisitedAt: null,
            deletedAt: null,
          };
          const providerThread: OrchestrationV2ProviderThread = {
            id: providerThreadId,
            driver,
            providerInstanceId: thread.providerInstanceId,
            providerSessionId: null,
            appThreadId: threadId,
            ownerNodeId: null,
            nativeThreadRef: {
              driver,
              nativeId: nativeThreadId,
              strength: "strong",
            },
            nativeConversationHeadRef: null,
            status: "idle",
            firstRunOrdinal: null,
            lastRunOrdinal: null,
            handoffIds: [],
            forkedFrom: null,
            pendingBackgroundTasks: [],
            createdAt,
            updatedAt,
          };

          const committed = yield* eventSink.write({
            unlessNativeThreadOwned: providerThread,
            events: [
              {
                id: EventId.make(`${IMPORT_EVENT_PREFIX}:thread:${threadId}:created`),
                type: "thread.created",
                threadId,
                providerInstanceId: thread.providerInstanceId,
                occurredAt: createdAt,
                payload: appThread,
              },
              ...(thread.omittedMessageCount > 0
                ? [
                    {
                      id: EventId.make(`${IMPORT_EVENT_PREFIX}:history-truncated:${threadId}`),
                      type: "turn-item.updated" as const,
                      threadId,
                      occurredAt: createdAt,
                      payload: {
                        id: TurnItemId.make(`${IMPORT_EVENT_PREFIX}:history-truncated:${threadId}`),
                        threadId,
                        runId: null,
                        nodeId: null,
                        providerThreadId: null,
                        providerTurnId: null,
                        nativeItemRef: null,
                        parentItemId: null,
                        ordinal: 0,
                        status: "completed" as const,
                        title: null,
                        startedAt: createdAt,
                        completedAt: createdAt,
                        updatedAt: createdAt,
                        type: "system_notice" as const,
                        message: `${thread.omittedMessageCount} earlier ${thread.omittedMessageCount === 1 ? "message was" : "messages were"} not imported. This thread keeps the first user message and the most recent history. The original session transcript is unchanged.`,
                      },
                    },
                  ]
                : []),
              ...thread.messages.flatMap((message, index) =>
                messageEvents({ threadId, index, message }),
              ),
              {
                id: EventId.make(`${IMPORT_EVENT_PREFIX}:provider-thread:${providerThreadId}`),
                type: "provider-thread.updated",
                threadId,
                driver,
                providerInstanceId: thread.providerInstanceId,
                occurredAt: updatedAt,
                payload: providerThread,
              },
            ],
          });
          if (committed.length === 0) {
            // A thread started in T3 owns this session. A terminal may have continued it.
            const ownerId = yield* findNativeOwner(thread, nativeThreadId);
            const owner = ownerId === undefined ? null : ThreadId.make(ownerId);
            if (owner !== null) {
              const appended = yield* appendOutsideTurns({
                threadId: owner,
                thread,
                namedWritersOnly: true,
              });
              if (appended !== null) {
                yield* presentNativeSession({
                  threadId: owner,
                  working: yield* outsideTurnOpen({ thread, source }),
                  continuedOutside: appended > 0,
                });
              }
            }
            markNative(source.filePath, owner);
            return null;
          }
          yield* persistRuntime;
          yield* runtimes.recordImportedTranscript({ threadId, source });
          return true;
        }).pipe(
          Effect.catch((cause) =>
            Effect.logWarning("Could not import an agent session", {
              provider: outcome.thread.source,
              sessionId: outcome.thread.providerSessionId,
              cause,
            }).pipe(Effect.as(false)),
          ),
        );
        if (imported === true) {
          importedThreadIds.add(threadId);
          importedCount += 1;
        } else if (imported === false) {
          skippedCount += 1;
        }
      }),
    );

    return { importedCount, skippedCount } satisfies AgentSessionImportResult;
  });

  /**
   * Loading a native session replays its history only into the adapter's own
   * snapshot, so copy that snapshot into the thread as run-less history. The
   * `v1_import` origin keeps it in forks and marks the import done, so a call
   * after a failed read repairs the thread and any later call does nothing.
   */
  const importNativeHistory = Effect.fn("importNativeHistoryV2")(function* (input: {
    readonly threadId: ThreadId;
    readonly workspaceRoot: string;
  }) {
    const pendingThread = Effect.map(
      orchestrator.getThreadRecords(input.threadId, ["providerThreads", "runs"]),
      (records) =>
        isNativeHistoryPending({
          historyOrigin: records.thread.historyOrigin,
          hasRun: records.runs.length > 0,
        })
          ? records
          : null,
    );
    // Thread commands write whole thread rows, so hold their lock from the
    // re-read through the write to keep both this marker and their changes.
    const commit = (messages: ReadonlyArray<HistoryMessage>) =>
      threadCommands.withLock(
        input.threadId,
        Effect.gen(function* () {
          const current = yield* pendingThread;
          if (current === null) return 0;
          yield* eventSink.write({
            events: [
              ...messages.flatMap((message, index) =>
                messageEvents({ threadId: input.threadId, index, message }),
              ),
              {
                id: EventId.make(`${IMPORT_EVENT_PREFIX}:thread:${input.threadId}:native-history`),
                type: "thread.metadata-updated",
                threadId: input.threadId,
                providerInstanceId: current.thread.providerInstanceId,
                occurredAt: yield* DateTime.now,
                payload: { ...current.thread, historyOrigin: "v1_import" },
              },
            ],
          });
          return messages.length;
        }),
      );
    const records = yield* pendingThread;
    if (records === null) return 0;
    const { thread, providerThreads } = records;
    const providerThread = providerThreads.find(
      (candidate) => candidate.appThreadId === input.threadId && candidate.nativeThreadRef !== null,
    );
    const nativeThreadId = providerThread?.nativeThreadRef?.nativeId;
    if (providerThread === undefined || nativeThreadId == null) return 0;
    const providerSessionId = yield* idAllocator.allocate.providerSession({
      providerInstanceId: providerThread.providerInstanceId,
      threadId: input.threadId,
    });
    // The registry admits this open against sign-in changes, like any other open.
    const adapter = yield* registry.get(thread.modelSelection.instanceId);
    const snapshot = yield* adapter
      .openSession({
        threadId: input.threadId,
        providerSessionId,
        modelSelection: thread.modelSelection,
        runtimePolicy: {
          runtimeMode: thread.runtimeMode,
          interactionMode: thread.interactionMode,
          cwd: thread.worktreePath ?? input.workspaceRoot,
        },
        initialNativeThreadId: nativeThreadId,
        ...(providerThread.nativeMetadata?.itemIdentityVersion === undefined
          ? {}
          : {
              initialProviderItemIdentityVersion: providerThread.nativeMetadata.itemIdentityVersion,
            }),
      })
      .pipe(
        Effect.flatMap((session) => session.readThreadSnapshot({ providerThread })),
        Effect.timeout(NATIVE_HISTORY_READ_TIMEOUT),
        Effect.scoped,
      );
    const reasoningIds = new Set(snapshot.reasoningMessageIds);
    const messages = snapshot.messages.flatMap((message) =>
      (message.role === "user" || message.role === "assistant") && message.text.trim() !== ""
        ? [
            {
              role: reasoningIds.has(message.id) ? ("reasoning" as const) : message.role,
              text: message.text,
              createdAt: DateTime.formatIso(message.createdAt),
            },
          ]
        : [],
    );
    // Loading the session can take a while, and the thread is usable meanwhile.
    return yield* commit(messages);
  });

  // One import at a time: the onboarding request and the background sync share
  // the ignored-transcript memo and would otherwise read the same files twice.
  const importLock = yield* Semaphore.make(1);
  const importLocked = (input: AgentSessionImportInput) =>
    importLock.withPermits(1)(importRecentAgentThreads(input));

  // Off with the background sync, so tests and one-shot commands leave no watchers behind.
  let watchWanted = false;
  const watchedRoots = new Set<string>();
  const changedTranscripts = new Set<string>();
  const unplacedTranscripts = new Map<string, number>();
  let lastFullSyncAt = 0;
  const serviceScope = yield* Effect.scope;

  const syncAllProjects: Effect.Effect<
    number,
    AgentSessionScanError | ProjectService.ProjectOperationError
  > = Effect.gen(function* () {
    const transcriptRoots = yield* scanner.refreshCandidates;
    lastFullSyncAt = yield* Clock.currentTimeMillis;
    for (const root of transcriptRoots) {
      // Claude's process files sit beside its transcripts and change when a turn ends.
      const processDir =
        path.basename(root) === "projects" ? path.join(path.dirname(root), "sessions") : null;
      if (processDir !== null) claudeConfigDirs.add(path.dirname(root));
      if (watchWanted && processDir !== null && !watchedRoots.has(processDir)) {
        watchedRoots.add(processDir);
        yield* fileSystem.watch(processDir).pipe(
          Stream.debounce(PROCESS_CHANGE_DEBOUNCE),
          Stream.runForEach(() => settleFinishedOutsideTurns),
          Effect.catchCause((cause) =>
            Effect.logDebug("Agent process watch stopped", { processDir, cause }).pipe(
              Effect.andThen(Effect.sync(() => watchedRoots.delete(processDir))),
            ),
          ),
          Effect.forkIn(serviceScope),
        );
      }
      if (!watchWanted || watchedRoots.has(root)) continue;
      watchedRoots.add(root);
      yield* fileSystem.watch(root, { recursive: true }).pipe(
        Stream.filter((event) => event.path.endsWith(".jsonl")),
        Stream.tap((event) =>
          Effect.sync(() => {
            changedTranscripts.add(path.resolve(root, event.path));
          }),
        ),
        Stream.debounce(TRANSCRIPT_CHANGE_DEBOUNCE),
        Stream.runForEach(() => syncChangedTranscripts),
        // A root that does not exist yet is watched once a later pass finds it.
        Effect.catchCause((cause) =>
          Effect.logDebug("Agent session watch stopped", { root, cause }).pipe(
            Effect.andThen(Effect.sync(() => watchedRoots.delete(root))),
          ),
        ),
        Effect.forkIn(serviceScope),
      );
    }
    let cursor: number | null = 0;
    let imported = 0;
    while (cursor !== null) {
      const page: Effect.Success<ReturnType<typeof projects.listPage>> = yield* projects.listPage({
        cursor,
        limit: SYNC_PROJECT_PAGE_SIZE,
      });
      for (const project of page.projects) {
        const result = yield* importLocked({
          projectId: project.id,
          expectedWorkspaceRoot: project.workspaceRoot,
        }).pipe(
          Effect.catch((cause) =>
            Effect.logDebug("Agent session sync skipped a project", {
              projectId: project.id,
              cause,
            }).pipe(Effect.as(null)),
          ),
        );
        if (result !== null) imported += result.importedCount;
      }
      cursor = page.nextCursor;
    }
    yield* settleFinishedOutsideTurns;
    return imported;
  });

  /** Syncs the projects whose transcripts changed; a file no project claims yet takes a full pass. */
  const syncChangedTranscripts: Effect.Effect<void> = Effect.gen(function* () {
    const changed = Array.from(changedTranscripts);
    changedTranscripts.clear();
    const now = yield* Clock.currentTimeMillis;
    const projectIds = new Set<ProjectId>();
    const unplaced: Array<string> = [];
    for (const filePath of changed) {
      const projectId = transcriptProjects.get(filePath);
      if (nativeTranscripts.has(filePath)) {
        const owner = nativeTranscripts.get(filePath);
        if (projectId === undefined || owner === undefined || owner === null) continue;
        // T3's own turn is what is writing the file; nothing to read until it settles.
        const running = yield* orchestrator.getThreadRecords(owner, ["runs"]).pipe(
          Effect.map((records) => records.runs.some((run) => OPEN_RUN_STATUSES.has(run.status))),
          Effect.orElseSucceed(() => true),
        );
        if (running) continue;
        changedNativeTranscripts.add(filePath);
      }
      if (projectId !== undefined) {
        projectIds.add(projectId);
      } else if (now - (unplacedTranscripts.get(filePath) ?? 0) >= UNPLACED_TRANSCRIPT_RETRY_MS) {
        unplaced.push(filePath);
      }
    }
    if (unplaced.length > 0 && now - lastFullSyncAt >= UNPLACED_TRANSCRIPT_RETRY_MS) {
      yield* syncAllProjects;
      if (unplacedTranscripts.size >= MAX_IGNORED_TRANSCRIPTS) unplacedTranscripts.clear();
      for (const filePath of unplaced) {
        if (!transcriptProjects.has(filePath)) unplacedTranscripts.set(filePath, now);
      }
      return;
    }
    for (const projectId of projectIds) {
      yield* importLocked({ projectId });
    }
  }).pipe(
    Effect.catchCause((cause) => Effect.logDebug("Agent session change sync failed", { cause })),
  );

  const syncInterval = yield* Config.option(Config.String(SYNC_INTERVAL_ENV)).pipe(
    Effect.map(
      Option.match({
        onNone: () => Option.some(DEFAULT_SYNC_INTERVAL),
        onSome: (raw) => {
          const trimmed = raw.trim();
          if (trimmed === "0" || trimmed === "off" || trimmed === "false") return Option.none();
          return Option.orElse(Duration.fromInput(trimmed as Duration.Input), () =>
            Option.some(DEFAULT_SYNC_INTERVAL),
          );
        },
      }),
    ),
    Effect.orElseSucceed(() => Option.some(DEFAULT_SYNC_INTERVAL)),
  );
  watchWanted = Option.isSome(syncInterval);
  if (Option.isSome(syncInterval)) {
    yield* syncAllProjects.pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("Agent session sync failed", { cause }).pipe(Effect.as(0)),
      ),
      Effect.repeat(Schedule.spaced(syncInterval.value)),
      Effect.delay(SYNC_STARTUP_DELAY),
      Effect.forkScoped,
    );
  }

  return { importRecentAgentThreads: importLocked, importNativeHistory, syncAllProjects };
});

type AgentSessionImporterShape = Effect.Success<typeof make>;

export class AgentSessionImporter extends Context.Service<
  AgentSessionImporter,
  AgentSessionImporterShape
>()("t3/project/AgentSessionImporter") {}

export const layer = Layer.effect(AgentSessionImporter, make).pipe(
  Layer.provide(TurnItemPositionStore.layer),
);
