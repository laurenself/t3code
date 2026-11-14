import {
  DEFAULT_MODEL,
  DEFAULT_MODEL_BY_PROVIDER,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_RUNTIME_MODE,
  isRuntimeModeAbove,
  AgentSessionImportProjectChangedError,
  AgentSessionImportProjectNotFoundError,
  AgentSessionImportSource,
  AgentSessionScanError,
  AgentSessionSource,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ThreadId,
  TurnItemId,
  type AgentSessionImportInput,
  type AgentSessionImportResult,
  type OrchestrationV2AppThread,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import { normalizeProjectPathForComparison } from "@t3tools/shared/path";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as TurnItemPositionStore from "../orchestration-v2/TurnItemPositionStore.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";
import * as ServerSettings from "../serverSettings.ts";
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
/** A run that can still write to its native session. */
const OPEN_RUN_STATUSES: ReadonlySet<string> = new Set([
  "preparing",
  "queued",
  "starting",
  "running",
  "waiting",
]);
/** Transcript records land slightly outside the run that wrote them. */
const RUN_WINDOW_SLACK_MS = 2_000;
const CLAUDE_SESSION_ID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const decodeImportedTranscriptPayload = Schema.decodeUnknownOption(
  Schema.Struct({
    cwd: Schema.optional(Schema.String),
    importedTranscripts: Schema.optional(Schema.Array(AgentSessionImportSource)),
  }),
);

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

function dateTime(value: string): DateTime.Utc {
  return DateTime.makeUnsafe(value);
}

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
    readonly message: AgentSessionScanner.AgentSessionThreadMessage;
  } & ({ readonly index: number } | { readonly key: string }),
): ReadonlyArray<OrchestrationV2DomainEvent> {
  const ordinal = "index" in input ? input.index + 1 : 1;
  const suffix = "index" in input ? String(input.index).padStart(6, "0") : input.key;
  const messageId = MessageId.make(`${input.threadId}:${suffix}`);
  const turnItemId = TurnItemId.make(
    `${IMPORT_EVENT_PREFIX}:turn-item:${input.threadId}:${suffix}`,
  );
  const at = dateTime(input.message.createdAt);
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
    {
      id: EventId.make(`${IMPORT_EVENT_PREFIX}:turn-item:${input.threadId}:${suffix}`),
      type: "turn-item.updated",
      threadId: input.threadId,
      occurredAt: at,
      payload: turnItem,
    },
  ];
}

const make = Effect.gen(function* () {
  const scanner = yield* AgentSessionScanner.AgentSessionScanner;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const projects = yield* ProjectService.ProjectService;
  const eventSink = yield* EventSink.EventSinkV2;
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const turnItemPositions = yield* TurnItemPositionStore.TurnItemPositionStoreV2;
  const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
  const settingsService = yield* ServerSettings.ServerSettingsService;
  const ceiling = settingsService.managedPolicy?.limits?.maxRuntimeMode;
  // Transcripts of threads T3 itself runs. Their turns rewrite the file constantly, so
  // without this every sync tick would re-read them only to find them owned again.
  const nativeTranscripts = new Set<string>();
  const markNative = (filePath: string) => {
    if (nativeTranscripts.size >= MAX_IGNORED_TRANSCRIPTS) nativeTranscripts.clear();
    nativeTranscripts.add(filePath);
  };
  const skipTranscript = (transcript: { readonly filePath: string }) =>
    nativeTranscripts.has(transcript.filePath);
  /**
   * Appends the turns a native session gained outside T3 (in a terminal) to the
   * thread that mirrors it. Turns T3 ran itself are already in the thread as
   * runs, so records written inside a run are left out. Returns false while a
   * run is open: its records are still arriving, and the next pass reads them.
   */
  const appendOutsideTurns = Effect.fn("appendOutsideAgentTurnsV2")(function* (input: {
    readonly threadId: ThreadId;
    readonly thread: AgentSessionScanner.AgentSessionThread;
  }) {
    const records = yield* orchestrator.getThreadRecords(input.threadId, ["messages", "runs"]);
    if (records.runs.some((run) => OPEN_RUN_STATUSES.has(run.status))) return false;
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
      if (message.writtenBySdk === true || message.createdAt === input.thread.updatedAt) {
        return false;
      }
      const at = millis(dateTime(message.createdAt));
      if (at <= mirroredThrough) return false;
      if (runWindows.some((window) => at >= window.from && at <= window.to)) return false;
      const key = syncedMessageKey(message);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
    if (outside.length === 0) return true;

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
    return true;
  });

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
    const completedSources = runtimeRows.flatMap((runtime) => {
      const payload = decodeImportedTranscriptPayload(runtime.runtimePayload);
      if (
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
              runtimePayload: { cwd: project.workspaceRoot },
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
            yield* persistRuntime;
            // Leaving the transcript unrecorded makes the next pass read it again.
            if (!(yield* appendOutsideTurns({ threadId, thread }))) return true;
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
          const appThread: OrchestrationV2AppThread = {
            createdBy: "system",
            creationSource: "server",
            id: threadId,
            projectId: input.projectId,
            title: thread.title.trim() === "" ? "Untitled thread" : thread.title,
            providerInstanceId: thread.providerInstanceId,
            modelSelection: { instanceId: thread.providerInstanceId, model },
            runtimeMode,
            interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
            branch: null,
            worktreePath: null,
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
            settledOverride: "settled",
            settledAt: updatedAt,
            unsettledAt: null,
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
            markNative(source.filePath);
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

  // One import at a time: the onboarding request and the background sync share
  // the ignored-transcript memo and would otherwise read the same files twice.
  const importLock = yield* Semaphore.make(1);
  const importLocked = (input: AgentSessionImportInput) =>
    importLock.withPermits(1)(importRecentAgentThreads(input));

  const syncAllProjects = Effect.gen(function* () {
    yield* scanner.refreshCandidates;
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
    return imported;
  });

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

  return { importRecentAgentThreads: importLocked, syncAllProjects };
});

type AgentSessionImporterShape = Effect.Success<typeof make>;

export class AgentSessionImporter extends Context.Service<
  AgentSessionImporter,
  AgentSessionImporterShape
>()("t3/project/AgentSessionImporter") {}

export const layer = Layer.effect(AgentSessionImporter, make).pipe(
  Layer.provide(TurnItemPositionStore.layer),
);
