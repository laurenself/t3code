import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  type AgentSessionSource,
  type ServerSettingsError,
  EventId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type ApplicationStoredEvent,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as TestClock from "effect/testing/TestClock";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProviderSessionManager from "../orchestration-v2/ProviderSessionManager.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import {
  CLAUDE_MODEL_SELECTION,
  CODEX_MODEL_SELECTION,
} from "../orchestration-v2/testkit/fixtures/shared.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as AgentSessionImporter from "./AgentSessionImporter.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";
import * as ProjectService from "./ProjectService.ts";

const projectId = ProjectId.make("agent-session-import-project");
const timestamp = "2026-09-01T10:00:00.000Z";
const now = DateTime.makeUnsafe(timestamp);
const stores = Layer.mergeAll(EventStore.layer, ProjectionStore.layer).pipe(
  Layer.provideMerge(SqlitePersistence.layerMemory),
);
const persistence = Layer.mergeAll(
  stores,
  EventSink.layer.pipe(Layer.provide(stores)),
  ProviderSessionRuntime.layer.pipe(Layer.provide(SqlitePersistence.layerMemory)),
  IdAllocator.layer,
  NodeServices.layer,
);

function candidate(
  modelSelection: typeof CODEX_MODEL_SELECTION,
  providerSessionId: string,
  provider: AgentSessionSource = "codex",
) {
  const providerInstanceId = modelSelection.instanceId;
  return {
    _tag: "Importable" as const,
    source: {
      provider,
      providerInstanceId,
      providerSessionId,
      filePath: `/tmp/${providerInstanceId}/${providerSessionId}.jsonl`,
      size: 100,
      mtimeMs: 2,
      device: 3,
      inode: 4,
      birthtimeMs: 1,
    },
    thread: {
      source: provider,
      providerInstanceId,
      providerSessionId,
      title: "Imported thread",
      model: modelSelection.model,
      createdAt: timestamp,
      updatedAt: timestamp,
      omittedMessageCount: 0,
      messages: [
        { role: "user" as const, text: "Fix it", createdAt: timestamp },
        { role: "assistant" as const, text: "Fixed", createdAt: timestamp },
      ],
    },
  } satisfies AgentSessionScanner.AgentSessionRecentThread;
}

const PI_MODEL_SELECTION = { instanceId: ProviderInstanceId.make("pi"), model: "default" };

// Pi threads are owned by their session file path, not the session UUID.
const nativeId = ({ source }: ReturnType<typeof candidate>) =>
  source.provider === "pi" ? source.filePath : source.providerSessionId;

function importId(source: ReturnType<typeof candidate>) {
  return ThreadId.make(
    `import:${source.source.providerInstanceId}:${source.source.providerSessionId}`,
  );
}

const seedOwner = Effect.fn(function* (
  source: ReturnType<typeof candidate>,
  lifecycle: "active" | "archived" | "deleted" = "active",
) {
  const sink = yield* EventSink.EventSinkV2;
  const ids = yield* IdAllocator.IdAllocatorV2;
  const { providerInstanceId, providerSessionId, provider } = source.source;
  const nativeThreadId = nativeId(source);
  const threadId = ThreadId.make(`owner:${providerInstanceId}:${providerSessionId}`);
  const driver = ProviderDriverKind.make(provider);
  const providerThread: OrchestrationV2ProviderThread = {
    id: ids.derive.providerThread({ driver, nativeThreadId }),
    driver,
    providerInstanceId,
    providerSessionId: null,
    appThreadId: threadId,
    ownerNodeId: null,
    nativeThreadRef: { driver, nativeId: nativeThreadId, strength: "strong" },
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: 1,
    lastRunOrdinal: 1,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
  };
  const thread: OrchestrationV2AppThread = {
    createdBy: "user",
    creationSource: "web",
    id: threadId,
    projectId,
    title: "Native thread",
    providerInstanceId,
    modelSelection: { instanceId: providerInstanceId, model: source.thread.model },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: providerThread.id,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: lifecycle === "archived" ? now : null,
    deletedAt: lifecycle === "deleted" ? now : null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
  };
  yield* sink.write({
    events: [
      {
        id: EventId.make(`${threadId}:created`),
        type: "thread.created",
        threadId,
        occurredAt: now,
        payload: thread,
      },
      {
        id: EventId.make(`${threadId}:provider`),
        type: "provider-thread.updated",
        threadId,
        occurredAt: now,
        payload: providerThread,
      },
    ],
  });
  return threadId;
});

const project = {
  id: projectId,
  title: "Project",
  workspaceRoot: "/workspace/project",
  defaultModelSelection: null,
  scripts: [],
  createdAt: timestamp,
  updatedAt: timestamp,
  deletedAt: null,
};

const makeImporter = Effect.fn(function* (
  candidates:
    | ReadonlyArray<ReturnType<typeof candidate>>
    | (() => ReadonlyArray<ReturnType<typeof candidate>>),
  beforeTransaction: Effect.Effect<void> = Effect.void,
  settings: Layer.Layer<
    ServerSettings.ServerSettingsService,
    ServerSettingsError
  > = ServerSettings.layerTest(),
) {
  const sql = yield* SqlClient.SqlClient;
  const events = yield* EventStore.EventStoreV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const published: Array<ApplicationStoredEvent> = [];
  const withTransaction: typeof sql.withTransaction = (effect) =>
    beforeTransaction.pipe(Effect.andThen(sql.withTransaction(effect)));
  const importSql = new Proxy(sql, {
    get: (target, key, receiver) =>
      key === "withTransaction" ? withTransaction : Reflect.get(target, key, receiver),
  });
  const sinkLayer = Layer.fresh(EventSink.layer).pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.succeed(SqlClient.SqlClient, importSql),
        Layer.succeed(ProjectionStore.ProjectionStoreV2, projections),
        Layer.succeed(EventStore.EventStoreV2, {
          ...events,
          publishCommitted: (batch) =>
            events.publishCommitted(batch).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  published.push(...batch);
                }),
              ),
            ),
        }),
      ),
    ),
  );
  const context = yield* Layer.build(
    AgentSessionImporter.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          sinkLayer,
          settings,
          Layer.succeed(IdAllocator.IdAllocatorV2, yield* IdAllocator.IdAllocatorV2),
          Layer.succeed(
            ProviderSessionRuntime.ProviderSessionRuntimeRepository,
            yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository,
          ),
          Layer.mock(AgentSessionScanner.AgentSessionScanner)({
            recentThreads: (_root, _completed, options) =>
              Stream.fromIterable(
                (typeof candidates === "function" ? candidates() : candidates).filter(
                  (entry) => options?.skipTranscript?.(entry.source) !== true,
                ),
              ),
            refreshCandidates: Effect.succeed([]),
          }),
          Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
            detach: () => Effect.void,
          }),
          Layer.mock(ProjectService.ProjectService)({
            getById: () => Effect.succeedSome(project),
            listPage: () => Effect.succeed({ projects: [project], nextCursor: null }),
          }),
          Layer.mock(Orchestrator.OrchestratorV2)({
            getThreadRecords: (threadId, fields) =>
              projections
                .getThreadRecords(threadId, fields)
                .pipe(
                  Effect.mapError(
                    (cause) => new Orchestrator.OrchestratorProjectionError({ threadId, cause }),
                  ),
                ),
          }),
        ),
      ),
    ),
  );
  const importer = Context.get(context, AgentSessionImporter.AgentSessionImporter);
  return {
    run: importer.importRecentAgentThreads({ projectId }),
    sync: importer.syncAllProjects,
    published,
  };
});

it.effect(
  "ignores owned sessions, including archived and deleted threads, but imports other instances and unowned sessions",
  () =>
    Effect.gen(function* () {
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const events = yield* EventStore.EventStoreV2;
      const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      const active = candidate(CODEX_MODEL_SELECTION, "active");
      const archived = candidate(
        CLAUDE_MODEL_SELECTION,
        "020e7f00-dce6-406b-8ec6-ed90498945d6",
        "claudeAgent",
      );
      const deleted = candidate(CODEX_MODEL_SELECTION, "deleted");
      const ownedPi = candidate(PI_MODEL_SELECTION, "owned-pi", "pi");
      const owners = [
        yield* seedOwner(active),
        yield* seedOwner(archived, "archived"),
        yield* seedOwner(deleted, "deleted"),
        yield* seedOwner(ownedPi),
      ];
      const before = yield* Effect.forEach(owners, (id) =>
        projections.getThreadRecords(id, ["providerThreads"]),
      );
      const otherInstance = candidate(
        { ...CLAUDE_MODEL_SELECTION, instanceId: ProviderInstanceId.make("claude-other") },
        archived.source.providerSessionId,
        "claudeAgent",
      );
      const unowned = candidate(CODEX_MODEL_SELECTION, "unowned");
      unowned.thread.omittedMessageCount = 1;
      const unownedPi = candidate(PI_MODEL_SELECTION, "unowned-pi", "pi");
      const importer = yield* makeImporter([
        active,
        archived,
        deleted,
        ownedPi,
        otherInstance,
        unowned,
        unownedPi,
      ]);

      expect(yield* importer.run).toEqual({ importedCount: 3, skippedCount: 0 });
      for (const source of [active, archived, deleted, ownedPi]) {
        const threadId = importId(source);
        expect(yield* Effect.option(projections.getThread(threadId))).toEqual(Option.none());
        expect(yield* events.read({ threadId }).pipe(Stream.runCollect)).toEqual([]);
        expect(yield* runtimes.getByThreadId({ threadId })).toEqual(Option.none());
      }
      expect(
        yield* Effect.forEach(owners, (id) =>
          projections.getThreadRecords(id, ["providerThreads"]),
        ),
      ).toEqual(before);
      expect(
        new Set(
          importer.published.flatMap((stored) =>
            "event" in stored ? [stored.event.threadId] : [],
          ),
        ),
      ).toEqual(new Set([importId(otherInstance), importId(unowned), importId(unownedPi)]));
      for (const source of [otherInstance, unowned, unownedPi]) {
        const threadId = importId(source);
        const records = yield* projections.getThreadRecords(threadId, [
          "messages",
          "turnItems",
          "providerThreads",
        ]);
        expect(records.messages.map((message) => message.text)).toEqual(["Fix it", "Fixed"]);
        const notices = records.turnItems.filter((item) => item.type === "system_notice");
        if (source === unowned) {
          expect(records.turnItems[0]).toBe(notices[0]);
          expect(notices).toMatchObject([
            {
              ordinal: 1,
              runId: null,
              message: expect.stringContaining("1 earlier message was not imported"),
            },
          ]);
        } else {
          expect(notices).toEqual([]);
        }
        expect(records.thread.historyOrigin).toBe("v1_import");
        expect(records.thread.activeProviderThreadId).toBe(records.providerThreads[0]?.id);
        expect(records.providerThreads[0]?.nativeThreadRef?.nativeId).toBe(nativeId(source));
        expect(Option.getOrThrow(yield* runtimes.getByThreadId({ threadId }))).toMatchObject({
          providerInstanceId: source.source.providerInstanceId,
          resumeCursor:
            source.source.provider === "claudeAgent"
              ? { threadId, resume: source.source.providerSessionId }
              : { threadId: nativeId(source) },
          runtimePayload: { importedTranscripts: [source.source] },
        });
      }
    }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect("rechecks ownership after the import reaches the commit transaction", () =>
  Effect.gen(function* () {
    const enteringTransaction = yield* Deferred.make<void>();
    const continueTransaction = yield* Deferred.make<void>();
    const source = candidate(CODEX_MODEL_SELECTION, "claimed-during-import");
    const importer = yield* makeImporter(
      [source],
      Deferred.succeed(enteringTransaction, undefined).pipe(
        Effect.andThen(Deferred.await(continueTransaction)),
      ),
    );
    const pending = yield* Effect.forkScoped(importer.run);
    yield* Deferred.await(enteringTransaction);
    // Commit ownership after all pre-transaction reads, before the import acquires SQLite.
    const ownerId = yield* seedOwner(source);
    yield* Deferred.succeed(continueTransaction, undefined);
    expect(yield* Fiber.join(pending)).toEqual({ importedCount: 0, skippedCount: 0 });
    expect(yield* importer.run).toEqual({ importedCount: 0, skippedCount: 0 });
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const events = yield* EventStore.EventStoreV2;
    const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
    expect(yield* Effect.option(projections.getThread(importId(source)))).toEqual(Option.none());
    expect(yield* events.read({ threadId: importId(source) }).pipe(Stream.runCollect)).toEqual([]);
    expect(yield* runtimes.list()).toEqual([]);
    expect(importer.published).toEqual([]);
    expect(
      (yield* projections.getThreadRecords(ownerId, ["providerThreads"])).providerThreads[0]
        ?.appThreadId,
    ).toBe(ownerId);
  }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect(
  "retries failed commits and runtime persistence without replaying committed history",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const events = yield* EventStore.EventStoreV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      const source = candidate(CODEX_MODEL_SELECTION, "retry");
      const threadId = importId(source);
      const importer = yield* makeImporter([source]);
      yield* sql`CREATE TEMP TRIGGER fail_import BEFORE INSERT ON orchestration_events WHEN NEW.event_type = 'message.updated' BEGIN SELECT RAISE(ABORT, 'test publication failure'); END`;
      expect(yield* importer.run).toEqual({ importedCount: 0, skippedCount: 1 });
      expect(yield* events.read({ threadId }).pipe(Stream.runCollect)).toEqual([]);
      expect(yield* Effect.option(projections.getThread(threadId))).toEqual(Option.none());
      expect(yield* runtimes.list()).toEqual([]);
      expect(importer.published).toEqual([]);
      yield* sql`DROP TRIGGER fail_import`;

      yield* sql`CREATE TEMP TRIGGER fail_runtime BEFORE INSERT ON provider_session_runtime BEGIN SELECT RAISE(ABORT, 'test runtime failure'); END`;
      expect(yield* importer.run).toEqual({ importedCount: 0, skippedCount: 1 });
      const committed = yield* events.read({ threadId }).pipe(Stream.runCollect);
      expect(committed).toHaveLength(6);
      expect(importer.published).toHaveLength(6);
      expect(yield* runtimes.list()).toEqual([]);
      yield* sql`DROP TRIGGER fail_runtime`;

      expect(yield* importer.run).toEqual({ importedCount: 1, skippedCount: 0 });
      expect(yield* importer.run).toEqual({ importedCount: 1, skippedCount: 0 });
      expect(yield* events.read({ threadId }).pipe(Stream.runCollect)).toEqual(committed);
      expect(importer.published).toHaveLength(6);
      expect(
        (yield* projections.getThreadRecords(threadId, ["messages"])).messages.map(
          (message) => message.text,
        ),
      ).toEqual(["Fix it", "Fixed"]);
      expect(Option.getOrThrow(yield* runtimes.getByThreadId({ threadId }))).toMatchObject({
        providerInstanceId: source.source.providerInstanceId,
        resumeCursor: { threadId: source.source.providerSessionId },
        runtimePayload: { importedTranscripts: [source.source] },
      });
    }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect("imports under a policy ceiling in the project's default, kept under the ceiling", () =>
  Effect.gen(function* () {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
    const source = candidate(CODEX_MODEL_SELECTION, "ceiling");
    const threadId = importId(source);
    const importer = yield* makeImporter(
      [source],
      Effect.void,
      ServerSettings.layerTest(
        { defaultRuntimeMode: "full-access" },
        {
          organization: "Example Corp",
          lockedSettings: [],
          limits: { maxRuntimeMode: "auto-accept-edits" },
        },
      ),
    );
    expect(yield* importer.run).toEqual({ importedCount: 1, skippedCount: 0 });
    expect((yield* projections.getThread(threadId)).runtimeMode).toBe("auto-accept-edits");
    expect(Option.getOrThrow(yield* runtimes.getByThreadId({ threadId })).runtimeMode).toBe(
      "auto-accept-edits",
    );
  }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect(
  "background sync imports sessions started after setup and stops re-reading T3's own transcripts",
  () =>
    Effect.gen(function* () {
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const native = candidate(CODEX_MODEL_SELECTION, "native");
      yield* seedOwner(native);
      const first = candidate(CODEX_MODEL_SELECTION, "terminal-1");
      const later = candidate(
        CLAUDE_MODEL_SELECTION,
        "7d0c4c1e-1f0a-4f53-9d43-4b1f5d1d6a10",
        "claudeAgent",
      );
      const onDisk = [native, first];
      const importer = yield* makeImporter(() => onDisk);

      // The native transcript is read once, found to be T3's own, and remembered.
      expect(yield* importer.sync).toBe(1);
      expect(yield* Effect.option(projections.getThread(importId(first)))).not.toEqual(
        Option.none(),
      );

      // A session started in a terminal after the first sync, and the native one
      // dropped from the stream: the mock only offers what skipTranscript lets through.
      onDisk.push(later);
      const before = importer.published.length;
      expect(yield* importer.sync).toBe(2);
      expect(yield* Effect.option(projections.getThread(importId(later)))).not.toEqual(
        Option.none(),
      );
      expect(yield* Effect.option(projections.getThread(importId(native)))).toEqual(Option.none());
      expect(
        importer.published
          .slice(before)
          .flatMap((stored) => ("event" in stored ? [stored.event.threadId] : [])),
      ).not.toContain(importId(first));
    }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect("appends turns a session gained outside T3, once, and leaves out the ones T3 wrote", () =>
  Effect.gen(function* () {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const original = candidate(CODEX_MODEL_SELECTION, "grows");
    const threadId = importId(original);
    let current: ReturnType<typeof candidate> = original;
    const importer = yield* makeImporter(() => [current]);
    expect(yield* importer.run).toEqual({ importedCount: 1, skippedCount: 0 });

    const grown = {
      ...original,
      source: { ...original.source, size: 300, mtimeMs: 9 },
      thread: {
        ...original.thread,
        updatedAt: "2026-09-01T10:09:00.000Z",
        messages: [
          ...original.thread.messages,
          {
            role: "user" as const,
            text: "From the terminal",
            createdAt: "2026-09-01T10:05:00.000Z",
          },
          {
            role: "assistant" as const,
            text: "Terminal reply",
            createdAt: "2026-09-01T10:05:30.000Z",
          },
          {
            role: "user" as const,
            text: "Sent from T3",
            createdAt: "2026-09-01T10:06:00.000Z",
            writtenByT3: true,
          },
        ],
      },
    };
    current = grown;
    const texts = Effect.map(projections.getThreadRecords(threadId, ["messages"]), (records) =>
      records.messages.map((message) => message.text),
    );
    expect(yield* importer.run).toEqual({ importedCount: 1, skippedCount: 0 });
    expect(yield* texts).toEqual(["Fix it", "Fixed", "From the terminal", "Terminal reply"]);

    // The same transcript read again, as after a restart, adds nothing.
    current = { ...grown, source: { ...grown.source, size: 301, mtimeMs: 10 } };
    expect(yield* importer.run).toEqual({ importedCount: 1, skippedCount: 0 });
    expect(yield* texts).toEqual(["Fix it", "Fixed", "From the terminal", "Terminal reply"]);
  }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect(
  "files an old session under Settled, keeps a recent one out, and titles both by project",
  () =>
    Effect.gen(function* () {
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const old = candidate(CODEX_MODEL_SELECTION, "old");
      const base = candidate(CODEX_MODEL_SELECTION, "recent");
      yield* TestClock.setTime(Date.parse("2026-09-10T10:00:00.000Z"));
      const recentAt = Date.parse("2026-09-10T09:00:00.000Z");
      const recent = {
        ...base,
        source: { ...base.source, mtimeMs: recentAt },
        thread: {
          ...base.thread,
          updatedAt: DateTime.formatIso(DateTime.makeUnsafe(recentAt)),
          turnOpen: true,
        },
      };
      const importer = yield* makeImporter([old, recent]);
      expect(yield* importer.run).toEqual({ importedCount: 2, skippedCount: 0 });

      const filed = yield* projections.getThread(importId(old));
      expect(filed.settledOverride).toBe("settled");
      expect(filed.title).toBe("Project · Imported thread");
      const live = yield* projections.getThread(importId(recent));
      expect(live.settledOverride).toBeNull();
      expect(live.settledAt).toBeNull();
      expect(live.outsideTurnStartedAt ?? null).toBeNull();

      // A turn that is still being written marks the thread working, and it loads again after.
      yield* TestClock.setTime(Date.parse("2026-09-10T09:05:00.000Z"));
      const open = candidate(CODEX_MODEL_SELECTION, "open");
      const running = yield* makeImporter([
        {
          ...open,
          source: { ...open.source, mtimeMs: recentAt },
          thread: { ...open.thread, updatedAt: recent.thread.updatedAt, turnOpen: true },
        },
      ]);
      expect(yield* running.run).toEqual({ importedCount: 1, skippedCount: 0 });
      expect((yield* projections.getThread(importId(open))).outsideTurnStartedAt).toBeDefined();
      expect(
        (yield* projections.getThreadRecords(importId(open), [])).thread.outsideTurnStartedAt,
      ).not.toBeNull();
    }).pipe(Effect.scoped, Effect.provide(persistence)),
);
