// @effect-diagnostics nodeBuiltinImport:off globalTimers:off - Codex owns its login file and its refresh flow; this reads the file and asks the Codex CLI to refresh, both outside the Effect runtime.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeReadline from "node:readline";
import { CloudSpeechError } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

/** The dictation endpoint behind ChatGPT and Codex, billed to the signed-in ChatGPT plan. */
const TRANSCRIBE_URL = "https://chatgpt.com/backend-api/transcribe";
const EXPIRY_MARGIN_MS = 60_000;
const REFRESH_TIMEOUT_MS = 30_000;

interface CodexLogin {
  readonly accessToken: string;
  readonly accountId: string;
  /** Null when the token carries no readable expiry; the upstream 401 decides then. */
  readonly expiresAtMs: number | null;
}

export interface CodexSpeechOptions {
  /** Codex home holding `auth.json`. Defaults to `CODEX_HOME`, then `~/.codex`. */
  readonly home?: string;
  readonly fetch?: typeof fetch;
  /** Makes Codex renew its own login. Defaults to asking `codex app-server`. */
  readonly refresh?: (home: string) => Promise<void>;
}

/** Transcribes with the ChatGPT login the Codex CLI on this environment already holds. */
export class CodexSpeech extends Context.Service<
  CodexSpeech,
  {
    /** Whether Codex on this environment is signed in with ChatGPT. */
    readonly available: Effect.Effect<boolean>;
    readonly transcribe: (wav: ArrayBuffer) => Effect.Effect<string, CloudSpeechError>;
  }
>()("t3/speech/CodexSpeech") {}

const resolveHome = (home: string | undefined) =>
  home ?? (process.env.CODEX_HOME?.trim() || NodePath.join(NodeOS.homedir(), ".codex"));

async function readLogin(home: string): Promise<CodexLogin | null> {
  try {
    const { tokens } = JSON.parse(await NodeFSP.readFile(NodePath.join(home, "auth.json"), "utf8"));
    const accessToken: unknown = tokens?.access_token;
    const accountId: unknown = tokens?.account_id;
    if (typeof accessToken !== "string" || typeof accountId !== "string") return null;
    if (!accessToken || !accountId) return null;
    let expiresAtMs: number | null = null;
    try {
      const claims = JSON.parse(
        Buffer.from(accessToken.split(".")[1] ?? "", "base64url").toString("utf8"),
      );
      if (typeof claims.exp === "number") expiresAtMs = claims.exp * 1000;
    } catch {}
    return { accessToken, accountId, expiresAtMs };
  } catch {
    return null;
  }
}

/**
 * Codex rotates its refresh token, so a second refresher would sign the CLI out.
 * `account/read` with `refreshToken` runs Codex's own flow and rewrites `auth.json`.
 */
function refreshWithCodexCli(home: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = NodeChildProcess.spawn("codex", ["app-server"], {
      env: { ...process.env, CODEX_HOME: home },
      stdio: ["pipe", "pipe", "ignore"],
    });
    const lines = NodeReadline.createInterface({ input: child.stdout });
    const finish = (error?: Error) => {
      clearTimeout(timer);
      lines.close();
      child.kill();
      if (error) reject(error);
      else resolve();
    };
    const timer = setTimeout(
      () => finish(new Error("Codex login refresh timed out.")),
      REFRESH_TIMEOUT_MS,
    );
    const send = (message: unknown) => child.stdin.write(`${JSON.stringify(message)}\n`);
    child.once("error", finish);
    child.once("exit", () => finish(new Error("Codex exited before refreshing its login.")));
    lines.on("line", (line) => {
      let message: { id?: unknown; error?: unknown };
      try {
        message = JSON.parse(line);
      } catch {
        return;
      }
      if (message.id === 0) {
        send({ method: "initialized" });
        send({ id: 1, method: "account/read", params: { refreshToken: true } });
      } else if (message.id === 1) {
        finish(message.error ? new Error("Codex could not refresh its login.") : undefined);
      }
    });
    send({
      id: 0,
      method: "initialize",
      params: { clientInfo: { name: "t3code-speech", title: "T3 Code", version: "1" } },
    });
  });
}

export const make = (options: CodexSpeechOptions = {}) => {
  const home = resolveHome(options.home);
  const refresh = options.refresh ?? refreshWithCodexCli;
  let refreshing: Promise<void> | null = null;
  const refreshOnce = () =>
    (refreshing ??= refresh(home).finally(() => {
      refreshing = null;
    }));
  const upload = (login: CodexLogin, wav: ArrayBuffer, signal: AbortSignal) => {
    const form = new FormData();
    form.set("file", new Blob([wav], { type: "audio/wav" }), "dictation.wav");
    return (options.fetch ?? fetch)(TRANSCRIBE_URL, {
      method: "POST",
      // The login and the recording never follow a redirect to another host.
      redirect: "error",
      signal,
      headers: {
        authorization: `Bearer ${login.accessToken}`,
        "chatgpt-account-id": login.accountId,
      },
      body: form,
    });
  };
  const fail = (reason: CloudSpeechError["reason"]) =>
    new CloudSpeechError({ provider: "openai", reason });
  const run = async (wav: ArrayBuffer, signal: AbortSignal): Promise<string> => {
    let login = await readLogin(home);
    if (!login) throw fail("missing-key");
    const renew = async () => {
      await refreshOnce().catch(() => {
        throw fail("unreadable-key");
      });
      const renewed = await readLogin(home);
      if (!renewed) throw fail("missing-key");
      return renewed;
    };
    if (login.expiresAtMs !== null && login.expiresAtMs - Date.now() < EXPIRY_MARGIN_MS)
      login = await renew();
    let response = await upload(login, wav, signal);
    if (response.status === 401) response = await upload(await renew(), wav, signal);
    if (response.status === 429) throw fail("rate-limit");
    if ([401, 402, 403].includes(response.status)) throw fail("rejected-key");
    if (!response.ok) throw fail("unavailable");
    const body: unknown = await response.json();
    const text = (body as { text?: unknown } | null)?.text;
    if (typeof text !== "string") throw fail("unavailable");
    return text;
  };
  return CodexSpeech.of({
    available: Effect.promise(() => readLogin(home)).pipe(Effect.map((login) => login !== null)),
    transcribe: (wav) =>
      Effect.tryPromise({
        try: (signal) => run(wav, signal),
        catch: (cause) => (cause instanceof CloudSpeechError ? cause : fail("unavailable")),
      }),
  });
};

export const layer = Layer.sync(CodexSpeech, () => make());
