// @effect-diagnostics nodeBuiltinImport:off - the login under test is a file Codex owns.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { it } from "@effect/vitest";
import { DEFAULT_SPEECH_TRANSCRIPTION_OPTIONS } from "@t3tools/contracts";
import * as SecretResolver from "@t3tools/secret-provider-core/server/SecretResolver";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import { expect, vi } from "vite-plus/test";
import * as ServerSettings from "../serverSettings.ts";
import * as CloudSpeech from "./CloudSpeech.ts";
import * as CodexSpeech from "./CodexSpeech.ts";

// Fixed expiries: one long past, one far ahead, so the test never reads the clock.
const EXPIRED = 1;
const YEAR_2100 = 4_102_444_800;
const token = (exp: number) => `h.${Buffer.from(JSON.stringify({ exp })).toString("base64url")}.s`;

const writeLogin = (home: string, accessToken: string) =>
  NodeFSP.writeFile(
    NodePath.join(home, "auth.json"),
    JSON.stringify({ tokens: { access_token: accessToken, account_id: "account-1" } }),
  );

it.effect("OpenAI dictation uses the Codex ChatGPT login when no API key is saved", () =>
  Effect.gen(function* () {
    const home = yield* Effect.promise(() =>
      NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "codex-speech-")),
    );
    const chatgpt = vi.fn<typeof fetch>(async () => Response.json({ text: "um hello" }));
    const openai = vi.fn<typeof fetch>(async () => Response.json({ text: "from api key" }));
    const fresh = token(YEAR_2100);
    const refresh = vi.fn(() => writeLogin(home, fresh));
    const service = yield* CloudSpeech.CloudSpeech.pipe(
      Effect.provide(
        CloudSpeech.layer.pipe(
          Layer.provide(FetchHttpClient.layer),
          Layer.provide(ServerSettings.ServerSettingsService.layerTest()),
          Layer.provide(Layer.mock(SecretResolver.SecretResolver)({})),
          Layer.provide(
            Layer.succeed(
              CodexSpeech.CodexSpeech,
              CodexSpeech.make({ home, fetch: chatgpt, refresh }),
            ),
          ),
        ),
      ),
    );
    const transcribe = () =>
      service
        .transcribe("openai", new Uint8Array(new Float32Array([0, 0.5]).buffer), {
          ...DEFAULT_SPEECH_TRANSCRIPTION_OPTIONS,
          speechLanguage: "en",
        })
        .pipe(Effect.provideService(FetchHttpClient.Fetch, openai));

    // Signed out of Codex and no key: nothing is configured and nothing is sent.
    expect((yield* service.configured).openai).toBe(false);
    expect(yield* Effect.flip(transcribe())).toMatchObject({ reason: "missing-key" });
    expect(chatgpt).not.toHaveBeenCalled();

    // An expiring login is renewed by Codex before the recording leaves.
    yield* Effect.promise(() => writeLogin(home, token(EXPIRED)));
    expect(yield* service.configured).toEqual({ openai: true, xai: false, openrouter: false });
    expect(yield* transcribe()).toBe("hello");
    expect(refresh).toHaveBeenCalledTimes(1);
    const [url, init] = chatgpt.mock.calls[0]!;
    expect(String(url)).toBe("https://chatgpt.com/backend-api/transcribe");
    expect(init?.redirect).toBe("error");
    const headers = new Headers(init?.headers);
    expect(headers.get("authorization")).toBe(`Bearer ${fresh}`);
    expect(headers.get("chatgpt-account-id")).toBe("account-1");
    const wav = new DataView(
      yield* Effect.promise(() => ((init!.body as FormData).get("file") as Blob).arrayBuffer()),
    );
    expect(wav.getUint32(24, true)).toBe(16_000);

    // A login ChatGPT rejects is renewed once, then reported.
    chatgpt.mockResolvedValue(new Response("", { status: 401 }));
    expect(yield* Effect.flip(transcribe())).toMatchObject({ reason: "rejected-key" });
    expect(refresh).toHaveBeenCalledTimes(2);
    expect(openai).not.toHaveBeenCalled();

    // A saved API key takes over from the Codex login.
    yield* service.setKey("openai", Redacted.make("chosen"));
    expect(yield* transcribe()).toBe("from api key");
    expect(chatgpt).toHaveBeenCalledTimes(3);
    yield* Effect.promise(() => NodeFSP.rm(home, { recursive: true }));
  }),
);
