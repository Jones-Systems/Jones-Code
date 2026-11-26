import { SYNTHETIC_CLAUDE_THINKING_MODEL } from "../provider/ClaudeModelCatalog.testFixtures.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { ClaudeSettings, ProviderInstanceId } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { createModelSelection } from "@t3tools/shared/model";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Result from "effect/Result";
import { expect } from "vite-plus/test";

import * as ServerConfig from "../config.ts";
import { makeProcessProbe, processProbeSource } from "../testUtils/processProbe.ts";
import {
  SYNTHETIC_CLAUDE_CAPABLE_MODEL,
  SYNTHETIC_CLAUDE_COLLIDING_ALIAS,
  SYNTHETIC_CLAUDE_MODEL_CATALOG,
  SYNTHETIC_CLAUDE_STANDARD_MODEL,
} from "../provider/ClaudeModelCatalog.testFixtures.ts";
import * as TextGeneration from "./TextGeneration.ts";
import {
  sanitizeThreadTitle,
  toJsonSchemaObject,
} from "@t3tools/provider-core/server/textGenerationUtils";
import {
  fromRunner,
  decodeJsonReply,
} from "@t3tools/provider-core/server/textGenerationOperations";
import { makeClaudeTextGeneration } from "./ClaudeTextGeneration.ts";
import { writeFakeCli } from "@t3tools/provider-testing/fakeCli";
const decodeClaudeSettings = Schema.decodeSync(ClaudeSettings);

const layerClaudeTextGenerationTest = ServerConfig.ServerConfig.layerTest(process.cwd(), {
  prefix: "t3code-claude-text-generation-test-",
}).pipe(Layer.provideMerge(NodeServices.layer));

// The stub behaviour lives in Node so the same implementation runs on Windows,
// where a shebang file is not executable and would fall through to the real
// Claude CLI on PATH; `writeFakeCli` picks the launcher shape per host.
function makeFakeClaudeBinary(dir: string, lifecyclePort?: number) {
  return Effect.gen(function* () {
    const path = yield* Path.Path;
    const fs = yield* FileSystem.FileSystem;
    const platform = yield* HostProcess.Platform;
    const binDir = path.join(dir, "bin");
    const fixturePath = yield* path.fromFileUrl(
      new URL("./testing/ClaudeTextGeneration.fixture.mjs", import.meta.url),
    );
    const source = yield* fs.readFileString(fixturePath);
    writeFakeCli({
      directory: binDir,
      name: "claude",
      platform,
      source:
        lifecyclePort === undefined
          ? source
          : `process.on("SIGTERM", () => {});\n${source}\n${processProbeSource(lifecyclePort)}`,
    });
    return binDir;
  });
}

function withFakeClaudeEnv<A, E, R>(
  input: {
    output: string;
    rejectsNoSessionPersistence?: boolean;
    lifecyclePort?: number;
    exitCode?: number;
    stderr?: string;
    argsMustContain?: string;
    expectedTools?: string;
    argsMustNotContain?: string;
    stdinMustContain?: string;
    imageRejection?: string;
    imageAttemptLog?: string;
    configDirMustBe?: string;
    cwdMustNotBe?: string;
    capturePath?: string;
    evaluation?: boolean;
    investigation?: boolean;
    managedMcpConfig?: boolean;
    claudeConfig?: Partial<ClaudeSettings>;
  },
  effectFn: (textGeneration: TextGeneration.TextGeneration["Service"]) => Effect.Effect<A, E, R>,
) {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const tempDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-claude-text-" });
    const binDir = yield* makeFakeClaudeBinary(tempDir, input.lifecyclePort);
    const pathDelimiter = (yield* HostProcess.isWindows) ? ";" : ":";
    const hostEnv = yield* HostProcess.Environment;
    const fakeClaudeEnv: Record<string, string | undefined> = {
      T3_FAKE_CLAUDE_OUTPUT: input.output,
      T3_FAKE_CLAUDE_REJECTS_NO_SESSION_PERSISTENCE: input.rejectsNoSessionPersistence
        ? "1"
        : undefined,
      T3_FAKE_CLAUDE_EXIT_CODE: input.exitCode === undefined ? undefined : String(input.exitCode),
      T3_FAKE_CLAUDE_STDERR: input.stderr,
      T3_FAKE_CLAUDE_EXPECTED_TOOLS: input.expectedTools,
      T3_FAKE_CLAUDE_ARGS_MUST_CONTAIN: input.argsMustContain,
      T3_FAKE_CLAUDE_ARGS_MUST_NOT_CONTAIN: input.argsMustNotContain,
      T3_FAKE_CLAUDE_STDIN_MUST_CONTAIN: input.stdinMustContain,
      T3_FAKE_CLAUDE_IMAGE_REJECTION: input.imageRejection,
      T3_FAKE_CLAUDE_IMAGE_ATTEMPT_LOG: input.imageAttemptLog,
      T3_FAKE_CLAUDE_CWD_MUST_NOT_BE: input.cwdMustNotBe,
      T3_FAKE_CLAUDE_SETTINGS_LOG: input.settingsLog,
      T3_FAKE_CLAUDE_CONFIG_DIR_MUST_BE: input.configDirMustBe,
      T3_FAKE_CLAUDE_CAPTURE_PATH: input.capturePath,
      T3_FAKE_CLAUDE_EVALUATION: input.evaluation ? "1" : undefined,
      T3_FAKE_CLAUDE_INVESTIGATION: input.investigation ? "1" : undefined,
      T3_FAKE_CLAUDE_MANAGED_MCP_CONFIG: input.managedMcpConfig ? "1" : undefined,
    };
    const environment: NodeJS.ProcessEnv = {
      ...Object.fromEntries(
        Object.entries(hostEnv).filter(([name]) => !name.startsWith("T3_FAKE_CLAUDE_")),
      ),
      ...Object.fromEntries(
        Object.entries(fakeClaudeEnv).filter(([, value]) => value !== undefined),
      ),
      PATH: `${binDir}${pathDelimiter}${hostEnv.PATH ?? ""}`,
    };

    const config = decodeClaudeSettings(input.claudeConfig ?? {});
    const textGeneration = yield* makeClaudeTextGeneration(
      config,
      environment,
      Effect.succeed(SYNTHETIC_CLAUDE_MODEL_CATALOG),
    );
    return yield* effectFn(textGeneration).pipe(
      Effect.provideService(FileSystem.FileSystem, {
        ...fs,
        exists: (candidate) =>
          /[/\\]managed-mcp\.json$/.test(candidate)
            ? Effect.succeed(input.managedMcpConfig ?? false)
            : fs.exists(candidate),
      }),
    );
  }).pipe(Effect.scoped);
}

const reviewGuideInput = {
  number: 1,
  title: "Synthetic guide",
  description: "Explain the parser change.",
  baseBranch: "main",
  baseCommit: "synthetic-base",
  settledReview: "No outstanding findings.",
  modelSelection: createModelSelection(
    ProviderInstanceId.make("claudeAgent"),
    SYNTHETIC_CLAUDE_STANDARD_MODEL,
  ),
};

it.layer(layerClaudeTextGenerationTest)("ClaudeTextGeneration", (it) => {
  it.effect.each(["pass", "hold", "malformed"] as const)(
    "evaluates remote requests with tools disabled and preserves %s output",
    (outcome) =>
      withFakeClaudeEnv(
        {
          output: JSON.stringify({
            structured_output: {
              verdict: outcome === "malformed" ? "pass" : outcome,
              category: outcome === "pass" ? "none" : "risky",
              reason: "Synthetic evaluation.",
            },
          }),
          argsMustContain: "--disallowedTools mcp__*",
          stdinMustContain: "Untrusted request JSON:",
          cwdMustNotBe: process.cwd(),
          evaluation: true,
        },
        (generation) =>
          Effect.gen(function* () {
            const result = yield* generation
              .evaluateRemoteRequest({
                cwd: process.cwd(),
                message: "Fix the parser",
                modelSelection: createModelSelection(
                  ProviderInstanceId.make("claudeAgent"),
                  SYNTHETIC_CLAUDE_STANDARD_MODEL,
                ),
              })
              .pipe(Effect.result);
            if (outcome === "malformed") {
              expect(Result.isFailure(result)).toBe(true);
              if (Result.isFailure(result)) {
                expect(result.failure.operation).toBe("evaluateRemoteRequest");
                expect(result.failure.detail).toContain("invalid structured output");
              }
            } else {
              expect(Result.isSuccess(result)).toBe(true);
              if (Result.isSuccess(result)) expect(result.success.verdict).toBe(outcome);
            }
          }),
      ),
  );

  it.effect.each([false, true])(
    "denies evaluator MCP tools with managed MCP configuration=%s",
    (managedMcpConfig) =>
      withFakeClaudeEnv(
        {
          output: JSON.stringify({
            structured_output: { verdict: "pass", category: "none", reason: "Bounded work." },
          }),
          evaluation: true,
          managedMcpConfig,
          cwdMustNotBe: process.cwd(),
          claudeConfig: {
            launchArgs: "--chrome --mcp-config ignored.json --tools Read WebSearch",
          },
        },
        (generation) =>
          Effect.gen(function* () {
            expect(
              yield* generation.evaluateRemoteRequest({
                cwd: process.cwd(),
                message: "Fix the parser",
                modelSelection: createModelSelection(
                  ProviderInstanceId.make("claudeAgent"),
                  SYNTHETIC_CLAUDE_STANDARD_MODEL,
                ),
              }),
            ).toEqual({ verdict: "pass", category: "none", reason: "Bounded work." });
          }),
      ),
  );

  it.effect("kills an interrupted evaluator and removes its isolated cwd", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const tempDir = yield* fs.makeTempDirectoryScoped({ prefix: "claude-evaluation-cancel-" });
      const capturePath = path.join(tempDir, "launch.json");
      const probe = yield* makeProcessProbe;
      yield* withFakeClaudeEnv(
        {
          output: "",
          lifecyclePort: probe.port,
          capturePath,
          evaluation: true,
          cwdMustNotBe: process.cwd(),
        },
        (generation) =>
          Effect.gen(function* () {
            const fiber = yield* generation
              .evaluateRemoteRequest({
                cwd: process.cwd(),
                message: "Fix the parser",
                modelSelection: createModelSelection(
                  ProviderInstanceId.make("claudeAgent"),
                  SYNTHETIC_CLAUDE_STANDARD_MODEL,
                ),
              })
              .pipe(Effect.forkScoped);
            yield* probe.started;
            const launch = yield* decodeCapturedLaunch(yield* fs.readFileString(capturePath));
            expect(yield* fs.exists(launch.cwd)).toBe(true);
            const interrupted = yield* Fiber.interrupt(fiber).pipe(Effect.forkScoped);
            yield* probe.exited;
            yield* Fiber.join(interrupted);
            const exit = yield* Fiber.await(fiber);
            expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true);
            expect(yield* fs.exists(launch.cwd)).toBe(false);
          }),
      );
    }).pipe(Effect.scoped),
  );

  it.effect("keeps review-guide investigation in the checkout with read-only tool policy", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const tempDir = yield* fs.makeTempDirectoryScoped({ prefix: "claude-review-guide-" });
      const capturePath = path.join(tempDir, "launch.json");
      yield* withFakeClaudeEnv(
        {
          output: JSON.stringify({ structured_output: { stops: [] } }),
          investigation: true,
          capturePath,
        },
        (generation) =>
          Effect.gen(function* () {
            const generated = yield* generation.generateReviewGuide({
              ...reviewGuideInput,
              cwd: process.cwd(),
            });
            expect(generated).toEqual({ stops: [] });
          }),
      );
      const launch = yield* decodeCapturedLaunch(yield* fs.readFileString(capturePath));
      expect(launch.cwd).toBe(yield* fs.realPath(process.cwd()));
      expect(launch.stdin).toContain("Investigate before you answer, read-only.");
    }).pipe(Effect.scoped),
  );

  it.effect("forwards Claude thinking settings without passing unsupported effort", () =>
    withFakeClaudeEnv(
      {
        output: JSON.stringify({
          structured_output: {
            subject: "Add important change",
            body: "",
          },
        }),
        argsMustContain: '--settings {"disableAllHooks":true,"alwaysThinkingEnabled":false}',
        argsMustNotContain: "--effort",
      },
      (textGeneration) =>
        Effect.gen(function* () {
          const generated = yield* textGeneration.generateCommitMessage({
            cwd: process.cwd(),
            branch: "feature/claude-effect",
            stagedSummary: "M README.md",
            stagedPatch: "diff --git a/README.md b/README.md",
            modelSelection: {
              ...createModelSelection(
                ProviderInstanceId.make("claudeAgent"),
                SYNTHETIC_CLAUDE_THINKING_MODEL,
                [
                  { id: "thinking", value: false },
                  { id: "effort", value: "high" },
                ],
              ),
            },
          });

          expect(generated.subject).toBe("Add important change");
        }),
    ),
  );

  it.effect("keeps a configured custom alias opaque to the Claude CLI", () =>
    withFakeClaudeEnv(
      {
        output: JSON.stringify({
          structured_output: {
            title: "Keep custom model",
            body: "",
          },
        }),
        argsMustContain: `--model ${SYNTHETIC_CLAUDE_COLLIDING_ALIAS} --effort max --settings {"disableAllHooks":true}`,
        claudeConfig: { customModels: [SYNTHETIC_CLAUDE_COLLIDING_ALIAS] },
      },
      (textGeneration) =>
        Effect.gen(function* () {
          const generated = yield* textGeneration.generatePrContent({
            cwd: process.cwd(),
            baseBranch: "main",
            headBranch: "feature/custom-model",
            commitSummary: "Keep custom model",
            diffSummary: "1 file changed",
            diffPatch: "diff --git a/README.md b/README.md",
            modelSelection: createModelSelection(
              ProviderInstanceId.make("claudeAgent"),
              SYNTHETIC_CLAUDE_COLLIDING_ALIAS,
              [
                { id: "effort", value: "max" },
                { id: "fastMode", value: true },
                { id: "contextWindow", value: "expanded" },
              ],
            ),
          });

          expect(generated.title).toBe("Keep custom model");
        }),
    ),
  );

  it.effect(
    "keeps canonical built-in capabilities when a custom model collides with its alias",
    () =>
      withFakeClaudeEnv(
        {
          output: JSON.stringify({
            structured_output: {
              title: "Improve orchestration flow",
              body: "Body",
            },
          }),
          argsMustContain: `--model ${SYNTHETIC_CLAUDE_CAPABLE_MODEL}[expanded] --effort max --settings {"disableAllHooks":true,"fastMode":true}`,
          claudeConfig: { customModels: [SYNTHETIC_CLAUDE_COLLIDING_ALIAS] },
        },
        (textGeneration) =>
          Effect.gen(function* () {
            const generated = yield* textGeneration.generatePrContent({
              cwd: process.cwd(),
              baseBranch: "main",
              headBranch: "feature/claude-effect",
              commitSummary: "Improve orchestration",
              diffSummary: "1 file changed",
              diffPatch: "diff --git a/README.md b/README.md",
              modelSelection: {
                ...createModelSelection(
                  ProviderInstanceId.make("claudeAgent"),
                  SYNTHETIC_CLAUDE_CAPABLE_MODEL,
                  [
                    { id: "effort", value: "max" },
                    { id: "fastMode", value: true },
                  ],
                ),
              },
            });

            expect(generated.title).toBe("Improve orchestration flow");
          }),
      ),
  );

  it.effect(
    "generates thread titles outside the project with tools, skills, and hooks disabled",
    () =>
      withFakeClaudeEnv(
        {
          output: JSON.stringify({
            structured_output: {
              title:
                '  "Reconnect failures after restart because the session state does not recover"  ',
            },
          }),
          cwdMustNotBe: process.cwd(),
          stdinMustContain: "/call-script",
        },
        (textGeneration) =>
          Effect.gen(function* () {
            const generated = yield* textGeneration.generateThreadTitle({
              cwd: process.cwd(),
              message: "/call-script",
              modelSelection: {
                instanceId: ProviderInstanceId.make("claudeAgent"),
                model: SYNTHETIC_CLAUDE_STANDARD_MODEL,
              },
            });

            expect(generated.title).toBe(
              sanitizeThreadTitle(
                '"Reconnect failures after restart because the session state does not recover"',
              ),
            );
          }),
      ),
  );

  it.effect("sends thread title instructions to the provider prompt", () =>
    withFakeClaudeEnv(
      {
        output: JSON.stringify({ structured_output: { title: "Reconnect failures" } }),
        stdinMustContain: "Title instructions from the user:\nUse two to four words.",
      },
      (textGeneration) =>
        textGeneration.generateThreadTitle({
          cwd: process.cwd(),
          message: "Investigate reconnect failures",
          instructions: "Use two to four words.",
          modelSelection: {
            instanceId: ProviderInstanceId.make("claudeAgent"),
            model: SYNTHETIC_CLAUDE_STANDARD_MODEL,
          },
        }),
    ),
  );

  it.effect("sends image attachments to Claude, not only their metadata", () =>
    withFakeClaudeEnv(
      {
        output: JSON.stringify({ structured_output: { branch: "fix/login-button" } }),
        stdinMustContain: Buffer.from("hello").toString("base64"),
      },
      (textGeneration) =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const { attachmentsDir } = yield* ServerConfig.ServerConfig;
          const attachmentId = "thread-1-claude-attachment";
          const imagePath = path.join(attachmentsDir, `${attachmentId}.png`);
          yield* fs.makeDirectory(attachmentsDir, { recursive: true });
          yield* fs.writeFile(imagePath, Buffer.from("hello"));

          yield* textGeneration
            .generateBranchName({
              cwd: process.cwd(),
              message: "fix this",
              attachments: [
                {
                  type: "image",
                  id: attachmentId,
                  name: "screen.png",
                  mimeType: "image/png",
                  sizeBytes: 5,
                },
              ],
              modelSelection: {
                instanceId: ProviderInstanceId.make("claudeAgent"),
                model: SYNTHETIC_CLAUDE_STANDARD_MODEL,
              },
            })
            .pipe(Effect.ensuring(fs.remove(imagePath).pipe(Effect.ignore)));
        }),
    ),
  );

  it.effect.each([
    // A model that cannot take images is remembered, so later calls go straight to text only.
    { rejection: "Image inputs are not supported by this model", imageAttempts: 1 },
    // A rejection of this request's images says nothing about the model.
    {
      rejection:
        "At least one of the image dimensions exceed max allowed size for many-image requests: 2000 pixels",
      imageAttempts: 2,
    },
  ])("still generates text when images are rejected: $rejection", ({ rejection, imageAttempts }) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const logDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-claude-image-log-" });
      const imageAttemptLog = path.join(logDir, "image-attempts.log");
      return yield* withFakeClaudeEnv(
        {
          output: JSON.stringify({ structured_output: { branch: "fix/login-button" } }),
          imageRejection: rejection,
          imageAttemptLog,
        },
        (textGeneration) =>
          Effect.gen(function* () {
            const { attachmentsDir } = yield* ServerConfig.ServerConfig;
            const attachmentId = "thread-1-claude-text-only-attachment";
            const imagePath = path.join(attachmentsDir, `${attachmentId}.png`);
            yield* fs.makeDirectory(attachmentsDir, { recursive: true });
            yield* fs.writeFile(imagePath, Buffer.from("hello"));
            const generate = textGeneration.generateBranchName({
              cwd: process.cwd(),
              message: "fix this",
              attachments: [
                {
                  type: "image",
                  id: attachmentId,
                  name: "screen.png",
                  mimeType: "image/png",
                  sizeBytes: 5,
                },
              ],
              modelSelection: {
                instanceId: ProviderInstanceId.make("claudeAgent"),
                model: SYNTHETIC_CLAUDE_STANDARD_MODEL,
              },
            });

            const [first, second] = yield* Effect.all([generate, generate]).pipe(
              Effect.ensuring(fs.remove(imagePath).pipe(Effect.ignore)),
            );

            expect(first.branch).toBe("fix/login-button");
            expect(second.branch).toBe("fix/login-button");
            const attempts = yield* fs.readFileString(imageAttemptLog);
            expect(attempts.trim().split("\n")).toHaveLength(imageAttempts);
          }),
      );
    }).pipe(Effect.scoped),
  );

  it.effect("runs Claude text generation with the configured CLAUDE_CONFIG_DIR", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const claudeConfigDir = path.join(process.cwd(), ".claude-work-test");
      return yield* withFakeClaudeEnv(
        {
          output: JSON.stringify({
            structured_output: {
              title: "Use Claude home",
            },
          }),
          configDirMustBe: claudeConfigDir,
          claudeConfig: { homePath: claudeConfigDir },
        },
        (textGeneration) =>
          Effect.gen(function* () {
            const generated = yield* textGeneration.generateThreadTitle({
              cwd: process.cwd(),
              message: "thread title",
              modelSelection: {
                instanceId: ProviderInstanceId.make("claudeAgent"),
                model: SYNTHETIC_CLAUDE_STANDARD_MODEL,
              },
            });

            expect(generated.title).toBe(sanitizeThreadTitle("Use Claude home"));
          }),
      );
    }),
  );

  it.effect("passes --settings from the instance launch args to Claude text generation", () =>
    withFakeClaudeEnv(
      {
        output: JSON.stringify({
          structured_output: {
            title: "Use instance settings",
          },
        }),
        argsMustContain: `"env":{"ANTHROPIC_VERTEX_PROJECT_ID":"vertex-project-a"}`,
        claudeConfig: {
          launchArgs: `--settings '{"env":{"ANTHROPIC_VERTEX_PROJECT_ID":"vertex-project-a"}}'`,
        },
      },
      (textGeneration) =>
        Effect.gen(function* () {
          const generated = yield* textGeneration.generateThreadTitle({
            cwd: process.cwd(),
            message: "thread title",
            modelSelection: {
              instanceId: ProviderInstanceId.make("claudeAgent"),
              model: SYNTHETIC_CLAUDE_STANDARD_MODEL,
            },
          });

          expect(generated.title).toBe(sanitizeThreadTitle("Use instance settings"));
        }),
    ),
  );

  it.effect("keeps evaluator isolation when retrying without --no-session-persistence", () =>
    withFakeClaudeEnv(
      {
        output: JSON.stringify({
          structured_output: { verdict: "pass", category: "none", reason: "Bounded work." },
        }),
        rejectsNoSessionPersistence: true,
        evaluation: true,
        cwdMustNotBe: process.cwd(),
      },
      (generation) =>
        Effect.gen(function* () {
          const generated = yield* generation.evaluateRemoteRequest({
            cwd: process.cwd(),
            message: "Fix the parser",
            modelSelection: createModelSelection(
              ProviderInstanceId.make("claudeAgent"),
              SYNTHETIC_CLAUDE_STANDARD_MODEL,
            ),
          });
          expect(generated).toEqual({ verdict: "pass", category: "none", reason: "Bounded work." });
        }),
    ),
  );

  it.effect("retries without --no-session-persistence when the Claude CLI rejects it", () =>
    withFakeClaudeEnv(
      {
        output: JSON.stringify({ structured_output: { title: "Reconnect failures" } }),
        rejectsNoSessionPersistence: true,
      },
      (textGeneration) =>
        Effect.gen(function* () {
          const generated = yield* textGeneration.generateThreadTitle({
            cwd: process.cwd(),
            message: "Investigate reconnect failures",
            modelSelection: {
              instanceId: ProviderInstanceId.make("claudeAgent"),
              model: SYNTHETIC_CLAUDE_STANDARD_MODEL,
            },
          });

          expect(generated.title).toBe("Reconnect failures");
        }),
    ),
  );

  it.effect.each([
    { verbose: false, label: "normal" },
    { verbose: true, label: "verbose" },
  ])("unwraps a JSON title in $label Claude output", ({ verbose }) => {
    const result = {
      type: "result",
      structured_output: { title: '{"title": "Refresh ev-stg APP ASG instances"}' },
    };
    return withFakeClaudeEnv(
      { output: JSON.stringify(verbose ? [result] : result) },
      (textGeneration) =>
        Effect.gen(function* () {
          const generated = yield* textGeneration.generateThreadTitle({
            cwd: process.cwd(),
            message: "Refresh ev-stg APP ASG instances",
            modelSelection: {
              instanceId: ProviderInstanceId.make("claudeAgent"),
              model: SYNTHETIC_CLAUDE_STANDARD_MODEL,
            },
          });

          expect(generated.title).toBe("Refresh ev-stg APP ASG instances");
        }),
    );
  });

  it.effect.each([
    { previousTitle: undefined, label: "generating" },
    { previousTitle: "Old thread title", label: "regenerating" },
  ])("reads the result from verbose Claude output when $label a title", ({ previousTitle }) =>
    withFakeClaudeEnv(
      {
        output: JSON.stringify([
          { type: "system", subtype: "init" },
          { type: "assistant", message: { content: [] } },
          { type: "user", message: { content: [] } },
          { type: "rate_limit_event" },
          {
            type: "result",
            subtype: "success",
            result: '{"title":"Refresh ev-stg APP ASG Instances"}',
            structured_output: { title: "Refresh ev-stg APP ASG Instances" },
          },
        ]),
      },
      (textGeneration) =>
        Effect.gen(function* () {
          const generated = yield* textGeneration.generateThreadTitle({
            cwd: process.cwd(),
            message: "Refresh ev-stg APP ASG instances",
            previousTitle,
            modelSelection: {
              instanceId: ProviderInstanceId.make("claudeAgent"),
              model: SYNTHETIC_CLAUDE_STANDARD_MODEL,
            },
          });

          expect(generated.title).toBe("Refresh ev-stg APP ASG Instances");
        }),
    ),
  );

  it.effect.each([
    ["empty message array", []],
    ["missing result", [{ type: "assistant", structured_output: { title: "Not a result" } }]],
    ["invalid title", [{ type: "result", structured_output: { title: 42 } }]],
    [
      "final result without structured output",
      [
        { type: "result", structured_output: { title: "Earlier result" } },
        { type: "result", subtype: "error_max_structured_output_retries" },
      ],
    ],
  ] as const)("rejects verbose Claude output with %s", ([, output]) =>
    withFakeClaudeEnv({ output: JSON.stringify(output) }, (textGeneration) =>
      Effect.gen(function* () {
        const error = yield* Effect.flip(
          textGeneration.generateThreadTitle({
            cwd: process.cwd(),
            message: "Name this thread",
            modelSelection: {
              instanceId: ProviderInstanceId.make("claudeAgent"),
              model: SYNTHETIC_CLAUDE_STANDARD_MODEL,
            },
          }),
        );

        expect(error._tag).toBe("TextGenerationError");
      }),
    ),
  );

  const apiError = "API Error: 400 Model blocked by guardrail: 4 endpoints excluded";
  const wrapperNotice = "Using the OpenRouter credential from the global credential file.";
  const structuredOutputError = "Failed to provide valid structured output after 5 attempts";
  const structuredOutputFailure = {
    type: "result",
    subtype: "error_max_structured_output_retries",
    is_error: true,
    errors: [structuredOutputError],
  };
  it.effect.each([
    [
      "the errors list of a result with no result text",
      JSON.stringify(structuredOutputFailure),
      structuredOutputError,
    ],
    [
      "the errors list from verbose output",
      JSON.stringify([{ type: "system", subtype: "init" }, structuredOutputFailure]),
      structuredOutputError,
    ],
    [
      "the errors list without CLI telemetry",
      JSON.stringify({
        ...structuredOutputFailure,
        subtype: "error_during_execution",
        errors: ["[ede_diagnostic] result_type=user last_content_type=n/a", apiError],
      }),
      apiError,
    ],
    [
      "the stdout error over a stderr notice",
      JSON.stringify({ is_error: true, api_error_status: 400, result: apiError }),
      apiError,
    ],
    [
      "the verbose stdout error over a stderr notice",
      JSON.stringify([
        { type: "system", subtype: "init" },
        { type: "result", is_error: true, result: apiError },
        { type: "result", is_error: true, result: " " },
      ]),
      apiError,
    ],
    ["stderr when stdout has no error", "not json", wrapperNotice],
    [
      "a stdout error naming --no-session-persistence without retrying",
      JSON.stringify({
        is_error: true,
        result: "error: unknown option '--no-session-persistence'",
      }),
      "error: unknown option '--no-session-persistence'",
    ],
  ] as const)("reports %s when the Claude CLI fails", ([, output, expected]) =>
    withFakeClaudeEnv({ output, stderr: wrapperNotice, exitCode: 1 }, (textGeneration) =>
      Effect.gen(function* () {
        const error = yield* Effect.flip(
          textGeneration.generateThreadTitle({
            cwd: process.cwd(),
            message: "Name this thread",
            modelSelection: {
              instanceId: ProviderInstanceId.make("claudeAgent"),
              model: SYNTHETIC_CLAUDE_STANDARD_MODEL,
            },
          }),
        );

        expect(error.detail).toBe(`Claude CLI command failed: ${expected}`);
      }),
    ),
  );

  it.effect("does not report the raw result JSON when a failed run writes nothing to stderr", () =>
    withFakeClaudeEnv(
      { output: JSON.stringify(structuredOutputFailure), exitCode: 1 },
      (textGeneration) =>
        Effect.gen(function* () {
          const error = yield* Effect.flip(
            textGeneration.generateThreadTitle({
              cwd: process.cwd(),
              message: "Name this thread",
              modelSelection: {
                instanceId: ProviderInstanceId.make("claudeAgent"),
                model: SYNTHETIC_CLAUDE_STANDARD_MODEL,
              },
            }),
          );

          expect(error.detail).toBe(`Claude CLI command failed: ${structuredOutputError}`);
        }),
    ),
  );

  it.effect.each([
    { operation: "evaluateRemoteRequest", verbose: false },
    { operation: "evaluateRemoteRequest", verbose: true },
    { operation: "generateThreadTitle", verbose: false },
  ] as const)(
    "reports exit-zero errors for $operation (verbose=$verbose)",
    ({ operation, verbose }) => {
      const result = {
        type: "result",
        is_error: true,
        result: `API Error: 401 ${"x".repeat(2_100)}`,
        structured_output: {
          verdict: "pass",
          category: "none",
          reason: "Must not accept an error result.",
          title: "Must not accept an error result",
        },
      };
      return withFakeClaudeEnv(
        { output: JSON.stringify(verbose ? [{ type: "system" }, result] : result), exitCode: 0 },
        (generation) =>
          Effect.gen(function* () {
            const input = {
              cwd: process.cwd(),
              message: "Fix the parser",
              modelSelection: createModelSelection(
                ProviderInstanceId.make("claudeAgent"),
                SYNTHETIC_CLAUDE_STANDARD_MODEL,
              ),
            };
            const error = yield* (
              operation === "evaluateRemoteRequest"
                ? generation.evaluateRemoteRequest(input).pipe(Effect.asVoid)
                : generation.generateThreadTitle(input).pipe(Effect.asVoid)
            ).pipe(Effect.flip);
            expect(error.operation).toBe(operation);
            expect(error.detail).toBe(
              `Claude CLI command failed: ${result.result.slice(0, 2_000)}`,
            );
          }),
      );
    },
  );

  it.effect.each([
    {
      flag: "--settings",
      launchArgs: `--settings '{"env":{"SYNTHETIC":"private-settings-value"}}'`,
    },
    { flag: "--unknown-account-flag", launchArgs: "--unknown-account-flag private-account-value" },
  ])(
    "refuses evaluator launch flag $flag while title generation remains available",
    ({ flag, launchArgs }) =>
      withFakeClaudeEnv(
        {
          output: JSON.stringify({ structured_output: { title: "Ordinary title" } }),
          claudeConfig: { launchArgs },
        },
        (generation) =>
          Effect.gen(function* () {
            const input = {
              cwd: process.cwd(),
              message: "Fix the parser",
              modelSelection: createModelSelection(
                ProviderInstanceId.make("claudeAgent"),
                SYNTHETIC_CLAUDE_STANDARD_MODEL,
              ),
            };
            const error = yield* generation.evaluateRemoteRequest(input).pipe(Effect.flip);
            expect(error.operation).toBe("evaluateRemoteRequest");
            expect(error.detail).toContain(flag);
            expect(error.detail).not.toContain("private-");
            expect(yield* generation.generateThreadTitle(input)).toEqual({
              title: "Ordinary title",
            });
          }),
      ),
  );

  it.effect.each([
    { label: "missing", result: undefined },
    { label: "non-string", result: { private: "SYNTHETIC_PRIVATE_RESULT" } },
    { label: "blank", result: "  " },
  ])("uses a fixed fallback for an exit-zero error with $label result", ({ result }) =>
    withFakeClaudeEnv(
      {
        output: JSON.stringify({
          is_error: true,
          subtype: "error_during_execution",
          errors: ["SYNTHETIC_PRIVATE_ERROR"],
          ...(result === undefined ? {} : { result }),
        }),
        exitCode: 0,
      },
      (generation) =>
        Effect.gen(function* () {
          const error = yield* generation
            .evaluateRemoteRequest({
              cwd: process.cwd(),
              message: "Fix the parser",
              modelSelection: createModelSelection(
                ProviderInstanceId.make("claudeAgent"),
                SYNTHETIC_CLAUDE_STANDARD_MODEL,
              ),
            })
            .pipe(Effect.flip);
          expect(error.detail).toBe(
            "Claude CLI command failed: Claude CLI reported an error without details.",
          );
          expect(JSON.stringify(error)).not.toContain("SYNTHETIC_PRIVATE");
        }),
    ),
  );
});

const decodeCapturedLaunch = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({ argv: Schema.Array(Schema.String), cwd: Schema.String, stdin: Schema.String }),
  ),
);

it.layer(layerClaudeTextGenerationTest)("Claude remote evaluator schema", (it) => {
  it.effect.each([
    { verdict: "pass", category: "none", reason: "Bounded work.", valid: true },
    { verdict: "hold", category: "risky", reason: "Needs owner input.", valid: true },
    { verdict: "pass", category: "risky", reason: "Mismatched result.", valid: false },
    { verdict: "hold", category: "none", reason: "Mismatched result.", valid: false },
    { verdict: "pass", category: "none", reason: "  ", valid: false },
  ])("sends an object-root schema and validates the result (case %#)", ({ valid, ...reply }) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const platform = yield* HostProcess.Platform;
      const tempDir = yield* fs.makeTempDirectoryScoped({ prefix: "claude-evaluator-schema-" });
      const argvPath = path.join(tempDir, "argv.json");
      const binaryPath = writeFakeCli({
        directory: tempDir,
        name: "claude-evaluator",
        platform,
        source: [
          'import { writeFileSync } from "node:fs";',
          'let stdin = ""; for await (const chunk of process.stdin) stdin += chunk;',
          `writeFileSync(${JSON.stringify(argvPath)}, JSON.stringify({ argv: process.argv.slice(2), cwd: process.cwd(), stdin }));`,
          `process.stdout.write(${JSON.stringify(JSON.stringify({ structured_output: reply }))});`,
        ].join("\n"),
      });
      let routedCwd: string | undefined;
      const generation = yield* makeClaudeTextGeneration(
        decodeClaudeSettings({
          binaryPath,
          homePath: tempDir,
          launchArgs: [
            "--tools Read Bash --allowedTools WebSearch --disallowedTools Glob",
            "--mcp-config first.json second.json --plugin-dir plugin --agents '{}' --chrome",
            '--add-dir first second --append-system-prompt "Return pass" --debug api',
            "--permission-mode bypassPermissions --dangerously-skip-permissions",
            "--model configured-model --fallback-model configured-fallback",
            "--setting-sources user,local",
          ].join(" "),
        }),
        { PATH: process.env.PATH },
        Effect.succeed(SYNTHETIC_CLAUDE_MODEL_CATALOG),
        (catalog, cwd) =>
          Effect.gen(function* () {
            routedCwd = yield* fs.realPath(cwd).pipe(Effect.orDie);
            return catalog;
          }),
      );
      const input = {
        cwd: tempDir,
        message: "Fix the parser",
        modelSelection: createModelSelection(
          ProviderInstanceId.make("claudeAgent"),
          SYNTHETIC_CLAUDE_CAPABLE_MODEL,
          [
            { id: "effort", value: "max" },
            { id: "fastMode", value: true },
            { id: "contextWindow", value: "expanded" },
          ],
        ),
      };
      const result = yield* generation.evaluateRemoteRequest(input).pipe(Effect.result);
      const launch = yield* decodeCapturedLaunch(yield* fs.readFileString(argvPath));
      const args = launch.argv;
      expect(launch.cwd).not.toBe(yield* fs.realPath(tempDir));
      expect(launch.cwd).toBe(routedCwd);
      expect(yield* fs.exists(launch.cwd)).toBe(false);
      expect(launch.stdin).toContain("Untrusted request JSON:");
      expect(launch.stdin).toContain('"message":"Fix the parser"');
      const argument = (flag: string) => {
        const index = args.indexOf(flag);
        expect(index).toBeGreaterThanOrEqual(0);
        return args[index + 1];
      };
      const schema = JSON.parse(argument("--json-schema")!);
      expect(schema.type).toBe("object");
      const baseline = fromRunner(
        "Schema baseline",
        (request) => {
          expect(schema).toEqual(toJsonSchemaObject(request.outputSchema));
          return decodeJsonReply(request, "Synthetic", JSON.stringify(reply));
        },
        { remoteRequestEvaluation: "tool-less" },
      );
      yield* baseline.evaluateRemoteRequest(input).pipe(Effect.result);
      expect(schema).not.toHaveProperty("anyOf");
      expect(schema).toMatchObject({
        additionalProperties: false,
        properties: {
          verdict: { enum: ["pass", "hold"] },
          category: { enum: ["none", "malicious", "ridiculous", "risky", "out_of_scope"] },
          reason: { type: "string", maxLength: 600 },
        },
        required: ["verdict", "category", "reason"],
      });
      expect(argument("--tools")).toBe("");
      expect(args.filter((arg) => arg === "--tools")).toHaveLength(1);
      expect(args.filter((arg) => arg === "--model")).toHaveLength(1);
      expect(argument("--model")).toBe(`${SYNTHETIC_CLAUDE_CAPABLE_MODEL}[expanded]`);
      expect(argument("--effort")).toBe("max");
      expect(args.filter((arg) => arg === "--settings")).toHaveLength(1);
      expect(argument("--setting-sources")).toBe("user,local");
      for (const flag of [
        "--chrome",
        "--mcp-config",
        "--plugin-dir",
        "--agents",
        "--add-dir",
        "--allowedTools",
        "--append-system-prompt",
        "--debug",
        "--fallback-model",
        "--dangerously-skip-permissions",
      ]) {
        expect(args).not.toContain(flag);
      }
      expect(args).not.toContain("configured-model");
      expect(argument("--disallowedTools")).toBe("mcp__*");
      expect(args.filter((arg) => arg === "--disallowedTools")).toHaveLength(1);
      expect(argument("--permission-mode")).toBe("dontAsk");
      expect(JSON.parse(argument("--settings")!)).toMatchObject({
        disableAllHooks: true,
        fastMode: true,
      });
      expect(Result.isSuccess(result)).toBe(valid);
      if (Result.isSuccess(result)) {
        expect(result.success).toEqual(reply);
      } else {
        expect(result.failure.operation).toBe("evaluateRemoteRequest");
        expect(result.failure.detail).toContain("invalid structured output");
      }
    }).pipe(Effect.scoped),
  );
});
