import * as NodeCrypto from "node:crypto";
import * as NodeBuffer from "node:buffer";
import {
  NativeBootstrapSubmission,
  NATIVE_BOOTSTRAP_MAX_PREPARATION_BYTES,
  ProviderInstanceId,
  RuntimeMode,
  ProviderInteractionMode,
  LegacyNativeBootstrapCommandV1,
  type OrchestrationV2Command,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

const validUnicode = (value: string) =>
  !/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/u.test(value);
const pythonStrip = (value: string) =>
  value.replace(
    // oxlint-disable-next-line no-control-regex -- Python str.strip includes C0 controls and NEL.
    /^[\u0009-\u000d\u001c-\u0020\u0085\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000]+|[\u0009-\u000d\u001c-\u0020\u0085\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000]+$/gu,
    "",
  );
const exactString = Schema.String.check(
  Schema.makeFilter(
    (value) => value.length > 0 && pythonStrip(value) === value && validUnicode(value),
  ),
);

function validPreparationTimestamp(value: string): boolean {
  const timestamp =
    /^(\d{4}-\d{2}-\d{2}|\d{8}|\d{4}-W\d{2}(?:-\d)?|\d{4}W\d{2}\d?)[\s\S](.+?)([+-].+)$/u.exec(
      value.replaceAll("Z", "+00:00"),
    );
  if (timestamp === null) return false;
  const date = timestamp[1]!;
  const year = Number(date.slice(0, 4));
  if (year < 1 || year > 9999) return false;
  const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const weekDate = /^\d{4}-?W(\d{2})(?:-?(\d))?$/.exec(date);
  if (weekDate === null) {
    const digits = date.replaceAll("-", "");
    const month = Number(digits.slice(4, 6));
    const day = Number(digits.slice(6, 8));
    const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if (month < 1 || month > 12 || day < 1 || day > days[month - 1]!) return false;
  } else {
    const previousYear = year - 1;
    const daysBeforeYear =
      365 * previousYear +
      Math.floor(previousYear / 4) -
      Math.floor(previousYear / 100) +
      Math.floor(previousYear / 400);
    const januaryFirst = (daysBeforeYear + 1) % 7;
    const week = Number(weekDate[1]);
    const day = Number(weekDate[2] ?? 1);
    if (
      week < 1 ||
      week > (januaryFirst === 4 || (leap && januaryFirst === 3) ? 53 : 52) ||
      day < 1 ||
      day > 7
    )
      return false;
    const januaryFourth = daysBeforeYear + 4;
    const ordinal = januaryFourth - ((januaryFourth + 6) % 7) + (week - 1) * 7 + day - 1;
    if (
      ordinal < 1 ||
      ordinal > 365 * 9999 + Math.floor(9999 / 4) - Math.floor(9999 / 100) + Math.floor(9999 / 400)
    )
      return false;
  }
  const timeComponents = /^(\d{2})(?:(:?)(\d{2})(?:\2(\d{2}))?)?(?:[.,](\d+))?$/;
  const time = timeComponents.exec(timestamp[2]!);
  if (
    time === null ||
    Number(time[1]) > 23 ||
    Number(time[3] ?? 0) > 59 ||
    Number(time[4] ?? 0) > 59
  )
    return false;
  const offset = timeComponents.exec(timestamp[3]!.slice(1));
  if (offset === null) return false;
  return (
    Number(offset[1]) * 3600 +
      Number(offset[3] ?? 0) * 60 +
      Number(offset[4] ?? 0) +
      Number(`0.${(offset[5] ?? "0").slice(0, 6)}`) <
    86400
  );
}
const modelSelection = Schema.Struct({
  instanceId: ProviderInstanceId,
  model: exactString,
  options: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({ id: exactString, value: Schema.Union([exactString, Schema.Boolean]) }),
    ),
  ),
});
export const NativePreparationBinding = Schema.Struct({
  backend_instance: exactString,
  environment_id: exactString,
  project_id: exactString,
  project_cwd: exactString.check(Schema.isPattern(/^\//)),
  account_ref: exactString,
  runtime_mode: RuntimeMode,
  interaction_mode: ProviderInteractionMode,
  base_branch: exactString,
  start_from_origin: Schema.Boolean,
  run_setup_script: Schema.Boolean,
  provider_model_selection: modelSelection,
});
export type NativePreparationBinding = typeof NativePreparationBinding.Type;

const seed = Schema.Struct({
  schema: Schema.Literal("voice.t3-bootstrap-preparation/v1"),
  preparation_id: exactString,
  operation_id: exactString,
  binding: NativePreparationBinding,
  binding_digest: exactString,
  prompt_digest: exactString,
  command_digest: exactString,
  command: Schema.Struct({
    message: Schema.Struct({
      text: Schema.String.check(
        Schema.makeFilter(
          (value) =>
            pythonStrip(value).length > 0 && [...value].length <= 100_000 && validUnicode(value),
        ),
      ),
    }),
    createdAt: exactString,
    bootstrap: Schema.Struct({ createThread: Schema.Struct({ title: exactString }) }),
  }),
});
const decodeSeed = Schema.decodeUnknownSync(seed);
const decodeBinding = Schema.decodeUnknownSync(NativePreparationBinding);
const decodeSubmission = Schema.decodeUnknownEffect(NativeBootstrapSubmission);
const decodeJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
// V1 producer bytes use Python stripping; type-side validation avoids wire trimming.
const decodeLegacyCommand = Schema.decodeUnknownSync(Schema.toType(LegacyNativeBootstrapCommandV1));

export class NativeCreationPreparationError extends Schema.TaggedError<NativeCreationPreparationError>()(
  "NativeCreationPreparationError",
  { code: Schema.Literal("invalid_preparation"), message: Schema.String },
) {}

export function nativeCreationCanonicalJson(value: unknown): string {
  return JSON.stringify(value, (_key, child: unknown) => {
    if (child !== null && typeof child === "object" && !Array.isArray(child)) {
      return Object.fromEntries(
        Object.entries(child).sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0)),
      );
    }
    return child;
  });
}

export const nativeCreationSha256 = (value: string | Uint8Array): string =>
  NodeCrypto.createHash("sha256").update(value).digest("hex");

export function nativePreparationCommand(
  operationId: string,
  binding: NativePreparationBinding,
  text: string,
  title: string,
  createdAt: string,
) {
  const identity = nativeCreationSha256(
    nativeCreationCanonicalJson({
      schema: "voice.t3-bootstrap-identity/v1",
      environment_id: binding.environment_id,
      project_id: binding.project_id,
      backend_instance: binding.backend_instance,
      operation_id: operationId,
    }),
  );
  return {
    type: "thread.turn.start" as const,
    commandId: `voice-command-${identity}`,
    threadId: `voice-thread-${identity}`,
    message: {
      messageId: `voice-message-${identity}`,
      role: "user" as const,
      text,
      attachments: [],
    },
    runtimeMode: binding.runtime_mode,
    interactionMode: binding.interaction_mode,
    createdAt,
    bootstrap: {
      createThread: {
        projectId: binding.project_id,
        title,
        modelSelection: binding.provider_model_selection,
        runtimeMode: binding.runtime_mode,
        interactionMode: binding.interaction_mode,
        branch: null,
        worktreePath: null,
        createdAt,
      },
      prepareWorktree: {
        projectCwd: binding.project_cwd,
        baseBranch: binding.base_branch,
        branch: `t3code/voice-${identity.slice(0, 24)}`,
        startFromOrigin: binding.start_from_origin,
        requireWorktree: true as const,
      },
      runSetupScript: binding.run_setup_script,
    },
  };
}

export interface ValidatedNativeCreationPreparation {
  readonly canonicalText: string;
  readonly preparationSha256: string;
  readonly preparationId: string;
  readonly operationId: string;
  readonly binding: NativePreparationBinding;
  readonly bindingDigest: string;
  readonly promptDigest: string;
  readonly commandDigest: string;
  readonly command: LegacyNativeBootstrapCommandV1;
}

export const validateNativeCreationPreparation = Effect.fn("validateNativeCreationPreparation")(
  function* (
    bytes: Uint8Array,
  ): Effect.fn.Return<ValidatedNativeCreationPreparation, NativeCreationPreparationError> {
    return yield* Effect.try({
      try: () => {
        if (bytes.byteLength === 0 || bytes.byteLength > NATIVE_BOOTSTRAP_MAX_PREPARATION_BYTES) {
          throw new Error("Preparation size is invalid");
        }
        const canonicalText = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(
          bytes,
        );
        const raw = decodeJson(canonicalText);
        if (nativeCreationCanonicalJson(raw) !== canonicalText)
          throw new Error("Preparation bytes are not canonical");
        const parsed = decodeSeed(raw);
        // Seed decoding reads only producer inputs; complete regeneration closes every original shape.
        const binding = decodeBinding(parsed.binding, {
          onExcessProperty: "error",
        });
        if (!validPreparationTimestamp(parsed.command.createdAt)) {
          throw new Error("Preparation timestamp requires a valid timezone");
        }
        const command = nativePreparationCommand(
          parsed.operation_id,
          binding,
          parsed.command.message.text,
          parsed.command.bootstrap.createThread.title,
          parsed.command.createdAt,
        );
        const identity = command.commandId.slice("voice-command-".length);
        const bindingDigest = nativeCreationSha256(nativeCreationCanonicalJson(binding));
        const promptDigest = nativeCreationSha256(command.message.text);
        const commandDigest = nativeCreationSha256(nativeCreationCanonicalJson(command));
        const preparationId = `voice-bootstrap-${identity}`;
        const expected = {
          schema: "voice.t3-bootstrap-preparation/v1",
          preparation_id: preparationId,
          operation_id: parsed.operation_id,
          binding,
          binding_digest: bindingDigest,
          prompt_digest: promptDigest,
          command_digest: commandDigest,
          command,
        };
        if (nativeCreationCanonicalJson(expected) !== canonicalText)
          throw new Error("Complete preparation disagrees with producer inputs");
        return {
          canonicalText,
          preparationSha256: nativeCreationSha256(bytes),
          preparationId,
          operationId: parsed.operation_id,
          binding,
          bindingDigest,
          promptDigest,
          commandDigest,
          command: decodeLegacyCommand(command),
        };
      },
      catch: () =>
        new NativeCreationPreparationError({
          code: "invalid_preparation",
          message: "Native creation preparation is invalid or noncanonical",
        }),
    });
  },
);

export const decodeNativeBootstrapSubmission = Effect.fn("decodeNativeBootstrapSubmission")(
  function* (input: unknown) {
    const submission = yield* decodeSubmission(input).pipe(
      Effect.mapError(
        () =>
          new NativeCreationPreparationError({
            code: "invalid_preparation",
            message: "Native bootstrap submission is invalid",
          }),
      ),
    );
    const bytes = NodeBuffer.Buffer.from(submission.preparationBase64, "base64");
    if (bytes.toString("base64") !== submission.preparationBase64) {
      return yield* new NativeCreationPreparationError({
        code: "invalid_preparation",
        message: "Preparation base64 is not canonical",
      });
    }
    const preparation = yield* validateNativeCreationPreparation(bytes);
    return { preparation, guard: submission.creationGuard };
  },
);

export const nativeCreationCommandDigest = (command: LegacyNativeBootstrapCommandV1): string =>
  nativeCreationSha256(nativeCreationCanonicalJson(command));

export const nativeCreationV2CommandDigest = (command: OrchestrationV2Command): string =>
  nativeCreationSha256(nativeCreationCanonicalJson(command));
