import { TextGenerationError } from "@t3tools/contracts";
import { encodeJson } from "./json.ts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as RemoteAdmission from "../remoteAdmission/RemoteAdmission.ts";
import { REMOTE_ADMISSION_EVALUATION_TIMEOUT_MS } from "../remoteAdmission/evaluationDiagnostic.ts";
import * as RemoteAdmissionIntake from "./RemoteAdmissionIntake.ts";
import * as TextGeneration from "../../textGeneration/TextGeneration.ts";
import * as ServerConfig from "../../config.ts";

const admissionLayer = Layer.unwrap(
  Effect.gen(function* () {
    const generation = yield* TextGeneration.TextGeneration;
    const config = yield* ServerConfig.ServerConfig;
    return RemoteAdmission.layerWithEvaluator(
      (envelope) => {
        if (envelope.receiverPolicy.modelSelection === null)
          return Effect.fail(
            new TextGenerationError({
              operation: "evaluateRemoteRequest",
              detail: "Choose an evaluator model.",
            }),
          );
        return generation.evaluateRemoteRequest({
          cwd: config.cwd,
          modelSelection: envelope.receiverPolicy.modelSelection,
          message: encodeJson({
            operation: envelope.operation,
            payload: envelope.payload,
            target: envelope.target,
          }),
        });
      },
      { timeoutMs: REMOTE_ADMISSION_EVALUATION_TIMEOUT_MS },
    );
  }),
);
export const servicesLayer = RemoteAdmissionIntake.layer.pipe(Layer.provide(admissionLayer));
export const resumeLayer = Layer.effectDiscard(
  RemoteAdmissionIntake.RemoteAdmissionIntake.pipe(
    Effect.flatMap((intake) => intake.resumeApproved()),
  ),
);
