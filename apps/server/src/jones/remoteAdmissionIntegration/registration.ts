import { TextGenerationError } from "@t3tools/contracts";
import { encodeJson } from "./json.ts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as ReviewChanges from "../remoteAdmission/ReviewChanges.ts";
import { runtimeAdaptersLayer } from "./RuntimeAdapters.ts";
import { claimTrackingLayer } from "./ClaimTracker.ts";
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
        const payload = envelope.payload;
        // Keep legacy evaluator bytes intact and omit only the new advisory.
        const evaluatorPayload =
          payload !== null &&
          typeof payload === "object" &&
          !Array.isArray(payload) &&
          Object.hasOwn(payload, "advisory")
            ? Object.fromEntries(Object.entries(payload).filter(([key]) => key !== "advisory"))
            : payload;
        return generation.evaluateRemoteRequest({
          cwd: config.cwd,
          modelSelection: envelope.receiverPolicy.modelSelection,
          message: encodeJson({
            operation: envelope.operation,
            payload: evaluatorPayload,
            target: envelope.target,
          }),
        });
      },
      { timeoutMs: REMOTE_ADMISSION_EVALUATION_TIMEOUT_MS },
    );
  }),
);
export const servicesLayer = RemoteAdmissionIntake.layer.pipe(Layer.provideMerge(admissionLayer));
export const resumeLayer = Layer.effectDiscard(
  RemoteAdmissionIntake.RemoteAdmissionIntake.pipe(
    Effect.flatMap((intake) => intake.resumeApproved()),
  ),
);

import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import { makeKeyedLaunchResultReader } from "./EvidenceReader.ts";
import * as Invocation from "../../mcp/McpInvocationContext.ts";
import * as ThreadManagement from "../../orchestration-v2/ThreadManagementService.ts";
import * as SqlClient from "effect/sql/SqlClient";
import * as ServerSettings from "../../serverSettings.ts";
import { ReceiverRecoveryEvidence } from "./RecoveryStore.ts";
import {
  ReceiverReviewReaders,
  RemoteAdmissionReviewService,
  makeReviewService,
} from "./ReviewService.ts";
import { ReceiverPeerProtocol } from "./protocolTools.ts";
import { makePeerProtocol } from "./PeerProtocol.ts";
import { ReceiverInspection, inspectionStatus } from "./Inspection.ts";

/** Parent supplies evidence/read adapters bound to this same receiver; no orchestrator construction cycle. */
const reviewServicesLayer = Layer.mergeAll(
  Layer.effect(
    RemoteAdmissionReviewService,
    Effect.gen(function* () {
      const changes = yield* Effect.serviceOption(ReviewChanges.ReviewChanges);
      return makeReviewService(
        yield* SqlClient.SqlClient,
        yield* RemoteAdmission.RemoteAdmission,
        yield* RemoteAdmissionIntake.RemoteAdmissionIntake,
        yield* ServerSettings.ServerSettingsService,
        yield* ReceiverRecoveryEvidence,
        yield* ReceiverReviewReaders,
        Option.getOrUndefined(changes),
      );
    }),
  ),
  Layer.effect(
    ReceiverPeerProtocol,
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const changes = yield* Effect.serviceOption(ReviewChanges.ReviewChanges);
      return makePeerProtocol(
        sql,
        yield* RemoteAdmission.RemoteAdmission,
        makeKeyedLaunchResultReader(
          sql,
          yield* Receipts.CommandReceiptStoreV2,
          yield* Projections.ProjectionStoreV2,
        ),
        Option.getOrUndefined(changes),
      );
    }),
  ),
  Layer.effect(
    ReceiverInspection,
    Effect.gen(function* () {
      const context = yield* Effect.context<
        | SqlClient.SqlClient
        | RemoteAdmission.RemoteAdmission
        | ReceiverRecoveryEvidence
        | ThreadManagement.ThreadManagementService
      >();
      return {
        status: (input) =>
          Effect.gen(function* () {
            const scope = yield* Invocation.McpInvocationContext;
            return yield* inspectionStatus(input).pipe(
              Effect.provideService(Invocation.McpInvocationContext, scope),
              Effect.provide(context),
            );
          }),
      };
    }),
  ),
);

/** One receiver graph; all domain/read/claim services share the already-provided SQL client. */
const receiverDomainLayer = Layer.merge(admissionLayer, claimTrackingLayer);
const receiverAdaptersLayer = runtimeAdaptersLayer.pipe(Layer.provideMerge(receiverDomainLayer));
const receiverIntakeLayer = RemoteAdmissionIntake.layer.pipe(
  Layer.provideMerge(receiverAdaptersLayer),
);
export const runtimeLayer = reviewServicesLayer.pipe(
  Layer.provideMerge(receiverIntakeLayer),
  Layer.provideMerge(ReviewChanges.layer),
);
