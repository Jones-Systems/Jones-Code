import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Integration from "../integration.ts";
import * as Forwarder from "../forwarding/forwarder.ts";
import * as Readmission from "../forwarding/sourceReadmissionPort.ts";
import * as Retained from "../forwarding/retainedSourceEffectPort.ts";
import * as Reads from "../read/routes.ts";
import * as MemberGateway from "../read/memberGateway.ts";
import * as MemberPlacements from "../read/memberPlacements.ts";
import * as PublicOperations from "../operations/memberPublicOperations.ts";
import * as Registration from "../../registrationContext/service.ts";
import * as Application from "./memberApplication.ts";
import * as Recovery from "./memberRecovery.ts";
import * as Pins from "./peerSessionPins.ts";
import * as Startup from "./startup.ts";

/** Selected member resources are application-owned; authentication and current admission stay per call.
 * Historical enrollment never supplies home permission. Unimplemented public adapters remain unavailable.
 */
export const layer = (
  selection: Startup.Selection & {
    readonly mode: "native-member";
    readonly sourceSessionId: string;
  },
) =>
  Layer.unwrap(
    Effect.sync(() => {
      const application = Application.layer.pipe(
        Layer.provideMerge(Integration.nativeMemberPreparationLayer),
        Layer.provideMerge(Pins.layer),
        Layer.provideMerge(Forwarder.locksLayer),
        Layer.provideMerge(Integration.nativePersistenceLayer),
        Layer.provide(Layer.succeed(Startup.Startup, selection)),
      );
      return Layer.mergeAll(
        // Public adapters retain application resources only; every call qualifies its original caller.
        Layer.merge(
          Startup.unavailableServicesLayer(selection),
          Layer.mergeAll(
            MemberGateway.layer,
            MemberPlacements.layer,
            PublicOperations.layer,
            Layer.effect(
              Registration.WorkstreamsRegistrationContext,
              Effect.map(Application.MemberApplication, (app) => ({
                read: () =>
                  app.registration().pipe(
                    Effect.mapError(
                      (error) =>
                        new Registration.RegistrationContextError({
                          reason:
                            error.code === "invalid_response"
                              ? "invalid_response"
                              : "registry_unavailable",
                        }),
                    ),
                  ),
              })),
            ),
          ),
        ),
        Recovery.layer,
        Layer.effect(
          Reads.AuthorizedReads,
          Effect.map(Application.MemberApplication, (app) => ({ read: app.read })),
        ),
        Layer.effect(
          Readmission.SourceReadmission,
          Effect.map(Application.MemberApplication, (app) => ({
            lookupParent: app.lookupParent,
            lookupRaw: app.lookupRaw,
          })),
        ),
        Layer.effect(
          Retained.RetainedSourceEffects,
          Effect.map(Application.MemberApplication, (app) => ({
            invoke: app.invokeRetainedEffect,
          })),
        ),
      ).pipe(Layer.provideMerge(application));
    }),
  );
