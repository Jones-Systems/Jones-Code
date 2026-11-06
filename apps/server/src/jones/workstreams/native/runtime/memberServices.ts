import { EnvironmentId } from "@t3tools/contracts";
import { decodeWorkstreamsHomeBindBody } from "@t3tools/contracts/jones/workstreamsHomeBind";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import * as PeerLinks from "../../../../peer/PeerLinks.ts";
import * as NativeDescriptor from "./descriptor.ts";
import * as MemberPreparation from "./memberPreparation.ts";
import * as ReadExport from "../read/export.ts";
import * as Stream from "effect/Stream";
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
      const services = Layer.mergeAll(
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
          ReadExport.ReadExport,
          Effect.map(Application.MemberApplication, (app) => ({
            stream: (pageSize) => Stream.fromEffect(app.export(pageSize)),
          })),
        ),
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
      return Layer.effect(
        NativeDescriptor.NativeDescriptor,
        Effect.gen(function* () {
          yield* Reads.AuthorizedReads;
          const preparation = yield* MemberPreparation.MemberPreparation;
          const links = yield* PeerLinks.PeerLinks;
          const registry = selection.config?.registry;
          return {
            read: Effect.gen(function* () {
              if (!registry) return undefined;
              const status = yield* Effect.gen(function* () {
                const link = yield* links.get(EnvironmentId.make(registry.home_environment_id));
                if (Option.isNone(link)) return "missing" as const;
                if (DateTime.isLessThanOrEqualTo(link.value.expiresAt, yield* DateTime.now))
                  return "expired" as const;
                // This is local association/link readiness, not a current receiving-home grant.
                return yield* preparation.withPrepared((members) =>
                  members.getValidatedBySessionId(selection.sourceSessionId).pipe(
                    Effect.map((bound) => {
                      if (Option.isNone(bound)) return "unknown" as const;
                      const body = decodeWorkstreamsHomeBindBody(
                        bound.value.intent.bind_request_body,
                      );
                      const home = body.expected_home;
                      return home.registry_instance_id === registry.registry_instance_id &&
                        home.owner_id === registry.owner_id &&
                        home.home_environment_id === registry.home_environment_id &&
                        home.server_generation === registry.server_generation &&
                        body.expected_canonical_origin === registry.canonical_origin
                        ? ("ok" as const)
                        : ("unknown" as const);
                    }),
                  ),
                );
              }).pipe(Effect.orElseSucceed(() => "unknown" as const));
              return Startup.capability(
                selection,
                { readProjection: true, placementsV2: true },
                status,
              );
            }),
          };
        }),
      ).pipe(Layer.provideMerge(services));
    }),
  );
