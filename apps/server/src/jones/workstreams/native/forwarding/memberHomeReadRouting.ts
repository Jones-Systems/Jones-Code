import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { ownPlacementRouting } from "../read/memberReadAdmission.ts";
import * as Layer from "effect/Layer";
import { canonicalJson } from "../domain/validation.ts";
import { RegistryError } from "../registry/session.ts";
import * as Admission from "../runtime/memberAdmission.ts";
import * as Client from "./peerHomeReadClient.ts";

/** @public Local validated association only; obtaining routing never contacts the home. */
export const layer = Layer.unwrap(
  Effect.sync(() =>
    Layer.effect(
      Client.MemberHomeReadRouting,
      Effect.gen(function* () {
        const admission = yield* Admission.MemberAdmission;
        return Client.MemberHomeReadRouting.of({
          current: Effect.fn("MemberHomeReadRouting.current")(function* () {
            const own = yield* ownPlacementRouting();
            if (Option.isSome(own)) return own.value;
            const admitted = yield* admission.current();
            const { association } = admitted.record;
            const limits = yield* admitted.checkCurrent();
            const checkCurrent = Effect.fn("MemberHomeReadRouting.checkCurrent")(function* () {
              const now = yield* admitted.checkCurrent();
              // A reduction requires a new request so already captured transport limits cannot widen it.
              if (canonicalJson(now) !== canonicalJson(limits))
                return yield* new RegistryError({ code: "forbidden" });
            });
            return {
              home: association.home,
              link_generation: association.link_generation,
              limits,
              checkCurrent,
            };
          }),
        });
      }),
    ),
  ),
);
