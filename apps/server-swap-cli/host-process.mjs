import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";

const identity = Effect.all({
  platform: HostProcess.Platform,
  arch: HostProcess.Architecture,
});

export function readHostProcess() {
  return Effect.runSync(identity);
}
