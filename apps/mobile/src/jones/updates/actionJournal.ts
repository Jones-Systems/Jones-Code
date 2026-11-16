import * as Effect from "effect/Effect";
import { createJonesActionJournal } from "@t3tools/client-runtime/jones/updates/action-journal";
import { runtime } from "../../lib/runtime";
import { MobileSecureStorage } from "../../persistence/mobile-secure-storage";

export const mobileJonesJournal = createJonesActionJournal({
  getItem: (key) =>
    runtime.runPromise(MobileSecureStorage.pipe(Effect.flatMap((storage) => storage.getItem(key)))),
  setItem: (key, value) =>
    runtime.runPromise(
      MobileSecureStorage.pipe(Effect.flatMap((storage) => storage.setItem(key, value))),
    ),
});
