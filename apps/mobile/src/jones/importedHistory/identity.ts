import { ImportedHistoryStart } from "@t3tools/contracts";
import { importedHistoryCanonicalJson } from "@t3tools/shared/jones/importedHistoryCanonical";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Crypto from "expo-crypto";

export class MobileImportedHistoryIdentityUnavailable extends Schema.TaggedError<MobileImportedHistoryIdentityUnavailable>()(
  "MobileImportedHistoryIdentityUnavailable",
  { cause: Schema.Unknown },
) {}
export const mobileImportedHistoryIdentity = Effect.fn("mobile.importedHistory.identity")(
  function* (command: ImportedHistoryStart) {
    const encoded = yield* Schema.encodeEffect(ImportedHistoryStart)(command);
    const digest = (value: unknown) =>
      Effect.tryPromise({
        try: () =>
          Crypto.digestStringAsync(
            Crypto.CryptoDigestAlgorithm.SHA256,
            importedHistoryCanonicalJson(value),
            { encoding: Crypto.CryptoEncoding.HEX },
          ),
        catch: (cause) => new MobileImportedHistoryIdentityUnavailable({ cause }),
      });
    return {
      commandDigest: yield* digest(encoded),
      deliveryDigest: yield* digest(encoded.delivery),
    };
  },
);
