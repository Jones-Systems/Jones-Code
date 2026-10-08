import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import { ImportedHistoryStart, CommandId, ThreadId, MessageId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { importedHistoryCanonicalJson } from "@t3tools/shared/jones/importedHistoryCanonical";
vi.mock("expo-crypto", async () => {
  const { createHash } = await import("node:crypto");
  return {
    CryptoDigestAlgorithm: { SHA256: "SHA-256" },
    CryptoEncoding: { HEX: "hex" },
    digestStringAsync: async (_algorithm: string, bytes: string) =>
      createHash("sha256").update(bytes).digest("hex"),
  };
});
import { mobileImportedHistoryIdentity } from "./identity";
it.effect("uses schema encoded canonical UTF8 bytes for native and server identical digests", () =>
  Effect.gen(function* () {
    const command: ImportedHistoryStart = {
      type: "thread.imported-history.start",
      commandId: CommandId.make("command:test"),
      threadId: ThreadId.make("thread:test"),
      reviewedBasis: "c".repeat(64),
      delivery: {
        type: "message",
        command: {
          type: "message.dispatch",
          commandId: CommandId.make("command:test"),
          threadId: ThreadId.make("thread:test"),
          messageId: MessageId.make("message:test"),
          createdBy: "user",
          creationSource: "mobile",
          text: "Synthetic π 😀",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
        },
      },
    };
    const encoded = yield* Schema.encodeEffect(ImportedHistoryStart)(command);
    expect(importedHistoryCanonicalJson(encoded)).toBe(
      '{"commandId":"command:test","delivery":{"command":{"attachments":[],"commandId":"command:test","createdBy":"user","creationSource":"mobile","dispatchMode":{"type":"start_immediately"},"messageId":"message:test","text":"Synthetic π 😀","threadId":"thread:test","type":"message.dispatch"},"type":"message"},"reviewedBasis":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","threadId":"thread:test","type":"thread.imported-history.start"}',
    );
    expect(yield* mobileImportedHistoryIdentity(command)).toEqual({
      commandDigest: "15b6513d8cc2f38058c38c122c2c26653d6267993e016bac9dd7a6ae776794b9",
      deliveryDigest: "38edad639147b7978505a297152edc20e5ec5d4f26fa049f1d376a83256b3810",
    });
  }),
);
