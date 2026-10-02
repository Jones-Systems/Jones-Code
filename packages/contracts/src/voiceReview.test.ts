import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import {
  VoiceReviewRevisionPayload,
  VoiceReviewEditSavePayload,
  VoiceReviewEditCancelPayload,
} from "./voiceReview.ts";

const decodeRevision = Schema.decodeUnknownSync(VoiceReviewRevisionPayload);
const decodeEditSave = Schema.decodeUnknownSync(VoiceReviewEditSavePayload);
const decodeEditCancel = Schema.decodeUnknownSync(VoiceReviewEditCancelPayload);

describe("voice review mutation wire contract", () => {
  it("rejects coercion, nonpositive revisions, and extra fields", () => {
    const decode = decodeRevision;
    expect(decode({ expected_revision: 1 })).toEqual({ expected_revision: 1 });
    for (const value of [true, false, "1", 0, -1, 1.5, null]) {
      expect(() => decode({ expected_revision: value })).toThrow();
    }
    expect(() => decode({ expected_revision: 1, source_id: "other" })).toThrow();
    expect(() => decode({ expected_revision: 1, edit_handle: "secret" })).toThrow();
  });
  it("requires an edit handle and preserves literal text with the 100000 character ceiling", () => {
    const decode = decodeEditSave;
    const valid = {
      expected_revision: 2,
      edit_handle: "opaque",
      text: " <script>literal</script> ",
    };
    expect(decode(valid)).toEqual(valid);
    expect(() => decode({ ...valid, text: " \n " })).toThrow();
    expect(() => decode({ ...valid, text: "a".repeat(100_001) })).toThrow();
    expect(() => decode({ ...valid, edit_handle: "" })).toThrow();
    expect(() => decodeEditCancel({ expected_revision: 1 })).toThrow();
  });
});
