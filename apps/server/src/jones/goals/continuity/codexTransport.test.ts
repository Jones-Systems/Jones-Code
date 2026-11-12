import { describe, expect, it } from "vite-plus/test";
import { CONTINUITY_RELOAD_CUE } from "./capsule.ts";
import { codexContinuityContext } from "./codexTransport.ts";
import { CONTINUITY_LIMITS, utf8Bytes } from "./model.ts";

function capsule(bytes: number, multibyte = false) {
  const room = bytes - utf8Bytes(CONTINUITY_RELOAD_CUE) - 11;
  const content = multibyte
    ? "界".repeat(Math.floor(room / 3)) + "x".repeat(room % 3)
    : "x".repeat(room);
  return `{"data":"${content}"}` + CONTINUITY_RELOAD_CUE;
}

describe("Codex continuity context transport", () => {
  it.each([3000, 3999, 4000])("keeps %s bytes in a byte-identical single value", (bytes) => {
    const text = capsule(bytes, true);
    expect(utf8Bytes(text)).toBe(bytes);
    expect(codexContinuityContext(text)).toEqual({
      jones_goal_continuity: { kind: "untrusted", value: text },
    });
  });

  it.each([false, true])(
    "splits only at the cue for every overflow size, multibyte=%s",
    (multibyte) => {
      for (let bytes = 4001; bytes <= CONTINUITY_LIMITS.capsuleBytes; bytes++) {
        const text = capsule(bytes, multibyte);
        expect(utf8Bytes(text)).toBe(bytes);
        const context = codexContinuityContext(text)!;
        expect(Object.keys(context)).toEqual([
          "jones_goal_continuity",
          "jones_goal_continuity_reload",
        ]);
        expect(context.jones_goal_continuity_reload!.value).toBe(CONTINUITY_RELOAD_CUE);
        const parts = Object.values(context).map((entry) => entry.value);
        expect(parts.join("")).toBe(text);
        for (const part of parts) expect(utf8Bytes(part)).toBeLessThanOrEqual(4000);
        expect(JSON.parse(context.jones_goal_continuity!.value)).toEqual(
          JSON.parse(text.slice(0, -CONTINUITY_RELOAD_CUE.length)),
        );
      }
    },
  );

  it("fails closed for invalid endings, an oversized logical capsule or an oversized body", () => {
    const text = capsule(4001);
    for (const invalid of [
      text.slice(0, -1),
      text + "\n",
      text.replace("Reload with", "Reload via"),
      "missing cue",
      capsule(4097),
      capsule(4200),
    ]) {
      expect(codexContinuityContext(invalid)).toBeNull();
    }
  });
});
