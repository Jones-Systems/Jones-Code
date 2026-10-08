import { describe, expect, it, vi } from "vite-plus/test";
import { registerCompanionGuest } from "./guest.ts";

describe("companion guest registration", () => {
  it("does not navigate until registration has installed guards", async () => {
    let finish!: () => void;
    const navigate = vi.fn();
    const register = vi.fn(
      () =>
        new Promise<void>((resolve) => {
          finish = resolve;
        }),
    );
    const result = registerCompanionGuest({
      ready: Promise.resolve(),
      isCurrent: () => true,
      register,
      url: () => "https://preview.test/",
      navigate,
    });
    await Promise.resolve();
    expect(register).toHaveBeenCalledOnce();
    expect(navigate).not.toHaveBeenCalled();
    finish();
    await expect(result).resolves.toBe(true);
    expect(navigate).toHaveBeenCalledWith("https://preview.test/");
  });
  it("never loads an assignment removed during registration", async () => {
    let current = true;
    const navigate = vi.fn();
    await expect(
      registerCompanionGuest({
        ready: Promise.resolve(),
        isCurrent: () => current,
        register: async () => {
          current = false;
        },
        url: () => "https://preview.test/",
        navigate,
      }),
    ).resolves.toBe(false);
    expect(navigate).not.toHaveBeenCalled();
  });
  it("keeps about:blank when registration rejects", async () => {
    const navigate = vi.fn();
    await expect(
      registerCompanionGuest({
        ready: Promise.resolve(),
        isCurrent: () => true,
        register: async () => {
          throw new Error("unassigned");
        },
        url: () => "https://preview.test/",
        navigate,
      }),
    ).rejects.toThrow("unassigned");
    expect(navigate).not.toHaveBeenCalled();
  });
});
