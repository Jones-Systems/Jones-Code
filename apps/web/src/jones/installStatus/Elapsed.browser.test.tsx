import { describe, expect, it, vi } from "vite-plus/test";
import { render } from "../../../test/render";
import "../../index.css";
import { Elapsed } from "./Elapsed";

const epoch = Date.parse("2026-11-10T12:00:00Z");
const text = (container: HTMLElement) => container.querySelector("[data-elapsed]")?.textContent;

/** Mocks only Date; the second clock's own timer stays real, so ticks still arrive. */
function fixedDate(at: number) {
  vi.setSystemTime(at);
  return {
    set: (next: number) => vi.setSystemTime(next),
    [Symbol.dispose]: () => vi.useRealTimers(),
  };
}

describe("Elapsed", () => {
  it("counts host time with the host clock's skew and ticks each second", async () => {
    using clock = fixedDate(epoch);
    // The host clock runs 5 minutes ahead of this client. A step that started 90 host-seconds
    // ago reads 1:30 here, not 6:30 and not a negative time.
    const skewMs = 5 * 60_000;
    await using screen = await render(<Elapsed since={epoch + skewMs - 90_000} skewMs={skewMs} />, {
      router: false,
    });
    expect(text(screen.container)).toBe("1:30");
    clock.set(epoch + 2_000);
    await expect.poll(() => text(screen.container), { timeout: 4_000 }).toBe("1:32");
  });

  it("switches to hours after an hour", async () => {
    using _clock = fixedDate(epoch);
    await using screen = await render(
      <Elapsed since={epoch - (2 * 3600 + 5 * 60 + 7) * 1000} skewMs={0} />,
      { router: false },
    );
    expect(text(screen.container)).toBe("2:05:07");
  });
});
