import * as NodeEvents from "node:events";
import type { BrowserContext, Page, Request } from "playwright";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import { WordCapture, wordTraceLimits, type WordWorkerEvidence } from "./wordCiTrace";

const evidence: WordWorkerEvidence = {
  marks: [
    {
      worker: "exact document worker object",
      traceId: "unique",
      mark: "unique-start",
      phase: "session-start",
    },
    {
      worker: "exact document worker object",
      traceId: "unique",
      mark: "unique-ready",
      phase: "session-ready",
    },
  ],
  disposed: true,
  capped: false,
};

const traceEvents = [
  { name: "process_name", ph: "M", pid: 1, tid: 0, ts: 0, args: { name: "Renderer" } },
  { name: "thread_name", ph: "M", pid: 1, tid: 2, ts: 0, args: { name: "DedicatedWorker thread" } },
  { name: "unique-start", cat: "blink.user_timing", ph: "I", pid: 1, tid: 2, ts: 100 },
  { name: "unique-ready", cat: "blink.user_timing", ph: "I", pid: 1, tid: 2, ts: 200 },
  { name: "V8.WasmCompile", cat: "v8.wasm", ph: "X", pid: 1, tid: 2, ts: 120, dur: 30 },
  { name: "unrelated worker", cat: "v8.wasm", ph: "X", pid: 1, tid: 3, ts: 120, dur: 30 },
];

class Session extends NodeEvents.EventEmitter {
  calls: { method: string; params: Record<string, unknown> | undefined }[] = [];
  body = JSON.stringify({ traceEvents });
  chunks: { data: string; eof: boolean; base64Encoded: boolean }[] = [];
  fail = new Set<string>();
  hang = new Set<string>();
  completion = true;
  stream = true;
  loss: boolean | undefined = false;
  bufferFull = false;
  detach = vi.fn(async () => {
    if (this.fail.has("detach")) throw new Error("detach");
  });
  async send(method: string, params?: Record<string, unknown>) {
    this.calls.push({ method, params });
    if (this.hang.has(method)) return new Promise(() => {});
    if (this.fail.has(method)) throw new Error(method);
    if (method === "Browser.getVersion")
      return { product: "fake qualification browser", protocolVersion: "1.3" };
    if (method === "Tracing.getCategories")
      return { categories: ["__metadata", "blink.user_timing", "toplevel", "v8.wasm"] };
    if (method === "Tracing.start")
      this.emit("Tracing.bufferUsage", { percentFull: this.bufferFull ? 1 : 0.1 });
    if (method === "Tracing.end" && this.completion) {
      this.emit("Tracing.tracingComplete", {
        dataLossOccurred: this.loss,
        ...(this.stream ? { stream: "owned-stream" } : {}),
      });
    }
    if (method === "IO.read") return this.chunks.shift() ?? { data: this.body, eof: true };
    return {};
  }
}

function fixture() {
  const session = new Session();
  const context = new NodeEvents.EventEmitter();
  const browser = {};
  Object.assign(context, { browser: () => browser, newCDPSession: async () => session });
  const capture = new WordCapture(context as unknown as BrowserContext, {} as Page);
  return { session, context, capture, browser };
}

afterEach(() => {
  vi.useRealTimers();
});

describe("bounded Word native trace capture", () => {
  it("requires exact worker marks, native interval, metadata, no loss/caps, and independent cleanup", async () => {
    const { session, capture, context } = fixture();
    await capture.start();
    const request = {
      url: () => "https://fixture.invalid/file.wasm?token=never-retain#secret",
      timing: () => ({ startTime: 123, responseStart: 10, responseEnd: -1 }),
    } as Request;
    context.emit("request", request);
    context.emit("requestfinished", request);
    await capture.finish();
    const packet = capture.packet(evidence);
    expect(packet.oracles).toEqual({
      attribution: true,
      completion: true,
      coverage: true,
      cleanup: true,
    });
    expect(packet.resources[0]).toMatchObject({
      url: "https://fixture.invalid/file.wasm",
      events: ["request", "requestfinished"],
      timing: { responseEnd: -1 },
    });
    expect(packet.resources[0]?.attribution).toContain("unattributed");
    expect(packet.events).toContainEqual(traceEvents[4]);
    expect(packet.events).not.toContainEqual(traceEvents[5]);
    expect(packet.coverage.threadCpuEvidence).toContain("no CPU conclusion");
    expect(session.calls.find((call) => call.method === "Tracing.start")?.params).toMatchObject({
      transferMode: "ReturnAsStream",
      streamFormat: "json",
      traceConfig: {
        recordMode: "recordUntilFull",
        traceBufferSizeInKb: 16384,
        enableSampling: false,
      },
    });
    expect(session.calls.find((call) => call.method === "IO.read")?.params).toEqual({
      handle: "owned-stream",
      size: 65536,
    });
    expect(session.eventNames()).toEqual([]);
    expect(context.eventNames()).toEqual([]);
    expect(session.detach).toHaveBeenCalledOnce();
  });

  it("never ends a competing recorder and holds after a rejected or uncertain start", async () => {
    const { session, capture, context } = fixture();
    session.fail.add("Tracing.start");
    await capture.start();
    expect(session.calls.some((call) => call.method === "Tracing.end")).toBe(false);
    expect(session.detach).toHaveBeenCalledOnce();
    const next = new WordCapture(context as unknown as BrowserContext, {} as Page);
    await next.start();
    await next.finish();
    expect(session.calls.filter((call) => call.method === "Tracing.start")).toHaveLength(1);
    expect(next.packet(evidence).status).toBe("hold");
  });

  it("does not let a concurrent page stop the first page's owned recording", async () => {
    const { session, capture, context } = fixture();
    await capture.start();
    const second = new WordCapture(context as unknown as BrowserContext, {} as Page);
    await second.start();
    await second.finish();
    expect(session.calls.some((call) => call.method === "Tracing.end")).toBe(false);
    await capture.finish();
    expect(session.calls.filter((call) => call.method === "Tracing.end")).toHaveLength(1);
  });

  it("has an independent collection watchdog, censors early stop and removes its timers", async () => {
    vi.useFakeTimers();
    const { capture } = fixture();
    await capture.start();
    await vi.advanceTimersByTimeAsync(wordTraceLimits.collectionMs);
    await capture.finish();
    expect(capture.packet(evidence).coverage.watchdogFired).toBe(true);
    expect(capture.packet(evidence).oracles.coverage).toBe(false);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("decodes base64 stream chunks across UTF-8 boundaries without altering original events", async () => {
    const { session, capture } = fixture();
    const original = { ...traceEvents[4], args: { name: "é" } };
    const bytes = Buffer.from(JSON.stringify({ traceEvents: [...traceEvents, original] }));
    const split = bytes.indexOf(Buffer.from("é")) + 1;
    session.chunks = [bytes.subarray(0, split), bytes.subarray(split)].map((chunk, index) => ({
      data: chunk.toString("base64"),
      base64Encoded: true,
      eof: index === 1,
    }));
    await capture.start();
    await capture.finish();
    expect(capture.packet(evidence).events).toContainEqual(original);
    expect(capture.packet(evidence).oracles.completion).toBe(true);
  });

  it("attempts every handler removal and detach when a removal throws", async () => {
    const { context, session, capture } = fixture();
    const off = context.off.bind(context);
    vi.spyOn(context, "off").mockImplementation((name, listener) => {
      off(name, listener);
      if (name === "request") throw new Error("controlled removal failure");
      return context;
    });
    await capture.start();
    await capture.finish();
    expect(context.eventNames()).toEqual([]);
    expect(session.eventNames()).toEqual([]);
    expect(session.detach).toHaveBeenCalledOnce();
    expect(capture.packet(evidence).oracles.cleanup).toBe(false);
  });

  it.each(["Tracing.end", "IO.read", "IO.close", "detach", "parse", "no-completion", "no-stream"])(
    "attempts remaining cleanup independently after %s failure",
    async (failure) => {
      vi.useFakeTimers();
      const { session, capture, context } = fixture();
      if (failure === "parse") session.body = "invalid JSON";
      else if (failure === "no-completion") session.completion = false;
      else if (failure === "no-stream") session.stream = false;
      else session.fail.add(failure);
      await capture.start();
      const finished = capture.finish();
      await vi.runAllTimersAsync();
      await finished;
      const packet = capture.packet(evidence);
      expect(session.calls.some((call) => call.method === "Tracing.end")).toBe(true);
      if (failure !== "Tracing.end" && failure !== "no-completion" && failure !== "no-stream") {
        expect(session.calls.some((call) => call.method === "IO.close")).toBe(true);
      }
      expect(session.detach).toHaveBeenCalledOnce();
      expect(session.eventNames()).toEqual([]);
      expect(context.eventNames()).toEqual([]);
      expect(vi.getTimerCount()).toBe(0);
      expect(packet.oracles.completion && packet.oracles.cleanup).toBe(false);
    },
  );

  it.each(["Tracing.start", "Tracing.end", "IO.read"])(
    "bounds an unresolved %s operation",
    async (method) => {
      vi.useFakeTimers();
      const { session, capture } = fixture();
      session.hang.add(method);
      const started = capture.start();
      await vi.advanceTimersByTimeAsync(1_001);
      await started;
      const finished = capture.finish();
      await vi.runAllTimersAsync();
      await finished;
      expect(capture.packet(evidence).oracles.completion).toBe(false);
      expect(session.detach).toHaveBeenCalledOnce();
      expect(vi.getTimerCount()).toBe(0);
    },
  );

  it.each([
    "missing marks",
    "missing metadata",
    "loss unknown",
    "data loss",
    "buffer full",
    "not disposed",
    "worker evidence capped",
  ])("cannot qualify coverage with %s", async (condition) => {
    const { session, capture } = fixture();
    if (condition === "missing marks")
      session.body = JSON.stringify({ traceEvents: traceEvents.slice(0, 2) });
    if (condition === "missing metadata")
      session.body = JSON.stringify({ traceEvents: traceEvents.slice(2) });
    if (condition === "loss unknown") session.loss = undefined;
    if (condition === "data loss") session.loss = true;
    if (condition === "buffer full") session.bufferFull = true;
    await capture.start();
    await capture.finish();
    const packet = capture.packet({
      ...evidence,
      disposed: condition !== "not disposed",
      capped: condition === "worker evidence capped",
    });
    expect(packet.oracles.completion).toBe(true);
    expect(packet.oracles.coverage).toBe(false);
  });

  it("closes an oversized stream and rejects an oversized evidence packet without positive truncation", async () => {
    const first = fixture();
    first.session.body = "x".repeat(wordTraceLimits.streamBytes + 1);
    await first.capture.start();
    await first.capture.finish();
    expect(first.capture.packet(evidence).coverage.streamCap).toBe(true);
    expect(first.session.calls.some((call) => call.method === "IO.close")).toBe(true);
    const second = fixture();
    second.session.body = JSON.stringify({
      traceEvents: [
        ...traceEvents,
        { ...traceEvents[4], args: { oversized: "x".repeat(wordTraceLimits.packetBytes) } },
      ],
    });
    await second.capture.start();
    await second.capture.finish();
    const packet = second.capture.packet(evidence);
    expect(packet.status).toBe("insufficient");
    expect(packet.events).toEqual([]);
    expect(packet.oracles.coverage).toBe(false);
    expect(Buffer.byteLength(JSON.stringify(packet))).toBeLessThan(wordTraceLimits.packetBytes);
  });
});
