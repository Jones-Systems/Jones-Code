import type { BrowserContext, CDPSession, Page, Request } from "playwright";

export const wordTraceLimits = {
  collectionMs: 15_000,
  finalizationMs: 5_000,
  streamBytes: 32 * 1024 * 1024,
  packetBytes: 512 * 1024,
  readBytes: 64 * 1024,
} as const;

const categoryCandidates = [
  "__metadata",
  "blink.user_timing",
  "devtools.timeline",
  "toplevel",
  "v8",
  "v8.wasm",
  "v8.gc",
  "disabled-by-default-v8.gc",
  "disabled-by-default-v8.wasm",
];

export interface WordWorkerMark {
  worker: string;
  traceId: string;
  mark: string;
  phase: string;
}

export interface WordWorkerEvidence {
  marks: WordWorkerMark[];
  disposed: boolean;
  capped: boolean;
}

interface TraceEvent {
  name: string;
  cat?: string;
  ph: string;
  pid: number;
  tid: number;
  ts: number;
  dur?: number;
  tts?: number;
  tdur?: number;
  args?: Record<string, unknown>;
}

interface ResourceRecord {
  id: number;
  url: string;
  events: string[];
  timing: ReturnType<Request["timing"]> | null;
  attribution: "unattributed: native worker/request association unavailable";
}

export interface WordTracePacket {
  schema: "jones-word-ci-trace/1";
  status: "captured" | "hold" | "insufficient";
  identity: unknown;
  configuration: unknown;
  worker: unknown;
  resources: ResourceRecord[];
  events: TraceEvent[];
  coverage: Record<string, unknown>;
  cleanup: Record<string, string>;
  oracles: { attribution: boolean; completion: boolean; coverage: boolean; cleanup: boolean };
  interpretation: string;
  errors: string[];
}

type Completion = { stream?: string; dataLossOccurred?: boolean };

// Tracing is browser-wide even when controlled through a page session. A failed
// or uncertain owner blocks subsequent captures in that browser; never stop it.
const activeBrowsers = new WeakSet<object>();
const heldBrowsers = new WeakSet<object>();
const captures = new WeakMap<Page, WordCapture>();

function bounded<T>(operation: Promise<T>, milliseconds: number): Promise<T> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error("diagnostic operation deadline")),
      milliseconds,
    );
    operation.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error: unknown) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

function safeUrl(value: string): string {
  try {
    const url = new URL(value);
    return url.protocol === "http:" || url.protocol === "https:"
      ? url.origin + url.pathname
      : url.protocol;
  } catch {
    return "unavailable";
  }
}

export class WordCapture {
  private owned = false;
  private startAttempted = false;
  private done = false;
  private session?: CDPSession;
  private watchdog?: ReturnType<typeof setTimeout>;
  private finishing?: Promise<void>;
  private resolveComplete!: (event: Completion) => void;
  private readonly complete = new Promise<Completion>((resolve) => {
    this.resolveComplete = resolve;
  });
  private completion?: Completion;
  private stream: string | undefined;
  private eof = false;
  private parsed = false;
  private events: TraceEvent[] = [];
  private readonly resources = new Map<Request, ResourceRecord>();
  private resourceCap = false;
  private streamCap = false;
  private watchdogFired = false;
  private decodedBytes = 0;
  private finalizationElapsedMs: number | null = null;
  private bufferFull = false;
  private bufferUsageObserved = false;
  private draining = false;
  private categories: string[] = [];
  private supportedCategories: string[] = [];
  private identity: unknown = null;
  private readonly errors: string[] = [];
  private readonly cleanup: Record<string, string> = {};
  private readonly browser: object;

  constructor(
    private readonly context: BrowserContext,
    private readonly page: Page,
  ) {
    this.browser = context.browser() ?? context;
  }

  private readonly onComplete = (event: Completion) => {
    this.completion = event;
    this.stream = event.stream;
    this.resolveComplete(event);
  };
  private readonly onBufferUsage = (event: { percentFull?: number; value?: number }) => {
    const fullness = event.percentFull ?? event.value;
    if (fullness !== undefined && Number.isFinite(fullness)) {
      this.bufferUsageObserved = true;
      this.bufferFull ||= fullness >= 1;
    }
  };

  private resource(request: Request, event: string) {
    let record = this.resources.get(request);
    if (!record) {
      if (this.resources.size >= 128) {
        this.resourceCap = true;
        return;
      }
      record = {
        id: this.resources.size + 1,
        url: safeUrl(request.url()),
        events: [],
        timing: null,
        attribution: "unattributed: native worker/request association unavailable",
      };
      this.resources.set(request, record);
    }
    record.events.push(event);
    try {
      record.timing = request.timing();
    } catch {
      record.timing = null;
    }
  }
  private readonly onRequest = (request: Request) => this.resource(request, "request");
  private readonly onFinished = (request: Request) => this.resource(request, "requestfinished");
  private readonly onFailed = (request: Request) => this.resource(request, "requestfailed");

  async start(): Promise<void> {
    if (activeBrowsers.has(this.browser) || heldBrowsers.has(this.browser)) {
      this.errors.push("browser recorder already owned or held; no start attempted");
      this.done = true;
      return;
    }
    activeBrowsers.add(this.browser);
    this.watchdog = setTimeout(() => {
      this.watchdogFired = true;
      void this.finish();
    }, wordTraceLimits.collectionMs);
    try {
      const attaching = this.context.newCDPSession(this.page);
      void attaching.then(
        (session) => {
          if (this.done) void session.detach().catch(() => {});
        },
        () => {},
      );
      this.session = await bounded(attaching, 1_000);
      this.session.on("Tracing.tracingComplete", this.onComplete);
      this.session.on("Tracing.bufferUsage", this.onBufferUsage);
      this.identity = await bounded(this.session.send("Browser.getVersion"), 1_000);
      const supported = await bounded(this.session.send("Tracing.getCategories"), 1_000);
      this.supportedCategories = supported.categories;
      this.categories = categoryCandidates.filter((category) =>
        supported.categories.includes(category),
      );
      if (!this.categories.includes("blink.user_timing"))
        throw new Error("worker user timing unsupported");
      this.context.on("request", this.onRequest);
      this.context.on("requestfinished", this.onFinished);
      this.context.on("requestfailed", this.onFailed);
      this.startAttempted = true;
      await bounded(
        this.session.send("Tracing.start", {
          transferMode: "ReturnAsStream",
          streamFormat: "json",
          bufferUsageReportingInterval: 250,
          traceConfig: {
            recordMode: "recordUntilFull",
            traceBufferSizeInKb: 16384,
            includedCategories: this.categories,
            enableSampling: false,
          },
        }),
        1_000,
      );
      this.owned = true;
    } catch {
      this.errors.push(
        this.startAttempted
          ? "start rejected or uncertain; capture hold"
          : "capture initialization unavailable",
      );
      heldBrowsers.add(this.browser);
      await this.finish();
    }
  }

  private async attempt(name: string, operation: () => Promise<unknown>, budget: number) {
    try {
      await bounded(operation(), budget);
      this.cleanup[name] = "ok";
    } catch {
      this.cleanup[name] = "failed or unknown";
      this.errors.push(name + " failed or unknown");
    }
  }

  finish(): Promise<void> {
    this.finishing ??= this.finalize();
    return this.finishing;
  }

  private async finalize() {
    if (this.done) return;
    const finalizationStarted = performance.now();
    const session = this.session;
    if (this.watchdog) clearTimeout(this.watchdog);
    try {
      if (session && this.owned) {
        // Keep independent budgets for end, completion/drain, stream close and
        // detach. A hung operation must not consume the later cleanup attempts.
        await this.attempt("end", () => session.send("Tracing.end"), 750);
        try {
          await bounded(this.complete, 750);
          this.cleanup.completion = "ok";
        } catch {
          this.cleanup.completion = "missing";
        }
        if (this.stream) {
          this.draining = true;
          try {
            await bounded(this.drain(session, this.stream), 2_000);
          } catch {
            this.errors.push("stream drain or JSON parse failed");
          } finally {
            this.draining = false;
          }
        }
      }
    } finally {
      if (session && this.stream) {
        await this.attempt(
          "streamClose",
          () => session.send("IO.close", { handle: this.stream! }),
          500,
        );
      }
      // Handler removal and detach are independent, including after parse failure.
      this.cleanup.handlers = "ok";
      for (const remove of [
        () => this.context.off("request", this.onRequest),
        () => this.context.off("requestfinished", this.onFinished),
        () => this.context.off("requestfailed", this.onFailed),
        () => session?.off("Tracing.tracingComplete", this.onComplete),
        () => session?.off("Tracing.bufferUsage", this.onBufferUsage),
      ]) {
        try {
          remove();
        } catch {
          this.cleanup.handlers = "failed";
        }
      }
      if (session) await this.attempt("detach", () => session.detach(), 500);
      this.finalizationElapsedMs = performance.now() - finalizationStarted;
      this.cleanup.finalizationBudget =
        this.finalizationElapsedMs <= wordTraceLimits.finalizationMs ? "ok" : "exceeded";
      this.done = true;
      activeBrowsers.delete(this.browser);
      if (this.startAttempted && !this.clean()) heldBrowsers.add(this.browser);
    }
  }

  private async drain(session: CDPSession, handle: string) {
    const chunks: string[] = [];
    const decoder = new TextDecoder();
    while (this.draining) {
      const chunk = await session.send("IO.read", { handle, size: wordTraceLimits.readBytes });
      if (!this.draining) return;
      const bytes = chunk.base64Encoded
        ? Buffer.from(chunk.data, "base64")
        : Buffer.from(chunk.data);
      this.decodedBytes += bytes.byteLength;
      if (this.decodedBytes > wordTraceLimits.streamBytes) {
        this.streamCap = true;
        return;
      }
      chunks.push(decoder.decode(bytes, { stream: !chunk.eof }));
      if (chunk.eof) {
        this.eof = true;
        break;
      }
    }
    if (!this.eof) return;
    const trace: unknown = JSON.parse(chunks.join(""));
    if (
      !trace ||
      typeof trace !== "object" ||
      !("traceEvents" in trace) ||
      !Array.isArray(trace.traceEvents)
    ) {
      throw new Error("missing traceEvents");
    }
    this.events = trace.traceEvents.filter(
      (event): event is TraceEvent =>
        !!event &&
        typeof event === "object" &&
        typeof event.name === "string" &&
        typeof event.ph === "string" &&
        Number.isFinite(event.pid) &&
        Number.isFinite(event.tid) &&
        Number.isFinite(event.ts),
    );
    this.parsed = this.events.length === trace.traceEvents.length;
  }

  private clean() {
    return (
      this.cleanup.end === "ok" &&
      this.cleanup.completion === "ok" &&
      this.cleanup.handlers === "ok" &&
      this.cleanup.detach === "ok" &&
      this.cleanup.finalizationBudget === "ok" &&
      !!this.stream &&
      this.cleanup.streamClose === "ok"
    );
  }

  packet(evidence: WordWorkerEvidence): WordTracePacket {
    const marks = evidence.marks;
    const markNames = new Set(marks.map((mark) => mark.mark));
    const nativeMarks = this.events.filter(
      (event) => markNames.has(event.name) && event.cat?.split(",").includes("blink.user_timing"),
    );
    const workers = new Set(marks.map((mark) => mark.worker));
    const traceIds = new Set(marks.map((mark) => mark.traceId));
    const threads = new Set(nativeMarks.map((event) => event.pid + ":" + event.tid));
    const target = threads.size === 1 ? nativeMarks[0] : undefined;
    const targetEvents = target
      ? this.events.filter((event) => event.pid === target.pid && event.tid === target.tid)
      : [];
    const metadata = target
      ? this.events.filter(
          (event) =>
            event.ph === "M" &&
            event.pid === target.pid &&
            (event.name === "process_name" ||
              (event.name === "thread_name" && event.tid === target.tid)),
        )
      : [];
    const phase = (name: string) =>
      nativeMarks.find((event) =>
        marks.some((mark) => mark.phase === name && mark.mark === event.name),
      );
    const start = phase("session-start");
    const ready = phase("session-ready");
    const intervalPresent = !!start && !!ready && ready.ts > start.ts;
    const attribution =
      workers.size === 1 &&
      traceIds.size === 1 &&
      threads.size === 1 &&
      nativeMarks.length === marks.length &&
      marks.length > 0 &&
      !evidence.capped;
    const completion =
      this.cleanup.end === "ok" && !!this.completion && !!this.stream && this.eof && this.parsed;
    const coverage =
      completion &&
      attribution &&
      intervalPresent &&
      metadata.some((event) => event.name === "thread_name") &&
      this.completion?.dataLossOccurred === false &&
      !this.streamCap &&
      !this.resourceCap &&
      !evidence.capped &&
      !this.watchdogFired &&
      evidence.disposed &&
      this.bufferUsageObserved &&
      !this.bufferFull;
    const packet: WordTracePacket = {
      schema: "jones-word-ci-trace/1",
      status: completion ? "captured" : "hold",
      identity: this.identity,
      configuration: {
        supportedCategories: this.supportedCategories,
        requestedCategories: this.categories,
        recordMode: "recordUntilFull",
        traceBufferSizeInKb: 16384,
        transferMode: "ReturnAsStream",
        streamFormat: "json",
        limits: wordTraceLimits,
        sampling: false,
        scope: "browser-wide collection; exact worker thread analysis",
        overhead: "unmeasured; not zero",
      },
      worker: {
        marks,
        traceThread: target ? { pid: target.pid, tid: target.tid } : null,
        sessionInterval: intervalPresent
          ? { start: start!.ts, end: ready!.ts, unit: "trace microseconds" }
          : null,
        censored: !!start && !ready,
        originalDisposalObserved: evidence.disposed,
      },
      resources: [...this.resources.values()],
      events: [...new Set([...metadata, ...targetEvents])],
      coverage: {
        dataLossOccurred: this.completion?.dataLossOccurred ?? null,
        decodedBytes: this.decodedBytes,
        eof: this.eof,
        parsed: this.parsed,
        streamCap: this.streamCap,
        resourceCap: this.resourceCap,
        workerEvidenceCap: evidence.capped,
        watchdogFired: this.watchdogFired,
        intervalPresent,
        bufferUsageObserved: this.bufferUsageObserved,
        bufferFull: this.bufferFull,
        finalizationElapsedMs: this.finalizationElapsedMs,
        packetCap: false,
        nativeResourceAssociation: "unsupported",
        backgroundCompilerAssociation: "unattributed",
        threadCpuEvidence: "unqualified: no CPU conclusion",
        threadCpuTimestampsPresent: targetEvents.some((event) => Number.isFinite(event.tts)),
        unfinishedEvents: "censored; durations overlap, not an additive partition",
      },
      cleanup: { ...this.cleanup },
      oracles: { attribution, completion, coverage, cleanup: this.clean() },
      interpretation:
        "Capture qualification only. Missing evidence is unattributed, never waiting. A pass proves neither performance acceptance nor repair.",
      errors: [...this.errors],
    };
    if (Buffer.byteLength(JSON.stringify(packet)) > wordTraceLimits.packetBytes) {
      packet.status = "insufficient";
      packet.events = [];
      packet.resources = [];
      packet.worker = { evidence: "necessary evidence exceeds packet limit" };
      packet.coverage.packetCap = true;
      packet.oracles.attribution = false;
      packet.oracles.coverage = false;
      packet.errors.push("necessary evidence does not fit; no truncated positive proof");
      if (Buffer.byteLength(JSON.stringify(packet)) > wordTraceLimits.packetBytes) {
        packet.identity = null;
        packet.configuration = {
          limits: wordTraceLimits,
          evidence: "identity/configuration also exceeds packet limit",
        };
      }
    }
    return packet;
  }
}

export async function wordCiTraceCommand(
  { context, page }: { context: BrowserContext; page: Page },
  action: "start" | "finish",
  evidence: WordWorkerEvidence = { marks: [], disposed: false, capped: false },
): Promise<WordTracePacket | null> {
  if (action === "start") {
    if (captures.has(page)) return captures.get(page)!.packet(evidence);
    const capture = new WordCapture(context, page);
    captures.set(page, capture);
    await capture.start();
    return null;
  }
  const capture = captures.get(page);
  if (!capture) throw new Error("no owned Word capture");
  await capture.finish();
  return capture.packet(evidence);
}
