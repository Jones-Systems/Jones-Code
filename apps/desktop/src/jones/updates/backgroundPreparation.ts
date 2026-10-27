// @effect-diagnostics nodeBuiltinImport:off - Jones-owned settings file beside the desktop settings.
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import type { JonesActionsCandidate } from "@t3tools/shared/jones/jonesActions";
import * as Schema from "effect/Schema";

import {
  BACKGROUND_FAILURE_BACKOFF_MS,
  decideBackgroundPreparation,
  type JonesDesktopPreparationState,
  type PreparationHold,
} from "./backgroundPreparationPolicy.ts";
import type { JonesSupersedeResult } from "./JonesDesktopUpdates.ts";
import type { JonesReclaimResult } from "./stagingJobs.ts";

const HANDLE = /^[a-f0-9]{64}$/;
const SOURCE = /^[a-f0-9]{40}$/;
/** A payload whose removal failed is retried at most this often. */
const RECLAIM_RETRY_MS = 60 * 60_000;
/** The ledger never grows past this; replacement pauses long before it is reached. */
const MAX_LEDGER_ENTRIES = 16;
const UNREADABLE =
  "The background preparation settings could not be read, so background preparation stays off.";

const BackgroundPreparationDocument = Schema.Struct({
  schema: Schema.Literal(1),
  backgroundPreparation: Schema.Struct({
    enabled: Schema.Boolean,
    declinedSource: Schema.optionalKey(Schema.String.check(Schema.isPattern(SOURCE))),
    unreclaimed: Schema.Array(Schema.String.check(Schema.isPattern(HANDLE))).check(
      Schema.isMaxLength(MAX_LEDGER_ENTRIES),
    ),
  }),
});
const decodeDocument = Schema.decodeUnknownSync(BackgroundPreparationDocument);

export interface BackgroundPreparationRecord {
  readonly enabled: boolean;
  readonly declinedSource?: string;
  /** Replaced handles whose payload removal has not been proved. */
  readonly unreclaimed: ReadonlyArray<string>;
}

export interface BackgroundPreparationStore {
  readonly read: () => Promise<BackgroundPreparationRecord>;
  readonly write: (record: BackgroundPreparationRecord) => Promise<void>;
}

/** Off unless the user enabled it: a missing file is the default, not an error. */
export function fileBackgroundPreparationStore(path: string): BackgroundPreparationStore {
  return {
    read: async () => {
      let raw: string;
      try {
        const info = await NodeFSP.lstat(path);
        if (!info.isFile() || info.size > 65536)
          throw new Error("The background preparation settings are not a bounded file.");
        raw = await NodeFSP.readFile(path, "utf8");
      } catch (cause) {
        if ((cause as NodeJS.ErrnoException).code === "ENOENT")
          return { enabled: false, unreclaimed: [] };
        throw cause;
      }
      return decodeDocument(JSON.parse(raw)).backgroundPreparation;
    },
    write: async (record) => {
      const directory = NodePath.dirname(path);
      await NodeFSP.mkdir(directory, { recursive: true, mode: 0o700 });
      const scratch = `${path}.${NodeCrypto.randomBytes(8).toString("hex")}.tmp`;
      const document = decodeDocument({ schema: 1, backgroundPreparation: record });
      const file = await NodeFSP.open(scratch, "wx", 0o600);
      try {
        await file.writeFile(JSON.stringify(document) + "\n", "utf8");
        await file.sync();
      } finally {
        await file.close();
      }
      try {
        await NodeFSP.rename(scratch, path);
      } catch (cause) {
        await NodeFSP.unlink(scratch).catch(() => undefined);
        throw cause;
      }
      const parent = await NodeFSP.open(directory, "r");
      try {
        await parent.sync();
      } finally {
        await parent.close();
      }
    },
  };
}

export interface BackgroundPreparationController {
  readonly busy: string | null;
  readonly latestAvailable: JonesActionsCandidate | undefined;
  readonly stagedBuild:
    | { readonly handle: string; readonly source: string; readonly version: string }
    | undefined;
  readonly preparationHold: PreparationHold | undefined;
  readonly check: () => Promise<boolean>;
  readonly download: (selection: {
    readonly artifactId: number;
    readonly sourceSha: string;
  }) => Promise<{ readonly completed: boolean }>;
  readonly discard: (handle: string) => Promise<{ accepted: boolean; completed: boolean }>;
  readonly supersede: (
    handle: string,
    next: JonesActionsCandidate,
    reclaim: (handle: string) => Promise<void>,
  ) => Promise<JonesSupersedeResult>;
  readonly reclaimExclusive: (reclaim: () => Promise<void>) => Promise<boolean>;
  readonly setPreparationStatus: (status: JonesDesktopPreparationState) => Promise<void>;
}

export interface JonesBackgroundPreparationOptions {
  readonly controller: BackgroundPreparationController;
  readonly store: BackgroundPreparationStore;
  /** Removes a replaced payload in the preparation process. */
  readonly reclaim: (handle: string) => Promise<JonesReclaimResult>;
  readonly now?: () => number;
  readonly settleMs?: number;
  readonly backoffMs?: number;
  readonly maxUnreclaimed?: number;
}

/**
 * The desktop's own background preparation. It acts only after a check, holds one prepared
 * build, replaces an unaccepted one with a settled newer build, and never installs.
 */
export class JonesBackgroundPreparation {
  readonly #options: JonesBackgroundPreparationOptions;
  readonly #controller: BackgroundPreparationController;
  #record: BackgroundPreparationRecord = { enabled: false, unreclaimed: [] };
  /** False after an unreadable ledger: replaced payloads may exist that this process cannot see. */
  #ledgerKnown = true;
  #latestSince: { readonly source: string; readonly since: number } | undefined;
  readonly #retryAt = new Map<string, number>();
  readonly #reclaimRetryAt = new Map<string, number>();
  #running: Promise<void> | undefined;
  #again = false;
  #closed = false;

  constructor(options: JonesBackgroundPreparationOptions) {
    this.#options = options;
    this.#controller = options.controller;
  }

  get enabled() {
    return this.#record.enabled;
  }

  #now() {
    return (this.#options.now ?? Date.now)();
  }

  /** Reads the setting; preparation itself never delays the caller. */
  async start(): Promise<void> {
    try {
      this.#record = await this.#options.store.read();
    } catch {
      this.#ledgerKnown = false;
      this.#record = { enabled: false, unreclaimed: [] };
    }
    this.#kick();
  }

  /** The scheduled and manual check path: prepare only from what a check just observed. */
  async check(): Promise<boolean> {
    const checked = await this.#controller.check();
    this.#kick();
    return checked;
  }

  /** Turning off does not cancel a running download; it stops every later action. */
  async setEnabled(enabled: boolean): Promise<void> {
    if (this.#record.enabled !== enabled) await this.#persist({ ...this.#record, enabled });
    // A running action can hold the next decision for minutes; the switch reflects the change now.
    await this.#controller.setPreparationStatus(this.#decide().status);
    this.#kick();
  }

  /** A user's Discard declines that build; only a newer one is prepared automatically. */
  async discard(handle: string): Promise<{ accepted: boolean; completed: boolean }> {
    const staged = this.#controller.stagedBuild;
    const result = await this.#controller.discard(handle);
    if (result.completed && staged?.handle === handle)
      await this.#persist({ ...this.#record, declinedSource: staged.source }).catch(
        () => undefined,
      );
    this.#kick();
    return result;
  }

  /** Keeps the status truthful around a manual Download or Install. */
  async track<T>(action: () => Promise<T>): Promise<T> {
    const pending = action();
    this.#kick();
    try {
      return await pending;
    } finally {
      this.#kick();
    }
  }

  close(): void {
    this.#closed = true;
  }

  /** Settles once no decision or action is running. */
  idle(): Promise<void> {
    return this.#running ?? Promise.resolve();
  }

  #kick(): void {
    void this.tick().catch(() => undefined);
  }

  tick(): Promise<void> {
    if (this.#running !== undefined) {
      this.#again = true;
      return this.#running;
    }
    const running = (async () => {
      try {
        do {
          this.#again = false;
          await this.#run();
        } while (this.#again && !this.#closed);
      } finally {
        this.#running = undefined;
      }
    })();
    this.#running = running;
    return running;
  }

  async #persist(record: BackgroundPreparationRecord): Promise<void> {
    // Never replace settings this process could not read: they may record replaced payloads.
    if (!this.#ledgerKnown) throw new Error(UNREADABLE);
    await this.#options.store.write(record);
    this.#record = record;
  }

  async #forget(handle: string): Promise<void> {
    this.#reclaimRetryAt.delete(handle);
    if (!this.#record.unreclaimed.includes(handle)) return;
    await this.#persist({
      ...this.#record,
      unreclaimed: this.#record.unreclaimed.filter((entry) => entry !== handle),
    });
  }

  /** A referenced handle is live again or activation evidence; either way it is not ours. */
  async #reclaim(handle: string): Promise<void> {
    try {
      await this.#options.reclaim(handle);
    } catch (cause) {
      this.#reclaimRetryAt.set(handle, this.#now() + RECLAIM_RETRY_MS);
      throw cause;
    }
    await this.#forget(handle);
  }

  /** Only while nothing holds the controller: retained or claimed work may still need a payload. */
  async #retryReclaims(): Promise<void> {
    if (this.#controller.preparationHold !== undefined) return;
    for (const handle of this.#record.unreclaimed) {
      if ((this.#reclaimRetryAt.get(handle) ?? 0) > this.#now()) continue;
      await this.#controller.reclaimExclusive(() => this.#reclaim(handle)).catch(() => undefined);
    }
  }

  #decide() {
    const now = this.#now();
    const latest = this.#controller.latestAvailable;
    if (latest?.source !== this.#latestSince?.source)
      this.#latestSince = latest === undefined ? undefined : { source: latest.source, since: now };
    const staged = this.#controller.stagedBuild;
    return decideBackgroundPreparation({
      enabled: this.#record.enabled,
      now,
      hold: this.#controller.preparationHold,
      busy: this.#controller.busy,
      staged: staged === undefined ? undefined : { source: staged.source, version: staged.version },
      latest: latest === undefined ? undefined : { source: latest.source, version: latest.version },
      latestSince: this.#latestSince?.since,
      declinedSource: this.#record.declinedSource,
      retryAt: latest === undefined ? undefined : this.#retryAt.get(latest.source),
      unreclaimed: this.#record.unreclaimed.length,
      ...(this.#options.settleMs === undefined ? {} : { settleMs: this.#options.settleMs }),
      ...(this.#options.maxUnreclaimed === undefined
        ? {}
        : { maxUnreclaimed: this.#options.maxUnreclaimed }),
    });
  }

  async #run(): Promise<void> {
    if (this.#closed) return;
    if (!this.#ledgerKnown) {
      await this.#controller.setPreparationStatus({
        enabled: false,
        phase: "blocked",
        reason: UNREADABLE,
      });
      return;
    }
    await this.#retryReclaims();
    // Each action changes what the next decision sees; a check cannot run during one.
    for (let step = 0; step < 3 && !this.#closed; step++) {
      const decision = this.#decide();
      await this.#controller.setPreparationStatus(decision.status);
      if (decision.action === "none") return;
      const latest = this.#controller.latestAvailable;
      if (latest === undefined || latest.source !== decision.target.source) return;
      const backoff = () =>
        this.#retryAt.set(
          latest.source,
          this.#now() + (this.#options.backoffMs ?? BACKGROUND_FAILURE_BACKOFF_MS),
        );
      if (decision.action === "download") {
        const result = await this.#controller.download({
          artifactId: latest.artifactId,
          sourceSha: latest.source,
        });
        if (result.completed) this.#retryAt.delete(latest.source);
        else backoff();
        continue;
      }
      const staged = this.#controller.stagedBuild;
      if (staged === undefined) return;
      // Record the replaced handle before its selection can disappear.
      try {
        await this.#persist({
          ...this.#record,
          unreclaimed: [...this.#record.unreclaimed, staged.handle],
        });
      } catch {
        await this.#controller.setPreparationStatus({
          enabled: true,
          phase: "blocked",
          targetVersion: latest.version,
          reason: "The replaced download could not be recorded, so replacing it is paused.",
        });
        return;
      }
      const outcome = await this.#controller.supersede(staged.handle, latest, (handle) =>
        this.#reclaim(handle),
      );
      // A refused discard left the stage selected: nothing was replaced.
      if (outcome === "refused") await this.#forget(staged.handle).catch(() => undefined);
      if (outcome === "replaced") this.#retryAt.delete(latest.source);
      else backoff();
    }
  }
}
