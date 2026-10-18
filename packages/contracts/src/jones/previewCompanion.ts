import * as Schema from "effect/Schema";

import {
  EnvironmentId,
  NonNegativeInt,
  PositiveInt,
  ThreadId,
  TrimmedNonEmptyString,
} from "../baseSchemas.ts";
import { DesktopBrowserCommand, DesktopBrowserEvent } from "../desktopBrowser.ts";
import { PreviewTabId } from "../preview.ts";
import { PreviewAutomationRuntimeIdentity } from "../previewAutomation.ts";

export const PREVIEW_COMPANION_PROTOCOL = 1;
export const PREVIEW_COMPANION_WS_PATH = "/api/jones/preview-companion/ws";
export const PREVIEW_COMPANION_HTTP_BASE = "/api/jones/preview-companion";
export const PREVIEW_STREAM_RENDER_HOST_UNAVAILABLE_CLOSE_CODE = 4504;

export const PreviewCompanionHostId = TrimmedNonEmptyString.check(
  Schema.isMaxLength(64),
  Schema.isPattern(/^[A-Za-z0-9_-]+$/),
);
export type PreviewCompanionHostId = typeof PreviewCompanionHostId.Type;

export const PreviewCompanionCapabilities = Schema.Struct({
  cdp: Schema.Literal(true),
  clipboardText: Schema.Boolean,
  uploads: Schema.Literal(false),
  downloads: Schema.Literal(false),
  recording: Schema.Literal(false),
});
export type PreviewCompanionCapabilities = typeof PreviewCompanionCapabilities.Type;

export const PreviewRenderHostSelection = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("server") }),
  Schema.Struct({ _tag: Schema.Literal("companion"), hostId: PreviewCompanionHostId }),
]);
export type PreviewRenderHostSelection = typeof PreviewRenderHostSelection.Type;

const HostLabel = TrimmedNonEmptyString.check(Schema.isMaxLength(64));
const Platform = TrimmedNonEmptyString.check(Schema.isMaxLength(64));
const SafeNonNegativeInt = NonNegativeInt.check(
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
);
const TabKey = {
  threadId: ThreadId.check(Schema.isMaxLength(128)),
  tabId: PreviewTabId,
};
const Heartbeat = Schema.Struct({
  type: Schema.Literal("heartbeat"),
  // Unix epoch milliseconds, shared with host status lastSeenAt.
  sentAt: SafeNonNegativeInt,
});
const Chunk = Schema.Struct({
  type: Schema.Literal("chunk"),
  id: TrimmedNonEmptyString.check(Schema.isMaxLength(128)),
  index: SafeNonNegativeInt,
  final: Schema.Boolean,
  // UTF-16 length only: transport must also cap UTF-8 chunks and total reassembly bytes.
  data: Schema.String.check(Schema.isMaxLength(256 * 1024)),
});

export const CompanionUp = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("hello"),
    protocol: Schema.Literal(PREVIEW_COMPANION_PROTOCOL),
    hostId: PreviewCompanionHostId,
    label: HostLabel,
    platform: Platform,
    runtimeIdentity: PreviewAutomationRuntimeIdentity,
    capabilities: PreviewCompanionCapabilities,
  }),
  Heartbeat,
  Schema.Struct({ type: Schema.Literal("browser"), event: DesktopBrowserEvent }),
  Chunk,
  Schema.Struct({ type: Schema.Literal("mounted"), ...TabKey }),
  Schema.Struct({ type: Schema.Literal("unmountedAck"), ...TabKey }),
]);
export type CompanionUp = typeof CompanionUp.Type;

export const CompanionDown = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("welcome"),
    protocol: Schema.Literal(PREVIEW_COMPANION_PROTOCOL),
    environmentId: EnvironmentId.check(Schema.isMaxLength(128)),
    connectionGeneration: SafeNonNegativeInt,
    heartbeatMs: PositiveInt.check(Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)),
  }),
  Heartbeat,
  Schema.Struct({ type: Schema.Literal("browser"), command: DesktopBrowserCommand }),
  Chunk,
  Schema.Struct({ type: Schema.Literal("mount"), ...TabKey }),
  Schema.Struct({ type: Schema.Literal("unmount"), ...TabKey }),
  Schema.Struct({ type: Schema.Literal("assignments"), tabs: Schema.Array(Schema.Struct(TabKey)) }),
]);
export type CompanionDown = typeof CompanionDown.Type;

export const PreviewCompanionHostStatus = Schema.Struct({
  hostId: PreviewCompanionHostId,
  label: HostLabel,
  platform: Platform,
  online: Schema.Boolean,
  // Unix epoch milliseconds.
  lastSeenAt: SafeNonNegativeInt,
  runtimeIdentity: Schema.NullOr(PreviewAutomationRuntimeIdentity),
  capabilities: PreviewCompanionCapabilities,
  connectionGeneration: Schema.NullOr(SafeNonNegativeInt),
});
export type PreviewCompanionHostStatus = typeof PreviewCompanionHostStatus.Type;

export const PreviewCompanionHostsResponse = Schema.Struct({
  hosts: Schema.Array(PreviewCompanionHostStatus),
  environmentDefault: PreviewRenderHostSelection,
});
export type PreviewCompanionHostsResponse = typeof PreviewCompanionHostsResponse.Type;

export const PreviewCompanionDefaultSelectionInput = Schema.Struct({
  selection: PreviewRenderHostSelection,
});
export type PreviewCompanionDefaultSelectionInput =
  typeof PreviewCompanionDefaultSelectionInput.Type;

export const PreviewCompanionThreadSelectionResponse = Schema.Struct({
  selection: Schema.NullOr(PreviewRenderHostSelection),
  effective: PreviewRenderHostSelection,
  tabs: Schema.Array(
    Schema.Struct({
      tabId: PreviewTabId,
      hostId: Schema.NullOr(PreviewCompanionHostId),
    }),
  ),
});
export type PreviewCompanionThreadSelectionResponse =
  typeof PreviewCompanionThreadSelectionResponse.Type;

export const PreviewCompanionThreadSelectionInput = Schema.Struct({
  selection: Schema.NullOr(PreviewRenderHostSelection),
});
export type PreviewCompanionThreadSelectionInput = typeof PreviewCompanionThreadSelectionInput.Type;

export const DesktopCompanionConfig = Schema.Struct({
  enabled: Schema.Boolean,
  environmentId: Schema.NullOr(EnvironmentId),
  hostId: PreviewCompanionHostId,
  label: HostLabel,
  browserOnly: Schema.Boolean,
});
export type DesktopCompanionConfig = typeof DesktopCompanionConfig.Type;

export const DesktopCompanionConfigureInput = Schema.Struct({
  enabled: Schema.Boolean,
  environmentId: Schema.NullOr(EnvironmentId),
  label: HostLabel,
  browserOnly: Schema.Boolean,
});
export type DesktopCompanionConfigureInput = typeof DesktopCompanionConfigureInput.Type;

export const DesktopCompanionState = Schema.Struct({
  browserOnlyLocked: Schema.optional(Schema.Boolean),
  config: DesktopCompanionConfig,
  status: Schema.Literals([
    "disabled",
    "awaiting_ticket",
    "connecting",
    "online",
    "reconnecting",
    "auth_required",
    "unavailable",
    "unsupported",
    "superseded",
    "restart_required",
  ]),
  connectionGeneration: Schema.NullOr(SafeNonNegativeInt),
  assignments: Schema.Array(Schema.Struct(TabKey)),
});
export type DesktopCompanionState = typeof DesktopCompanionState.Type;

export const DesktopCompanionTicketRequest = Schema.Struct({
  requestId: TrimmedNonEmptyString.check(Schema.isMaxLength(128)),
  environmentId: EnvironmentId,
});
export type DesktopCompanionTicketRequest = typeof DesktopCompanionTicketRequest.Type;

export const DesktopCompanionTicketResponse = Schema.Struct({
  ...DesktopCompanionTicketRequest.fields,
  result: Schema.Union([
    Schema.Struct({
      _tag: Schema.Literal("ready"),
      url: TrimmedNonEmptyString.check(Schema.isMaxLength(16384)),
    }),
    Schema.Struct({ _tag: Schema.Literals(["auth_required", "unavailable", "unsupported"]) }),
  ]),
});
export type DesktopCompanionTicketResponse = typeof DesktopCompanionTicketResponse.Type;

export interface DesktopCompanionBridge {
  readonly getState: () => Promise<DesktopCompanionState>;
  readonly configure: (input: DesktopCompanionConfigureInput) => Promise<DesktopCompanionState>;
  readonly setTicketProviderReady: (ready: boolean) => Promise<void>;
  readonly completeTicket: (response: DesktopCompanionTicketResponse) => Promise<void>;
  readonly retry: () => Promise<void>;
  readonly onTicketRequest: (
    listener: (request: DesktopCompanionTicketRequest) => void,
  ) => () => void;
  readonly onState: (listener: (state: DesktopCompanionState) => void) => () => void;
  readonly onNotice: (listener: (notice: DesktopCompanionPopupNotice) => void) => () => void;
}

export const DesktopCompanionPopupNotice = Schema.Struct({
  kind: Schema.Literal("popup_blocked"),
  origin: Schema.NullOr(Schema.String.check(Schema.isMaxLength(2048))),
});
export type DesktopCompanionPopupNotice = typeof DesktopCompanionPopupNotice.Type;
