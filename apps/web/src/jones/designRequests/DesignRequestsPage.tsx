import {
  DESIGN_REQUEST_PROTOCOL,
  type DesignRequestAttachmentData,
  type DesignRequestGalleryMessage,
} from "@t3tools/contracts/jones/designRequests";
import {
  designRequestPacketIdentity,
  isPairableGalleryOrigin,
  PacketV1Error,
  parsePacketV1Text,
  type DesignRequestOutcome,
  type DesignRequestResolvedRoute,
  type PacketV1,
} from "@t3tools/client-runtime/jones/design-requests";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { WorkspacePageContainer } from "../../components/WorkspacePageContainer";
import { WorkspacePageHeader } from "../../components/WorkspacePageHeader";
import { Alert, AlertDescription, AlertTitle } from "../../components/ui/alert";
import { Badge } from "../../components/ui/badge";
import { Button } from "../../components/ui/button";
import { SidebarInset } from "../../components/ui/sidebar";
import { Textarea } from "../../components/ui/textarea";
import { isElectron } from "../../env";
import {
  findDesignRequestBinding,
  pairDesignRequestBinding,
  readDesignRequestBindings,
  removeDesignRequestBinding,
} from "./bindingStore";
import { designRequestHeldFix } from "./heldReasons";
import { useDesignRequestRouter } from "./useDesignRequestRouter";
import { useGalleryChannel } from "./useGalleryChannel";

interface LogEntry {
  readonly at: number;
  readonly source: "gallery" | "import";
  readonly outcome: DesignRequestOutcome;
}

interface ImportedPacket {
  readonly packet: PacketV1;
  readonly attachmentData: DesignRequestAttachmentData | undefined;
  readonly origin: string | null;
}

const browserStorage = () => window.localStorage;

const originOf = (value: string): string | null => {
  try {
    return new URL(value).origin;
  } catch {
    return null;
  }
};

const statusVariant = (status: DesignRequestOutcome["status"]) =>
  status === "accepted"
    ? "default"
    : status === "unknown"
      ? "info"
      : status === "held"
        ? "outline"
        : "error";

function RouteSummary({ route }: { route: DesignRequestResolvedRoute }) {
  if (route.state === "routable")
    return (
      <div className="space-y-1 text-sm">
        <p className="font-medium">
          Queue to {route.thread.title || route.thread.id} · {route.workstream.name}
        </p>
        <p className="text-muted-foreground">
          Queues after the active run; an idle thread starts a run. Modes stay{" "}
          {route.thread.runtimeMode} / {route.thread.interactionMode}. Route{" "}
          {route.routeToken.slice(0, 8)}
        </p>
      </div>
    );
  return (
    <Alert variant="warning">
      <AlertTitle>Held · {route.reason}</AlertTitle>
      <AlertDescription>
        {designRequestHeldFix(route.reason)} Nothing is sent while a request is held.
      </AlertDescription>
    </Alert>
  );
}

export function DesignRequestsPage() {
  const [bindings, setBindings] = useState(() => readDesignRequestBindings(browserStorage()));
  const [imported, setImported] = useState<ImportedPacket | null>(null);
  const [importError, setImportError] = useState<string | null>(null);
  const [pasted, setPasted] = useState("");
  const [selectedWorkstream, setSelectedWorkstream] = useState("");
  const [log, setLog] = useState<readonly LogEntry[]>([]);
  const [busy, setBusy] = useState(false);
  const handleMessage = useRef<(message: DesignRequestGalleryMessage) => void>(() => {});
  const channel = useGalleryChannel((message) => handleMessage.current(message));
  const pairing =
    channel.phase === "connected" && channel.galleryOrigin !== null && channel.projectKey !== null
      ? { origin: channel.galleryOrigin, projectKey: channel.projectKey }
      : imported !== null && imported.origin !== null
        ? { origin: imported.origin, projectKey: imported.packet.collection.key }
        : null;
  const pairable = pairing !== null && isPairableGalleryOrigin(pairing.origin);
  const binding =
    pairing !== null && pairable
      ? findDesignRequestBinding(bindings, pairing.origin, pairing.projectKey)
      : null;
  const router = useDesignRequestRouter(binding);
  const route: DesignRequestResolvedRoute =
    pairing !== null && !pairable ? { state: "held", reason: "origin-not-allowed" } : router.route;

  const record = useCallback((source: LogEntry["source"], outcome: DesignRequestOutcome) => {
    setLog((entries) => [{ at: Date.now(), source, outcome }, ...entries].slice(0, 50));
  }, []);

  // Tell the gallery the current destination whenever it changes, so its Send button names it.
  const routeMessage = useMemo(
    () =>
      route.state === "routable"
        ? {
            type: "jones.design-request.route" as const,
            protocol: DESIGN_REQUEST_PROTOCOL,
            state: "routable" as const,
            routeToken: route.routeToken,
            workstream: route.workstream,
            thread: { id: route.thread.id, title: route.thread.title },
          }
        : {
            type: "jones.design-request.route" as const,
            protocol: DESIGN_REQUEST_PROTOCOL,
            state: "held" as const,
            reason: route.reason,
            ...(route.workstream ? { workstream: route.workstream } : {}),
          },
    [route],
  );
  const routeKey = JSON.stringify(routeMessage);
  useEffect(() => {
    if (channel.phase === "connected") channel.send(routeMessage);
    // routeKey captures every field of routeMessage.
  }, [channel.phase, routeKey]);

  // Reassigned every render so a gallery message always sees the current binding and route.
  handleMessage.current = (message) => {
    if (message.type === "jones.design-request.hello") return;
    const reply = (outcome: DesignRequestOutcome) => {
      record("gallery", outcome);
      // A receipt must echo the bound packet ID and full digest; without one it is not sent and
      // the gallery's pending send times out as unknown.
      const { digest, ...rest } = outcome;
      if (digest === undefined) return;
      channel.send({
        type: "jones.design-request.receipt",
        protocol: DESIGN_REQUEST_PROTOCOL,
        requestNonce: message.requestNonce,
        ...rest,
        digest,
      });
    };
    if (message.type === "jones.design-request.reconcile") {
      void router.gateway
        .reconcile({
          threadId: message.threadId,
          commandId: message.commandId,
          messageId: message.messageId,
          packetId: message.packetId,
          digest: message.digest,
          workstreamId: binding?.workstreamId ?? null,
        })
        .then(reply);
      return;
    }
    if (binding === null || message.projectKey !== channel.projectKey) {
      reply({
        status: "held",
        ...designRequestPacketIdentity(message.packet),
        reason: pairable ? "no-binding" : "origin-not-allowed",
      });
      return;
    }
    void router.gateway
      .submit({
        binding,
        projectKey: message.projectKey,
        packet: message.packet,
        attachmentData: message.attachmentData,
        expectedRouteToken: message.expectedRouteToken,
        attempt: message.attempt,
      })
      .then(reply, () =>
        reply({
          status: "unknown",
          ...designRequestPacketIdentity(message.packet),
          reason: "gateway-error",
        }),
      );
  };

  const importText = useCallback((text: string) => {
    try {
      const parsed = parsePacketV1Text(text);
      setImported({ ...parsed, origin: originOf(parsed.packet.collection.key) });
      setImportError(null);
    } catch (cause) {
      setImported(null);
      setImportError(cause instanceof PacketV1Error ? cause.reason : "packet-invalid");
    }
  }, []);

  const queueImported = useCallback(async () => {
    if (imported === null || binding === null || route.state !== "routable" || busy) return;
    setBusy(true);
    try {
      record(
        "import",
        await router.gateway.submit({
          binding,
          projectKey: binding.projectKey,
          packet: imported.packet,
          attachmentData: imported.attachmentData,
          expectedRouteToken: route.routeToken,
          // A file import has no gallery outbox, so it always uses attempt 1: an accepted attempt
          // replays and a rejected one stays rejected rather than advancing silently.
          attempt: 1,
        }),
      );
    } finally {
      setBusy(false);
    }
  }, [binding, busy, imported, record, route, router.gateway]);

  const pair = () => {
    if (pairing === null || !pairable || selectedWorkstream === "") return;
    pairDesignRequestBinding(browserStorage(), {
      galleryOrigin: pairing.origin,
      projectKey: pairing.projectKey,
      workstreamId: selectedWorkstream,
    });
    setBindings(readDesignRequestBindings(browserStorage()));
    router.refresh();
  };

  const unpair = () => {
    if (binding === null) return;
    removeDesignRequestBinding(browserStorage(), binding.bindingId);
    setBindings(readDesignRequestBindings(browserStorage()));
  };

  const workstreams = router.registry.data?.items ?? [];

  return (
    <SidebarInset className="h-dvh overflow-hidden">
      <WorkspacePageHeader electron={isElectron}>
        <span className="text-sm font-medium">Design requests</span>
      </WorkspacePageHeader>
      <div className="min-h-0 flex-1 overflow-y-auto">
        <WorkspacePageContainer width="expanded">
          <section className="space-y-2">
            <h2 className="text-sm font-medium">Gallery connection</h2>
            {channel.phase === "no-opener" && (
              <p className="text-sm text-muted-foreground">
                No gallery opened this window. Import a downloaded packet below.
              </p>
            )}
            {channel.phase === "invalid-origin" && (
              <Alert variant="error">
                <AlertTitle>Gallery origin refused</AlertTitle>
                <AlertDescription>
                  The opener did not name an exact http(s) origin, so nothing was announced.
                </AlertDescription>
              </Alert>
            )}
            {channel.phase === "waiting" && (
              <p className="text-sm text-muted-foreground">
                Waiting for the gallery at {channel.galleryOrigin} to say hello.
              </p>
            )}
            {pairing !== null && (
              <p className="text-sm break-all">
                Origin {pairing.origin} · project {pairing.projectKey}
              </p>
            )}
          </section>

          {pairing !== null && pairable && binding === null && (
            <section className="mt-6 space-y-2">
              <h2 className="text-sm font-medium">Pair to an existing workstream</h2>
              <p className="text-sm text-muted-foreground">
                Requests from this origin and project will queue on that workstream's primary
                thread. Workstreams and threads are never created here.
              </p>
              <div className="flex items-center gap-2">
                <select
                  aria-label="Workstream"
                  className="rounded-md border bg-background px-2 py-1 text-sm"
                  value={selectedWorkstream}
                  onChange={(event) => setSelectedWorkstream(event.target.value)}
                >
                  <option value="">Choose a workstream</option>
                  {workstreams.map((item) => (
                    <option key={item.workstreamId} value={item.workstreamId}>
                      {item.name}
                    </option>
                  ))}
                </select>
                <Button size="sm" disabled={selectedWorkstream === ""} onClick={pair}>
                  Pair
                </Button>
              </div>
            </section>
          )}

          <section className="mt-6 space-y-2">
            <div className="flex items-center gap-2">
              <h2 className="text-sm font-medium">Destination</h2>
              <Button size="sm" variant="ghost" onClick={router.refresh}>
                Refresh
              </Button>
              {binding !== null && (
                <Button size="sm" variant="ghost" onClick={unpair}>
                  Unpair
                </Button>
              )}
            </div>
            {pairing === null ? (
              <p className="text-sm text-muted-foreground">No gallery or packet yet.</p>
            ) : (
              <RouteSummary route={route} />
            )}
          </section>

          <section className="mt-6 space-y-2">
            <h2 className="text-sm font-medium">Import a packet</h2>
            <p className="text-sm text-muted-foreground">
              Paste a copied packet, or choose or drop a downloaded packet file. Images are only in
              the downloaded file.
            </p>
            <div
              className="space-y-2"
              onDragOver={(event) => event.preventDefault()}
              onDrop={(event) => {
                event.preventDefault();
                const file = event.dataTransfer.files[0];
                if (file) void file.text().then(importText);
              }}
            >
              <Textarea
                aria-label="Packet text"
                value={pasted}
                placeholder="Paste a design request packet"
                onChange={(event) => setPasted(event.target.value)}
              />
              <div className="flex items-center gap-2">
                <Button
                  size="sm"
                  variant="outline"
                  disabled={!pasted.trim()}
                  onClick={() => importText(pasted)}
                >
                  Check pasted packet
                </Button>
                <input
                  aria-label="Packet file"
                  type="file"
                  accept="application/json,.json"
                  className="text-sm"
                  onChange={(event) => {
                    const file = event.target.files?.[0];
                    if (file) void file.text().then(importText);
                  }}
                />
              </div>
            </div>
            {importError !== null && (
              <Alert variant="warning">
                <AlertTitle>Held · {importError}</AlertTitle>
                <AlertDescription>
                  {designRequestHeldFix(importError)} Nothing was sent.
                </AlertDescription>
              </Alert>
            )}
            {imported !== null && (
              <div className="space-y-2 text-sm">
                <p>
                  {imported.packet.collection.title} · packet {imported.packet.packetId} ·{" "}
                  {imported.packet.request.attachments.length} image(s) · {imported.packet.digest}
                </p>
                <Button
                  size="sm"
                  disabled={busy || route.state !== "routable"}
                  onClick={() => void queueImported()}
                >
                  {busy
                    ? "Queuing…"
                    : route.state === "routable"
                      ? `Queue to ${route.thread.title || route.thread.id}`
                      : `Held · ${route.reason}`}
                </Button>
              </div>
            )}
          </section>

          <section className="mt-6 space-y-2">
            <h2 className="text-sm font-medium">This session</h2>
            {log.length === 0 ? (
              <p className="text-sm text-muted-foreground">No requests yet.</p>
            ) : (
              <ul className="space-y-1 text-sm">
                {log.map((entry) => (
                  <li
                    key={`${entry.at}-${entry.outcome.packetId}`}
                    className="flex flex-wrap items-center gap-2"
                  >
                    <Badge variant={statusVariant(entry.outcome.status)}>
                      {entry.outcome.status}
                    </Badge>
                    <span>{entry.outcome.packetId}</span>
                    {entry.outcome.reason && <span>· {entry.outcome.reason}</span>}
                    {entry.outcome.delivery && <span>· {entry.outcome.delivery}</span>}
                    {entry.outcome.destination && (
                      <span className="text-muted-foreground">
                        · {entry.outcome.destination.commandId}
                      </span>
                    )}
                    <span className="text-muted-foreground">
                      · {entry.source} · {new Date(entry.at).toLocaleTimeString()}
                    </span>
                  </li>
                ))}
              </ul>
            )}
          </section>
        </WorkspacePageContainer>
      </div>
    </SidebarInset>
  );
}
