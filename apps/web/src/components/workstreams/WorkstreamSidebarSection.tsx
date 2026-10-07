import {
  canonicalGitHubPullRequestUrl,
  type T3WorkstreamMetadata,
  type WorkstreamCommand,
  type WorkstreamReceipt,
} from "@t3tools/contracts";
import {
  orderWorkstreamMetadata,
  planWorkstreamOwnerOrder,
  workstreamBindingKey,
  resolveWorkstreamCompletionAuthority,
} from "@t3tools/client-runtime/state/workstreams";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import { ChevronDownIcon, ChevronUpIcon, GripVerticalIcon, MoreHorizontalIcon } from "lucide-react";
import { useLayoutEffect, useMemo, useRef, useState, type ReactNode, type DragEvent } from "react";

import * as Schema from "effect/Schema";
import { useLocalStorage } from "../../hooks/useLocalStorage";
import { canEditWorkstreams, workstreamTint } from "./nativeWorkstreamActions";

import { runtime } from "../../lib/runtime";
import type { WorkstreamListView } from "../../state/workstreams";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { Menu, MenuItem, MenuPopup, MenuTrigger } from "../ui/menu";
import { Spinner } from "../ui/spinner";
import {
  EMPTY_WORKSTREAM_THREAD_STATUS,
  type WorkstreamThreadStatusSummary,
} from "./workstreamThreadStatus";

export const workstreamCommandId = () =>
  runtime.runPromise(
    Crypto.Crypto.pipe(
      Effect.flatMap((crypto) => crypto.randomUUIDv4),
      Effect.map((uuid) => `t3-workstream-${uuid}`),
    ),
  );

const bindingSuperseded = Symbol("binding superseded");

export function WorkstreamCreateForm({
  controller,
  open,
  onClose,
  onPendingChange,
}: {
  readonly controller: WorkstreamListView;
  readonly open: boolean;
  readonly onClose: () => void;
  readonly onPendingChange: (pending: boolean) => void;
}) {
  const [name, setName] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);
  const pendingRef = useRef(false);
  const formRef = useRef<HTMLFormElement | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const sessionRef = useRef(0);
  const binding = controller.data ? workstreamBindingKey(controller.data.binding) : null;
  const bindingRef = useRef(binding);
  const canWrite = canEditWorkstreams(controller.data);

  useLayoutEffect(() => {
    if (bindingRef.current !== binding) onClose();
    bindingRef.current = binding;
    sessionRef.current += 1;
    setName("");
    setError(null);
    return () => {
      sessionRef.current += 1;
    };
  }, [binding, open, onClose]);

  useLayoutEffect(() => {
    if (!open) return;
    if (!canWrite) {
      onClose();
      return;
    }
    inputRef.current?.focus();
    const outside = (event: PointerEvent) => {
      if (event.target instanceof Node && !formRef.current?.contains(event.target)) onClose();
    };
    const escape = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        event.preventDefault();
        onClose();
      }
    };
    document.addEventListener("pointerdown", outside, true);
    document.addEventListener("keydown", escape, true);
    return () => {
      document.removeEventListener("pointerdown", outside, true);
      document.removeEventListener("keydown", escape, true);
    };
  }, [open, canWrite, onClose]);

  if (!open || !canWrite) return null;
  return (
    <form
      ref={formRef}
      aria-label="Create Workstream"
      className="px-1 pt-1"
      onSubmit={(event) => {
        event.preventDefault();
        const data = controller.data;
        const trimmedName = name.trim();
        if (!data || !canWrite || controller.loading || pendingRef.current || !trimmedName) return;
        const startedSession = sessionRef.current;
        const startedBinding = binding;
        pendingRef.current = true;
        setPending(true);
        onPendingChange(true);
        setError(null);
        void (async () => {
          const id = await workstreamCommandId();
          if (sessionRef.current !== startedSession || bindingRef.current !== startedBinding)
            return;
          const receipt = await controller.submit({
            command_id: id,
            expected_server_generation: data.binding.serverGeneration,
            expected_registry_version: data.binding.registryVersion,
            action: {
              operation: "create_workstream",
              name: trimmedName,
              lifecycle: "planned",
              progress: { state: "unknown" },
              sort_order: Math.min(
                2_147_483_647,
                Math.max(-1, ...data.items.map((item) => item.sortOrder)) + 1,
              ),
            },
          });
          if (sessionRef.current !== startedSession || bindingRef.current !== startedBinding)
            return;
          if (receipt.state === "committed") onClose();
          else
            setError(
              receipt.state === "rejected"
                ? `Creation was rejected: ${receipt.error.code}.`
                : "Creation is still pending.",
            );
        })()
          .catch((cause: unknown) => {
            if (sessionRef.current === startedSession && bindingRef.current === startedBinding)
              setError(cause instanceof Error ? cause.message : "Workstream creation failed.");
          })
          .finally(() => {
            pendingRef.current = false;
            setPending(false);
            onPendingChange(false);
          });
      }}
    >
      <div className="flex gap-1">
        <Input
          ref={inputRef}
          aria-label="New Workstream name"
          placeholder="Workstream name"
          nativeInput
          size="compact"
          value={name}
          disabled={pending}
          onChange={(event) => setName(event.target.value)}
        />
        <Button size="xs" type="submit" disabled={pending || controller.loading || !name.trim()}>
          {pending ? "Creating…" : "Create"}
        </Button>
      </div>
      {error ? (
        <p role="alert" className="pt-1 text-xs text-destructive">
          {error}
        </p>
      ) : null}
    </form>
  );
}

export function WorkstreamSidebarSection(props: {
  readonly controller: WorkstreamListView;
  readonly threadStatusSummaries?: ReadonlyMap<string, WorkstreamThreadStatusSummary>;
  readonly renderMembers?: (workstreamId: string | null) => ReactNode;
  readonly onThreadDragOver?: (event: DragEvent, workstreamId: string | null) => boolean;
  readonly threadDropTarget?: string | null | undefined;
  readonly onThreadDragLeave?: () => void;
  readonly threadActionBusy?: boolean;
  readonly onThreadDrop?: (event: DragEvent, workstreamId: string | null) => boolean;
}) {
  const { data, placementInventory, submit, runBindingOperation, loadDetail, loadReference } =
    props.controller;
  const [collapsed, setCollapsed] = useLocalStorage<readonly string[], readonly string[]>(
    `t3:workstreams:collapsed:${data?.binding.registryId ?? "none"}:${data?.binding.ownerId ?? "none"}`,
    [],
    Schema.Array(Schema.String),
  );
  const [editing, setEditing] = useState<string | null>(null);
  const [name, setName] = useState("");
  const [dragging, setDragging] = useState<string | null>(null);
  const [groupDropTarget, setGroupDropTarget] = useState<{ id: string; after: boolean } | null>(
    null,
  );
  const [selected, setSelected] = useState<string | null>(null);
  const [targetId, setTargetId] = useState("");
  const [declarationText, setDeclarationText] = useState("");
  const [receipt, setReceipt] = useState<WorkstreamReceipt | null>(null);
  const [pullRequestStatus, setPullRequestStatus] = useState<Record<string, string>>({});
  const [commandError, setCommandError] = useState<string | null>(null);
  const [detail, setDetail] = useState<Awaited<ReturnType<typeof loadDetail>> | null>(null);
  const detailRequest = useRef<AbortController | null>(null);
  const manualRefreshRequest = useRef<AbortController | null>(null);
  const items = useMemo(() => orderWorkstreamMetadata(data?.items ?? []), [data]);
  const bindingKey = data ? workstreamBindingKey(data.binding) : null;
  const bindingKeyRef = useRef(bindingKey);

  useLayoutEffect(() => {
    bindingKeyRef.current = bindingKey;
    detailRequest.current?.abort();
    detailRequest.current = null;
    manualRefreshRequest.current?.abort();
    manualRefreshRequest.current = null;
    setDetail(null);
    setPullRequestStatus({});
    setReceipt(null);
    setEditing(null);
    setDragging(null);
    setGroupDropTarget(null);
    setCommandError(null);
    if (bindingKey === null) {
      setSelected(null);
    }
    return () => {
      detailRequest.current?.abort();
      detailRequest.current = null;
      manualRefreshRequest.current?.abort();
      manualRefreshRequest.current = null;
    };
  }, [bindingKey]);
  if (!data) return null;
  const canWrite = canEditWorkstreams(data) && !props.controller.loading && !props.threadActionBusy;

  const showDetail = (workstreamId: string) => {
    detailRequest.current?.abort();
    const controller = new AbortController();
    detailRequest.current = controller;
    const startedBindingKey = bindingKey;
    setSelected(workstreamId);
    setDetail(null);
    setPullRequestStatus({});
    setTargetId(items.find((item) => item.workstreamId !== workstreamId)?.workstreamId ?? "");
    void loadDetail(workstreamId, { signal: controller.signal }).then(
      (value) => {
        if (controller.signal.aborted || bindingKeyRef.current !== startedBindingKey) return;
        setDetail(value);
        for (const reference of value.references.items) {
          if (!reference.pr_locator) continue;
          void loadReference(reference.native_reference_id, { signal: controller.signal }).then(
            (result) => {
              if (controller.signal.aborted || bindingKeyRef.current !== startedBindingKey) return;
              setPullRequestStatus((current) => ({
                ...current,
                [reference.native_reference_id]:
                  result.latest_observation?.last_success?.state ??
                  result.latest_observation?.outcome ??
                  "not refreshed",
              }));
            },
            () => undefined,
          );
        }
      },
      () => {
        if (!controller.signal.aborted && bindingKeyRef.current === startedBindingKey)
          setDetail(null);
      },
    );
  };
  const run = async (
    action: WorkstreamCommand["action"],
    registryVersion?: number,
    startedBindingKey = bindingKey,
  ) => {
    const id = await workstreamCommandId();
    if (bindingKeyRef.current !== startedBindingKey) throw bindingSuperseded;
    const value = await submit({
      command_id: id,
      expected_server_generation: data.binding.serverGeneration,
      expected_registry_version: registryVersion ?? data.binding.registryVersion,
      action,
    });
    if (bindingKeyRef.current !== startedBindingKey) throw bindingSuperseded;
    setReceipt(value);
    setCommandError(null);
    if (selected) showDetail(selected);
    return value;
  };
  const invoke = (action: WorkstreamCommand["action"]) => {
    if (!canWrite) return;
    const startedBindingKey = bindingKey;
    void run(action, undefined, startedBindingKey).catch((cause: unknown) => {
      if (cause === bindingSuperseded || bindingKeyRef.current !== startedBindingKey) return;
      setCommandError(cause instanceof Error ? cause.message : "Workstream command failed.");
    });
  };
  const update = (
    item: T3WorkstreamMetadata,
    changes: { readonly name?: string; readonly lifecycle?: T3WorkstreamMetadata["lifecycle"] },
  ) =>
    invoke({
      operation: "update_workstream",
      workstream_id: item.workstreamId,
      expected_version: item.version,
      name: changes.name ?? item.name,
      lifecycle: changes.lifecycle ?? item.lifecycle,
      progress: item.progress,
      sort_order: item.sortOrder,
    });
  const reorder = (sourceId: string, targetIndex: number) => {
    if (!canWrite) return;
    const startedBindingKey = bindingKey;
    void runBindingOperation(async (submitStep) => {
      const plan = planWorkstreamOwnerOrder(items, sourceId, targetIndex);
      let registryVersion = data.binding.registryVersion;
      let latestReceipt: WorkstreamReceipt | null = null;
      const versions = new Map(items.map((item) => [item.workstreamId, item.version]));
      for (const step of plan) {
        if (step.item.sortOrder === step.sortOrder) continue;
        const id = await workstreamCommandId();
        const value = await submitStep({
          command_id: id,
          expected_server_generation: data.binding.serverGeneration,
          expected_registry_version: registryVersion,
          action: {
            operation: "update_workstream",
            workstream_id: step.item.workstreamId,
            expected_version: versions.get(step.item.workstreamId) ?? step.item.version,
            name: step.item.name,
            lifecycle: step.item.lifecycle,
            progress: step.item.progress,
            sort_order: step.sortOrder,
          },
        });
        latestReceipt = value;
        if (value.state !== "committed") return value;
        registryVersion = value.registry_version;
        for (const version of value.effects.workstream_versions)
          versions.set(version.workstream_id, version.version);
      }
      return latestReceipt;
    })
      .then((value) => {
        if (value === null || bindingKeyRef.current !== startedBindingKey) return;
        setReceipt(value);
        setCommandError(null);
        if (selected) showDetail(selected);
      })
      .catch((cause: unknown) => {
        if (cause === bindingSuperseded || bindingKeyRef.current !== startedBindingKey) return;
        setCommandError(cause instanceof Error ? cause.message : "Workstream reorder failed.");
      });
  };

  const workstream = detail?.detail.workstream;
  const selectedMetadata = items.find((item) => item.workstreamId === selected);
  const target = items.find((item) => item.workstreamId === targetId);
  const references = new Map(
    detail?.references.items.map((reference) => [reference.native_reference_id, reference]) ?? [],
  );

  const completionAuthority =
    workstream && detail ? resolveWorkstreamCompletionAuthority(workstream, detail.history) : null;

  return (
    <section aria-label="Owner Workstreams" className="border-b border-sidebar-border/60 px-2 pb-2">
      {!canWrite ? (
        <p className="px-1 pb-1 text-xs text-muted-foreground">Workstreams are read-only.</p>
      ) : null}
      {placementInventory.coverage === "partial" ? (
        <p className="px-1 pb-1 text-xs text-sidebar-muted-foreground">
          Thread placement lookup scope is partial (
          {placementInventory.identities.length.toLocaleString()} of{" "}
          {placementInventory.totalIdentities.toLocaleString()} identities selected).
        </p>
      ) : null}
      {commandError ? <p className="px-1 pb-1 text-xs text-destructive">{commandError}</p> : null}
      <ul className="space-y-0.5">
        {items.map((item, index) => (
          <li
            className={`relative rounded-md border-l-2 ${workstreamTint(item.workstreamId)} ${props.threadDropTarget === item.workstreamId ? "ring-2 ring-primary bg-primary/10" : ""}`}
            data-drop-target={props.threadDropTarget === item.workstreamId ? "thread" : undefined}
            key={item.workstreamId}
            onDragLeave={(event) => {
              if (!event.currentTarget.contains(event.relatedTarget as Node | null)) {
                props.onThreadDragLeave?.();
                setGroupDropTarget(null);
              }
            }}
            onDragOver={(event) => {
              if (props.onThreadDragOver?.(event, item.workstreamId)) return;
              if (!canWrite || !dragging || dragging === item.workstreamId) return;
              event.preventDefault();
              event.stopPropagation();
              event.dataTransfer.dropEffect = "move";
              const bounds = event.currentTarget.getBoundingClientRect();
              setGroupDropTarget({
                id: item.workstreamId,
                after: event.clientY > bounds.top + bounds.height / 2,
              });
            }}
            onDrop={(event) => {
              if (props.onThreadDrop?.(event, item.workstreamId)) return;
              if (!canWrite || !dragging || dragging === item.workstreamId) return;
              event.preventDefault();
              event.stopPropagation();
              const bounds = event.currentTarget.getBoundingClientRect();
              const after = event.clientY > bounds.top + bounds.height / 2;
              const sourceIndex = items.findIndex((entry) => entry.workstreamId === dragging);
              reorder(dragging, index + (after ? 1 : 0) - (sourceIndex < index ? 1 : 0));
              setDragging(null);
              setGroupDropTarget(null);
            }}
          >
            {groupDropTarget?.id === item.workstreamId ? (
              <div
                aria-hidden
                className={`pointer-events-none absolute inset-x-0 z-20 h-0.5 bg-primary ${groupDropTarget.after ? "bottom-0" : "top-0"}`}
              />
            ) : null}
            <div className="flex min-h-8 items-center gap-1 px-1">
              {props.renderMembers ? (
                <button
                  type="button"
                  aria-label={`${collapsed.includes(item.workstreamId) ? "Expand" : "Collapse"} ${item.name}`}
                  aria-expanded={!collapsed.includes(item.workstreamId)}
                  onClick={() =>
                    setCollapsed((values) =>
                      values.includes(item.workstreamId)
                        ? values.filter((id) => id !== item.workstreamId)
                        : [...values, item.workstreamId],
                    )
                  }
                  className="rounded p-1 focus-visible:ring-2 focus-visible:ring-ring"
                >
                  <ChevronDownIcon
                    aria-hidden
                    className={`size-3.5 ${collapsed.includes(item.workstreamId) ? "-rotate-90" : ""}`}
                  />
                </button>
              ) : null}
              {canWrite ? (
                <button
                  type="button"
                  draggable
                  aria-label={`Drag Workstream ${item.name} to reorder`}
                  className="cursor-grab rounded p-1 text-muted-foreground focus-visible:ring-2 focus-visible:ring-ring active:cursor-grabbing"
                  onDragStart={(event) => {
                    event.stopPropagation();
                    if (!canWrite) {
                      event.preventDefault();
                      return;
                    }
                    event.dataTransfer.effectAllowed = "move";
                    event.dataTransfer.setData(
                      "application/x-t3-workstream-group",
                      item.workstreamId,
                    );
                    setDragging(item.workstreamId);
                  }}
                  onDragEnd={(event) => {
                    event.stopPropagation();
                    setDragging(null);
                    setGroupDropTarget(null);
                  }}
                >
                  <GripVerticalIcon aria-hidden className="size-3.5 shrink-0" />
                </button>
              ) : null}
              {canWrite && editing === item.workstreamId ? (
                <Input
                  aria-label="Workstream name"
                  autoFocus
                  nativeInput
                  onBlur={() => {
                    const next = name.trim();
                    if (next && next !== item.name) update(item, { name: next });
                    setEditing(null);
                  }}
                  onChange={(event) => setName(event.target.value)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter") event.currentTarget.blur();
                    if (event.key === "Escape") setEditing(null);
                  }}
                  size="compact"
                  value={name}
                />
              ) : (
                <button
                  className="min-w-0 flex-1 truncate px-1 text-left text-sm"
                  onClick={() => showDetail(item.workstreamId)}
                  type="button"
                >
                  {item.name}
                </button>
              )}
              <WorkstreamHeaderStatus
                name={item.name}
                summary={
                  props.threadStatusSummaries?.get(item.workstreamId) ??
                  EMPTY_WORKSTREAM_THREAD_STATUS
                }
              />
              {canWrite ? (
                <Menu>
                  <MenuTrigger
                    aria-label={`Actions for ${item.name}`}
                    render={<Button size="icon-micro" variant="ghost-muted" />}
                  >
                    <MoreHorizontalIcon />
                  </MenuTrigger>
                  <MenuPopup align="end">
                    <MenuItem
                      onClick={() => {
                        setName(item.name);
                        setEditing(item.workstreamId);
                      }}
                    >
                      Rename
                    </MenuItem>
                    <MenuItem
                      disabled={index === 0}
                      onClick={() => reorder(item.workstreamId, index - 1)}
                    >
                      <ChevronUpIcon /> Move up
                    </MenuItem>
                    <MenuItem
                      disabled={index === items.length - 1}
                      onClick={() => reorder(item.workstreamId, index + 1)}
                    >
                      <ChevronDownIcon /> Move down
                    </MenuItem>
                  </MenuPopup>
                </Menu>
              ) : null}
            </div>
            {props.renderMembers && !collapsed.includes(item.workstreamId)
              ? props.renderMembers(item.workstreamId)
              : null}
          </li>
        ))}
      </ul>
      {props.renderMembers ? (
        <div
          className={`mt-2 rounded-md border-l-2 border-sidebar-border ${props.threadDropTarget === null ? "ring-2 ring-primary bg-primary/10" : ""}`}
          data-drop-target={props.threadDropTarget === null ? "thread" : undefined}
          onDragOver={(event) => props.onThreadDragOver?.(event, null)}
          onDragLeave={(event) => {
            if (!event.currentTarget.contains(event.relatedTarget as Node | null))
              props.onThreadDragLeave?.();
          }}
          onDrop={(event) => {
            props.onThreadDrop?.(event, null);
          }}
        >
          <button
            type="button"
            className="flex items-center gap-1 px-2 py-1 text-xs font-medium focus-visible:ring-2 focus-visible:ring-ring"
            aria-label={`${collapsed.includes("__unassigned__") ? "Expand" : "Collapse"} Unassigned`}
            aria-expanded={!collapsed.includes("__unassigned__")}
            onClick={() =>
              setCollapsed((values) =>
                values.includes("__unassigned__")
                  ? values.filter((id) => id !== "__unassigned__")
                  : [...values, "__unassigned__"],
              )
            }
          >
            <ChevronDownIcon
              aria-hidden
              className={`size-3.5 ${collapsed.includes("__unassigned__") ? "-rotate-90" : ""}`}
            />{" "}
            Unassigned
          </button>
          {!collapsed.includes("__unassigned__") ? props.renderMembers(null) : null}
        </div>
      ) : null}
      {workstream && selectedMetadata && detail ? (
        <div className="mx-1 mt-2 space-y-2 rounded-md border border-sidebar-border/70 p-2 text-xs">
          <div className="flex items-center gap-2">
            <strong className="min-w-0 flex-1 truncate">{workstream.name}</strong>
            <select
              aria-label="Owner-declared workstream lifecycle"
              disabled={!canWrite}
              value={workstream.lifecycle}
              onChange={(event) =>
                update(selectedMetadata, {
                  lifecycle: event.target.value as T3WorkstreamMetadata["lifecycle"],
                })
              }
            >
              {(["planned", "active", "paused", "completed", "deferred", "abandoned"] as const).map(
                (value) => (
                  <option key={value}>{value}</option>
                ),
              )}
            </select>
          </div>
          {completionAuthority ? (
            <div
              aria-label="Workstream completion authority"
              className="space-y-1 rounded border p-2"
            >
              <div>
                Completion:{" "}
                {completionAuthority.state === "owner-declared"
                  ? `owner-declared by ${completionAuthority.declaration.actor.principal_id}`
                  : completionAuthority.state === "unverified"
                    ? `unverified owner declaration (${completionAuthority.reason.replaceAll("-", " ")})`
                    : "not declared"}
              </div>
              {completionAuthority.state === "owner-declared" ? (
                <div className="text-muted-foreground">
                  Revision {completionAuthority.declaration.revision} · registry version{" "}
                  {completionAuthority.declaration.registry_version} · recorded{" "}
                  {completionAuthority.declaration.recorded_at} · command{" "}
                  {completionAuthority.declaration.command_id}
                </div>
              ) : null}
              <p className="text-muted-foreground">
                Terminal turns, member disposition, pull request status, and T3 thread settlement
                are separate evidence. None completes this workstream.
              </p>
            </div>
          ) : null}
          <label className="block">
            Target Workstream{" "}
            <select value={targetId} onChange={(event) => setTargetId(event.target.value)}>
              {items
                .filter((item) => item.workstreamId !== workstream.workstream_id)
                .map((item) => (
                  <option key={item.workstreamId} value={item.workstreamId}>
                    {item.name}
                  </option>
                ))}
            </select>
          </label>
          <ul aria-label="Membership history" className="space-y-1">
            {detail.memberships.items.map((membership) => {
              const reference = references.get(membership.native_reference_id);
              const label = reference
                ? `${reference.identity.provider}: ${reference.identity.native_id}`
                : membership.native_reference_id;
              return (
                <li className="rounded border p-1" key={membership.membership_id}>
                  <span>
                    {label} · {membership.kind} · {membership.closed ? "removed" : "current"}
                  </span>
                  <div className="flex flex-wrap gap-1">
                    {canWrite && !membership.closed && membership.kind === "primary" && target ? (
                      <Button
                        size="xs"
                        variant="ghost"
                        onClick={() =>
                          invoke({
                            operation: "move_primary",
                            source_workstream_id: workstream.workstream_id,
                            expected_source_version: workstream.version,
                            source_membership_id: membership.membership_id,
                            destination_workstream_id: target.workstreamId,
                            expected_destination_version: target.version,
                          })
                        }
                      >
                        Move
                      </Button>
                    ) : null}
                    {canWrite && !membership.closed && target ? (
                      <Button
                        size="xs"
                        variant="ghost"
                        onClick={() =>
                          invoke({
                            operation: "link_secondary",
                            workstream_id: target.workstreamId,
                            expected_version: target.version,
                            native_reference_id: membership.native_reference_id,
                          })
                        }
                      >
                        Link
                      </Button>
                    ) : null}
                    {canWrite && !membership.closed ? (
                      <Button
                        size="xs"
                        variant="ghost"
                        onClick={() =>
                          invoke({
                            operation: "remove_membership",
                            workstream_id: workstream.workstream_id,
                            expected_version: workstream.version,
                            membership_id: membership.membership_id,
                          })
                        }
                      >
                        {membership.kind === "secondary" ? "Unlink" : "Remove"}
                      </Button>
                    ) : null}
                    {canWrite && !membership.closed ? (
                      <Button
                        size="xs"
                        variant="ghost"
                        onClick={() =>
                          invoke({
                            operation: "set_coordination_disposition",
                            workstream_id: workstream.workstream_id,
                            expected_version: workstream.version,
                            membership_id: membership.membership_id,
                            disposition: "completed",
                            other_disposition: null,
                          })
                        }
                      >
                        Record member completed
                      </Button>
                    ) : null}
                    {reference?.identity.provider === "t3" ? (
                      !canWrite ? (
                        <span className="text-muted-foreground">
                          T3 settlement unavailable: write authority is required.
                        </span>
                      ) : reference.registration.state === "attested" ? (
                        <>
                          <Button
                            size="xs"
                            variant="ghost"
                            onClick={() =>
                              invoke({
                                operation: "request_native_t3_settlement",
                                native_reference_id: reference.native_reference_id,
                                expected_attestation_version:
                                  reference.registration.attestation_version,
                                native_action: "settle",
                              })
                            }
                          >
                            Request T3 thread settlement
                          </Button>
                          <Button
                            size="xs"
                            variant="ghost"
                            onClick={() =>
                              invoke({
                                operation: "request_native_t3_settlement",
                                native_reference_id: reference.native_reference_id,
                                expected_attestation_version:
                                  reference.registration.attestation_version,
                                native_action: "unsettle",
                              })
                            }
                          >
                            Request T3 thread restore
                          </Button>
                        </>
                      ) : (
                        <span className="text-muted-foreground">
                          T3 settlement unavailable: reference is {reference.registration.state}.
                        </span>
                      )
                    ) : null}
                    {canWrite && membership.closed ? (
                      <Button
                        size="xs"
                        variant="ghost"
                        onClick={() =>
                          invoke(
                            membership.kind === "primary"
                              ? {
                                  operation: "reattach_primary",
                                  workstream_id: workstream.workstream_id,
                                  expected_version: workstream.version,
                                  native_reference_id: membership.native_reference_id,
                                }
                              : {
                                  operation: "link_secondary",
                                  workstream_id: workstream.workstream_id,
                                  expected_version: workstream.version,
                                  native_reference_id: membership.native_reference_id,
                                },
                          )
                        }
                      >
                        Reattach
                      </Button>
                    ) : null}
                  </div>
                  {reference?.pr_locator ? (
                    <div className="flex items-center gap-1">
                      <a href={canonicalGitHubPullRequestUrl(reference.pr_locator)}>
                        PR #{reference.pr_locator.number} ·{" "}
                        {pullRequestStatus[reference.native_reference_id] ?? "loading"}
                      </a>
                      {canWrite ? (
                        <Button
                          size="xs"
                          variant="ghost"
                          onClick={() => {
                            manualRefreshRequest.current?.abort();
                            const controller = new AbortController();
                            manualRefreshRequest.current = controller;
                            const startedBindingKey = bindingKey;
                            void (async () => {
                              const value = await loadReference(reference.native_reference_id, {
                                signal: controller.signal,
                              });
                              if (
                                controller.signal.aborted ||
                                bindingKeyRef.current !== startedBindingKey
                              )
                                return;
                              if (!value.latest_observation) return;
                              await run(
                                {
                                  operation: "refresh_linked_pr",
                                  workstream_id: workstream.workstream_id,
                                  expected_version: workstream.version,
                                  membership_id: membership.membership_id,
                                  expected_observation_version:
                                    value.latest_observation.observation_version,
                                },
                                undefined,
                                startedBindingKey,
                              );
                              const refreshed = await loadReference(reference.native_reference_id, {
                                signal: controller.signal,
                              });
                              if (
                                controller.signal.aborted ||
                                bindingKeyRef.current !== startedBindingKey
                              )
                                return;
                              setPullRequestStatus((current) => ({
                                ...current,
                                [reference.native_reference_id]:
                                  refreshed.latest_observation?.last_success?.state ??
                                  refreshed.latest_observation?.outcome ??
                                  "unknown",
                              }));
                            })()
                              .catch((cause: unknown) => {
                                if (
                                  cause === bindingSuperseded ||
                                  controller.signal.aborted ||
                                  bindingKeyRef.current !== startedBindingKey
                                )
                                  return;
                                setCommandError(
                                  cause instanceof Error ? cause.message : "PR refresh failed.",
                                );
                              })
                              .finally(() => {
                                if (manualRefreshRequest.current === controller)
                                  manualRefreshRequest.current = null;
                              });
                          }}
                        >
                          Refresh status
                        </Button>
                      ) : null}
                    </div>
                  ) : null}
                </li>
              );
            })}
          </ul>
          {canWrite ? (
            <form
              onSubmit={(event) => {
                event.preventDefault();
                if (!declarationText.trim()) return;
                const latest = detail.declarations.items.toSorted(
                  (a, b) => b.revision - a.revision,
                )[0];
                invoke({
                  operation: "set_declaration",
                  workstream_id: workstream.workstream_id,
                  expected_version: workstream.version,
                  declaration_id: latest?.declaration_id ?? null,
                  expected_revision: latest?.revision ?? 0,
                  text: declarationText.trim(),
                });
                setDeclarationText("");
              }}
            >
              <Input
                aria-label="Owner statement"
                nativeInput
                value={declarationText}
                onChange={(event) => setDeclarationText(event.target.value)}
              />
              <Button size="xs" type="submit">
                Record statement
              </Button>
            </form>
          ) : null}
          <p className="text-muted-foreground">
            Owner statements add context only; they do not change lifecycle or grant execution
            authority.
          </p>
          <ul aria-label="Owner statements">
            {detail.declarations.items.map((entry) => (
              <li key={`${entry.declaration_id}:${entry.revision}`}>
                {entry.state}: {entry.text}
              </li>
            ))}
          </ul>
          <div>Continuation and supersession</div>
          <ul>
            {detail.edges.items.map((edge) => (
              <li key={edge.edge_id}>
                {edge.relation}: {edge.from_workstream_id} → {edge.to_workstream_id}
              </li>
            ))}
          </ul>
          {canWrite && target ? (
            <div className="flex gap-1">
              <Button
                size="xs"
                variant="ghost"
                onClick={() =>
                  invoke({
                    operation: "add_edge",
                    from_workstream_id: workstream.workstream_id,
                    expected_from_version: workstream.version,
                    to_workstream_id: target.workstreamId,
                    expected_to_version: target.version,
                    relation: "continues_as",
                  })
                }
              >
                Continues as
              </Button>
              <Button
                size="xs"
                variant="ghost"
                onClick={() =>
                  invoke({
                    operation: "add_edge",
                    from_workstream_id: workstream.workstream_id,
                    expected_from_version: workstream.version,
                    to_workstream_id: target.workstreamId,
                    expected_to_version: target.version,
                    relation: "superseded_by",
                  })
                }
              >
                Superseded by
              </Button>
            </div>
          ) : null}
          <ol aria-label="Permanent history">
            {detail.history.items.map((event) => (
              <li key={event.event_id}>
                {event.operation} · {event.occurred_at}
              </li>
            ))}
          </ol>
          <p>History complete: {detail.history.next_cursor === null ? "yes" : "no"}</p>
          {receipt ? (
            <div role="status">
              <p>
                Coordination:{" "}
                {receipt.state === "committed" && receipt.effects.coordination_disposition
                  ? receipt.effects.coordination_disposition.disposition
                  : "unchanged"}
              </p>
              <p>
                Native T3 settlement:{" "}
                {receipt.state === "committed" && receipt.effects.native_settlement
                  ? receipt.effects.native_settlement.outcome
                  : "unchanged"}
              </p>
            </div>
          ) : null}
        </div>
      ) : null}
    </section>
  );
}

function WorkstreamHeaderStatus({
  name,
  summary,
}: {
  readonly name: string;
  readonly summary: WorkstreamThreadStatusSummary;
}) {
  return (
    <span className="flex shrink-0 items-center gap-1.5 text-3xs">
      {summary.failed > 0 ? (
        <span
          className="font-medium text-thread-failed"
          role="img"
          aria-label={`${summary.failed} failed ${summary.failed === 1 ? "thread" : "threads"}`}
        >
          Failed
        </span>
      ) : (
        <>
          {summary.waiting > 0 ? (
            <span
              role="img"
              aria-label={`${summary.waiting} ${summary.waiting === 1 ? "thread" : "threads"} waiting for input or approval`}
              className="size-1.5 rounded-full bg-waiting"
            />
          ) : null}
          {summary.running > 0 ? (
            <span className="text-info-foreground">
              <Spinner
                size="xs"
                aria-label={`${summary.running} running ${summary.running === 1 ? "thread" : "threads"}`}
              />
            </span>
          ) : null}
        </>
      )}
      <span
        role="img"
        className="tabular-nums text-muted-foreground"
        aria-label={`${name}: ${summary.running} of ${summary.total} known threads running`}
      >
        {summary.running}/{summary.total}
      </span>
    </span>
  );
}
