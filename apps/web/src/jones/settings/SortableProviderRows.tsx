import {
  DndContext,
  type DragEndEvent,
  KeyboardSensor,
  PointerSensor,
  closestCenter,
  useSensor,
  useSensors,
} from "@dnd-kit/core";
import { restrictToParentElement, restrictToVerticalAxis } from "@dnd-kit/modifiers";
import {
  SortableContext,
  sortableKeyboardCoordinates,
  useSortable,
  verticalListSortingStrategy,
} from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import {
  AuthProvidersManageScope,
  defaultInstanceIdForDriver,
  type EnvironmentId,
  type ProviderDriverKind,
  type ProviderInstanceConfig,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import { GripVerticalIcon } from "lucide-react";
import { useEffect, useRef, useState, type ReactNode } from "react";
import { toastManager } from "~/components/ui/toast";
import { useEnvironmentSettings } from "~/hooks/useSettings";
import { useEnvironmentScope } from "~/state/session";
import { serverEnvironment } from "~/state/server";
import { useAtomCommand } from "~/state/use-atom-command";
import { reorderProviderInstance } from "./providerOrder";

interface ProviderRow {
  readonly instanceId: ProviderInstanceId;
  readonly driver: ProviderDriverKind;
  readonly explicitInstance: ProviderInstanceConfig | undefined;
  readonly isDefault: boolean;
}

interface Props<Row extends ProviderRow> {
  readonly environmentId: EnvironmentId;
  readonly rows: readonly Row[];
  readonly readOnly: boolean;
  readonly renderRow: (row: Row) => ReactNode;
}

export function SortableProviderRows<Row extends ProviderRow>(props: Props<Row>) {
  return <EnvironmentProviderRows key={props.environmentId} {...props} />;
}

function EnvironmentProviderRows<Row extends ProviderRow>({
  environmentId,
  rows,
  readOnly,
  renderRow,
}: Props<Row>) {
  const { providerInstances } = useEnvironmentSettings(environmentId);
  const canManage = useEnvironmentScope(environmentId, AuthProvidersManageScope);
  const persist = useAtomCommand(serverEnvironment.updateSettings, { reportFailure: false });
  const pending = useRef(false);
  const mounted = useRef(true);
  const [saving, setSaving] = useState(false);
  const [optimistic, setOptimistic] = useState<{
    base: typeof providerInstances;
    next: typeof providerInstances;
  } | null>(null);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);
  const instances = optimistic?.base === providerInstances ? optimistic.next : providerInstances;
  const rank = new Map(Object.keys(instances).map((id, index) => [id, index]));
  const groups = new Map<ProviderDriverKind, Row[]>();
  for (const row of rows) {
    const group = groups.get(row.driver) ?? [];
    group.push(row);
    groups.set(row.driver, group);
  }
  const pinned = (row: Row) =>
    row.isDefault || row.instanceId === defaultInstanceIdForDriver(row.driver);
  const reorder = async ({ active, over }: DragEndEvent) => {
    if (readOnly || !canManage || pending.current || over === null) return;
    const next = reorderProviderInstance(instances, String(active.id), String(over.id));
    if (next === instances) return;
    pending.current = true;
    setSaving(true);
    setOptimistic({ base: providerInstances, next });
    try {
      const result = await persist({
        environmentId,
        input: { patch: { providerInstances: next } },
      });
      if (!mounted.current) return;
      if (result._tag === "Failure") {
        setOptimistic(null);
        if (!isAtomCommandInterrupted(result)) {
          const error = squashAtomCommandFailure(result);
          toastManager.add({
            type: "error",
            title: "Could not reorder providers",
            description: error instanceof Error ? error.message : "The settings update failed.",
          });
        }
      }
    } catch (error) {
      if (mounted.current) {
        setOptimistic(null);
        toastManager.add({
          type: "error",
          title: "Could not reorder providers",
          description: error instanceof Error ? error.message : "The settings update failed.",
        });
      }
    } finally {
      pending.current = false;
      if (mounted.current) setSaving(false);
    }
  };
  return (
    <div className="divide-y divide-border/50" aria-busy={saving}>
      {[...groups].map(([driver, group]) => {
        const defaults = group.filter(pinned);
        const customs = group
          .filter((row) => !pinned(row))
          .sort(
            (a, b) => (rank.get(a.instanceId) ?? Infinity) - (rank.get(b.instanceId) ?? Infinity),
          );
        return (
          <div key={driver} className="divide-y divide-border/50">
            {defaults.map((row) => (
              <div key={row.instanceId}>{renderRow(row)}</div>
            ))}
            <ProviderDragGroup
              rows={customs}
              disabled={readOnly || !canManage || saving || customs.length < 2}
              onDragEnd={(event) => void reorder(event)}
              renderRow={renderRow}
            />
          </div>
        );
      })}
    </div>
  );
}

function ProviderDragGroup<Row extends ProviderRow>({
  rows,
  disabled,
  onDragEnd,
  renderRow,
}: {
  readonly rows: readonly Row[];
  readonly disabled: boolean;
  readonly onDragEnd: (event: DragEndEvent) => void;
  readonly renderRow: (row: Row) => ReactNode;
}) {
  const sensors = useSensors(
    useSensor(PointerSensor, { activationConstraint: { distance: 4 } }),
    useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates }),
  );
  return (
    <DndContext
      sensors={sensors}
      collisionDetection={closestCenter}
      modifiers={[restrictToVerticalAxis, restrictToParentElement]}
      onDragEnd={onDragEnd}
    >
      <SortableContext
        items={rows.map((row) => row.instanceId)}
        strategy={verticalListSortingStrategy}
      >
        <div className="divide-y divide-border/50">
          {rows.map((row) => (
            <SortableProviderRow key={row.instanceId} row={row} disabled={disabled}>
              {renderRow(row)}
            </SortableProviderRow>
          ))}
        </div>
      </SortableContext>
    </DndContext>
  );
}

function SortableProviderRow({
  row,
  disabled,
  children,
}: {
  readonly row: ProviderRow;
  readonly disabled: boolean;
  readonly children: ReactNode;
}) {
  const {
    attributes,
    listeners,
    setNodeRef,
    setActivatorNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({ id: row.instanceId, disabled });
  const label = row.explicitInstance?.displayName?.trim() || String(row.driver);
  return (
    <div
      ref={setNodeRef}
      style={{ transform: CSS.Translate.toString(transform), transition }}
      className={isDragging ? "relative z-10 bg-background shadow-md" : "relative"}
    >
      {children}
      {!disabled && (
        <button
          type="button"
          ref={setActivatorNodeRef}
          {...attributes}
          {...listeners}
          aria-label={`Reorder ${label}`}
          className="absolute inset-y-0 left-0 z-10 flex w-3 cursor-grab touch-none items-center justify-center text-muted-foreground/50 outline-none hover:text-foreground focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring active:cursor-grabbing sm:w-4"
        >
          <GripVerticalIcon className="h-3.5 w-2.5" />
        </button>
      )}
    </div>
  );
}
