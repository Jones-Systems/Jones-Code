import {
  DndContext,
  KeyboardSensor,
  PointerSensor,
  closestCenter,
  useSensor,
  useSensors,
} from "@dnd-kit/core";
import { restrictToParentElement, restrictToVerticalAxis } from "@dnd-kit/modifiers";
import {
  arrayMove,
  SortableContext,
  sortableKeyboardCoordinates,
  useSortable,
  verticalListSortingStrategy,
} from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import { ArrowDownIcon, ArrowUpIcon, GripVerticalIcon } from "lucide-react";
import type { ReactNode } from "react";
import { Button } from "../../components/ui/button";

export interface OrderRow {
  readonly key: string;
  readonly label: string;
  readonly detail?: string;
  readonly icon?: ReactNode;
}

export function SortableOrderList({
  label,
  rows,
  onReorder,
}: {
  readonly label: string;
  readonly rows: readonly OrderRow[];
  readonly onReorder: (keys: string[]) => void;
}) {
  const sensors = useSensors(
    useSensor(PointerSensor, { activationConstraint: { distance: 4 } }),
    useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates }),
  );
  const move = (from: number, to: number) => {
    if (from < 0 || to < 0 || to >= rows.length || from === to) return;
    onReorder(
      arrayMove(
        rows.map((row) => row.key),
        from,
        to,
      ),
    );
  };
  return (
    <DndContext
      sensors={sensors}
      collisionDetection={closestCenter}
      modifiers={[restrictToVerticalAxis, restrictToParentElement]}
      onDragEnd={({ active, over }) => {
        if (over)
          move(
            rows.findIndex((row) => row.key === active.id),
            rows.findIndex((row) => row.key === over.id),
          );
      }}
    >
      <SortableContext items={rows.map((row) => row.key)} strategy={verticalListSortingStrategy}>
        <ol aria-label={label} className="my-2 divide-y divide-border/50">
          {rows.map((row, index) => (
            <SortableOrderRow
              key={row.key}
              row={row}
              disabled={rows.length < 2}
              first={index === 0}
              last={index === rows.length - 1}
              onUp={() => move(index, index - 1)}
              onDown={() => move(index, index + 1)}
            />
          ))}
        </ol>
      </SortableContext>
    </DndContext>
  );
}

function SortableOrderRow({
  row,
  disabled,
  first,
  last,
  onUp,
  onDown,
}: {
  readonly row: OrderRow;
  readonly disabled: boolean;
  readonly first: boolean;
  readonly last: boolean;
  readonly onUp: () => void;
  readonly onDown: () => void;
}) {
  const {
    attributes,
    listeners,
    setNodeRef,
    setActivatorNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({ id: row.key, disabled });
  return (
    <li
      ref={setNodeRef}
      style={{ transform: CSS.Translate.toString(transform), transition }}
      className={`relative flex items-center gap-2 py-2 ${isDragging ? "z-10 bg-background shadow-md" : ""}`}
    >
      <button
        ref={setActivatorNodeRef}
        type="button"
        {...attributes}
        {...listeners}
        disabled={disabled}
        aria-label={`Reorder ${row.label}`}
        className="flex size-7 shrink-0 cursor-grab touch-none items-center justify-center rounded text-muted-foreground hover:text-foreground focus-visible:outline-2 focus-visible:outline-ring disabled:opacity-40 active:cursor-grabbing"
      >
        <GripVerticalIcon className="size-4" />
      </button>
      {row.icon}
      <div className="min-w-0 flex-1">
        <div className="break-words text-sm">{row.label}</div>
        {row.detail && (
          <div className="break-words text-xs text-muted-foreground">{row.detail}</div>
        )}
      </div>
      <div className="flex shrink-0 gap-1">
        <Button
          variant="ghost"
          size="icon-sm"
          disabled={first}
          onClick={onUp}
          aria-label={`Move ${row.label} up`}
        >
          <ArrowUpIcon className="size-3.5" />
        </Button>
        <Button
          variant="ghost"
          size="icon-sm"
          disabled={last}
          onClick={onDown}
          aria-label={`Move ${row.label} down`}
        >
          <ArrowDownIcon className="size-3.5" />
        </Button>
      </div>
    </li>
  );
}
