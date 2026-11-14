import { useRef, useState, type ReactNode } from "react";
import { useAtomValue } from "@effect/atom-react";
import type {
  PersonalWorkstreamsController,
  PersonalWorkstreamView,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { Menu, MenuItem, MenuPopup, MenuTrigger } from "../ui/menu";
import { Dialog, DialogPopup, DialogHeader, DialogTitle } from "../ui/dialog";
import { showPersonalFailure } from "../../jones/workstreams/personalNotifications";
import * as Schema from "effect/Schema";
import { useLocalStorage } from "../../hooks/useLocalStorage";
import { PersonalSidebarGroupHeader } from "../../jones/workstreams/PersonalSidebarGroupHeader";
import type { PersonalThreadSummary } from "../../jones/workstreams/personalPresentation";

const collapsedSchema = Schema.Boolean;
const emptySummary: PersonalThreadSummary = { total: 0, running: 0, attention: 0, failed: 0 };
export function WorkstreamCreateForm({
  controller,
  open,
  onClose,
  onPendingChange,
}: {
  controller: PersonalWorkstreamsController;
  open: boolean;
  onClose: () => void;
  onPendingChange: (pending: boolean) => void;
}) {
  const [name, setName] = useState("");
  const [pending, setPending] = useState(false);
  const pendingRef = useRef(false);
  if (!open) return null;
  return (
    <form
      className="flex gap-1 p-2"
      onSubmit={async (event) => {
        event.preventDefault();
        if (pendingRef.current || !name.trim()) return;
        pendingRef.current = true;
        setPending(true);
        onPendingChange(true);
        try {
          const result = await controller.create({ name: name.trim() });
          if (result.ok) {
            setName("");
            onClose();
          } else showPersonalFailure(result.failure);
        } catch (error) {
          setPending(false);
          onPendingChange(false);
          pendingRef.current = false;
          throw error;
        }
        setPending(false);
        onPendingChange(false);
        pendingRef.current = false;
      }}
    >
      <Input
        autoFocus
        aria-label="Workstream name"
        maxLength={160}
        value={name}
        onChange={(event) => setName(event.target.value)}
      />
      <Button type="submit" disabled={pending || !name.trim()}>
        Create
      </Button>
      <Button type="button" variant="ghost" disabled={pending} onClick={onClose}>
        Cancel
      </Button>
    </form>
  );
}
function Group({
  item,
  controller,
  children,
  summary,
}: {
  item: PersonalWorkstreamView;
  controller: PersonalWorkstreamsController;
  children: ReactNode;
  summary: PersonalThreadSummary;
}) {
  const state = useAtomValue(controller.state);
  const [collapsed, setCollapsed] = useLocalStorage(
    `jones:workstreams:collapsed:${item.id}`,
    false,
    collapsedSchema,
  );
  const [editing, setEditing] = useState<"name" | "color" | null>(null);
  const [draft, setDraft] = useState("");
  const [pending, setPending] = useState(false);
  const run = async (operation: () => ReturnType<typeof controller.remove>) => {
    if (pending) return;
    setPending(true);
    try {
      const result = await operation();
      if (!result.ok) showPersonalFailure(result.failure);
    } catch (error) {
      setPending(false);
      throw error;
    }
    setPending(false);
  };
  const index = state.workstreams.findIndex((entry) => entry.id === item.id);
  return (
    <section
      data-native-workstream={JSON.stringify(item.id)}
      className="my-2 overflow-hidden rounded-lg border border-sidebar-border/70 border-l-4 bg-sidebar-control-surface/30"
      style={{ borderLeftColor: item.color ?? undefined }}
    >
      <div
        className="flex items-center gap-1 pr-2"
        draggable={state.canCreate && !pending}
        onDragStart={(event) => {
          event.dataTransfer.setData("application/x-jones-workstream", item.id);
          event.dataTransfer.effectAllowed = "move";
        }}
        onDragOver={(event) => {
          if (event.dataTransfer.types.includes("application/x-jones-workstream"))
            event.preventDefault();
        }}
        onDrop={(event) => {
          const moved = event.dataTransfer.getData("application/x-jones-workstream");
          if (!moved || moved === item.id || !state.canCreate) return;
          event.preventDefault();
          const others = state.workstreams.filter((entry) => entry.id !== moved);
          const target = others.findIndex((entry) => entry.id === item.id);
          void run(async () => {
            const result = await controller.reorder(moved, {
              beforeId: others[target - 1]?.id ?? null,
              afterId: item.id,
            });
            return result.ok ? { ok: true, value: undefined } : result;
          });
        }}
      >
        <PersonalSidebarGroupHeader
          name={item.name}
          collapsed={collapsed}
          onToggle={() => setCollapsed((value) => !value)}
          summary={summary}
          lifecycle={item.lifecycle}
        />
        <Menu>
          <MenuTrigger aria-label={`Edit ${item.name}`}>…</MenuTrigger>
          <MenuPopup>
            <MenuItem
              disabled={!state.canCreate || pending}
              onClick={() => {
                setDraft(item.name);
                setEditing("name");
              }}
            >
              Rename
            </MenuItem>
            <MenuItem
              disabled={!state.canCreate || pending}
              onClick={() => {
                setDraft(item.color ?? "");
                setEditing("color");
              }}
            >
              Change color
            </MenuItem>
            {(["planned", "active", "paused", "completed", "deferred", "abandoned"] as const).map(
              (lifecycle) => (
                <MenuItem
                  key={lifecycle}
                  disabled={!state.canCreate || pending || item.lifecycle === lifecycle}
                  onClick={() =>
                    void run(async () => {
                      const result = await controller.setLifecycle(item.id, lifecycle);
                      return result.ok ? { ok: true, value: undefined } : result;
                    })
                  }
                >
                  {lifecycle[0]!.toUpperCase() + lifecycle.slice(1)}
                </MenuItem>
              ),
            )}
            <MenuItem
              disabled={!state.canCreate || pending || index === 0}
              onClick={() =>
                void run(async () => {
                  const result = await controller.reorder(item.id, {
                    afterId: state.workstreams[index - 1]!.id,
                    beforeId: state.workstreams[index - 2]?.id ?? null,
                  });
                  return result.ok ? { ok: true, value: undefined } : result;
                })
              }
            >
              Move up
            </MenuItem>
            <MenuItem
              disabled={!state.canCreate || pending || index === state.workstreams.length - 1}
              onClick={() =>
                void run(async () => {
                  const result = await controller.reorder(item.id, {
                    beforeId: state.workstreams[index + 1]!.id,
                    afterId: state.workstreams[index + 2]?.id ?? null,
                  });
                  return result.ok ? { ok: true, value: undefined } : result;
                })
              }
            >
              Move down
            </MenuItem>
            <MenuItem
              disabled={!state.canCreate || pending}
              onClick={() => void run(() => controller.remove(item.id))}
            >
              Delete Workstream
            </MenuItem>
          </MenuPopup>
        </Menu>
      </div>
      {!collapsed ? children : null}
      {editing ? (
        <Dialog
          open
          onOpenChange={(open) => {
            if (!open && !pending) setEditing(null);
          }}
        >
          <DialogPopup>
            <DialogHeader>
              <DialogTitle>
                {editing === "name" ? "Rename Workstream" : "Workstream color"}
              </DialogTitle>
            </DialogHeader>
            <form
              onSubmit={async (event) => {
                event.preventDefault();
                if (pending) return;
                setPending(true);
                try {
                  const result =
                    editing === "name"
                      ? await controller.rename(item.id, draft.trim())
                      : await controller.setColor(item.id, draft ? draft.toUpperCase() : null);
                  if (result.ok) setEditing(null);
                  else showPersonalFailure(result.failure);
                } catch (error) {
                  setPending(false);
                  throw error;
                }
                setPending(false);
              }}
            >
              <Input
                aria-label={editing === "name" ? "Workstream name" : "Hex color"}
                maxLength={editing === "name" ? 160 : 7}
                value={draft}
                onChange={(event) => setDraft(event.target.value)}
              />
              {editing === "color" ? (
                <Button type="button" variant="ghost" onClick={() => setDraft("")}>
                  Automatic
                </Button>
              ) : null}
              <Button
                type="submit"
                disabled={
                  pending ||
                  (editing === "name"
                    ? !draft.trim()
                    : draft !== "" && !/^#[0-9a-fA-F]{6}$/.test(draft))
                }
              >
                Save
              </Button>
            </form>
          </DialogPopup>
        </Dialog>
      ) : null}
    </section>
  );
}
export function WorkstreamSidebarSection({
  controller,
  renderMembers,
  summaries,
  unassignedSummary = emptySummary,
}: {
  controller: PersonalWorkstreamsController;
  renderMembers: (id: string | null) => ReactNode;
  summaries?: ReadonlyMap<string, PersonalThreadSummary>;
  unassignedSummary?: PersonalThreadSummary;
}) {
  const state = useAtomValue(controller.state);
  const [unassignedCollapsed, setUnassignedCollapsed] = useLocalStorage(
    "jones:workstreams:unassigned-collapsed",
    false,
    collapsedSchema,
  );
  return (
    <div aria-label="Workstreams">
      {state.workstreams.map((item) => (
        <Group
          key={item.id}
          item={item}
          controller={controller}
          summary={summaries?.get(item.id) ?? emptySummary}
        >
          {renderMembers(item.id)}
        </Group>
      ))}
      <section
        data-native-workstream="null"
        className="my-2 overflow-hidden rounded-lg border border-sidebar-border/70"
      >
        <div className="flex">
          <PersonalSidebarGroupHeader
            name="Unassigned"
            collapsed={unassignedCollapsed}
            onToggle={() => setUnassignedCollapsed((value) => !value)}
            summary={unassignedSummary}
          />
        </div>
        {!unassignedCollapsed ? renderMembers(null) : null}
      </section>
    </div>
  );
}
