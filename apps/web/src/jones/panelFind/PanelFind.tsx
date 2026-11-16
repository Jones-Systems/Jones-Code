import "./panelFind.css";
import { useStableCallback } from "../../hooks/useStableCallback";
import {
  createContext,
  use,
  useEffect,
  useEffectEvent,
  useRef,
  useState,
  type ComponentProps,
  type ReactNode,
} from "react";
import { ChevronDownIcon, ChevronUpIcon, SearchIcon, XIcon } from "lucide-react";
import { Button } from "../../components/ui/button";
import { Input } from "../../components/ui/input";
import { collectFindRanges, scrollFindRangeIntoView } from "./panelFind";
import { isMacPlatform } from "../../lib/utils";

let openFocused: (() => boolean) | undefined;

/** Routes keyboard and native-menu find to the last active visible panel. */
export function openFocusedPanelFind() {
  return openFocused?.() ?? false;
}

const FindCoordinator = createContext<{
  register: (root: HTMLElement, toggle: (open: boolean) => void) => () => void;
} | null>(null);

export function PanelFindProvider({ children }: { children: ReactNode }) {
  const scopes = useRef(new Map<HTMLElement, (open: boolean) => void>());
  const lastScope = useRef<HTMLElement | null>(null);
  const register = useStableCallback((root: HTMLElement, toggle: (open: boolean) => void) => {
    scopes.current.set(root, toggle);
    return () => {
      scopes.current.delete(root);
    };
  });
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- owns DOM focus/key listeners and native menu subscription for the scope registry
  useEffect(() => {
    const remember = (event: Event) => {
      if (!(event.target instanceof Element)) return;
      const root = event.target.closest<HTMLElement>("[data-find-scope]");
      if (root && scopes.current.has(root)) lastScope.current = root;
    };
    const open = () => {
      const dialog = [
        ...document.querySelectorAll<HTMLElement>('[role="dialog"], [role="alertdialog"]'),
      ]
        .filter((element) => element.getClientRects().length > 0)
        .at(-1);
      const available = (scope: HTMLElement) =>
        scope.isConnected &&
        scope.getClientRects().length > 0 &&
        !scope.closest('[inert], [aria-hidden="true"]') &&
        (!dialog || dialog.contains(scope));
      const focused = document.activeElement?.closest<HTMLElement>("[data-find-scope]");
      let root =
        lastScope.current && scopes.current.has(lastScope.current) && available(lastScope.current)
          ? lastScope.current
          : focused && scopes.current.has(focused) && available(focused)
            ? focused
            : [...scopes.current.keys()].find(
                (scope) => scope.hasAttribute("data-find-default") && available(scope),
              );
      if (!root) return false;
      const parent = root;
      root =
        [...scopes.current.keys()].find(
          (scope) =>
            scope !== parent &&
            parent.contains(scope) &&
            scope.hasAttribute("data-find-default") &&
            available(scope),
        ) ?? root;
      for (const [scope, toggle] of scopes.current) if (scope !== root) toggle(false);
      scopes.current.get(root)?.(true);
      return true;
    };
    openFocused = open;
    const keydown = (event: KeyboardEvent) => {
      if (event.isComposing || event.altKey || event.shiftKey || event.defaultPrevented) return;
      if (event.target instanceof Element && event.target.closest("[data-keybinding-capture]"))
        return;
      const modifier = isMacPlatform(navigator.platform)
        ? event.metaKey && !event.ctrlKey
        : event.ctrlKey && !event.metaKey;
      if (event.key.toLowerCase() === "f" && modifier && open()) {
        event.preventDefault();
        event.stopImmediatePropagation();
      }
    };
    window.addEventListener("pointerdown", remember, true);
    window.addEventListener("focusin", remember, true);
    window.addEventListener("keydown", keydown, true);
    const unsubscribe = window.desktopBridge?.onMenuAction((action) => {
      if (action === "find-in-panel") open();
    });
    return () => {
      if (openFocused === open) openFocused = undefined;
      window.removeEventListener("pointerdown", remember, true);
      window.removeEventListener("focusin", remember, true);
      window.removeEventListener("keydown", keydown, true);
      unsubscribe?.();
    };
  }, []);
  const value = { register };
  return <FindCoordinator value={value}>{children}</FindCoordinator>;
}

export function PanelFindScope({
  label,
  resetKey,
  enabled = true,
  nativeFind,
  children,
  ...props
}: ComponentProps<"div"> & {
  label: string;
  resetKey?: string | undefined;
  enabled?: boolean;
  nativeFind?: { open: () => void; close: () => void } | undefined;
}) {
  const coordinator = use(FindCoordinator);
  const root = useRef<HTMLDivElement>(null);
  const input = useRef<HTMLInputElement>(null);
  const returnFocus = useRef<HTMLElement | null>(null);
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [index, setIndex] = useState(0);
  const [ranges, setRanges] = useState<Range[]>([]);
  const scrolledMatch = useRef<string | null>(null);
  const count = ranges.length;
  const activeIndex = count === 0 ? 0 : Math.min(index, count - 1);
  const show = useEffectEvent(() => {
    if (!open)
      returnFocus.current =
        document.activeElement instanceof HTMLElement ? document.activeElement : null;
    scrolledMatch.current = null;
    setOpen(true);
    requestAnimationFrame(() => {
      input.current?.focus();
      input.current?.select();
    });
  });
  const toggle = useEffectEvent((visible: boolean) => {
    if (nativeFind) {
      if (visible) nativeFind.open();
      else nativeFind.close();
    } else if (visible) show();
    else setOpen(false);
  });
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- registers this mounted DOM scope with the find coordinator
  useEffect(() => {
    if (!root.current || !enabled) return;
    return coordinator?.register(root.current, toggle);
  }, [coordinator, enabled]);
  const [identity, setIdentity] = useState(resetKey);
  if (identity !== resetKey || (!enabled && open)) {
    setIdentity(resetKey);
    setOpen(false);
    setIndex(0);
  }
  const close = () => {
    setOpen(false);
    if (returnFocus.current?.isConnected) returnFocus.current.focus({ preventScroll: true });
    else root.current?.focus({ preventScroll: true });
  };
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- observes rendered DOM text while find is open, with frame-coalesced range collection
  useEffect(() => {
    const element = root.current;
    if (!open || !element) return;
    let frame: number | null = null;
    const refresh = () => {
      if (frame !== null) return;
      frame = requestAnimationFrame(() => {
        frame = null;
        setRanges(collectFindRanges(element, query));
      });
    };
    refresh();
    const observer = new MutationObserver((mutations) => {
      if (
        mutations.some(
          (mutation) =>
            !(
              mutation.target instanceof Element ? mutation.target : mutation.target.parentElement
            )?.closest("[data-panel-find-bar]"),
        )
      )
        refresh();
    });
    observer.observe(element, {
      subtree: true,
      childList: true,
      characterData: true,
      attributes: true,
      attributeFilter: ["hidden", "class", "style", "aria-expanded"],
    });
    return () => {
      observer.disconnect();
      if (frame !== null) cancelAnimationFrame(frame);
    };
  }, [open, query]);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- keeps the browser CSS Highlight registry and scroll position in sync with the active match
  useEffect(() => {
    if (!open || !root.current) return;
    const active = ranges[activeIndex];
    if (typeof Highlight !== "undefined" && CSS.highlights) {
      CSS.highlights.set("panel-find", new Highlight(...ranges));
      CSS.highlights.set("panel-find-active", new Highlight(...(active ? [active] : [])));
    }
    const identity = `${query}:${activeIndex}`;
    if (active && scrolledMatch.current !== identity) {
      scrollFindRangeIntoView(active);
      scrolledMatch.current = identity;
    }
    return () => {
      globalThis.CSS?.highlights?.delete("panel-find");
      globalThis.CSS?.highlights?.delete("panel-find-active");
    };
  }, [open, ranges, activeIndex, query]);
  const move = (direction: number) =>
    setIndex(count ? (activeIndex + direction + count) % count : 0);
  const onFindKey = useEffectEvent((event: KeyboardEvent) => {
    if (!open || event.isComposing || event.altKey) return;
    if (
      event.target instanceof Element &&
      event.target.closest('[role="dialog"], [role="alertdialog"]') &&
      !root.current?.contains(event.target)
    )
      return;
    const modifier = isMacPlatform(navigator.platform) ? event.metaKey : event.ctrlKey;
    if (
      event.key === "Escape" ||
      event.key === "F3" ||
      (modifier && event.key.toLowerCase() === "g")
    ) {
      event.preventDefault();
      event.stopImmediatePropagation();
      if (event.key === "Escape") close();
      else move(event.shiftKey ? -1 : 1);
    }
  });
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- owns keyboard navigation and dismissal listeners while this DOM scope is mounted
  useEffect(() => {
    const listener = (event: KeyboardEvent) => onFindKey(event);
    window.addEventListener("keydown", listener, true);
    return () => window.removeEventListener("keydown", listener, true);
  }, []);
  return (
    <div {...props} ref={root} data-find-scope={label} tabIndex={-1}>
      {children}
      {open ? (
        <div
          data-panel-find-bar
          role="search"
          aria-label={`Find in ${label}`}
          className="absolute right-3 top-2 z-50 flex max-w-[calc(100%-1.5rem)] items-center gap-1 rounded-lg border border-border bg-popover p-1.5 text-popover-foreground shadow-lg"
          onKeyDown={(event) => {
            if (event.key === "Escape") {
              event.preventDefault();
              event.stopPropagation();
              close();
            }
            if (
              event.key === "Enter" &&
              event.target === input.current &&
              !event.nativeEvent.isComposing
            ) {
              event.preventDefault();
              event.stopPropagation();
              move(event.shiftKey ? -1 : 1);
            }
          }}
        >
          <SearchIcon className="mx-1 size-3.5 shrink-0 text-muted-foreground" />
          <Input
            ref={input}
            size="compact"
            aria-label={`Find in ${label}`}
            placeholder="Find…"
            value={query}
            onChange={(event) => {
              setQuery(event.target.value);
              setIndex(0);
            }}
          />
          <span
            role="status"
            aria-live="polite"
            className="min-w-14 shrink-0 px-1 text-center text-xs tabular-nums text-muted-foreground"
          >
            {query ? `${count ? activeIndex + 1 : 0} of ${count}` : ""}
          </span>
          <Button
            size="icon-xs"
            variant="ghost"
            aria-label="Previous match"
            title="Previous match (Shift+Enter)"
            disabled={!count}
            onClick={() => move(-1)}
          >
            <ChevronUpIcon />
          </Button>
          <Button
            size="icon-xs"
            variant="ghost"
            aria-label="Next match"
            title="Next match (Enter)"
            disabled={!count}
            onClick={() => move(1)}
          >
            <ChevronDownIcon />
          </Button>
          <Button
            size="icon-xs"
            variant="ghost"
            aria-label="Close find"
            title="Close (Escape)"
            onClick={close}
          >
            <XIcon />
          </Button>
        </div>
      ) : null}
    </div>
  );
}
