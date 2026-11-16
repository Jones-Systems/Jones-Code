import {
  createContext,
  use,
  useCallback,
  useEffect,
  useEffectEvent,
  useMemo,
  useRef,
  useState,
  type ComponentProps,
  type ReactNode,
} from "react";
import { ChevronDownIcon, ChevronUpIcon, SearchIcon, XIcon } from "lucide-react";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { collectFindRanges, findTextMatches, scrollFindRangeIntoView } from "./panelFind";
import { isMacPlatform } from "../../lib/utils";

interface FindSource {
  getItems: () => ReadonlyArray<{ id: string; text: string }>;
  reveal: (id: string) => void;
  loading: boolean;
  history?: { cursor: string; load: () => void };
}

const FindCoordinator = createContext<{
  register: (root: HTMLElement, toggle: (open: boolean) => void) => () => void;
} | null>(null);

export function PanelFindProvider({ children }: { children: ReactNode }) {
  const scopes = useRef(new Map<HTMLElement, (open: boolean) => void>());
  const lastScope = useRef<HTMLElement | null>(null);
  const register = useCallback((root: HTMLElement, toggle: (open: boolean) => void) => {
    scopes.current.set(root, toggle);
    return () => {
      scopes.current.delete(root);
    };
  }, []);
  useEffect(() => {
    const remember = (event: Event) => {
      if (!(event.target instanceof Element)) return;
      const root = event.target.closest<HTMLElement>("[data-find-scope]");
      if (root && scopes.current.has(root)) lastScope.current = root;
    };
    const open = () => {
      const focused = document.activeElement?.closest<HTMLElement>("[data-find-scope]");
      let root =
        lastScope.current?.isConnected &&
        scopes.current.has(lastScope.current) &&
        lastScope.current.getClientRects().length > 0
          ? lastScope.current
          : focused && scopes.current.has(focused)
            ? focused
            : [...scopes.current.keys()].find(
                (scope) =>
                  scope.hasAttribute("data-find-default") && scope.getClientRects().length > 0,
              );
      if (!root) return false;
      const parent = root;
      root =
        [...scopes.current.keys()].find(
          (scope) =>
            scope !== parent &&
            parent.contains(scope) &&
            scope.hasAttribute("data-find-default") &&
            scope.getClientRects().length > 0,
        ) ?? root;
      for (const [scope, toggle] of scopes.current) toggle(scope === root);
      return true;
    };
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
      window.removeEventListener("pointerdown", remember, true);
      window.removeEventListener("focusin", remember, true);
      window.removeEventListener("keydown", keydown, true);
      unsubscribe?.();
    };
  }, []);
  const value = useMemo(() => ({ register }), [register]);
  return <FindCoordinator value={value}>{children}</FindCoordinator>;
}

const FindContext = createContext<{
  query: string;
  selectedId: string | null;
  setSource: (source: FindSource | null) => void;
} | null>(null);

export function usePanelFindSource(source: FindSource) {
  const context = use(FindContext);
  const setSource = context?.setSource;
  useEffect(() => {
    setSource?.(source);
    return () => setSource?.(null);
  }, [setSource, source]);
  return { query: context?.query ?? "", selectedId: context?.selectedId ?? null };
}

export function usePanelFindTarget(id: string) {
  return use(FindContext)?.selectedId === id;
}

export function PanelFindScope({
  label,
  resetKey,
  enabled = true,
  children,
  ...props
}: ComponentProps<"div"> & { label: string; resetKey?: string | undefined; enabled?: boolean }) {
  const coordinator = use(FindCoordinator);
  const root = useRef<HTMLDivElement>(null);
  const input = useRef<HTMLInputElement>(null);
  const returnFocus = useRef<HTMLElement | null>(null);
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [index, setIndex] = useState(0);
  const [source, setSource] = useState<FindSource | null>(null);
  const [ranges, setRanges] = useState<Range[]>([]);
  const requestedPages = useRef({ query: "", open: false, cursors: new Set<string>() });
  const scrolledMatch = useRef<string | null>(null);
  const sourceMatches = useMemo(
    () =>
      !open || !query || !source
        ? []
        : source.getItems().flatMap((item) =>
            findTextMatches(item.text, query).map((match, occurrence) => ({
              ...match,
              id: item.id,
              occurrence,
            })),
          ),
    [open, query, source],
  );
  const count = source ? sourceMatches.length : ranges.length;
  const activeIndex = count === 0 ? 0 : Math.min(index, count - 1);
  const selected = sourceMatches[activeIndex];
  const selectedId = selected?.id;
  const selectedOccurrence = selected?.occurrence;
  const reveal = source?.reveal;
  const show = useEffectEvent(() => {
    if (!open)
      returnFocus.current =
        document.activeElement instanceof HTMLElement ? document.activeElement : null;
    setOpen(true);
    requestAnimationFrame(() => {
      input.current?.focus();
      input.current?.select();
    });
  });
  useEffect(() => {
    if (!root.current || !enabled) return;
    return coordinator?.register(root.current, (visible) => {
      if (visible) show();
      else setOpen(false);
    });
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
  useEffect(() => {
    requestedPages.current = { query, open, cursors: new Set() };
    scrolledMatch.current = null;
  }, [query, open]);
  useEffect(() => {
    if (!open || !query || !source?.history || source.loading) return;
    if (requestedPages.current.cursors.has(source.history.cursor)) return;
    requestedPages.current.cursors.add(source.history.cursor);
    source.history.load();
  }, [open, query, source]);
  useEffect(() => {
    if (selectedId) reveal?.(selectedId);
  }, [selectedId, reveal]);
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
  useEffect(() => {
    if (!open || !root.current) return;
    const matchingRanges = selectedId
      ? ranges.filter(
          (range) =>
            range.startContainer.parentElement
              ?.closest("[data-find-item]")
              ?.getAttribute("data-find-item") === selectedId,
        )
      : ranges;
    const active = matchingRanges[selectedOccurrence ?? activeIndex];
    if (typeof Highlight !== "undefined" && CSS.highlights) {
      CSS.highlights.set("panel-find", new Highlight(...ranges));
      CSS.highlights.set("panel-find-active", new Highlight(...(active ? [active] : [])));
    }
    const identity = `${query}:${selectedId ?? "dom"}:${selectedOccurrence ?? activeIndex}`;
    if (active && scrolledMatch.current !== identity) {
      scrollFindRangeIntoView(active);
      scrolledMatch.current = identity;
    }
    return () => {
      globalThis.CSS?.highlights?.delete("panel-find");
      globalThis.CSS?.highlights?.delete("panel-find-active");
    };
  }, [open, ranges, activeIndex, selectedId, selectedOccurrence, query]);
  const value = useMemo(
    () => ({
      query: open ? query : "",
      selectedId: open ? (selected?.id ?? null) : null,
      setSource,
    }),
    [open, query, selected?.id],
  );
  const move = (direction: number) =>
    setIndex(count ? (activeIndex + direction + count) % count : 0);
  const onFindKey = useEffectEvent((event: KeyboardEvent) => {
    if (!open || event.isComposing || event.altKey) return;
    if (event.target instanceof Element && event.target.closest('[role="dialog"]')) return;
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
  useEffect(() => {
    const listener = (event: KeyboardEvent) => onFindKey(event);
    window.addEventListener("keydown", listener, true);
    return () => window.removeEventListener("keydown", listener, true);
  }, []);
  return (
    <FindContext value={value}>
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
              if (event.key === "Enter" && !event.nativeEvent.isComposing) {
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
            {source?.loading || (query && source?.history) ? (
              <span className="max-w-28 text-xs text-muted-foreground" role="status">
                Searching history…
              </span>
            ) : null}
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
    </FindContext>
  );
}
