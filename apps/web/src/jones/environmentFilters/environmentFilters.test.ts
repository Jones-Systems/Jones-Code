import { AtomRegistry } from "effect/reactivity";
import { describe, expect, it, vi } from "vite-plus/test";
import {
  DEFAULT_SIDEBAR_ENVIRONMENT_FILTERS,
  SIDEBAR_ENVIRONMENT_FILTERS,
  excludedSidebarEnvironmentIds,
  filterSidebarEnvironmentThreads,
  sidebarEnvironmentExclusionKey,
  toggleSidebarEnvironmentFilter,
} from "./model";
import { sidebarEnvironmentFiltersAtom } from "./state";

const identities = [
  { environmentId: "vps-one", label: "VPS" },
  { environmentId: "test-one", label: "Test" },
  { environmentId: "mini-one", label: "Mini" },
  { environmentId: "home-one", label: "Laptop" },
];
const threads = identities.map(({ environmentId }) => ({ environmentId, title: environmentId }));

describe("sidebar environment filters", () => {
  it("includes all four environments by default without copying the thread inventory", () => {
    expect(SIDEBAR_ENVIRONMENT_FILTERS).toEqual(["VPS", "Test", "Mini", "Laptop"]);
    const excluded = excludedSidebarEnvironmentIds(identities, DEFAULT_SIDEBAR_ENVIRONMENT_FILTERS);
    expect([...excluded]).toEqual([]);
    expect(filterSidebarEnvironmentThreads(threads, excluded)).toBe(threads);
  });

  it.each(SIDEBAR_ENVIRONMENT_FILTERS)("toggles %s off and back on independently", (label) => {
    const off = toggleSidebarEnvironmentFilter(DEFAULT_SIDEBAR_ENVIRONMENT_FILTERS, label);
    const excluded = excludedSidebarEnvironmentIds(identities, off);
    expect(
      filterSidebarEnvironmentThreads(threads, excluded).map((thread) => thread.title),
    ).toEqual(
      identities
        .filter((identity) => identity.label !== label)
        .map((identity) => identity.environmentId),
    );
    expect(toggleSidebarEnvironmentFilter(off, label)).toEqual(DEFAULT_SIDEBAR_ENVIRONMENT_FILTERS);
    expect(DEFAULT_SIDEBAR_ENVIRONMENT_FILTERS[label]).toBe(true);
  });

  it("allows all four off while leaving unknown and missing labels visible", () => {
    const allOff = SIDEBAR_ENVIRONMENT_FILTERS.reduce(
      toggleSidebarEnvironmentFilter,
      DEFAULT_SIDEBAR_ENVIRONMENT_FILTERS,
    );
    const catalog = [
      ...identities,
      { environmentId: "other", label: "My VPS" },
      { environmentId: "custom-test", label: "My Test Server" },
      { environmentId: "backup", label: "Test Server Backup" },
      { environmentId: "contest", label: "Contest Server" },
      { environmentId: "blank", label: "  " },
      { environmentId: "missing" },
      { environmentId: "null", label: null },
    ];
    const inventory = [...catalog, { environmentId: "not-in-catalog" }];
    const excluded = excludedSidebarEnvironmentIds(catalog, allOff);
    expect(
      filterSidebarEnvironmentThreads(inventory, excluded).map((row) => row.environmentId),
    ).toEqual([
      "other",
      "custom-test",
      "backup",
      "contest",
      "blank",
      "missing",
      "null",
      "not-in-catalog",
    ]);
  });

  it.each([
    ["Test", " tEsT SeRvEr "],
    ["Laptop", " Home "],
    ["Laptop", " Laptop "],
    ["Mini", " Mac Mini "],
    ["Mini", "MAC-MINI"],
  ] as const)(
    "hides and restores %s threads labelled %s without machine metadata",
    (filter, label) => {
      const catalog = [{ environmentId: "host", label }];
      const off = toggleSidebarEnvironmentFilter(DEFAULT_SIDEBAR_ENVIRONMENT_FILTERS, filter);
      expect(
        filterSidebarEnvironmentThreads(catalog, excludedSidebarEnvironmentIds(catalog, off)),
      ).toEqual([]);
      expect(
        filterSidebarEnvironmentThreads(
          catalog,
          excludedSidebarEnvironmentIds(catalog, DEFAULT_SIDEBAR_ENVIRONMENT_FILTERS),
        ),
      ).toEqual(catalog);
    },
  );

  it("uses machine identity for custom host names and keeps generic or unknown hosts visible", () => {
    const catalog = [
      { environmentId: "portable", label: "My workstation" },
      { environmentId: "small", label: "Build machine" },
      { environmentId: "generic", label: "Other host" },
      { environmentId: "unknown", label: "Unclassified" },
      { environmentId: "lookalike", label: "Miniature laptop test" },
    ];
    const machines = new Map([
      ["portable", "laptop"],
      ["small", "mac-mini"],
      ["generic", "server"],
    ] as const);
    const homeOff = { ...DEFAULT_SIDEBAR_ENVIRONMENT_FILTERS, Laptop: false };
    const miniOff = { ...DEFAULT_SIDEBAR_ENVIRONMENT_FILTERS, Mini: false };
    expect([...excludedSidebarEnvironmentIds(catalog, homeOff, machines)]).toEqual(["portable"]);
    expect([...excludedSidebarEnvironmentIds(catalog, miniOff, machines)]).toEqual(["small"]);
    const bothOff = { ...homeOff, Mini: false };
    expect(
      filterSidebarEnvironmentThreads(
        catalog,
        excludedSidebarEnvironmentIds(catalog, bothOff, machines),
      ),
    ).toEqual(catalog.slice(2));
    expect([
      ...excludedSidebarEnvironmentIds(catalog, DEFAULT_SIDEBAR_ENVIRONMENT_FILTERS, machines),
    ]).toEqual([]);
  });

  it.each(["Test", "Test Server"])("keeps %s ahead of machine kinds", (label) => {
    const catalog = [
      { environmentId: "test-mini", label },
      { environmentId: "test-laptop", label },
      { environmentId: "vps", label: "VPS" },
      { environmentId: "home", label: "Laptop" },
    ];
    const machines = new Map([
      ["test-mini", "mac-mini"],
      ["test-laptop", "laptop"],
      ["vps", "laptop"],
      ["home", "mac-mini"],
    ] as const);
    expect([
      ...excludedSidebarEnvironmentIds(
        catalog,
        { ...DEFAULT_SIDEBAR_ENVIRONMENT_FILTERS, Mini: false },
        machines,
      ),
    ]).toEqual([]);
    expect([
      ...excludedSidebarEnvironmentIds(
        catalog,
        { ...DEFAULT_SIDEBAR_ENVIRONMENT_FILTERS, Test: false },
        machines,
      ),
    ]).toEqual(["test-mini", "test-laptop"]);
    expect([
      ...excludedSidebarEnvironmentIds(
        catalog,
        { ...DEFAULT_SIDEBAR_ENVIRONMENT_FILTERS, Laptop: false },
        machines,
      ),
    ]).toEqual(["home"]);
  });

  it("updates exclusions and reset keys when machine metadata arrives or changes", () => {
    const catalog = [{ environmentId: "host", label: "Custom name" }];
    const filters = { ...DEFAULT_SIDEBAR_ENVIRONMENT_FILTERS, Laptop: false };
    const key = (machine: "server" | "laptop" | "mac-mini") =>
      sidebarEnvironmentExclusionKey(
        excludedSidebarEnvironmentIds(catalog, filters, new Map([["host", machine]])),
      );
    expect(key("laptop")).not.toBe(key("server"));
    expect(key("mac-mini")).toBe(key("server"));
  });

  it.each([
    ["VPS", " vPs ", "VPS", "VPS test"],
    ["Test", " tEsT SeRvEr ", "Test Server", "Test Server Backup"],
  ] as const)(
    "groups %s labels by stable ID even when disconnected",
    (filter, first, second, other) => {
      const catalog = [
        { environmentId: "first", label: first, connection: { phase: "disconnected" } },
        { environmentId: "second", label: second, connection: { phase: "connected" } },
        { environmentId: "third", label: other, connection: { phase: "disconnected" } },
      ];
      const filters = toggleSidebarEnvironmentFilter(DEFAULT_SIDEBAR_ENVIRONMENT_FILTERS, filter);
      const excluded = excludedSidebarEnvironmentIds(catalog, filters);
      expect([...excluded]).toEqual(["first", "second"]);
      const inventory = [...catalog, { environmentId: "first" }];
      expect(filterSidebarEnvironmentThreads(inventory, excluded)).toEqual([catalog[2]]);
      const restored = excludedSidebarEnvironmentIds(
        catalog,
        toggleSidebarEnvironmentFilter(filters, filter),
      );
      expect(filterSidebarEnvironmentThreads(inventory, restored)).toBe(inventory);
      expect(sidebarEnvironmentExclusionKey(excluded)).not.toBe(
        sidebarEnvironmentExclusionKey(restored),
      );
      expect(sidebarEnvironmentExclusionKey(restored)).toBe("[]");
    },
  );

  it("changes the reset key on label reassignment but not catalog reordering", () => {
    const filters = toggleSidebarEnvironmentFilter(DEFAULT_SIDEBAR_ENVIRONMENT_FILTERS, "VPS");
    const key = (catalog: typeof identities) =>
      sidebarEnvironmentExclusionKey(excludedSidebarEnvironmentIds(catalog, filters));
    expect(key(identities.toReversed())).toBe(key(identities));
    expect(key(identities.map((identity) => ({ ...identity, label: "Laptop" })))).not.toBe(
      key(identities),
    );
  });

  it("retains filters across sidebar unmounts and resets in a fresh renderer registry", async () => {
    vi.useFakeTimers();
    const registry = AtomRegistry.make({ defaultIdleTTL: 0 });
    const freshRegistry = AtomRegistry.make();
    try {
      const unmount = registry.mount(sidebarEnvironmentFiltersAtom);
      const filters = toggleSidebarEnvironmentFilter(
        registry.get(sidebarEnvironmentFiltersAtom),
        "Mini",
      );
      registry.set(sidebarEnvironmentFiltersAtom, filters);
      unmount();
      await vi.runAllTimersAsync();
      const remount = registry.mount(sidebarEnvironmentFiltersAtom);
      expect(registry.get(sidebarEnvironmentFiltersAtom).Mini).toBe(false);
      expect(freshRegistry.get(sidebarEnvironmentFiltersAtom)).toEqual(
        DEFAULT_SIDEBAR_ENVIRONMENT_FILTERS,
      );
      remount();
    } finally {
      registry.dispose();
      freshRegistry.dispose();
      vi.useRealTimers();
    }
  });
});
