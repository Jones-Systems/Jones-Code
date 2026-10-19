import { useAtomValue } from "@effect/atom-react";
import { useLinkTo } from "@react-navigation/native";
import type { SettlementEntry } from "@t3tools/client-runtime/jones/settlement-notifications";
import { enabledEnvironmentIds } from "@t3tools/client-runtime/state/connections";
import type { EnvironmentId } from "@t3tools/contracts";
import { useEffect, useState, useSyncExternalStore } from "react";
import { FlatList, Pressable, View, useWindowDimensions } from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";

import { AppText } from "../../components/ui/AppText";
import { SymbolView } from "../../components/ui/AppSymbol";
import { GlassSurface } from "../../components/ui/GlassSurface";
import { IconButton } from "../../components/ui/IconButton";
import { environmentCatalog } from "../../connection/catalog";
import { MOBILE_SHAPE } from "../../lib/designTokens";
import { APP_BAR_HEIGHT } from "../../lib/layoutMetrics";
import { environmentShell } from "../../state/shell";
import { settlementNotificationController as controller } from "./controller";
import { encodeThreadDeepLink } from "./navigation";

function EnvironmentObserver(props: { readonly environmentId: EnvironmentId }) {
  const state = useAtomValue(environmentShell.stateValueAtom(props.environmentId));
  useEffect(() => {
    if (state.status === "live") controller.observe(props.environmentId, state);
  }, [props.environmentId, state]);
  return null;
}

function SettlementTitle(props: { readonly entry: SettlementEntry }) {
  const linkTo = useLinkTo();
  return (
    <Pressable
      accessibilityRole="link"
      accessibilityLabel={props.entry.title}
      onPress={() => linkTo(encodeThreadDeepLink(props.entry))}
      className="min-h-11 justify-center py-2"
    >
      <AppText textRole="footnote" className="text-primary" numberOfLines={2}>
        {props.entry.title}
      </AppText>
      {props.entry.threadGone ? (
        <AppText textRole="label" className="text-foreground-muted">
          Thread no longer available
        </AppText>
      ) : null}
    </Pressable>
  );
}

function SettlementBanner(props: { readonly entries: readonly SettlementEntry[] }) {
  const [expanded, setExpanded] = useState(false);
  const insets = useSafeAreaInsets();
  const { height } = useWindowDimensions();
  const single = props.entries.length === 1 ? props.entries[0] : undefined;
  const shownIds = props.entries.map((entry) => entry.id);
  const title = single ? "Thread settled" : `${props.entries.length} threads settled`;

  return (
    <View
      className="absolute inset-x-3 z-[100]"
      style={{ top: insets.top + APP_BAR_HEIGHT + 8 }}
      pointerEvents="box-none"
    >
      <GlassSurface className="p-3" style={{ borderRadius: MOBILE_SHAPE.toast }}>
        <View className="flex-row items-center gap-3">
          <SymbolView name="checkmark.circle" size={20} tintColorClassName="accent-icon" />
          <View className="min-w-0 flex-1" accessibilityLiveRegion="polite">
            {single ? (
              <AppText textRole="footnote" className="font-t3-semibold">
                {title}
              </AppText>
            ) : (
              <Pressable
                accessibilityRole="button"
                accessibilityLabel={title}
                accessibilityState={{ expanded }}
                onPress={() => setExpanded((value) => !value)}
                className="min-h-11 flex-row items-center justify-between gap-2"
              >
                <AppText textRole="footnote" className="font-t3-semibold flex-1">
                  {title}
                </AppText>
                <SymbolView
                  name={expanded ? "chevron.up" : "chevron.down"}
                  size={16}
                  tintColorClassName="accent-icon"
                />
              </Pressable>
            )}
          </View>
          <IconButton
            accessibilityLabel="Dismiss settlement notification"
            icon="xmark"
            onPress={() => controller.dismiss(shownIds)}
          />
        </View>
        {single ? <SettlementTitle entry={single} /> : null}
        {!single && expanded ? (
          <FlatList
            data={props.entries}
            keyExtractor={(entry) => entry.id}
            renderItem={({ item }) => <SettlementTitle entry={item} />}
            style={{ maxHeight: Math.min(280, height * 0.4), flexGrow: 0 }}
            keyboardShouldPersistTaps="handled"
          />
        ) : null}
      </GlassSurface>
    </View>
  );
}

export function SettlementNotificationOverlay() {
  const catalog = useAtomValue(environmentCatalog.catalogValueAtom);
  const snapshot = useSyncExternalStore(controller.subscribe, controller.getSnapshot);
  useEffect(() => {
    if (catalog.isReady && snapshot.persistence !== "loading") {
      controller.noteCatalog([...catalog.entries.keys()]);
    }
  }, [catalog, snapshot.persistence]);

  if (snapshot.persistence === "loading") return null;

  return (
    <>
      {[...enabledEnvironmentIds(catalog)].map((environmentId) => (
        <EnvironmentObserver key={environmentId} environmentId={environmentId} />
      ))}
      {snapshot.visible.length > 0 ? <SettlementBanner entries={snapshot.visible} /> : null}
    </>
  );
}
