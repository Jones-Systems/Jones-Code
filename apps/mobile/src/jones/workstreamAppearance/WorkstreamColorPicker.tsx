import { useRef, useState } from "react";
import { Pressable, Text, TextInput, View } from "react-native";
import type { WorkstreamAppearance, WorkstreamAppearanceWrite } from "@t3tools/contracts";
import {
  WORKSTREAM_COLOR_PRESETS,
  workstreamPaletteIndex,
  normalizeWorkstreamColor,
} from "@t3tools/client-runtime/state/workstreams";

function RgbChannel(props: {
  label: string;
  value: number;
  disabled: boolean;
  onChange: (value: number) => void;
}) {
  const width = useRef(1);
  const change = (x: number) =>
    props.onChange(Math.round(Math.max(0, Math.min(1, x / width.current)) * 255));
  return (
    <View style={{ gap: 6 }}>
      <Text>
        {props.label}: {props.value}
      </Text>
      <View
        accessible
        accessibilityRole="adjustable"
        accessibilityLabel={props.label}
        accessibilityValue={{ min: 0, max: 255, now: props.value }}
        accessibilityState={{ disabled: props.disabled }}
        accessibilityActions={[{ name: "increment" }, { name: "decrement" }]}
        onAccessibilityAction={(event) => {
          if (!props.disabled)
            props.onChange(
              Math.max(
                0,
                Math.min(
                  255,
                  props.value + (event.nativeEvent.actionName === "increment" ? 1 : -1),
                ),
              ),
            );
        }}
        onLayout={(event) => {
          width.current = event.nativeEvent.layout.width;
        }}
        onStartShouldSetResponder={() => !props.disabled}
        onResponderGrant={(event) => change(event.nativeEvent.locationX)}
        onResponderMove={(event) => change(event.nativeEvent.locationX)}
        style={{ height: 32, backgroundColor: "#e5e7eb", justifyContent: "center" }}
      >
        <View
          pointerEvents="none"
          style={{
            position: "absolute",
            height: 24,
            width: 6,
            left: `${(props.value / 255) * 98}%`,
            backgroundColor: "#374151",
          }}
        />
      </View>
    </View>
  );
}

export function WorkstreamColorPicker(props: {
  readonly name: string;
  readonly saved: WorkstreamAppearance;
  readonly generation: number;
  readonly createCommandId: () => Promise<string>;
  readonly save: (input: WorkstreamAppearanceWrite) => Promise<WorkstreamAppearance>;
  readonly onClose: () => void;
}) {
  const [saved] = useState(() => props.saved);
  const [draft, setDraft] = useState(saved.border_color ?? "#0284C7");
  const [automatic, setAutomatic] = useState(saved.border_color === null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [retry, setRetry] = useState<WorkstreamAppearanceWrite | null>(null);
  const busyRef = useRef(false);
  const color = normalizeWorkstreamColor(draft);
  const fallback = ["#0284c7", "#7c3aed", "#059669", "#d97706", "#e11d48"][
    workstreamPaletteIndex(saved.workstream_id)
  ]!;
  const channels = [1, 3, 5].map((start) =>
    parseInt((color ?? "#0284C7").slice(start, start + 2), 16),
  );
  const select = (value: string) => {
    setDraft(value);
    setAutomatic(false);
  };
  const disabled = busy || retry !== null;
  const commit = async () => {
    if (busyRef.current || (!automatic && !color)) return;
    busyRef.current = true;
    setBusy(true);
    setError(null);
    try {
      const request = retry ?? {
        command_id: await props.createCommandId(),
        workstream_id: saved.workstream_id,
        expected_server_generation: props.generation,
        expected_version: saved.version,
        border_color: automatic ? null : color,
      };
      setRetry(request);
      await props.save(request);
      props.onClose();
    } catch {
      setError("Could not confirm the color. Retry this save or refresh to check the result.");
    } finally {
      busyRef.current = false;
      setBusy(false);
    }
  };
  return (
    <View style={{ gap: 12, paddingVertical: 12 }}>
      <Text style={{ fontSize: 18, fontWeight: "600" }}>Color for {props.name}</Text>
      <View
        accessibilityLabel="Border preview"
        style={{
          borderLeftWidth: 3,
          borderLeftColor: automatic ? fallback : (color ?? fallback),
          padding: 12,
        }}
      >
        <Text>
          {props.name}
          {automatic ? " · Automatic" : ""}
        </Text>
      </View>
      <View style={{ flexDirection: "row", flexWrap: "wrap", gap: 8 }}>
        {WORKSTREAM_COLOR_PRESETS.map(([name, value]) => (
          <Pressable
            key={value}
            accessibilityRole="button"
            accessibilityLabel={`${name} ${value}`}
            accessibilityState={{ selected: !automatic && color === value, disabled }}
            disabled={disabled}
            onPress={() => select(value)}
            style={{
              backgroundColor: value,
              width: 40,
              height: 40,
              justifyContent: "center",
              alignItems: "center",
            }}
          >
            {!automatic && color === value ? (
              <Text style={{ backgroundColor: "#ffffff", color: "#111827" }}>✓</Text>
            ) : null}
          </Pressable>
        ))}
      </View>
      {(["Red", "Green", "Blue"] as const).map((label, index) => (
        <RgbChannel
          key={label}
          label={label}
          value={channels[index]!}
          disabled={disabled}
          onChange={(value) =>
            select(
              `#${channels.map((channel, channelIndex) => (channelIndex === index ? value : channel).toString(16).padStart(2, "0")).join("")}`,
            )
          }
        />
      ))}
      <Text>Hex color</Text>
      <TextInput
        accessibilityLabel="Hex color"
        value={draft}
        editable={!disabled}
        autoCapitalize="characters"
        autoCorrect={false}
        onChangeText={select}
        style={{ borderWidth: 1, borderColor: "#9ca3af", padding: 12, color: "#111827" }}
      />
      <Pressable
        accessibilityRole="button"
        accessibilityState={{ selected: automatic, disabled }}
        disabled={disabled}
        onPress={() => setAutomatic(true)}
      >
        <Text>Reset to automatic</Text>
      </Pressable>
      {error ? <Text accessibilityRole="alert">{error}</Text> : null}
      <Pressable
        accessibilityRole="button"
        disabled={busy || (!automatic && !color)}
        onPress={() => void commit()}
      >
        <Text>{busy ? "Saving…" : retry ? "Retry save" : "Save color"}</Text>
      </Pressable>
      <Pressable accessibilityRole="button" disabled={busy} onPress={props.onClose}>
        <Text>Cancel</Text>
      </Pressable>
    </View>
  );
}
