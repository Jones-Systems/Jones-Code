import { useState } from "react";
import { View } from "react-native";
import {
  jonesBuildLabel,
  type JonesBuildLabel,
  type JonesBuildRef,
} from "@t3tools/shared/jones/buildTime";
import { AppText as Text } from "../../components/ui/AppText";

export function JonesBuildVersion({
  build,
  label = jonesBuildLabel(build),
  title = "Version",
}: {
  readonly build: JonesBuildRef;
  readonly label?: JonesBuildLabel;
  readonly title?: string;
}) {
  const [expanded, setExpanded] = useState(false);
  return (
    <View className="gap-1">
      <Text
        accessibilityRole="button"
        accessibilityState={{ expanded }}
        accessibilityLabel={`${title}: ${label.spoken}. ${build.version}. Show build details`}
        onPress={() => setExpanded((value) => !value)}
        className="text-base text-foreground"
      >
        {title} {label.text}
      </Text>
      {expanded ? (
        <>
          <Text selectable className="text-sm text-foreground-muted">
            {label.detail}
          </Text>
          <Text selectable className="text-sm text-foreground-muted">
            {build.version}
          </Text>
          {build.sourceSha ? (
            <Text selectable className="text-sm text-foreground-muted">
              Source {build.sourceSha}
            </Text>
          ) : null}
        </>
      ) : null}
    </View>
  );
}
