import type { ExpoConfig } from "expo/config";

import { BRAND_ASSET_PATHS } from "../../scripts/lib/brand-assets.ts";
import { loadRepoEnv } from "../../scripts/lib/public-config.ts";

type AppVariant = "development" | "preview" | "production" | "jones-testflight";

const repoEnv = loadRepoEnv();
Object.assign(process.env, repoEnv);
// Expo's TypeScript setup otherwise installs `typescript` on every start; we typecheck with tsrs.
process.env.EXPO_NO_TYPESCRIPT_SETUP = "1";

const APP_VARIANT = resolveAppVariant(repoEnv.APP_VARIANT);
const isIosPersonalTeamBuild = repoEnv.T3CODE_IOS_PERSONAL_TEAM === "1";
const isJonesTestFlight = APP_VARIANT === "jones-testflight";
const runtimeVersionPolicy =
  process.env.MOBILE_VERSION_POLICY ??
  (APP_VARIANT === "development" ? "appVersion" : "fingerprint");

const personalTeamBundleIdentifier = repoEnv.T3CODE_IOS_PERSONAL_TEAM_BUNDLE_ID?.trim();
const IOS_BUNDLE_IDENTIFIER_PATTERN = /^[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+$/;
const UPSTREAM_IOS_BUNDLE_IDENTIFIER = "com.t3tools.t3code";
const UPSTREAM_APPLE_TEAM_ID = "ARK85ZXQ4Z";
const UPSTREAM_EXPO_OWNER = "pingdotgg";
const UPSTREAM_EAS_PROJECT_ID = "d763fcb8-d37c-41ea-a773-b54a0ab4a454";

if (repoEnv.EAS_BUILD_PROFILE === "jones-testflight" && !isJonesTestFlight) {
  throw new Error("The jones-testflight build requires APP_VARIANT=jones-testflight.");
}

// Compiled into the native project. Without a relay, Observe's native side could dispatch to
// Expo's own ingest before observe.ts turns it off; `.invalid` never resolves (RFC 6761).
const observeEndpointUrl = repoEnv.T3CODE_RELAY_URL?.trim()
  ? new URL("/v1/mobile/observe", repoEnv.T3CODE_RELAY_URL.trim()).toString()
  : "https://observe.invalid";

const fromRepoRoot = (relativePath: string) => `../../${relativePath}`;
const androidAdaptiveForeground = "./assets/android-icon-foreground.png";

if (
  !isJonesTestFlight &&
  isIosPersonalTeamBuild &&
  (!personalTeamBundleIdentifier ||
    !IOS_BUNDLE_IDENTIFIER_PATTERN.test(personalTeamBundleIdentifier))
) {
  throw new Error(
    "T3CODE_IOS_PERSONAL_TEAM_BUNDLE_ID must be a reverse-DNS identifier such as com.example.t3code when T3CODE_IOS_PERSONAL_TEAM=1.",
  );
}

const DEVELOPMENT_ASSETS = {
  appIcon: fromRepoRoot(BRAND_ASSET_PATHS.developmentIosIconPng),
  iosIcon: fromRepoRoot(BRAND_ASSET_PATHS.developmentIconComposerProject),
  splashIcon: fromRepoRoot(BRAND_ASSET_PATHS.developmentIosIconPng),
  androidAdaptiveForeground,
  androidAdaptiveBackgroundColor: "#347FF8",
  androidAdaptiveBackgroundImage: "./assets/android-icon-background-dev.png",
  androidSplashIcon: "./assets/android-splash-icon-dev.png",
  androidMonochromeIcon: "./assets/android-icon-mark.png",
  androidNotificationIcon: "./assets/android-notification-icon.png",
  androidNotificationColor: "#00639B",
} as const;

const PREVIEW_ASSETS = {
  appIcon: fromRepoRoot(BRAND_ASSET_PATHS.nightlyIosIconPng),
  iosIcon: fromRepoRoot(BRAND_ASSET_PATHS.nightlyIconComposerProject),
  splashIcon: fromRepoRoot(BRAND_ASSET_PATHS.nightlyIosIconPng),
  androidAdaptiveForeground,
  androidAdaptiveBackgroundColor: "#111533",
  androidAdaptiveBackgroundImage: "./assets/android-icon-background-nightly.png",
  androidSplashIcon: "./assets/android-splash-icon-nightly.png",
  androidMonochromeIcon: "./assets/android-icon-mark.png",
  androidNotificationIcon: "./assets/android-notification-icon.png",
  androidNotificationColor: "#7565C7",
} as const;

const RELEASE_ASSETS = {
  appIcon: fromRepoRoot(BRAND_ASSET_PATHS.productionIosIconPng),
  iosIcon: fromRepoRoot(BRAND_ASSET_PATHS.productionIconComposerProject),
  splashIcon: fromRepoRoot(BRAND_ASSET_PATHS.productionIosIconPng),
  androidAdaptiveForeground,
  androidAdaptiveBackgroundColor: "#000000",
  androidAdaptiveBackgroundImage: undefined,
  androidSplashIcon: "./assets/android-splash-icon-prod.png",
  androidMonochromeIcon: "./assets/android-icon-mark.png",
  androidNotificationIcon: "./assets/android-notification-icon.png",
  androidNotificationColor: "#FFFFFF",
} as const;

const VARIANT_CONFIG = {
  development: {
    appName: "T3 Code Dev",
    scheme: "t3code-dev",
    iosBundleIdentifier: "com.t3tools.t3code.dev",
    androidPackage: "com.t3tools.t3code.dev",
    relyingParty: "clerk.t3.codes",
    assets: DEVELOPMENT_ASSETS,
  },
  preview: {
    appName: "T3 Code Preview",
    scheme: "t3code-preview",
    iosBundleIdentifier: "com.t3tools.t3code.preview",
    androidPackage: "com.t3tools.t3code.preview",
    relyingParty: "clerk.t3.codes",
    assets: PREVIEW_ASSETS,
  },
  production: {
    appName: "T3 Code",
    scheme: "t3code",
    iosBundleIdentifier: UPSTREAM_IOS_BUNDLE_IDENTIFIER,
    androidPackage: "com.t3tools.t3code",
    relyingParty: "clerk.t3.codes",
    assets: RELEASE_ASSETS,
  },
} as const;

function resolveAppVariant(value: string | undefined): AppVariant {
  switch (value) {
    case "development":
    case "preview":
    case "production":
    case "jones-testflight":
      return value;
    case undefined:
      return "production";
    default:
      throw new Error("APP_VARIANT must be development, preview, production, or jones-testflight.");
  }
}

function requireJonesTestFlightValue(key: string, pattern: RegExp) {
  const value = repoEnv[key]?.trim();
  if (!value || !pattern.test(value)) {
    throw new Error(`Set ${key} to a valid Jones Code value for jones-testflight.`);
  }
  return value;
}

function resolveJonesTestFlight() {
  if (repoEnv.JONES_TESTFLIGHT_PLATFORM === "android" || repoEnv.EAS_BUILD_PLATFORM === "android") {
    throw new Error("The jones-testflight profile only supports iOS.");
  }
  if (isIosPersonalTeamBuild) {
    throw new Error("jones-testflight requires an Apple Developer Program team for distribution.");
  }

  const owner = requireJonesTestFlightValue(
    "JONES_TESTFLIGHT_EXPO_OWNER",
    /^[a-z0-9][a-z0-9_-]*$/i,
  );
  const projectId = requireJonesTestFlightValue(
    "JONES_TESTFLIGHT_EAS_PROJECT_ID",
    /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i,
  ).toLowerCase();
  const bundleIdentifier = requireJonesTestFlightValue(
    "JONES_TESTFLIGHT_IOS_BUNDLE_ID",
    IOS_BUNDLE_IDENTIFIER_PATTERN,
  );
  const appleTeamId = requireJonesTestFlightValue(
    "JONES_TESTFLIGHT_APPLE_TEAM_ID",
    /^[A-Z0-9]{10}$/,
  );
  if (
    owner.toLowerCase() === UPSTREAM_EXPO_OWNER ||
    projectId === UPSTREAM_EAS_PROJECT_ID ||
    /^com\.t3tools(?:\.|$)/i.test(bundleIdentifier) ||
    appleTeamId === UPSTREAM_APPLE_TEAM_ID
  ) {
    throw new Error("jones-testflight requires Jones-owned Expo and Apple identifiers.");
  }
  if (repoEnv.EAS_BUILD_PROJECT_ID && repoEnv.EAS_BUILD_PROJECT_ID !== projectId) {
    throw new Error("EAS_BUILD_PROJECT_ID does not match JONES_TESTFLIGHT_EAS_PROJECT_ID.");
  }

  const clerkDomain = repoEnv.JONES_TESTFLIGHT_CLERK_DOMAIN?.trim().toLowerCase();
  if (clerkDomain && !/^[a-z0-9-]+(?:\.[a-z0-9-]+)+$/.test(clerkDomain)) {
    throw new Error("JONES_TESTFLIGHT_CLERK_DOMAIN must be a hostname.");
  }
  return { owner, projectId, bundleIdentifier, appleTeamId, clerkDomain };
}

const jonesTestFlight = isJonesTestFlight ? resolveJonesTestFlight() : undefined;
const variant = VARIANT_CONFIG[APP_VARIANT === "jones-testflight" ? "production" : APP_VARIANT];
// app.t3.codes/.well-known lists the upstream identifiers for copied thread links.
const HOSTED_APP_HOST = "app.t3.codes";
const relyingParty = isJonesTestFlight ? jonesTestFlight?.clerkDomain : variant.relyingParty;
const iosBundleIdentifier =
  jonesTestFlight?.bundleIdentifier ??
  (isIosPersonalTeamBuild ? personalTeamBundleIdentifier! : variant.iosBundleIdentifier);

const dmSansFonts = {
  regular: "@expo-google-fonts/dm-sans/400Regular/DMSans_400Regular.ttf",
  medium: "@expo-google-fonts/dm-sans/500Medium/DMSans_500Medium.ttf",
  bold: "@expo-google-fonts/dm-sans/700Bold/DMSans_700Bold.ttf",
} as const;

const widgetsPlugin: NonNullable<ExpoConfig["plugins"]>[number] = [
  "expo-widgets",
  {
    bundleIdentifier: `${iosBundleIdentifier}.widgets`,
    groupIdentifier: `group.${iosBundleIdentifier}`,
    enablePushNotifications: true,
    // Frequent updates need the iOS entitlement to avoid early throttling.
    frequentUpdates: true,
    enableAndroid: !isJonesTestFlight,
    widgets: [
      {
        name: "SubscriptionUsage",
        displayName: "Subscription usage",
        description: "Subscription quotas from your connected T3 Code environments.",
        ios: {
          configuration: {
            title: "Subscription usage",
            description:
              "Both shows Session and Weekly when available. The Lock Screen shows the tightest selected limit.",
            parameters: {
              codexPeriod: {
                title: "Codex limits",
                type: "enum",
                default: "auto",
                values: [
                  { name: "Both", value: "auto" },
                  { name: "Session", value: "session" },
                  { name: "Weekly", value: "weekly" },
                ],
              },
              claudePeriod: {
                title: "Claude limits",
                type: "enum",
                default: "auto",
                values: [
                  { name: "Both", value: "auto" },
                  { name: "Session", value: "session" },
                  { name: "Weekly", value: "weekly" },
                ],
              },
            },
          },
          supportedFamilies: [
            "systemSmall",
            "systemMedium",
            "systemLarge",
            "systemExtraLarge",
            "accessoryRectangular",
          ],
        },
        android: {
          minWidth: 250,
          minHeight: 180,
          minResizeWidth: 180,
          minResizeHeight: 130,
          targetCellWidth: 4,
          targetCellHeight: 3,
          resizeMode: "both",
          // The embedded layout renders before first launch; publishing replaces it with stored props.
          initialLayout: "./src/widgets/SubscriptionUsage.android.tsx",
        },
      },
      {
        name: "AgentActivity",
        displayName: "Agent Activity",
        description: "Shows the current state of active T3 Code agents.",
        android: null,
        ios: {
          supportedFamilies: ["systemSmall", "systemMedium", "accessoryRectangular"],
          // Embeds the layout so the gallery preview and a new widget render
          // before the app has run once; the app replaces it on publish.
          initialLayout: "./src/widgets/AgentActivityWidget.tsx",
        },
      },
    ],
  },
];

const sharingPlugin: NonNullable<ExpoConfig["plugins"]>[number] = [
  "expo-sharing",
  {
    ios: {
      // Personal Teams cannot sign App Groups or extension targets; keep local builds usable.
      enabled: !isIosPersonalTeamBuild,
      extensionBundleIdentifier: `${iosBundleIdentifier}.sharing`,
      appGroupId: `group.${iosBundleIdentifier}`,
      activationRule: {
        supportsText: true,
        supportsWebUrlWithMaxCount: 1,
        supportsImageWithMaxCount: 8,
        supportsMovieWithMaxCount: 8,
        supportsFileWithMaxCount: 8,
      },
    },
    android: {
      enabled: !isJonesTestFlight,
      singleShareMimeTypes: ["*/*"],
      multipleShareMimeTypes: ["*/*"],
    },
  },
];

// Match iOS PostScript names on Android so RN and the native composer share fonts before runtime loading.

const config: ExpoConfig = {
  name: isJonesTestFlight ? "Jones Code TestFlight" : variant.appName,
  slug: isJonesTestFlight ? "jones-code-testflight" : "t3-code",
  platforms: isJonesTestFlight ? ["ios"] : ["ios", "android"],
  scheme: isJonesTestFlight ? "jonescode-testflight" : variant.scheme,
  version: repoEnv.T3CODE_MOBILE_VERSION ?? "2.4.0",
  // Skip costly fingerprinting on dev launches. OTA-enabled release variants match native projects.
  runtimeVersion: isJonesTestFlight ? undefined : { policy: runtimeVersionPolicy },
  orientation: "portrait",
  icon: variant.assets.appIcon,
  userInterfaceStyle: "automatic",
  updates: isJonesTestFlight
    ? { enabled: false }
    : {
        enabled: repoEnv.T3CODE_MOBILE_UPDATES_ENABLED !== "0",
        url: `https://u.expo.dev/${UPSTREAM_EAS_PROJECT_ID}`,
        checkAutomatically: "ON_LOAD",
        fallbackToCacheTimeout: 0,
      },
  ios: {
    icon: variant.assets.iosIcon,
    supportsTablet: true,
    requireFullScreen: process.env.T3_SHOWCASE_CAPTURE_BUILD === "1",
    bundleIdentifier: iosBundleIdentifier,
    // Avoid Personal Team fallback: it cannot sign App Groups, Apple sign-in or push entitlements.
    appleTeamId: jonesTestFlight?.appleTeamId ?? UPSTREAM_APPLE_TEAM_ID,
    associatedDomains: relyingParty
      ? [
          `applinks:${relyingParty}`,
          `webcredentials:${relyingParty}`,
          ...(isJonesTestFlight ? [] : [`applinks:${HOSTED_APP_HOST}`]),
        ]
      : undefined,
    entitlements: {
      "keychain-access-groups": [
        `$(AppIdentifierPrefix)${jonesTestFlight?.bundleIdentifier ?? variant.iosBundleIdentifier}`,
      ],
      ...(isJonesTestFlight ? { "aps-environment": "production" } : {}),
    },
    infoPlist: {
      UISupportedInterfaceOrientations: [
        "UIInterfaceOrientationPortrait",
        "UIInterfaceOrientationLandscapeLeft",
        "UIInterfaceOrientationLandscapeRight",
      ],
      NSAppTransportSecurity: {
        NSAllowsArbitraryLoads: true,
      },
      NSLocalNetworkUsageDescription: isJonesTestFlight
        ? "Allow Jones Code to connect to servers on your local network or tailnet."
        : "Allow T3 Code to connect to T3 Code servers on your local network or tailnet.",
      // Built with the iOS 27 SDK, only view controllers can hide or style the status bar.
      UIViewControllerBasedStatusBarAppearance: true,
      NSPhotoLibraryAddUsageDescription: "Allow T3 Code to save images to your photo library.",
      // System picture in picture needs background audio to start and survive outside the app.
      UIBackgroundModes: ["audio"],
      ITSAppUsesNonExemptEncryption: false,
      // Capture builds disable multitasking because iPadOS otherwise ignores orientation requests.
      ...(process.env.T3_SHOWCASE_CAPTURE_BUILD === "1"
        ? {
            "UISupportedInterfaceOrientations~ipad": [
              "UIInterfaceOrientationPortrait",
              "UIInterfaceOrientationPortraitUpsideDown",
              "UIInterfaceOrientationLandscapeLeft",
              "UIInterfaceOrientationLandscapeRight",
            ],
          }
        : {}),
    },
  },
  android: isJonesTestFlight
    ? undefined
    : {
        icon: variant.assets.appIcon,
        package: variant.androidPackage,
        ...(repoEnv.T3CODE_ANDROID_GOOGLE_SERVICES_FILE
          ? { googleServicesFile: repoEnv.T3CODE_ANDROID_GOOGLE_SERVICES_FILE }
          : {}),
        adaptiveIcon: {
          backgroundColor: variant.assets.androidAdaptiveBackgroundColor,
          ...(variant.assets.androidAdaptiveBackgroundImage
            ? { backgroundImage: variant.assets.androidAdaptiveBackgroundImage }
            : {}),
          foregroundImage: variant.assets.androidAdaptiveForeground,
          monochromeImage: variant.assets.androidMonochromeIcon,
        },
        // JS back dispatch needs RN's Android 16 shim and withAndroidPredictiveBackCompat on Android 13-15.
        predictiveBackGestureEnabled: true,
        // expo-sensors declares this for its pedometer, which the app does not use.
        blockedPermissions: ["android.permission.ACTIVITY_RECOGNITION"],
        intentFilters: [
          {
            action: "VIEW",
            autoVerify: true,
            data: [{ scheme: "https", host: HOSTED_APP_HOST, pathPrefix: "/threads/" }],
            category: ["BROWSABLE", "DEFAULT"],
          },
        ],
      },
  web: {
    favicon: variant.assets.appIcon,
  },
  plugins: [
    "expo-asset",
    [
      "expo-font",
      {
        ios: {
          fonts: [dmSansFonts.regular, dmSansFonts.medium, dmSansFonts.bold],
        },
        android: {
          fonts: [
            {
              fontFamily: "DMSans-Regular",
              fontDefinitions: [{ path: dmSansFonts.regular, weight: 400 }],
            },
            {
              fontFamily: "DMSans-Medium",
              fontDefinitions: [{ path: dmSansFonts.medium, weight: 500 }],
            },
            {
              fontFamily: "DMSans-Bold",
              // Android picks fallback glyphs (such as Vietnamese) at the requested weight, so bold text
              // asks for 700; the 400 entry lets a relative bold step (Typeface.BOLD) land on 700 too.
              fontDefinitions: [
                { path: dmSansFonts.bold, weight: 400 },
                { path: dmSansFonts.bold, weight: 700 },
              ],
            },
          ],
        },
      },
    ],
    "expo-secure-store",
    "expo-sqlite",
    ...(isIosPersonalTeamBuild
      ? [sharingPlugin]
      : ["./plugins/withShareExtensionDisplayName.cjs", sharingPlugin]),
    [
      "expo-notifications",
      {
        icon: variant.assets.androidNotificationIcon,
        color: variant.assets.androidNotificationColor,
        mode: APP_VARIANT === "development" ? "development" : "production",
      },
    ],
    // Gate Clerk here: withoutIosPersonalTeamCapabilities runs too early to strip its entitlement.
    ["@clerk/expo", { theme: "./clerk-theme.json", appleSignIn: !isIosPersonalTeamBuild }],
    "expo-web-browser",
    [
      "expo-quick-actions",
      {
        androidIcons: {
          shortcut_icon: {
            foregroundImage: variant.assets.androidAdaptiveForeground,
            backgroundColor: variant.assets.androidAdaptiveBackgroundColor,
            ...(variant.assets.androidAdaptiveBackgroundImage
              ? { backgroundImage: variant.assets.androidAdaptiveBackgroundImage }
              : {}),
          },
        },
      },
    ],
    [
      "expo-audio",
      {
        microphonePermission: "Allow T3 Code to use your microphone for voice input.",
        recordAudioAndroid: true,
        enableBackgroundPlayback: false,
        enableBackgroundRecording: false,
      },
    ],
    [
      "expo-camera",
      {
        cameraPermission: "Allow T3 Code to access your camera so you can scan pairing QR codes.",
        microphonePermission: false,
        barcodeScannerEnabled: true,
        recordAudioAndroid: false,
      },
    ],
    [
      "expo-image-picker",
      {
        photosPermission: false,
        // `false` makes this plugin add RECORD_AUDIO to blockedPermissions, breaking Android dictation.
        microphonePermission: "Allow T3 Code to use your microphone for voice input.",
      },
    ],
    [
      "expo-splash-screen",
      {
        image: variant.assets.splashIcon,
        resizeMode: "contain",
        backgroundColor: "#ffffff",
        imageWidth: 220,
        dark: {
          image: variant.assets.splashIcon,
          backgroundColor: "#0a0a0a",
        },
        android: {
          // Android 12+ masks the central two thirds of the 288dp canvas; adaptive layers avoid clipped corners.
          image: variant.assets.androidSplashIcon,
          imageWidth: 288,
          dark: { image: variant.assets.androidSplashIcon },
        },
      },
    ],
    [
      "expo-build-properties",
      {
        android: {
          minSdkVersion: 24,
          // SDK 58 lacks Kotlin 2.3's runtime-inert annotation. R8 needs explicit InputMerger constructors for widgets.
          extraProguardRules: [
            "-dontwarn kotlin.MustUseReturnValues",
            "-keep class * extends androidx.work.InputMerger { <init>(); }",
          ].join("\n"),
        },
        ios: {
          deploymentTarget: "18.0",
          // AppCheckCore 11.3+ includes Swift and needs module maps for these Objective-C dependencies.
          extraPods: [
            { name: "GoogleUtilities", modular_headers: true },
            { name: "RecaptchaInterop", modular_headers: true },
          ],
        },
      },
    ],
    "./plugins/withIosCocoaPodsUuidCache.cjs",
    // Only the accelerometer is used (device viewer shake). Compile out the
    // pedometer so iOS needs no motion purpose string.
    ["expo-sensors", { motionPermission: false }],
    // Mods run in reverse order; expo-widgets must create and wipe the target before assets are added.
    ...(!isIosPersonalTeamBuild ? ["./plugins/withWidgetLogoAsset.cjs", widgetsPlugin] : []),
    "./plugins/withAndroidNetworkSecurityConfig.cjs",
    "./plugins/withAndroidGradleHeap.cjs",
    "./plugins/withAndroidInputBackground.cjs",
    "./plugins/withAndroidModernPopupMenu.cjs",
    "./plugins/withAndroidModernAlertDialog.cjs",
    "./plugins/withAndroidPredictiveBackCompat.cjs",
    // MainActivity mods run in reverse order; extend the tablet-orientation override.
    "./plugins/withAndroidWindowDisplayMetrics.cjs",
    "./plugins/withAndroidTabletOrientation.cjs",
    ...(isIosPersonalTeamBuild ? ["./plugins/withoutIosPersonalTeamCapabilities.cjs"] : []),
  ],
  extra: {
    appVariant: APP_VARIANT,
    iosPersonalTeamBuild: isIosPersonalTeamBuild,
    relay: {
      url: repoEnv.T3CODE_RELAY_URL ?? null,
    },
    clerk: {
      publishableKey: repoEnv.EXPO_PUBLIC_CLERK_PUBLISHABLE_KEY ?? null,
      jwtTemplate: repoEnv.EXPO_PUBLIC_CLERK_JWT_TEMPLATE ?? null,
    },
    // Clerk reads these exact names at prebuild. Omit unset values: manifest null becomes truthy {}.
    EXPO_PUBLIC_CLERK_GOOGLE_WEB_CLIENT_ID: repoEnv.EXPO_PUBLIC_CLERK_GOOGLE_WEB_CLIENT_ID,
    EXPO_PUBLIC_CLERK_GOOGLE_IOS_CLIENT_ID: repoEnv.EXPO_PUBLIC_CLERK_GOOGLE_IOS_CLIENT_ID,
    EXPO_PUBLIC_CLERK_GOOGLE_ANDROID_CLIENT_ID: repoEnv.EXPO_PUBLIC_CLERK_GOOGLE_ANDROID_CLIENT_ID,
    EXPO_PUBLIC_CLERK_GOOGLE_IOS_URL_SCHEME: repoEnv.EXPO_PUBLIC_CLERK_GOOGLE_IOS_URL_SCHEME,
    observability: {
      tracesUrl: repoEnv.EXPO_PUBLIC_OTLP_TRACES_URL ?? "https://api.axiom.co/v1/traces",
      tracesDataset: repoEnv.EXPO_PUBLIC_OTLP_TRACES_DATASET ?? null,
      tracesToken: repoEnv.EXPO_PUBLIC_OTLP_TRACES_TOKEN ?? null,
    },
    eas: {
      projectId: jonesTestFlight?.projectId ?? UPSTREAM_EAS_PROJECT_ID,
      observe: { endpointUrl: observeEndpointUrl },
    },
  },
  owner: jonesTestFlight?.owner ?? UPSTREAM_EXPO_OWNER,
  experiments: {
    // Metro only runs babel-plugin-react-compiler when this is set; see src/reactCompilerCoverage.test.ts.
    reactCompiler: true,
  },
};

export default config;
