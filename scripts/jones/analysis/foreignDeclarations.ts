// Source identities reviewed for the finite foreign contracts selected in L8 Analysis270.
// These are change guards, not native API/hardware proof. Never regenerate during analysis.
export const sourcePins = {
  gjs: {
    "apps/desktop/gnome-extension/metadata.json":
      "34721bf5980dbce86ce8b3eafb77682403681d26dccd186ed60d11937920c796",
    "apps/desktop/gnome-extension/bundle.json":
      "7b4655ad26feed2f8295fbed19e68f1282dceb61359ca3e3320d35b99c795aef",
    "apps/desktop/gnome-extension/extension.js":
      "ac1847c576f738a07145cbc0d45d42a01714cd74f756f4df5bc15e584e395f37",
    "apps/desktop/gnome-extension/captureService.js":
      "8f3fd1e6f378a4d0da96b60fa734507bccc8364827279172e1af2eec72af8f8d",
    "apps/desktop/gnome-extension/captureFeedback.js":
      "c147a53955136721046a4581a8f1821f0ea5f40876c30bc58ae72f0f6e407d84",
    "apps/desktop/gnome-extension/feedbackGeometry.js":
      "32e9415cbd549216d051380af012423b696f1e297ae153e82d0187143d74e2a4",
    "apps/desktop/src/snapShot/gnomeCaptureBundle.ts":
      "dd4957590444b6d96dd436abca1236316a4524cfd30d300cfe180b39a1a43531",
    "apps/desktop/src/snapShot/GnomeCaptureSetup.ts":
      "6326cc5a064f263e1d11c30404bb510779307bc15421ea6b8795309a12f3fb1e",
    "apps/desktop/src/snapShot/DesktopSnapShot.ts":
      "2817c375b8c32820a9a7c3ea82e1d3913a9266aa21cfea842e135f59382dab50",
    "scripts/build-desktop-artifact.ts":
      "4ee0e9d63a28338d31f96b84f54de98e12cac195008edca938f15093470e9873",
  },
  pgrep: {
    "apps/desktop/scripts/electron-launcher.test.mjs":
      "bb700a999ff5b196c822cb164dba5980fb75c6178e9cafc6f794f32a2c50f61f",
  },
  hyprctl: {
    "apps/desktop/src/app/DesktopPreReadyPlatform.ts":
      "6e71e9b21ef42b679f725783b24aad1275eae4255b946019af98ce8a9b1a4300",
    "apps/desktop/src/app/DesktopEarlyElectronStartup.ts":
      "b89a17714fd4dd24061f07dd395e8900a27366e0a6111a4c8c20700076343b9a",
  },
  compiler: {
    "tsconfig.base.json": "68c74b9c2b2295b28221b027a457f3837da638b59d89ee8a88a745cd1c981fe3",
    "apps/web/tsconfig.json": "87733dd7a4202fad909854cabde0a2970d2377578a10b19875d61641474cf4c0",
    "apps/web/vite.config.ts": "4505e22976f69bfec3f580562ffa5f1ef12491540cb7b168642e2ac9a99fb673",
    "scripts/jones/tsrs/typecheck.ts":
      "b9035485249e0e8c48a28554ea9f250de1efadf6e9b74a9c1e99a667d91bfd77",
    "scripts/jones/tsrs/local-mac-capacity.ts":
      "0380c7c1472686bdf0937129c90a3c2542b5a4d35922afadbfa9fb250cbd97cd",
    "scripts/jones/tsrs/capacity-lock.ts":
      "6ecee4732a1ba19bf26447941e1e134625d4aef87abb580815507de51ebff692",
    "scripts/jones/worktree-setup/local-mac-admission.ts":
      "b7dda78d068d2c460e21eb1eff45d4481e0470fd12ad12301e0b3132fc4e3596",
    "scripts/jones/tsrs/identity.ts":
      "88331f048d91498f9710332b1f1e9a839cdcdf86fdbbb4ec930346a67c3f94b9",
    "scripts/jones/tsrs/task-cache.ts":
      "f0924c7f2a11bd5da89cf645231e10d36d597618e8916bf6054723b9eb446625",
  },
} as const;

export const nativeImports = {
  "captureFeedback.js": [
    "Clutter=gi://Clutter",
    "Gio=gi://Gio",
    "GLib=gi://GLib",
    "Meta=gi://Meta",
    "St=gi://St",
    "*Main=resource:///org/gnome/shell/ui/main.js",
  ],
  "extension.js": [
    "Gio=gi://Gio",
    "GLib=gi://GLib",
    "Meta=gi://Meta",
    "Shell=gi://Shell",
    "St=gi://St",
    "{Extension}=resource:///org/gnome/shell/extensions/extension.js",
    "*Main=resource:///org/gnome/shell/ui/main.js",
  ],
} as const;
