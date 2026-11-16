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
      "74761927bb4a70ff197b712a0fb2b456209e0ba6a4267efcff5971632a024da3",
  },
  pgrep: {
    "apps/desktop/scripts/electron-launcher.test.mjs":
      "d2a1f1e46102a7bbf3dca523f4a3b8845751814d51d5d4f8cfe54ae3d1436706",
  },
  hyprctl: {
    "apps/desktop/src/app/DesktopPreReadyPlatform.ts":
      "9614a1eefedc79563d71bbd2423b1a35da7c17a646450ee24ed69ed8b6780141",
    "apps/desktop/src/app/DesktopEarlyElectronStartup.ts":
      "b89a17714fd4dd24061f07dd395e8900a27366e0a6111a4c8c20700076343b9a",
  },
  compiler: {
    "tsconfig.base.json": "68c74b9c2b2295b28221b027a457f3837da638b59d89ee8a88a745cd1c981fe3",
    "apps/web/tsconfig.json": "f2e28fdde25617023b6a1f08da01e53a4671db41a56acc2bfd8310275b53acd6",
    "apps/web/vite.config.ts": "1cf367544816d5f36a39a467817c5da8365bdaae5ec1cb36da1c44ac31daed45",
    "scripts/jones/tsrs/typecheck.ts":
      "bd567db55d72ffd8fd1c30f50f4453ba27ae744b76927a98ed9d0f0196fc5aad",
    "scripts/jones/tsrs/local-mac-capacity.ts":
      "10ef5a526fb9ab7c2e31d75bd171edb4aff12adaf110f321f468410ce450eccc",
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
