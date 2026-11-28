# Jones Code on TestFlight

The `jones-testflight` EAS profile builds a standalone iOS app named **Jones Code
TestFlight** with the `jonescode-testflight://` scheme. It uses a Jones-owned Expo
project and Apple Developer Program team. OTA updates are disabled. Upload the
reviewed `.ipa` separately to the Jones Code app record in App Store Connect.

## Prepare the accounts and signing

1. Create an EAS project with the slug `jones-code-testflight` in the owner’s Expo
   account or organization. Record the account name and project UUID. To initialize
   it from the CLI, run `eas init --account <expo-owner>` in a disposable Expo app
   with that slug and copy its resulting project UUID into the environment below.
   [Expo project setup](https://docs.expo.dev/tutorial/eas/configure-development-build/)
2. In the owner’s paid Apple Developer Program team, register explicit App IDs
   for the chosen main bundle ID, `<bundle-id>.widgets`, and
   `<bundle-id>.sharing`. Register `group.<bundle-id>` and assign it to all three
   App IDs. Enable Push Notifications and Sign in with Apple for the main app.
   Enable Associated Domains on the main app if configuring a Clerk domain.
   [Apple capabilities](https://developer.apple.com/help/account/identifiers/enable-app-capabilities/),
   [Apple App Groups](https://developer.apple.com/help/account/identifiers/register-an-app-group/),
   [Expo app extensions](https://docs.expo.dev/build-reference/app-extensions/)
3. In App Store Connect, create the iOS app record using the same main bundle ID.
   Prepare App Store distribution signing for the main app and both extensions
   on the same Apple team. Each target needs a provisioning profile matching its
   App ID and entitlements and a valid distribution certificate. Apple requires
   new profiles after capability changes.
   [App Store Connect app records](https://developer.apple.com/help/app-store-connect/create-an-app-record/add-a-new-app/),
   [Apple distribution profiles](https://developer.apple.com/help/account/provisioning-profiles/create-an-app-store-provisioning-profile/)

## Resolve the profile and build

From a clean build checkout, set these four public identifiers in the shell
running EAS CLI. Also set the same values as project-scoped, plain-text EAS
variables in the Jones project’s `production` environment for the cloud worker.
The CLI needs them up front to locate the project. Signing keys and certificates
belong in the signing workflow. [Expo build environments](https://docs.expo.dev/eas/environment-variables/usage/)

| Variable                          | Owner-supplied value                                           |
| --------------------------------- | -------------------------------------------------------------- |
| `JONES_TESTFLIGHT_EXPO_OWNER`     | Name of the Expo account or organization that owns the project |
| `JONES_TESTFLIGHT_EAS_PROJECT_ID` | UUID of the `jones-code-testflight` project                    |
| `JONES_TESTFLIGHT_IOS_BUNDLE_ID`  | Explicit bundle ID of the Jones Code app                       |
| `JONES_TESTFLIGHT_APPLE_TEAM_ID`  | Ten-character Apple Developer Program team ID                  |

Use a shell with `APP_VARIANT=jones-testflight` for EAS commands such as
`project:info` that lack a build profile argument. Verify the owner, project
UUID, bundle ID, Apple team, scheme, and disabled updates before configuring
signing or starting the build. Use a checkout with no generated `apps/mobile/ios`
directory so Expo applies the profile’s config to the generated native targets.
[EAS CLI reference](https://docs.expo.dev/eas/cli/),
[Expo Continuous Native Generation](https://docs.expo.dev/workflow/continuous-native-generation/)

```sh
cd apps/mobile
export APP_VARIANT=jones-testflight
eas whoami
eas project:info
eas config --platform ios --profile jones-testflight
```

When the owner is ready to prepare signing and build, select the verified Apple
team and check all three target profiles in EAS. Use the exact build ID to open
the result and download the `.ipa` from its build page. The non-interactive
build uses the prepared remote credentials. [Expo iOS signing](https://docs.expo.dev/build/setup/),
[EAS CLI reference](https://docs.expo.dev/eas/cli/)

```sh
eas credentials:configure-build --platform ios --profile jones-testflight
eas build --platform ios --profile jones-testflight --non-interactive --freeze-credentials
eas build:view <build-id>
```

Before a separately approved upload, verify the signed archive’s team, main
and extension bundle IDs, App Group, push environment, entitlements, and build
number. Upload that `.ipa` with Apple Transporter using the confirmed App Store
Connect account. After processing, add the build to an internal testing group
and invite the owner’s App Store Connect user. On the iPhone, accept the
invitation in Apple’s TestFlight app and install **Jones Code TestFlight**.
[Apple upload instructions](https://developer.apple.com/help/app-store-connect/manage-builds/upload-builds/),
[Apple internal testing](https://developer.apple.com/help/app-store-connect/test-a-beta-version/add-internal-testers/)

## Pair and qualify on the phone

For the first pass, pair directly over LAN or Tailscale. With a reachable Jones
Code server, open **Settings → Environments → Add environment** in the app,
then scan the server’s pairing QR code or enter its address and pairing code.
Allow local network access if prompted. Verify connection to the intended
environment, thread loading, and reconnect after relaunch. TestFlight has a
separate app sandbox, so pair again in this app. [Remote access](../user/remote-access.md)

Open **Orchestrator** and choose a conversation with Jones Code thread and
Workstream tools as the routing thread. Dictate or paste into the composer,
send one request, and open the routing thread to check its response. Use
**Input** for approvals and questions and **Done** for threads that finished
in the last hour. Swipe right for the next thread and left for the previous
one. The compose button returns to the Orchestrator draft.

Relay is optional for direct pairing. Keep the Clerk and Relay variables unset
in the local and EAS environments for a direct-only build. A Relay build needs
matching public Clerk and HTTPS Relay settings from the intended deployment. Set
`JONES_TESTFLIGHT_CLERK_DOMAIN` to the Clerk hostname when using iOS associated
domains. Verify the Clerk native app registration and domain association list
the Jones Apple team and bundle ID, and check sign-in and callbacks with the
Jones scheme. For push and Live Activities, verify that the Relay’s APNs key
can send to the Jones bundle’s production topics. Test the share extension and
widget data separately. Agent Activity widget and Live Activity thread links
currently emit `t3code://` and may open the upstream app. Copied links on the
upstream hosted domain also require an Apple association for the fork to open
them. [Connect setup](./02_connect-setup.md),
[Apple Sign in with Apple](https://developer.apple.com/help/account/capabilities/about-sign-in-with-apple/)
