/** The native transaction carries this policy outside Electron before shutdown. */
export const jonesSigningSource = String.raw`
PRODUCTION_BUNDLE_ID = 'com.jonessystems.jonescode'
PRODUCTION_TEAM_ID = '2CUPSQS986'

def signing_bundle_id(app):
    with open(pathlib.Path(app) / 'Contents/Info.plist', 'rb') as stream:
        return plistlib.load(stream).get('CFBundleIdentifier')

def verify_production_signature(app):
    if signing_bundle_id(app) != PRODUCTION_BUNDLE_ID:
        raise RuntimeError('Production code identity differs; activation held.')
    requirement = ('identifier "' + PRODUCTION_BUNDLE_ID + '" and anchor apple generic '
        'and certificate 1[field.1.2.840.113635.100.6.2.6] exists '
        'and certificate leaf[field.1.2.840.113635.100.6.1.13] exists '
        'and certificate leaf[subject.OU] = "' + PRODUCTION_TEAM_ID + '"')
    for command in (
        ['/usr/bin/codesign', '--verify', '--deep', '--strict', '-R', '=' + requirement, str(app)],
        ['/usr/bin/xcrun', 'stapler', 'validate', str(app)],
        ['/usr/sbin/spctl', '--assess', '--type', 'execute', str(app)],
    ):
        result = subprocess.run(command, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            raise RuntimeError('Production signature, notarization or sealed resources failed; activation held.')

def verify_signing_transition(expected, candidate):
    installed_id = signing_bundle_id(expected['appPath'])
    candidate_id = signing_bundle_id(candidate)
    if PRODUCTION_BUNDLE_ID not in (installed_id, candidate_id, expected.get('bundleIdentifier')):
        return
    if installed_id != PRODUCTION_BUNDLE_ID or candidate_id != PRODUCTION_BUNDLE_ID:
        raise RuntimeError('Signing identity transition requires exceptional owner bootstrap; ordinary update held.')
    verify_production_signature(expected['appPath'])
    verify_production_signature(candidate)

def verify_rollback_signature(expected, retained):
    retained_id = signing_bundle_id(retained)
    if PRODUCTION_BUNDLE_ID in (expected.get('bundleIdentifier'), retained_id):
        if expected.get('bundleIdentifier') != PRODUCTION_BUNDLE_ID:
            raise RuntimeError('Retained signed rollback identity is not bound to this transaction.')
        verify_production_signature(retained)
`;
