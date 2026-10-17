/** Held receiver records only; no definition here performs an app, gate, process or owner transition. */
export const jonesColdReceiverSource = String.raw`
def _cold_receiver_transfer_value(owner, transfer_id, phase):
    require(hex_value(transfer_id, 64) and phase in ('transfer-intent', 'transferred-held'))
    receiver, handoff = owner.journal['desktopReceiver'], owner.journal['desktopHandoff']
    require(receiver['phase'] == 'prepared')
    _validate_cold_handoff_cursor(owner)
    candidate, state = handoff['desktop']['candidate'], handoff['state']
    binding, cold = owner.plan['candidateBinding'], owner.journal['coldOwner']
    require(transfer_id not in (cold['sessionId'], owner.plan['maintenanceGeneration'],
                               handoff['desktop']['transactionId'], state['transaction']['transactionId']))
    require(cold['sessionId'] == handoff['sessionId'] and cold['bootSessionUuid'] == handoff['bootSessionUuid'])
    # These are immutable source bindings, never evidence that a child exited or a launch was admitted.
    for key in ('artifactReceiptSha256', 'macAppReceiptSha256', 'bundleSha256', 'executorSha256'):
        require(hex_value(binding[key], 64))
    require(hex_value(candidate['sourceSha'], 40) and hex_value(candidate['sourceTree'], 40))
    require(hex_value(candidate['appDigest'], 64) and hex_value(candidate['handle'], 64))
    return {'schema': 1, 'phase': phase, 'transferId': transfer_id,
            'sessionId': cold['sessionId'], 'bootSessionUuid': cold['bootSessionUuid'],
            'maintenanceGeneration': owner.plan['maintenanceGeneration'],
            'planSha256': owner.journal['planSha256'], 'handoffSha256': sha(encoded(handoff)),
            'receiverIntentSha256': sha(receiver['intent'].encode()),
            'receiverJournalSha256': sha(receiver['journal'].encode()),
            'maintenanceOwnerSha256': handoff['maintenanceOwnerSha256'],
            'trialOwnerSha256': receiver['trialOwnerSha256'], 'finalOwnerSha256': receiver['finalOwnerSha256'],
            'stateTransaction': dict(state['transaction']), 'pairSha256': state['pairSha256'],
            'preparationEvidenceSha256': state['evidenceSha256'], 'custodySha256': state['custodySha256'],
            'transactionId': handoff['desktop']['transactionId'], 'stagedHandle': candidate['handle'],
            'sourceSha': candidate['sourceSha'], 'sourceTree': candidate['sourceTree'], 'appDigest': candidate['appDigest'],
            'artifactReceiptSha256': binding['artifactReceiptSha256'], 'macAppReceiptSha256': binding['macAppReceiptSha256'],
            'bundleSha256': binding['bundleSha256'], 'executorSha256': binding['executorSha256']}

def _validate_cold_receiver_transfer(owner):
    receiver = owner.journal['desktopReceiver']
    if 'transfer' not in receiver: return
    value = receiver['transfer']
    require(isinstance(value, dict) and type(value.get('schema')) is int and value['schema'] == 1)
    require(value == _cold_receiver_transfer_value(owner, value.get('transferId'), value.get('phase')))

def _cold_receiver_advance_value(owner):
    _validate_cold_receiver_transfer(owner)
    value = owner.journal['desktopReceiver']['transfer']
    require(value['phase'] in ('transfer-intent', 'transferred-held'))
    return dict(value, phase='transferred-held')

def _cold_receiver_reconcile_report(owner):
    _validate_cold_receiver_transfer(owner)
    value = owner.journal['desktopReceiver']['transfer']
    return maintenance_report('receiver-' + value['phase'], transactionId=value['transactionId'],
                              transferId=value['transferId'], handoffSha256=value['handoffSha256'],
                              receiverEffectsAllowed=False, candidateCustody='unqualified', backendCustody='unqualified')
`;
