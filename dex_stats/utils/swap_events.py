maker_swap_success_events = [
    "Started",
    "Negotiated",
    "TakerFeeValidated",
    "MakerPaymentSent",
    "TakerPaymentReceived",
    "TakerPaymentWaitConfirmStarted",
    "TakerPaymentValidatedAndConfirmed",
    "TakerPaymentSpent",
    "Finished"
]

taker_swap_success_events = [
    "Started",
    "Negotiated",
    "TakerFeeSent",
    "MakerPaymentReceived",
    "MakerPaymentWaitConfirmStarted",
    "MakerPaymentValidatedAndConfirmed",
    "TakerPaymentSent",
    "TakerPaymentSpent",
    "MakerPaymentSpent",
    "Finished"
]

maker_swap_error_events = [
    "StartFailed",
    "NegotiateFailed",
    "TakerFeeValidateFailed",
    "MakerPaymentTransactionFailed",
    "MakerPaymentDataSendFailed",
    "TakerPaymentValidateFailed",
    "TakerPaymentSpendFailed",
    "MakerPaymentRefunded",
    "MakerPaymentRefundFailed"
]

taker_swap_error_events = [
    "StartFailed",
    "NegotiateFailed",
    "TakerFeeSendFailed",
    "MakerPaymentValidateFailed",
    "MakerPaymentWaitConfirmFailed",
    "TakerPaymentTransactionFailed",
    "TakerPaymentWaitConfirmFailed",
    "TakerPaymentDataSendFailed",
    "TakerPaymentWaitForSpendFailed",
    "MakerPaymentSpendFailed",
    "TakerPaymentWaitRefundStarted",
    "TakerPaymentRefunded",
    "TakerPaymentRefundFailed"
]
