package io.paramix.payreq.domain;

import io.paramix.payreq.wallet.WalletEventsConsumer.WalletEventHandler;

import org.springframework.stereotype.Component;

@Component
class WalletEventBridge implements WalletEventHandler {

    private final PaymentRequestService service;

    WalletEventBridge(PaymentRequestService service) {
        this.service = service;
    }

    @Override
    public boolean completed(long paymentRequestId, String transferId, long amountKurus) {
        return service.onTransferCompleted(paymentRequestId, transferId, amountKurus);
    }

    @Override
    public boolean reversed(String transferId, String reason) {
        return service.onTransferReversed(transferId, reason);
    }
}
