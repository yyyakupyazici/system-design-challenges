package io.paramix.notify.worker;

import static org.assertj.core.api.Assertions.assertThat;

import io.paramix.notify.domain.Priority;

import org.junit.jupiter.api.Test;

class DeliveryPolicyTest {

    @Test
    void unknownOutcome_otpIsResent_campaignIsAbandoned() {
        assertThat(DeliveryPolicy.onUnknown(Priority.CRITICAL)).isEqualTo(DeliveryPolicy.UnknownAction.RESEND);
        assertThat(DeliveryPolicy.onUnknown(Priority.TRANSACTIONAL)).isEqualTo(DeliveryPolicy.UnknownAction.RESEND);
        assertThat(DeliveryPolicy.onUnknown(Priority.MARKETING)).isEqualTo(DeliveryPolicy.UnknownAction.ABANDON);
    }

    @Test
    void marketingNeverUsesExpensiveFallbackProvider() {
        assertThat(DeliveryPolicy.mayUseFallbackProvider(Priority.CRITICAL)).isTrue();
        assertThat(DeliveryPolicy.mayUseFallbackProvider(Priority.MARKETING)).isFalse();
    }

    @Test
    void otpWaitsLeastForQuota() {
        assertThat(DeliveryPolicy.maxQuotaWait(Priority.CRITICAL))
                .isLessThan(DeliveryPolicy.maxQuotaWait(Priority.TRANSACTIONAL))
                .isLessThan(DeliveryPolicy.maxQuotaWait(Priority.MARKETING));
    }
}
