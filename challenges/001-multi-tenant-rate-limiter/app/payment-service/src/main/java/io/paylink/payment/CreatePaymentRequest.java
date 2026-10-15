package io.paylink.payment;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;

public record CreatePaymentRequest(
        @NotNull @DecimalMin("0.01") BigDecimal amount,
        @NotNull @Pattern(regexp = "[A-Z]{3}") String currency,
        @Size(max = 255) String description) {
}
