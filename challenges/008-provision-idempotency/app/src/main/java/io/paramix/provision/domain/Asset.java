package io.paramix.provision.domain;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;

/** Bir varlık ve tutar kuralları. Tutar asla yuvarlanmaz: en küçük birimden küçük basamak varsa REDDEDİLİR. */
public record Asset(String code, Kind kind, int scale) {

    public enum Kind { FIAT, CRYPTO }

    /** NUMERIC(38,18): tam sayı kısmı en fazla 20 basamak. */
    private static final int MAX_INTEGER_DIGITS = 20;

    public BigDecimal requireValidAmount(BigDecimal amount) {
        if (amount == null || amount.signum() <= 0) {
            throw new DomainException(ErrorCode.INVALID_AMOUNT, "Tutar sıfırdan büyük olmalı");
        }
        BigDecimal normalized = amount.stripTrailingZeros();
        if (normalized.scale() > scale) {
            // 0.123 TRY'yi 0.12'ye yuvarlamak, müşterinin istemediği bir işlemi yapmak demek.
            throw new DomainException(ErrorCode.INVALID_AMOUNT,
                    code + " en fazla " + scale + " ondalık basamak alır",
                    Map.of("asset", code, "maxScale", scale));
        }
        if (normalized.precision() - normalized.scale() > MAX_INTEGER_DIGITS) {
            throw new DomainException(ErrorCode.INVALID_AMOUNT, "Tutar çok büyük");
        }
        return normalized.setScale(scale, RoundingMode.UNNECESSARY);
    }

    /** Veritabanından gelen NUMERIC(38,18) değeri, varlığın kendi ölçeğinde gösterir (0.500000000000000000 → 0.50). */
    public String format(BigDecimal value) {
        return value == null ? null : value.setScale(scale, RoundingMode.UNNECESSARY).toPlainString();
    }
}
