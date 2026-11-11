package io.paramix.provision.api;

/**
 * Çağıranın kimliği. Bu demoda {@code X-Customer-Id} header'ından gelir; üretimde API gateway'in doğruladığı
 * JWT'den (sub) çıkarılır ve istemcinin bu header'ı göndermesine izin verilmez.
 */
public final class CustomerContext {

    public static final String HEADER = "X-Customer-Id";
    public static final String IDEMPOTENCY_HEADER = "Idempotency-Key";

    private CustomerContext() {
    }
}
