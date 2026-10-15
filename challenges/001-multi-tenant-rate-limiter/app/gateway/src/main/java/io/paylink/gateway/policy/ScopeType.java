package io.paylink.gateway.policy;

/** Kuralın kime uygulandığı. Öncelik: TENANT > PLAN. */
public enum ScopeType {
    PLAN,
    TENANT
}
