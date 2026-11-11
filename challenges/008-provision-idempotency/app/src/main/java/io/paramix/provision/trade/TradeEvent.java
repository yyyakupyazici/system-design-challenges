package io.paramix.provision.trade;

import java.math.BigDecimal;

/**
 * Emir motorunun yayınladığı olay ({@code trade.events}, key = orderId). Emir motoru provizyonu
 * {@code operationType=TRADE, operationRef=orderId} ile açar; olay o provizyonu kapatır.
 *
 * @param filledAmount ORDER_FILLED'da gerçekleşen tutar (provizyondan az olabilir: kalan serbest kalır).
 */
public record TradeEvent(Type type, long customerId, String orderId, BigDecimal filledAmount) {

    public enum Type { ORDER_FILLED, ORDER_CANCELLED }
}
