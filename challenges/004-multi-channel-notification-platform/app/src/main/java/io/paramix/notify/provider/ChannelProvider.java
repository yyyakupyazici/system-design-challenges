package io.paramix.notify.provider;

import io.paramix.notify.domain.Channel;

/**
 * Bir SMS / e-posta / push sağlayıcısının adaptörü. Gerçek entegrasyonlar (SMS operatörü, SES, FCM, APNs)
 * bu arayüzü uygular; bu projede {@link FakeProvider} kullanılıyor.
 */
public interface ChannelProvider {

    String name();

    Channel channel();

    SendResult send(OutboundMessage message);

    LookupResult lookup(String clientRef);
}
