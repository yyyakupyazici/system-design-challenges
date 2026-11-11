package io.paramix.chat.gateway;

import java.util.UUID;

import io.paramix.chat.config.ChatProperties;

import org.springframework.stereotype.Component;

/**
 * Bu gateway'in kimliği: ad + her açılışta yeni bir son ek (gw-7#a81f). Kubernetes pod'u aynı adla yeniden doğsa da
 * eski pod'un yönlendirme kayıtları yeni pod'unkilerle karışmaz (Redis Lab'daki kilit token'ı / fencing mantığı).
 */
@Component
public class GatewayIdentity {

    private final String instanceId;

    public GatewayIdentity(ChatProperties props) {
        this.instanceId = props.gateway().name() + "#" + UUID.randomUUID().toString().substring(0, 6);
    }

    public String instanceId() {
        return instanceId;
    }

    public static String channelOf(String instanceId) {
        return "gw:" + instanceId;
    }

    /** Park kutusu: Pub/Sub'da bu gateway'e ulaşamayan teslimatlar (bkz. DeliveryPublisher). */
    public static String parkedKeyOf(String instanceId) {
        return "gw:parked:" + instanceId;
    }
}
