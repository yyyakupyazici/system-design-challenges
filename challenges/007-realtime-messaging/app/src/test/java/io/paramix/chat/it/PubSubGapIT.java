package io.paramix.chat.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import io.paramix.chat.gateway.GatewayIdentity;
import io.paramix.chat.gateway.GatewayInbox;
import io.paramix.chat.gateway.PubSubHealth;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

/**
 * Yarışın kendisi, Redis'i kapatmadan: gateway'in Pub/Sub aboneliği bir anlığına yok (Redis'e yeniden bağlanırken
 * Lettuce aboneliği yeniden kurana kadar olan pencere). Bu arada yapılan PUBLISH'i kimse almaz: Redis "0 abone" der.
 *
 * <p>Düzeltmeden önce: mesaj canlı yoldan kaybolur, sadece RESYNC/HEADS ile (saniyeler–1 dk sonra) gelir.
 * Düzeltmeden sonra: yayıncı 0'ı görür, mesajı o gateway'in park kutusuna bırakır, gateway ≤1 sn içinde alıp teslim eder.
 */
class PubSubGapIT extends AbstractChatIT {

    @Test
    void messagePublishedWhileTheGatewayIsNotSubscribedIsStillDeliveredLive() {
        long ali = user("Ali");
        long ayse = user("Ayşe");
        long conv = direct(ali, ayse);
        TestClient aliC = connect(portA, ali, "phone");
        TestClient ayseC = connect(portB, ayse, "phone");

        RedisMessageListenerContainer container = bean(cluster.nodeB, RedisMessageListenerContainer.class);
        GatewayInbox inbox = bean(cluster.nodeB, GatewayInbox.class);
        String channel = GatewayIdentity.channelOf(bean(cluster.nodeB, GatewayIdentity.class).instanceId());
        StringRedisTemplate redis = bean(cluster.nodeA, StringRedisTemplate.class);

        container.removeMessageListener(inbox, new ChannelTopic(channel));
        try {
            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> subscribers(redis, channel) == 0);

            String id = aliC.sendText(conv, "abonelik yokken yazdım");
            aliC.sentAck(id);
            // Abonelik HÂLÂ yokken (sonda zaman aşımı + RESYNC devreye girmeden) mesaj canlı gelmeli
            ayseC.await("MSG", f -> f.at("/message/clientMsgId").asText().equals(id), Duration.ofSeconds(3));
            assertThat(ayseC.all("RESYNC")).as("tam RESYNC'e gerek kalmadı").isEmpty();
        } finally {
            container.addMessageListener(inbox, new ChannelTopic(channel));
            Awaitility.await().atMost(Duration.ofSeconds(10))
                    .until(() -> !bean(cluster.nodeB, PubSubHealth.class).isDegraded() && subscribers(redis, channel) == 1);
        }
    }

    private static long subscribers(StringRedisTemplate unused, String channel) {
        io.lettuce.core.RedisClient client = io.lettuce.core.RedisClient.create(
                io.lettuce.core.RedisURI.create(TestInfra.redisHost(), TestInfra.redisPort()));
        try (var c = client.connect()) {
            return c.sync().pubsubNumsub(channel).getOrDefault(channel, 0L);
        } finally {
            client.shutdown();
        }
    }
}
