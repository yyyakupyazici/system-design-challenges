package io.paramix.chat.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

@Configuration
public class RedisConfig {

    @Bean
    RedisMessageListenerContainer redisMessageListenerContainer(RedisConnectionFactory cf) {
        RedisMessageListenerContainer c = new RedisMessageListenerContainer();
        c.setConnectionFactory(cf);
        // Varsayılan: her Pub/Sub mesajı AYRI bir thread'de işlenir → aynı cihaza giden iki mesaj yarışır, sıra bozulur
        // (testte yakalandı). Mesajlar geldiği sırayla alınır; sıralı paralellik GatewayInbox'taki cihaz şeritlerinde.
        c.setTaskExecutor(new org.springframework.core.task.SyncTaskExecutor());
        // Bağlantı koparsa aboneliği 1 sn aralıklarla yeniden kurmayı dene (varsayılan 5 sn)
        c.setRecoveryInterval(1000);
        return c;
    }
}
