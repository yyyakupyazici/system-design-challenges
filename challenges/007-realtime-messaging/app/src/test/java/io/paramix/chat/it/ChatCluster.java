package io.paramix.chat.it;

import java.util.HashMap;
import java.util.Map;

import io.paramix.chat.ChatApplication;

import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

/**
 * Gerçek bir mini cluster: iki ayrı Spring uygulaması = iki gateway makinesi, aynı PostgreSQL, Redis ve Kafka'ya bağlı.
 *
 * <pre>
 *  node A (gw-a): gateway + relay + fan-out + ödeme işleri   ("her şey" pod'u)
 *  node B (gw-b): gateway + relay                             (relay'ler advisory lock ile tek lider seçer)
 * </pre>
 *
 * Bütün IT sınıfları aynı cluster'ı paylaşır (JVM başına bir kez başlar). Testler birbirini bozmasın diye her test
 * kendi rastgele kullanıcı ve sohbet id'lerini kullanır.
 */
public final class ChatCluster {

    private static ChatCluster instance;

    public final EmbeddedKafkaKraftBroker kafka;
    public final ConfigurableApplicationContext nodeA;
    public final ConfigurableApplicationContext nodeB;

    private ChatCluster() {
        kafka = new EmbeddedKafkaKraftBroker(1, 4, "chat.events", "wallet.events");
        kafka.afterPropertiesSet();
        nodeA = start("gw-a", true);
        nodeB = start("gw-b", false);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            nodeB.close();
            nodeA.close();
            kafka.destroy();
        }));
    }

    public static synchronized ChatCluster get() {
        if (instance == null) {
            instance = new ChatCluster();
        }
        return instance;
    }

    public ConfigurableApplicationContext start(String gatewayName, boolean workerRoles) {
        // Komut satırı argümanı olarak: application.yml'daki varsayılanları ezsin (properties() en düşük öncelikli)
        String[] args = props(gatewayName, workerRoles).entrySet().stream()
                .map(e -> "--" + e.getKey() + "=" + e.getValue()).toArray(String[]::new);
        return new SpringApplicationBuilder(ChatApplication.class).run(args);
    }

    private Map<String, Object> props(String gatewayName, boolean workerRoles) {
        Map<String, Object> p = new HashMap<>();
        p.put("server.port", "0");
        p.put("spring.datasource.url", TestInfra.pgUrl());
        p.put("spring.datasource.username", TestInfra.pgUser());
        p.put("spring.datasource.password", TestInfra.pgPassword());
        p.put("spring.data.redis.host", TestInfra.redisHost());
        p.put("spring.data.redis.port", String.valueOf(TestInfra.redisPort()));
        p.put("spring.kafka.bootstrap-servers", kafka.getBrokersAsString());
        p.put("chat.kafka.partitions", "4");
        p.put("chat.gateway.name", gatewayName);
        p.put("chat.gateway.heartbeat", "1s");
        p.put("chat.gateway.lease", "3s");
        p.put("chat.gateway.drain-spread", "2s");
        p.put("chat.gateway.probe-timeout", "2s");
        p.put("chat.gateway.resync-spread", "1s");
        p.put("chat.members-cache-ttl", "1s");
        p.put("chat.payment.reconcile-after", "1s");
        p.put("chat.payment.reconcile-every", "300ms");
        p.put("chat.roles.fanout", String.valueOf(workerRoles));
        p.put("chat.roles.payment", String.valueOf(workerRoles));
        p.put("spring.lifecycle.timeout-per-shutdown-phase", "5s");
        return p;
    }

    public static int port(ConfigurableApplicationContext ctx) {
        return Integer.parseInt(ctx.getEnvironment().getProperty("local.server.port"));
    }
}
