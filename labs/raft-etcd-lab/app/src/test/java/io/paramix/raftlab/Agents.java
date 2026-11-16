package io.paramix.raftlab;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.paramix.raftlab.ha.HaAgent;

import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/** Üç ajanı (mini-Patroni) aynı JVM'de, üç ayrı Spring uygulaması olarak çalıştırır. */
public final class Agents implements AutoCloseable {

    public record Spec(String node, String region, int pgPort, String etcdEndpoints) {
    }

    private final Map<String, ConfigurableApplicationContext> running = new LinkedHashMap<>();
    private final List<HaAgent.Event> archived = new ArrayList<>();
    private final String syncMode;

    public Agents(String syncMode) {
        this.syncMode = syncMode;
    }

    public static Spec ist(String endpoints) {
        return new Spec("pg-ist", "istanbul", 5511, endpoints);
    }

    public static Spec ank(String endpoints) {
        return new Spec("pg-ank", "ankara", 5512, endpoints);
    }

    public static Spec ist2(String endpoints) {
        return new Spec("pg-ist2", "istanbul", 5513, endpoints);
    }

    public static Spec ank2(String endpoints) {
        return new Spec("pg-ank2", "ankara", 5514, endpoints);
    }

    public Agents start(Spec s) {
        List<String> args = new ArrayList<>(List.of(
                "--server.port=0",
                "--spring.application.name=agent-" + s.node(),
                "--lab.node=" + s.node(),
                "--lab.region=" + s.region(),
                "--lab.pg-port=" + s.pgPort(),
                "--lab.etcd-endpoints=" + s.etcdEndpoints(),
                "--lab.ttl=6s",
                "--lab.loop-wait=500ms",
                "--lab.etcd-timeout=1s",
                "--lab.sync-mode=" + syncMode,
                "--spring.main.banner-mode=off"));
        running.put(s.node(), new SpringApplicationBuilder(RaftLabApplication.class).run(args.toArray(String[]::new)));
        return this;
    }

    public HaAgent agent(String node) {
        return running.get(node).getBean(HaAgent.class);
    }

    public <T> T bean(String node, Class<T> type) {
        return running.get(node).getBean(type);
    }

    /** Ajanı (bekçiyi) öldür. PostgreSQL'i ÖLDÜRMEZ: onu Lab.kill yapar. */
    public void stop(String node) {
        ConfigurableApplicationContext c = running.remove(node);
        if (c != null) {
            archived.addAll(c.getBean(HaAgent.class).events());
            c.close();
        }
    }

    public void printTimeline() {
        java.util.stream.Stream.concat(archived.stream(), running.values().stream()
                        .flatMap(c -> c.getBean(HaAgent.class).events().stream()))
                .sorted((a, b) -> a.at().compareTo(b.at()))
                .forEach(e -> System.out.println("   " + e));
    }

    @Override
    public void close() {
        new ArrayList<>(running.keySet()).forEach(this::stop);
    }
}
