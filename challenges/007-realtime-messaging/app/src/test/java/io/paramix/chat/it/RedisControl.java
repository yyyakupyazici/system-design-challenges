package io.paramix.chat.it;

import java.util.concurrent.TimeUnit;

/**
 * Testin ortasında Redis'i bozmak için. Sadece Redis bu makinede yerel bir süreç olarak çalışıyorsa
 * (IT_REDIS_HOST=localhost ve redis-server PATH'te) kullanılabilir; Testcontainers'ta bu testler atlanır.
 */
public final class RedisControl {

    private RedisControl() {
    }

    public static boolean available() {
        String host = System.getenv("IT_REDIS_HOST");
        return ("localhost".equals(host) || "127.0.0.1".equals(host)) && run(false, "redis-server", "--version");
    }

    private static String port() {
        return System.getenv().getOrDefault("IT_REDIS_PORT", "6379");
    }

    /** Redis bütün istemcileri dondurur (veri kalır): failover ya da ağ takılması gibi kısa bir kesinti. */
    public static void pause(long millis) {
        run(true, "redis-cli", "-p", port(), "CLIENT", "PAUSE", String.valueOf(millis), "ALL");
    }

    /** Redis'i kapatır (veriyi kaydetmeden). */
    public static void stop() {
        run(false, "redis-cli", "-p", port(), "SHUTDOWN", "NOSAVE");
        waitUntil(false);
    }

    /** Redis'i BOŞ olarak yeniden başlatır (kalıcılık kapalı: rehber, lease, son görülme hepsi gitti). */
    public static void start() {
        run(true, "redis-server", "--port", port(), "--daemonize", "yes", "--save", "", "--appendonly", "no");
        waitUntil(true);
    }

    private static void waitUntil(boolean up) {
        for (int i = 0; i < 100; i++) {
            if (run(false, "redis-cli", "-p", port(), "PING") == up) {
                return;
            }
            sleep(100);
        }
        throw new IllegalStateException("Redis " + (up ? "açılmadı" : "kapanmadı"));
    }

    private static boolean run(boolean mustSucceed, String... cmd) {
        try {
            Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
            String out = new String(p.getInputStream().readAllBytes());
            boolean ok = p.waitFor(10, TimeUnit.SECONDS) && p.exitValue() == 0 && !out.contains("Could not connect");
            if (mustSucceed && !ok) {
                throw new IllegalStateException(String.join(" ", cmd) + " → " + out);
            }
            return ok;
        } catch (Exception e) {
            if (mustSucceed) {
                throw new IllegalStateException(e);
            }
            return false;
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
