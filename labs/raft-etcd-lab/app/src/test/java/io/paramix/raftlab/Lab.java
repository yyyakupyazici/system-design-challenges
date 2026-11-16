package io.paramix.raftlab;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.paramix.raftlab.pg.PgNode;

/**
 * scripts/lab.sh'ın Java'dan kullanımı. Testler insanın terminalde yapacağı şeyin aynısını yapar:
 * "etcd-ist'i öldür", "pg-ank'i dondur"...
 */
public final class Lab {

    public static final String ENDPOINTS = "http://127.0.0.1:23791,http://127.0.0.1:23792,http://127.0.0.1:23793";
    public static final String ETCD_IST = "http://127.0.0.1:23791";
    public static final String ETCD_ANK = "http://127.0.0.1:23792";
    public static final String ETCD_WIT = "http://127.0.0.1:23793";

    private static final File SCRIPT = new File("../scripts/lab.sh").getAbsoluteFile();

    private Lab() {
    }

    /** etcd ve PostgreSQL ikili dosyaları var mı, ve script'i çalıştırabiliyor muyuz? */
    public static boolean available() {
        return SCRIPT.canExecute() && which("etcd") && which("etcdctl")
                && new File("/usr/lib/postgresql").isDirectory();
    }

    private static boolean which(String bin) {
        try {
            return new ProcessBuilder("bash", "-c", "command -v " + bin).start().waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    /** Sıfırdan küme: eski veriler silinir, 3 etcd + 3 PostgreSQL açılır. */
    public static void fresh() {
        run("clean");
        run("up");
    }

    public static void down() {
        run("down");
    }

    public static void kill(String node) {
        run("kill", node);
    }

    public static void pause(String node) {
        run("pause", node);
    }

    public static void resume(String node) {
        run("resume", node);
    }

    public static void start(String node) {
        run("start", node);
    }

    public static String run(String... args) {
        List<String> cmd = new ArrayList<>(List.of("bash", SCRIPT.getPath()));
        cmd.addAll(List.of(args));
        try {
            Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
            if (!p.waitFor(120, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                throw new IllegalStateException("lab.sh " + String.join(" ", args) + " zaman aşımı");
            }
            String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            if (p.exitValue() != 0) {
                throw new IllegalStateException("lab.sh " + String.join(" ", args) + " başarısız:\n" + out);
            }
            return out;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    public static int port(String pgNode) {
        return switch (pgNode) {
            case "pg-ist" -> 5511;
            case "pg-ank" -> 5512;
            case "pg-ist2" -> 5513;
            case "pg-ank2" -> 5514;
            default -> throw new IllegalArgumentException(pgNode);
        };
    }

    public static Object query(String pgNode, String sql) {
        try (Connection c = PgNode.connect("127.0.0.1", port(pgNode), "ledger");
             Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getObject(1) : null;
        } catch (Exception e) {
            throw new IllegalStateException(pgNode + ": " + e.getMessage(), e);
        }
    }

    public static void exec(String pgNode, String database, String sql) {
        try (Connection c = PgNode.connect("127.0.0.1", port(pgNode), database); Statement st = c.createStatement()) {
            st.execute(sql);
        } catch (Exception e) {
            throw new IllegalStateException(pgNode + ": " + e.getMessage(), e);
        }
    }

    public static void sleep(Duration d) {
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Ders çıktısı: test raporuna (target/failsafe-reports/*-output.txt) okunur bir anlatım düşer. */
    public static void say(String fmt, Object... args) {
        System.out.printf("%n▶ " + fmt + "%n", args);
    }
}
