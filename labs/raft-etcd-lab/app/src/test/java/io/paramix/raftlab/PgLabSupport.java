package io.paramix.raftlab;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import io.paramix.raftlab.etcd.EtcdStore;
import io.paramix.raftlab.ha.HaAgent.Role;
import io.paramix.raftlab.pg.PgNode;

import org.awaitility.Awaitility;

/** PostgreSQL'li derslerin ortak kurulumu: 3 ajan, pg-ist lider, pg-ank senkron standby. */
final class PgLabSupport {

    private PgLabSupport() {
    }

    static Agents startSteadyCluster(String syncMode, String istEndpoints, String ankEndpoints) {
        Lab.fresh();
        Agents agents = new Agents(syncMode);
        agents.start(Agents.ist(istEndpoints));                           // İstanbul önce: ilk lider o olsun
        Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> agents.agent("pg-ist").role() == Role.PRIMARY);
        agents.start(Agents.ank(ankEndpoints)).start(Agents.ist2(istEndpoints)).start(Agents.ank2(ankEndpoints));
        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> {
            EtcdStore etcd = agents.bean("pg-ist", EtcdStore.class);
            PgNode.Status st = agents.bean("pg-ist", PgNode.class).statusOrNull();
            boolean syncReady = "SINGLE".equals(syncMode)
                    ? etcd.sync().size() == 1 && etcd.sync().iterator().next().startsWith("pg-ank")       // başka bölgeden biri
                    : etcd.sync().size() == 3;
            return syncReady && st != null && !st.syncNames().isEmpty()
                    && agents.agent("pg-ank").role() == Role.STANDBY && agents.agent("pg-ist2").role() == Role.STANDBY
                    && agents.agent("pg-ank2").role() == Role.STANDBY;
        });
        Lab.exec("pg-ist", "ledger", "INSERT INTO accounts VALUES ('ali', 10000000), ('ayse', 0)");
        assertThat(Lab.query("pg-ist", "SELECT current_setting('synchronous_standby_names')")).isNotEqualTo("");
        Lab.say("Küme hazır: lider pg-ist, synchronous_standby_names = %s, /sync = %s",
                Lab.query("pg-ist", "SELECT current_setting('synchronous_standby_names')"),
                agents.bean("pg-ist", EtcdStore.class).sync());
        System.out.println(Lab.run("status"));
        return agents;
    }
}
