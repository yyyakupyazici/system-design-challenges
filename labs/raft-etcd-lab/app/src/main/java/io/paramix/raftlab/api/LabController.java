package io.paramix.raftlab.api;

import java.sql.SQLException;
import java.util.Map;

import io.paramix.raftlab.ha.HaAgent;
import io.paramix.raftlab.ha.HaAgent.Role;
import io.paramix.raftlab.ledger.LedgerService;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * <pre>
 *  GET  /primary          200 sadece liderken (HAProxy sağlık kontrolü: Patroni'deki gibi)
 *  GET  /replica          200 sadece standby'ken
 *  GET  /status           ajanın rolü ve son kararı
 *  GET  /events           ajanın karar zaman çizelgesi
 *  POST /v1/transfers     {from, to, amount}  → lider kimse oraya yazar
 *  GET  /v1/accounts/{id}?node=pg-ist2&minLsn=0/3A000128
 * </pre>
 */
@RestController
public class LabController {

    public record TransferBody(String from, String to, long amount) {
    }

    private final HaAgent agent;
    private final LedgerService ledger;

    public LabController(HaAgent agent, LedgerService ledger) {
        this.agent = agent;
        this.ledger = ledger;
    }

    @GetMapping("/primary")
    public ResponseEntity<Map<String, Object>> primary() {
        return ResponseEntity.status(agent.role() == Role.PRIMARY ? 200 : 503).body(agent.status());
    }

    @GetMapping("/replica")
    public ResponseEntity<Map<String, Object>> replica() {
        return ResponseEntity.status(agent.role() == Role.STANDBY ? 200 : 503).body(agent.status());
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        return agent.status();
    }

    @GetMapping("/events")
    public Object events() {
        return agent.events().stream().map(Object::toString).toList();
    }

    @PostMapping("/v1/transfers")
    public LedgerService.TransferResult transfer(@RequestBody TransferBody b) throws SQLException {
        return ledger.transfer(b.from(), b.to(), b.amount());
    }

    @GetMapping("/v1/accounts/{id}")
    public LedgerService.BalanceRead balance(@PathVariable String id, @RequestParam String node,
                                             @RequestParam(required = false) String minLsn) throws SQLException {
        return ledger.balance(id, node, minLsn, 1_000);
    }

    @ExceptionHandler({SQLException.class, IllegalStateException.class})
    public ResponseEntity<Map<String, String>> error(Exception e) {
        return ResponseEntity.status(503).body(Map.of("error", String.valueOf(e.getMessage())));
    }
}
