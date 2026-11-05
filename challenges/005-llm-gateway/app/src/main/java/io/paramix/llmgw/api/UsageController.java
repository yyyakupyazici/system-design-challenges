package io.paramix.llmgw.api;

import java.util.Map;

import io.paramix.llmgw.auth.ApiKeyAuthenticator;
import io.paramix.llmgw.auth.ClientContext;
import io.paramix.llmgw.budget.BudgetService;
import io.paramix.llmgw.usage.UsageLedger;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

/** Ekip kendi kullanımını görür (anahtarı hangi ekibe aitse sadece onu). */
@RestController
public class UsageController {

    private final ApiKeyAuthenticator auth;
    private final BudgetService budget;
    private final UsageLedger ledger;

    public UsageController(ApiKeyAuthenticator auth, BudgetService budget, UsageLedger ledger) {
        this.auth = auth;
        this.budget = budget;
        this.ledger = ledger;
    }

    @GetMapping("/v1/usage")
    public Map<String, Object> usage(@RequestHeader(value = "Authorization", required = false) String authorization,
                                     @RequestHeader(value = "X-User-Id", required = false) String userId) {
        ClientContext ctx = auth.authenticate(authorization, userId, null, null);
        return Map.of("budget", budget.status(ctx.team(), ctx.userId()),
                "last24hByModel", ledger.teamSummary(ctx.team()));
    }

    @GetMapping("/v1/usage/requests/{id}")
    public Map<String, Object> request(@RequestHeader(value = "Authorization", required = false) String authorization,
                                       @PathVariable String id) {
        ClientContext ctx = auth.authenticate(authorization, null, null, null);
        Map<String, Object> row = ledger.find(id);
        if (row == null || !ctx.team().equals(row.get("team"))) {
            throw new io.paramix.llmgw.core.GatewayException(org.springframework.http.HttpStatus.NOT_FOUND,
                    "not_found", "İstek bulunamadı");
        }
        return row;
    }
}
