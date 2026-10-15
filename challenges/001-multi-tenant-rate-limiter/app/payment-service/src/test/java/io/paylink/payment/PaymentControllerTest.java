package io.paylink.payment;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest(properties = "paylink.simulated-db-latency=0ms")
@AutoConfigureMockMvc
class PaymentControllerTest {

    @Autowired
    MockMvc mvc;

    @Test
    void createThenGet_isolatedPerTenant() throws Exception {
        String body = mvc.perform(post("/payments").header("X-Tenant-Id", "tenant_7")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":450.00,\"currency\":\"TRY\",\"description\":\"Sipariş #8812\"}"))
                .andExpect(status().isCreated())
                .andExpect(header().exists("Location"))
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andReturn().getResponse().getContentAsString();
        String id = JsonPath.read(body, "$.id");

        mvc.perform(get("/payments/" + id).header("X-Tenant-Id", "tenant_7"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tenantId").value("tenant_7"));

        // Başka tenant aynı id'yi göremez
        mvc.perform(get("/payments/" + id).header("X-Tenant-Id", "tenant_42"))
                .andExpect(status().isNotFound());
    }

    @Test
    void invalidRequest_is400_andMissingTenantHeader_is400() throws Exception {
        mvc.perform(post("/payments").header("X-Tenant-Id", "t")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"amount\":-1,\"currency\":\"try\"}"))
                .andExpect(status().isBadRequest());
        mvc.perform(get("/payments/x")).andExpect(status().isBadRequest());
    }
}
