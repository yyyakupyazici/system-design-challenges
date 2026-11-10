package io.paramix.chat.fanout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.paramix.chat.gateway.LiveGateways;
import io.paramix.chat.gateway.RoutingTable;

import org.springframework.stereotype.Component;

/**
 * "Bu kullanıcıların cihazları hangi gateway'lerde?" → gateway başına hedef listesi + hiç canlı cihazı olmayanlar.
 * Aynı gateway'deki 40 alıcı için 40 ayrı PUBLISH değil, tek PUBLISH (40 hedefli) gider.
 */
@Component
public class DeliveryRouter {

    public record Route(Map<String, List<String>> targetsByGateway, List<Long> offlineUsers) {
    }

    private final RoutingTable routing;
    private final LiveGateways live;

    public DeliveryRouter(RoutingTable routing, LiveGateways live) {
        this.routing = routing;
        this.live = live;
    }

    /** @param excludeDevice "userId:deviceId" ya da null (mesajı gönderen cihaz, ona tekrar gitmez) */
    public Route route(List<Long> users, String excludeDevice) {
        Map<String, List<String>> byGateway = new HashMap<>();
        List<Long> offline = new ArrayList<>();
        for (Long u : users) {
            boolean anyLive = false;
            for (Map.Entry<String, String> d : routing.devices(u).entrySet()) {
                String target = u + ":" + d.getKey();
                if (!live.isAlive(d.getValue())) {
                    continue;                                  // ölü gateway: kayıt bayat, yok say
                }
                anyLive = true;
                if (!target.equals(excludeDevice)) {
                    byGateway.computeIfAbsent(d.getValue(), k -> new ArrayList<>()).add(target);
                }
            }
            if (!anyLive) {
                offline.add(u);
            }
        }
        return new Route(byGateway, offline);
    }
}
