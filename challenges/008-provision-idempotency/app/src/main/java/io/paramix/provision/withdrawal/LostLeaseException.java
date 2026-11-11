package io.paramix.provision.withdrawal;

import java.util.UUID;

/**
 * Fencing: bu worker'ın kirası başka bir worker tarafından devralınmış. Elindeki bilgi bayat; hiçbir şey yazmadan
 * çekilmeli. (Örn. 40 sn'lik GC duraklaması, ağ kopması: süreç ölmedi ama artık sahibi değil.)
 */
public class LostLeaseException extends RuntimeException {

    public LostLeaseException(UUID withdrawalId) {
        super("Çekim " + withdrawalId + " üzerindeki kira kaybedildi", null, false, false);
    }
}
