package io.paramix.notify.api;

import java.util.Map;

import io.paramix.notify.router.UserDirectory;
import io.paramix.notify.store.CampaignStore;
import io.paramix.notify.store.UserRepository;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** Kampanya, cihaz token'ı ve tercih yönetimi. Değişiklikler kullanıcı cache'ini siler (cache-aside). */
@RestController
@ConditionalOnProperty(name = "notify.roles.api", havingValue = "true", matchIfMissing = true)
public class AdminController {

    public record CampaignRequest(@NotBlank String campaignId, @NotBlank String name) {
    }

    public record DeviceRequest(@NotBlank String installationId, @NotBlank String userId,
                                @Pattern(regexp = "IOS|ANDROID") String platform, @NotBlank String token) {
    }

    private final CampaignStore campaigns;
    private final UserRepository users;
    private final UserDirectory directory;

    public AdminController(CampaignStore campaigns, UserRepository users, UserDirectory directory) {
        this.campaigns = campaigns;
        this.users = users;
        this.directory = directory;
    }

    @PostMapping("/campaigns")
    public ResponseEntity<Void> createCampaign(@Valid @RequestBody CampaignRequest r) {
        campaigns.create(r.campaignId(), r.name());
        return ResponseEntity.status(201).build();
    }

    /** Zorluk #7: kuyruktaki milyonlarca mesaj silinmez, router/worker okuyup atlar. */
    @PostMapping("/campaigns/{id}/stop")
    public ResponseEntity<Map<String, Object>> stopCampaign(@PathVariable String id) {
        return campaigns.stop(id)
                ? ResponseEntity.ok(Map.of("campaignId", id, "status", "STOPPED"))
                : ResponseEntity.notFound().build();
    }

    /** Uygulama her açılışta token'ını kaydeder (silinme anı yakalanamaz; yakalanan an yeni kayıttır). */
    @PostMapping("/devices")
    public ResponseEntity<Void> registerDevice(@Valid @RequestBody DeviceRequest r) {
        users.upsertDevice(r.installationId(), r.userId(), r.platform(), r.token())
                .filter(previous -> !previous.equals(r.userId()))
                .ifPresent(directory::invalidate);
        directory.invalidate(r.userId());
        return ResponseEntity.noContent().build();
    }

    @PutMapping("/users/{userId}/preferences")
    public ResponseEntity<Void> updatePreferences(@PathVariable String userId,
                                                  @RequestBody UserRepository.PreferenceUpdate update) {
        users.updatePreferences(userId, update);
        directory.invalidate(userId);
        return ResponseEntity.noContent().build();
    }
}
