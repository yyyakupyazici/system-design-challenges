package io.paramix.provision.domain;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/** Varlık listesi küçük ve nadiren değişir: açılışta belleğe alınır. Yeni varlık = migration + yeniden başlatma. */
@Component
public class AssetCatalog {

    private final Map<String, Asset> assets;

    public AssetCatalog(JdbcTemplate jdbc) {
        this.assets = jdbc.query("SELECT code, kind, scale FROM assets",
                        (rs, i) -> new Asset(rs.getString(1), Asset.Kind.valueOf(rs.getString(2)), rs.getInt(3)))
                .stream().collect(Collectors.toUnmodifiableMap(Asset::code, Function.identity()));
    }

    public Asset require(String code) {
        Asset asset = code == null ? null : assets.get(code);
        if (asset == null) {
            throw new DomainException(ErrorCode.UNKNOWN_ASSET, "Desteklenmeyen varlık: " + code,
                    Map.of("supported", assets.keySet().stream().sorted().toList()));
        }
        return asset;
    }
}
