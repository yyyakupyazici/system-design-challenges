package io.paramix.llmgw.pii;

/**
 * Stream'de yer tutucu iki parçaya bölünebilir: "...müşteri <TC" + "KN_1> için...". Parçayı olduğu gibi
 * gönderirsek kullanıcı "<TC" görür, geri açamayız. Çözüm: açık kalan bir "<" varsa o kısmı tut,
 * kapanınca (ya da yer tutucu olamayacak kadar uzayınca) aç ve gönder.
 */
public final class StreamingUnmasker {

    private static final int MAX_PLACEHOLDER = 16;   // "<EMAIL_123>" en fazla 11 karakter; pay bırakıldı

    private final PiiVault vault;
    private final StringBuilder pending = new StringBuilder();

    public StreamingUnmasker(PiiVault vault) {
        this.vault = vault;
    }

    /** Bir parça al, güvenle gönderilebilecek açılmış metni döndür (boş olabilir). */
    public String accept(String chunk) {
        if (chunk == null || chunk.isEmpty()) {
            return "";
        }
        if (vault.isEmpty()) {
            return chunk;
        }
        pending.append(chunk);
        int open = pending.lastIndexOf("<");
        int close = pending.lastIndexOf(">");
        String ready;
        if (open > close && pending.length() - open < MAX_PLACEHOLDER) {
            ready = pending.substring(0, open);
            pending.delete(0, open);
        } else {
            ready = pending.toString();
            pending.setLength(0);
        }
        return vault.unmask(ready);
    }

    /** Stream bitti: elde kalanı aç ve gönder. */
    public String flush() {
        String rest = vault.unmask(pending.toString());
        pending.setLength(0);
        return rest;
    }
}
