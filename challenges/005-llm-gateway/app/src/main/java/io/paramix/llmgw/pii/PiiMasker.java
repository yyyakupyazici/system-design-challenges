package io.paramix.llmgw.pii;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Prompt'taki kişisel veriyi sağlayıcıya, cache'e ve loga gitmeden önce yer tutucuyla değiştirir.
 * Regex tek başına yetmez: rastgele 11 haneli bir sipariş numarası TC kimlik değildir. Bu yüzden her tür
 * kendi doğrulamasından geçer (TCKN algoritması, IBAN mod-97, kart Luhn).
 *
 * <p>Sıra önemli: IBAN içinde kart ya da telefon gibi görünen rakam dizileri var, önce IBAN maskelenir.
 * Hepsi derlenmiş regex + doğrusal tarama: istek başına mikro-saniyeler (30 ms gecikme bütçesini yemez).
 */
public final class PiiMasker {

    private record Rule(String type, Pattern pattern, java.util.function.Predicate<String> valid) {
    }

    private static final List<Rule> RULES = List.of(
            new Rule("IBAN", Pattern.compile("\\bTR\\d{2}(?:\\s?[0-9A-Z]{4}){5}\\s?[0-9A-Z]{2}\\b", Pattern.CASE_INSENSITIVE),
                    PiiMasker::validIban),
            new Rule("EMAIL", Pattern.compile("\\b[\\w.+-]+@[\\w-]+(?:\\.[\\w-]+)+\\b"), s -> true),
            new Rule("CARD", Pattern.compile("\\b(?:\\d[ -]?){12,18}\\d\\b"), PiiMasker::validCard),
            new Rule("TCKN", Pattern.compile("\\b[1-9]\\d{10}\\b"), PiiMasker::validTckn),
            new Rule("PHONE", Pattern.compile("(?<!\\d)(?:\\+90[ ]?|0)?5\\d{2}[ ]?\\d{3}[ ]?\\d{2}[ ]?\\d{2}(?!\\d)"), s -> true));

    private PiiMasker() {
    }

    public static String mask(String text, PiiVault vault) {
        if (text == null || text.isEmpty()) {
            return text;
        }
        String out = text;
        for (Rule rule : RULES) {
            Matcher m = rule.pattern().matcher(out);
            StringBuilder sb = new StringBuilder();
            while (m.find()) {
                String value = m.group();
                String replacement = rule.valid().test(value) ? vault.placeholderFor(rule.type(), value) : value;
                m.appendReplacement(sb, Matcher.quoteReplacement(replacement));
            }
            m.appendTail(sb);
            out = sb.toString();
        }
        return out;
    }

    /** T.C. kimlik no: 11 hane, ilki 0 değil; 10. ve 11. hane kontrol hanesi. */
    static boolean validTckn(String s) {
        if (s.length() != 11 || s.charAt(0) == '0') {
            return false;
        }
        int[] d = s.chars().map(c -> c - '0').toArray();
        int odd = d[0] + d[2] + d[4] + d[6] + d[8];
        int even = d[1] + d[3] + d[5] + d[7];
        int d10 = Math.floorMod(odd * 7 - even, 10);
        int sum10 = 0;
        for (int i = 0; i < 10; i++) {
            sum10 += i < 9 ? d[i] : d10;
        }
        return d[9] == d10 && d[10] == sum10 % 10;
    }

    /** ISO 13616 mod-97. */
    static boolean validIban(String raw) {
        String s = raw.replace(" ", "").toUpperCase();
        if (s.length() != 26) {
            return false;
        }
        String rearranged = s.substring(4) + s.substring(0, 4);
        int mod = 0;
        for (char c : rearranged.toCharArray()) {
            String part = Character.isLetter(c) ? String.valueOf(c - 'A' + 10) : String.valueOf(c);
            for (char p : part.toCharArray()) {
                mod = (mod * 10 + (p - '0')) % 97;
            }
        }
        return mod == 1;
    }

    /** Luhn; 13–19 hane. */
    static boolean validCard(String raw) {
        String s = raw.replaceAll("[ -]", "");
        if (s.length() < 13 || s.length() > 19) {
            return false;
        }
        int sum = 0;
        boolean dbl = false;
        for (int i = s.length() - 1; i >= 0; i--) {
            int n = s.charAt(i) - '0';
            if (dbl) {
                n *= 2;
                if (n > 9) {
                    n -= 9;
                }
            }
            sum += n;
            dbl = !dbl;
        }
        return sum % 10 == 0;
    }
}
