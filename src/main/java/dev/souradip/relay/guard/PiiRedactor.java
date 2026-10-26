package dev.souradip.relay.guard;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.springframework.stereotype.Component;

/**
 * Removes obvious personal data before a prompt leaves the company.
 *
 * <p>Once a prompt is sent, it is in the provider's logs and possibly their training data, and
 * there is no taking it back. Redaction happens at the gateway so every team gets it without
 * having to remember. Regexes catch the common, well structured cases. They are a floor, not a
 * guarantee, and names and addresses need a proper entity recogniser.
 */
@Component
public class PiiRedactor {

    private static final Pattern EMAIL = Pattern.compile("[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}");
    // 13 to 19 digits, optionally grouped with spaces or dashes. Checked with Luhn below so
    // order numbers and timestamps are left alone.
    private static final Pattern CARD = Pattern.compile("\\b(?:\\d[ -]?){12,18}\\d\\b");
    // An optional country code, then an area or prefix group that must be followed by a
    // separator, so plain runs of digits such as order numbers are not mistaken for phones.
    private static final Pattern PHONE = Pattern.compile(
            "(?<![\\w+])(?:\\+\\d{1,3}[ -]?)?(?:\\(\\d{2,5}\\)[ -]?|\\d{2,5}[ -])\\d{3,5}(?:[ -]?\\d{3,4})?(?!\\w)");

    public String redact(String text) {
        if (text == null || text.isEmpty()) {
            return text;
        }
        String result = EMAIL.matcher(text).replaceAll("[EMAIL]");
        result = replaceCards(result);
        return PHONE.matcher(result).replaceAll("[PHONE]");
    }

    private static String replaceCards(String text) {
        Matcher matcher = CARD.matcher(text);
        StringBuilder out = new StringBuilder();
        while (matcher.find()) {
            String digits = matcher.group().replaceAll("[ -]", "");
            matcher.appendReplacement(out, luhn(digits) ? "[CARD]" : Matcher.quoteReplacement(matcher.group()));
        }
        matcher.appendTail(out);
        return out.toString();
    }

    static boolean luhn(String digits) {
        int sum = 0;
        boolean doubleIt = false;
        for (int i = digits.length() - 1; i >= 0; i--) {
            int d = digits.charAt(i) - '0';
            if (doubleIt) {
                d *= 2;
                if (d > 9) {
                    d -= 9;
                }
            }
            sum += d;
            doubleIt = !doubleIt;
        }
        return sum % 10 == 0;
    }
}
