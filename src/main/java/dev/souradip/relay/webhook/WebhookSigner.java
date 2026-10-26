package dev.souradip.relay.webhook;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.HexFormat;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Signs deliveries the way Stripe does: {@code Relay-Signature: t=<unix seconds>,v1=<hex HMAC>}
 * over {@code t + "." + body}. Including the timestamp lets a receiver reject replays of an old
 * captured request, not just forged ones.
 */
public final class WebhookSigner {

    private WebhookSigner() {}

    public static String sign(String secret, long timestamp, String body) {
        return "t=" + timestamp + ",v1=" + HexFormat.of().formatHex(hmac(secret, timestamp + "." + body));
    }

    /** What a receiver runs. Constant time comparison, and a tolerance window for the timestamp. */
    public static boolean verify(String secret, String header, String body, long nowSeconds, long toleranceSeconds) {
        if (header == null) {
            return false;
        }
        long timestamp = -1;
        String signature = null;
        for (String part : header.split(",")) {
            if (part.startsWith("t=")) {
                timestamp = Long.parseLong(part.substring(2));
            } else if (part.startsWith("v1=")) {
                signature = part.substring(3);
            }
        }
        if (timestamp < 0 || signature == null || Math.abs(nowSeconds - timestamp) > toleranceSeconds) {
            return false;
        }
        byte[] expected = hmac(secret, timestamp + "." + body);
        byte[] given;
        try {
            given = HexFormat.of().parseHex(signature);
        } catch (IllegalArgumentException e) {
            return false;
        }
        return MessageDigest.isEqual(expected, given);
    }

    private static byte[] hmac(String secret, String message) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return mac.doFinal(message.getBytes(StandardCharsets.UTF_8));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }
}
