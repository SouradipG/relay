package dev.souradip.relay.guard;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PiiRedactorTest {

    private final PiiRedactor redactor = new PiiRedactor();

    @Test
    void redactsEmailsCardsAndPhoneNumbers() {
        String out = redactor.redact("Mail a.b+tag@example.co.uk, card 4111-1111-1111-1111, call +91 98765 43210");

        assertThat(out).isEqualTo("Mail [EMAIL], card [CARD], call [PHONE]");
    }

    @Test
    void leavesNumbersThatAreNotCardsAlone() {
        assertThat(redactor.redact("order 1234567890123 shipped")).as("fails the Luhn check").contains("1234567890123");
        assertThat(redactor.redact("in 2026 we sold 15 units")).isEqualTo("in 2026 we sold 15 units");
    }

    @Test
    void luhn() {
        assertThat(PiiRedactor.luhn("4111111111111111")).isTrue();
        assertThat(PiiRedactor.luhn("4111111111111112")).isFalse();
    }
}
