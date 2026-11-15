package demo.paymentrisk.common.rule;

import demo.paymentrisk.common.Money;

import java.io.Serializable;
import java.time.Duration;

/** Rule parameters. Defaults implement the v1 demo rule; the threshold is configurable for exercises. */
public final class RuleConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String RULE_VERSION = "payment-review-rule/v1";
    public static final long DEFAULT_THRESHOLD_MINOR = 5_000_000L; // USD 50,000.00
    public static final String DEFAULT_CURRENCY = "USD";
    public static final Duration DEFAULT_WINDOW = Duration.ofMinutes(10);

    public final long largePaymentThresholdMinor;
    public final String currency;
    public final long recentChangeWindowMs;

    public RuleConfig(long largePaymentThresholdMinor, String currency, Duration recentChangeWindow) {
        if (largePaymentThresholdMinor < 0) {
            throw new IllegalArgumentException("threshold must be >= 0");
        }
        this.largePaymentThresholdMinor = largePaymentThresholdMinor;
        this.currency = currency;
        this.recentChangeWindowMs = recentChangeWindow.toMillis();
    }

    public static RuleConfig defaults() {
        return new RuleConfig(DEFAULT_THRESHOLD_MINOR, DEFAULT_CURRENCY, DEFAULT_WINDOW);
    }

    public String describe() {
        return "recent_change: 0 <= payment.occurred_at - bank_change.occurred_at <= "
                + Duration.ofMillis(recentChangeWindowMs).toMinutes() + " min; "
                + "large_payment: currency == " + currency + " AND amount_minor > " + largePaymentThresholdMinor
                + " (" + Money.format(largePaymentThresholdMinor, currency) + ")";
    }
}
