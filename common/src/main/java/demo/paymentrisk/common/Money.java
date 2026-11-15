package demo.paymentrisk.common;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Currency;
import java.util.Locale;

/** Display helpers for integer minor-unit amounts. No floating point is ever used. */
public final class Money {
    private Money() {}

    public static String format(long amountMinor, String currencyCode) {
        int digits = 2;
        try {
            digits = Math.max(0, Currency.getInstance(currencyCode).getDefaultFractionDigits());
        } catch (Exception ignored) {
            // unknown code: keep 2 decimals for display only
        }
        BigDecimal major = BigDecimal.valueOf(amountMinor).movePointLeft(digits);
        DecimalFormat f = new DecimalFormat(digits == 0 ? "#,##0" : "#,##0." + "0".repeat(digits),
                DecimalFormatSymbols.getInstance(Locale.ROOT));
        f.setParseBigDecimal(true);
        return currencyCode + " " + f.format(major);
    }
}
