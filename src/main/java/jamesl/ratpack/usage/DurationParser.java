package jamesl.ratpack.usage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author jamesl
 */
public class DurationParser {
    private static final Logger logger = LoggerFactory.getLogger(DurationParser.class);
    static final Pattern regEx = Pattern.compile("^(\\d+)([a-z]+)$");

    /**
     * @param s
     * @return
     */
    public static Duration parse(String s) {
        Matcher matcher = regEx.matcher(s);
        if (!matcher.find()) {
            throw new IllegalArgumentException("Invalid duration spec - spec='" + s + "'");
        }

        long amount = Long.parseLong(matcher.group(1));
        String timeUnit = matcher.group(2);
        logger.trace("s={},amount={},timeUnit={}", s, amount, timeUnit);

        switch (timeUnit) {
            case "d":
                return Duration.ofDays(amount);
            case "h":
                return Duration.ofHours(amount);
            case "m":
                return Duration.ofMinutes(amount);
            case "s":
                return Duration.ofSeconds(amount);
            case "ms":
                return Duration.ofMillis(amount);
            default:
                throw new IllegalArgumentException("Invalid timeunit='" + timeUnit + "' - must be either 'h', 'm', 's' or 'ms'");
        }
    }
}
