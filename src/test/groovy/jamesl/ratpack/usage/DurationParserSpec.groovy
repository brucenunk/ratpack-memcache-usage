package jamesl.ratpack.usage

import spock.lang.Specification
import spock.lang.Unroll

import java.time.Duration

/**
 * @author jamesl
 */
class DurationParserSpec extends Specification {
    @Unroll
    def "parsing #s should return #expected"(String s, Duration expected) {
        when:
        def result = DurationParser.parse(s)

        then:
        result == expected

        where:
        s        | expected
        "1d"     | Duration.ofDays(1)
        "2h"     | Duration.ofHours(2)
        "20m"    | Duration.ofMinutes(20)
        "200s"   | Duration.ofSeconds(200)
        "2000ms" | Duration.ofMillis(2000)
    }

    def "parsing an invalid string should yield an exception"() {
        when:
        DurationParser.parse("frogmonkey")

        then:
        def ex = thrown(IllegalArgumentException)
        ex.message == "Invalid duration spec - spec='frogmonkey'"
    }

    def "parsing a string with an unsupported timeunit should yield an exception"() {
        when:
        DurationParser.parse("200frogs")

        then:
        def ex = thrown(IllegalArgumentException)
        ex.message == "Invalid timeunit='frogs' - must be either 'h', 'm', 's' or 'ms'"
    }
}
