package jamesl.ratpack.usage

import ratpack.exec.Promise
import ratpack.groovy.test.GroovyRatpackMainApplicationUnderTest
import ratpack.guice.BindingsImposition
import ratpack.impose.ImpositionsSpec
import ratpack.test.CloseableApplicationUnderTest
import ratpack.test.http.TestHttpClient
import spock.lang.Specification

/**
 * @author jamesl
 */
class AppSpec extends Specification {
    @Delegate
    TestHttpClient http

    CloseableApplicationUnderTest app
    SimpleUsageMonitor usageMonitor

    def setup() {
        usageMonitor = new SimpleUsageMonitor()
        app = new GroovyRatpackMainApplicationUnderTest() {
            @Override
            protected void addImpositions(ImpositionsSpec impositions) {
                impositions.add(BindingsImposition.of { spec ->
                    spec.bindInstance(UsageMonitor, usageMonitor)
                })
            }
        }
        http = testHttpClient(app)
    }

    def cleanup() {
        app.close()
    }

    def "should return 200 if within limits"() {
        usageMonitor.ok = true

        when:
        get("/test")

        then:
        response.statusCode == 200
        response.body.text == "OK"
    }

    def "should return 429 if not within limits"() {
        usageMonitor.ok = false

        when:
        get("/test")

        then:
        response.statusCode == 429
        response.body.text == "Too Many Requests"
    }

    /**
     *
     */
    static class SimpleUsageMonitor implements UsageMonitor {
        boolean ok

        Promise<Boolean> checkUsageIsWithinLimits(String remoteIp) {
            Promise.value(ok)
        }
    }
}
