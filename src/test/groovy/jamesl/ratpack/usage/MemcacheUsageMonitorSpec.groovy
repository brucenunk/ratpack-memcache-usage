package jamesl.ratpack.usage

import io.netty.buffer.UnpooledByteBufAllocator
import ratpack.test.exec.ExecHarness
import spock.lang.Specification

/**
 * @author jamesl
 */
class MemcacheUsageMonitorSpec extends Specification {
    ExecHarness exec
    Memcache memcache
    MemcacheServer memcacheServer
    UsageMonitor usageMonitor

    def setup() {
        exec = ExecHarness.harness()
        memcacheServer = new MemcacheServer(exec.controller.eventLoopGroup)
        memcacheServer.start()

        def remoteHost = memcacheServer.remoteHost
        def ms = "800ms"
        def ttl = "200ms"
        def config = new MemcacheUsageMonitor.Configuration(blockTtl: ttl, maxRequestsFromIp: 10, maxRequestsFromSubnet: 20, ttl: ttl)
        def config2 = new DefaultMemcache.Configuration(connectTimeout: ms, pool: 10, remoteHost: "${remoteHost.hostName}:${remoteHost.port}", readTimeout: ms)

        memcache = new DefaultMemcache(config2, exec.controller, UnpooledByteBufAllocator.DEFAULT)
        usageMonitor = new MemcacheUsageMonitor(memcache, config)
    }

    def cleanup() {
        memcacheServer.stop()
        exec.close()
    }

    def "check usage"() {
        when:
        def result = exec.yield { e ->
            usageMonitor.checkUsageIsWithinLimits("a.b.c.d")
        }

        then:
        result.success
        result.value == true
    }
}
