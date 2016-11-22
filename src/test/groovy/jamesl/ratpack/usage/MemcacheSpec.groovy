package jamesl.ratpack.usage

import groovy.util.logging.Slf4j
import io.netty.buffer.ByteBufAllocator
import ratpack.test.exec.ExecHarness
import spock.lang.Specification

import java.nio.charset.StandardCharsets
import java.time.Duration

/**
 * @author jamesl
 */
@Slf4j
class MemcacheSpec extends Specification {
    ExecHarness exec
    DefaultMemcache memcache
    MemcacheServer memcacheServer
    long timestamp
    Duration ttl

    def setup() {
        this.exec = ExecHarness.harness()

        memcacheServer = new MemcacheServer(exec.controller.eventLoopGroup)
        memcacheServer.start()

        def remoteHost = memcacheServer.remoteHost
        def s = "${remoteHost.hostName}:${remoteHost.port}"
        def configuration = new DefaultMemcache.Configuration(connectTimeout: "200ms", pool: 20, readTimeout: "800ms", remoteHost: s)

        this.memcache = new DefaultMemcache(configuration, exec.controller, ByteBufAllocator.DEFAULT)
        this.timestamp = System.currentTimeMillis()
        this.ttl = Duration.ofMillis(2000)
    }

    def cleanup() {
        memcacheServer.stop()
        exec.close()
    }

    def "add when key not found"() {
        when:
        def result = exec.yield { e ->
            memcache.add("add:missing", ttl) { allocator ->
                allocator.buffer().writeBytes("jamesl".bytes)
            }
        }

        then:
        result.success
        result.value
    }

    def "add when key is found"() {
        def key = "add:${timestamp}"

        when:
        exec.execute { e ->
            memcache.set(key, ttl) { allocator ->
                allocator.buffer().writeBytes("jamesl".bytes)
            }
        }

        def result = exec.yield { e ->
            memcache.add(key, ttl) { allocator ->
                allocator.buffer().writeBytes("jamesl2".bytes)
            }
        }

        def result2 = exec.yield { e ->
            memcache.get(key) { buffer ->
                buffer.toString(StandardCharsets.UTF_8)
            }
        }

        then: "add should return false as key exists"
        result.success
        !result.value

        and: "get should return the original value as add fails"
        result2.success
        result2.value == "jamesl"
    }

    def "decrement when key not found"() {
        def initial = 20L

        when:
        def result = exec.yield { e ->
            memcache.decrement("decr:missing", ttl, initial)
        }

        then:
        result.success
        result.value == initial
    }

    def "decrement when key is found"() {
        def key = "decr:${timestamp}"
        def initial = 20L

        when:
        def result = exec.yield { e ->
            memcache.decrement(key, ttl, initial)
        }

        def result2 = exec.yield { e ->
            memcache.decrement(key, ttl, initial)
        }

        then:
        result.success
        result.value == initial

        and:
        result2.success
        result2.value == initial - 1
    }

    def "exists when key not found"() {
        when:
        def result = exec.yield { e ->
            memcache.exists("exists:missing")
        }

        then:
        result.success
        !result.value
    }

    def "exists when key is found"() {
        def key = "exists:${timestamp}"

        when:
        exec.execute { e ->
            memcache.set(key, ttl) { allocator ->
                allocator.buffer().writeBytes("jamesl".bytes)
            }
        }

        def result = exec.yield { e2 ->
            memcache.exists(key)
        }

        then:
        result.success
        result.value
    }

    def "get when key not found"() {
        when:
        def result = exec.yield { e ->
            memcache.get("get:missing") { buffer ->
                buffer.toString(StandardCharsets.UTF_8)
            }
        }

        then:
        result.success
        result.value == null
    }

    def "get when key is found"() {
        def key = "get:${timestamp}"

        when:
        exec.execute { e ->
            memcache.set(key, ttl) { allocator ->
                allocator.buffer().writeBytes("jamesl".bytes)
            }
        }

        def result = exec.yield { e2 ->
            memcache.get(key) { buffer ->
                buffer.toString(StandardCharsets.UTF_8)
            }
        }

        then:
        result.success
        result.value == "jamesl"
    }

    def "increment when key not found"() {
        def initial = 20L

        when:
        def result = exec.yield { e ->
            memcache.increment("incr:missing", ttl, initial)
        }

        then:
        result.success
        result.value == initial
    }

    def "increment when key is found"() {
        def key = "incr:${timestamp}"
        def initial = 20L

        when:
        def result = exec.yield { e ->
            memcache.increment(key, ttl, initial)
        }

        def result2 = exec.yield { e ->
            memcache.increment(key, ttl, initial)
        }

        then:
        result.success
        result.value == initial

        and:
        result2.success
        result2.value == initial + 1
    }
}
