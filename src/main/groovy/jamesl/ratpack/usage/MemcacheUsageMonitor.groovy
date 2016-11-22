package jamesl.ratpack.usage

import groovy.util.logging.Slf4j
import ratpack.exec.Promise

import javax.inject.Inject
import java.time.Duration

/**
 * @author jamesl
 */
@Slf4j
class MemcacheUsageMonitor implements UsageMonitor {
    byte[] blocked
    Duration blockTtl
    long maxRequestsFromIp
    long maxRequestsFromSubnet
    Memcache memcache
    Duration ttl

    @Inject
    MemcacheUsageMonitor(Memcache memcache, Configuration configuration) {
        this.blocked = "blocked".bytes
        this.blockTtl = configuration.blockTtl
        this.maxRequestsFromIp = configuration.maxRequestsFromIp
        this.maxRequestsFromSubnet = configuration.maxRequestsFromSubnet
        this.memcache = memcache
        this.ttl = configuration.ttl
    }

    /**
     * Checks usage for the {@code req} and returns whether the request may continue.
     *
     * @param req
     * @return
     */
    Promise<Boolean> checkUsageIsWithinLimits(String remoteIp) {
        // JL assumes ipv4.
        def subnet = remoteIp.split("\\.").take(3).join(".")

        isBlocked(remoteIp, subnet)
        .flatMapIf({ !it }) {
            incrementUsage(remoteIp, maxRequestsFromIp)
            .flatRight { incrementUsage(subnet, maxRequestsFromSubnet) }
            .map { p -> p.left || p.right }
        }
        .map { !it }
    }

    /**
     * Increments usage and applies a block if usage exceeds the {@code max}.
     *
     * @param suffix
     * @param max
     * @return {@code true} if usage has been exceeded, otherwise {@code false}.
     */
    Promise<Boolean> incrementUsage(String suffix, long max) {
        memcache.increment("usage:${suffix}", ttl, 0L)
        .map { it > max }
        .flatMapIf({ it }) { exceedsUsage ->
            log.info("blocking {}", suffix)
            memcache.add("block:${suffix}", blockTtl) { allocator ->
                allocator.buffer().writeBytes(blocked)
            }
            .map { exceedsUsage }
        }
    }

    /**
     * Checks whether {@code ip} or {@code subnet] have been blocked.
     *
     * @param ip
     * @param subnet
     * @return {@code true} if the {@code ip} or {@code subnet} have been blocked, otherwise {@code false}.
     */
    Promise<Boolean> isBlocked(String ip, String subnet) {
        memcache.exists("block:${ip}")
        .flatRight { memcache.exists("block:${subnet}") }
        .map { p -> p.left || p.right }
    }

    /**
     *
     */
    static class Configuration {
        Duration blockTtl
        long maxRequestsFromIp
        long maxRequestsFromSubnet
        Duration ttl

        void setBlockTtl(String s) {
            this.blockTtl = DurationParser.parse(s)
        }

        void setTtl(String s) {
            this.ttl = DurationParser.parse(s)
        }
    }
}
