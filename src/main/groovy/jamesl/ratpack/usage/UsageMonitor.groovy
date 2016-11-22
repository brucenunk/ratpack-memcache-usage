package jamesl.ratpack.usage

import ratpack.exec.Promise

/**
 * @author jamesl
 */
interface UsageMonitor {
    /**
     * Checks whether usage is within the defined limits.
     *
     * @param remoteIp
     * @return
     */
    Promise<Boolean> checkUsageIsWithinLimits(String remoteIp)
}