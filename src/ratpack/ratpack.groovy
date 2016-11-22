import jamesl.ratpack.usage.ApplicationModule
import jamesl.ratpack.usage.DefaultMemcache
import jamesl.ratpack.usage.MemcacheUsageMonitor
import jamesl.ratpack.usage.UsageMonitor
import ratpack.form.Form
import ratpack.handling.RequestLogger

import static ratpack.groovy.Groovy.ratpack

ratpack {
    serverConfig {
        yaml "development-config.yaml"
        require "/memcache", DefaultMemcache.Configuration
        require "/usage", MemcacheUsageMonitor.Configuration
    }
    bindings {
        module(ApplicationModule)
    }
    handlers {
        all(RequestLogger)
        path("test") { UsageMonitor usageMonitor ->
            parse(Form.form(true))
            .flatMap { parameters ->
                def remoteIp = request.remoteAddress.hostText

                usageMonitor.checkUsageIsWithinLimits(remoteIp)
                .route({ ok -> !ok }) { clientError(429) }
                .map { parameters }
            }
            .then { parameters ->
                render "OK"
            }
        }
    }
}
