package jamesl.ratpack.usage

import com.google.inject.AbstractModule
import com.google.inject.Scopes
import ratpack.error.ClientErrorHandler
import ratpack.error.ServerErrorHandler
import ratpack.handling.RequestLogger

/**
 * @author jamesl
 */
class ApplicationModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(ClientErrorHandler).to(ApplicationErrorHandler).in(Scopes.SINGLETON)
        bind(Memcache).to(DefaultMemcache).in(Scopes.SINGLETON)
        bind(RequestLogger).toInstance(RequestLogger.ncsa())
        bind(ServerErrorHandler).to(ApplicationErrorHandler).in(Scopes.SINGLETON)
        bind(UsageMonitor).to(MemcacheUsageMonitor).in(Scopes.SINGLETON)
    }
}
