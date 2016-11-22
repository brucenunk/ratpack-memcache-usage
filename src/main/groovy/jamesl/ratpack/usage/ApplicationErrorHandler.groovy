package jamesl.ratpack.usage

import groovy.util.logging.Slf4j
import io.netty.handler.codec.http.HttpResponseStatus
import ratpack.error.ClientErrorHandler
import ratpack.error.ServerErrorHandler
import ratpack.handling.Context

import static ratpack.jackson.Jackson.json

/**
 * @author jamesl
 */
@Slf4j
class ApplicationErrorHandler implements ClientErrorHandler, ServerErrorHandler {
    @Override
    void error(Context context, int statusCode) throws Exception {
        def message = HttpResponseStatus.valueOf(statusCode).reasonPhrase()

        context.response.status(statusCode)
        context.render message
    }

    @Override
    void error(Context context, Throwable error) throws Exception {
        def message = error.message ?: error.class.name
        log.error("serverError: $message, uri: $context.request.uri", error)

        context.response.status(503)
        context.render error.class.simpleName
    }
}
