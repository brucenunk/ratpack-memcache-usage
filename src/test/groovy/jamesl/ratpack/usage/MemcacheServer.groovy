package jamesl.ratpack.usage

import groovy.util.logging.Slf4j
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.*
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.memcache.binary.*
import ratpack.util.internal.ChannelImplDetector

import java.nio.charset.StandardCharsets
import java.util.function.Function

/**
 * @author jamesl
 */
@Slf4j
class MemcacheServer {
    EventLoopGroup eventLoopGroup
    Channel serverChannel

    MemcacheServer(EventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = eventLoopGroup
    }

    InetSocketAddress getRemoteHost() {
        serverChannel.localAddress() as InetSocketAddress
    }

    void start() {
        serverChannel = new ServerBootstrap()
                .channel(ChannelImplDetector.serverSocketChannelImpl)
                .group(eventLoopGroup)
                .childHandler(new ChannelInitializer<SocketChannel>(){
                    void initChannel(SocketChannel channel) throws Exception {
                        channel.pipeline().addLast("codec", new BinaryMemcacheServerCodec())
                        channel.pipeline().addLast("aggregator", new BinaryMemcacheObjectAggregator(Integer.MAX_VALUE))
                        channel.pipeline().addLast("handler", new RequestHandler())
                    }
                })
                .bind("localhost", 0)
                .sync()
                .channel()
    }

    void stop() {
        serverChannel.close().sync()
    }

    /**
     *
     */
    private static class RequestHandler extends SimpleChannelInboundHandler<FullBinaryMemcacheRequest> {
        Map<BinaryMemcacheOpcodes, Function<FullBinaryMemcacheRequest, FullBinaryMemcacheResponse>> handlers
        Map<String, ByteBuf> items

        RequestHandler() {
            this.handlers = new HashMap<>()
            this.items = new HashMap<>()
        }

        void channelRead0(ChannelHandlerContext ctx, FullBinaryMemcacheRequest req) {
            def k = req.key().toString(StandardCharsets.UTF_8)

            FullBinaryMemcacheResponse response
            switch (req.opcode()) {
                case BinaryMemcacheOpcodes.ADD:
                    short status
                    if (items.containsKey(k)) {
                        status = BinaryMemcacheResponseStatus.KEY_EEXISTS
                    } else {
                        items.put(k, Unpooled.buffer(req.content().readableBytes()).writeBytes(req.content()))
                        status = BinaryMemcacheResponseStatus.SUCCESS
                    }

                    response = new DefaultFullBinaryMemcacheResponse(Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER)
                    response.setStatus(status)
                    break;
                case BinaryMemcacheOpcodes.DECREMENT:
                    def x = req.extras()
                    def delta = x.readLong()
                    def initial = x.readLong()

                    long value
                    if (items.containsKey(k)) {
                        value = Long.parseLong(items.get(k).toString(StandardCharsets.UTF_8)) - delta
                    } else {
                        value = initial
                    }

                    items.put(k, Unpooled.buffer().writeBytes(String.valueOf(value).bytes))

                    def v = Unpooled.buffer(8).writeLong(value)
                    response = new DefaultFullBinaryMemcacheResponse(Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER, v)
                    response.setStatus(BinaryMemcacheResponseStatus.SUCCESS)
                    break;
                case BinaryMemcacheOpcodes.GET:
                    def v = items.get(k)

                    if (v != null) {
                        response = new DefaultFullBinaryMemcacheResponse(Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER, v)
                        response.setStatus(BinaryMemcacheResponseStatus.SUCCESS)
                    } else {
                        response = new DefaultFullBinaryMemcacheResponse(Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER)
                        response.setStatus(BinaryMemcacheResponseStatus.KEY_ENOENT)
                    }

                    break;
                case BinaryMemcacheOpcodes.INCREMENT:
                    def x = req.extras()
                    def delta = x.readLong()
                    def initial = x.readLong()

                    long value
                    if (items.containsKey(k)) {
                        value = Long.parseLong(items.get(k).toString(StandardCharsets.UTF_8)) + delta
                    } else {
                        value = initial
                    }

                    items.put(k, Unpooled.buffer().writeBytes(String.valueOf(value).bytes))

                    def v = Unpooled.buffer(8).writeLong(value)
                    response = new DefaultFullBinaryMemcacheResponse(Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER, v)
                    response.setStatus(BinaryMemcacheResponseStatus.SUCCESS)
                    break;
                case BinaryMemcacheOpcodes.SET:
                    items.put(k, Unpooled.buffer(req.content().readableBytes()).writeBytes(req.content()))

                    response = new DefaultFullBinaryMemcacheResponse(Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER)
                    response.setStatus(BinaryMemcacheResponseStatus.SUCCESS)
                    break;
                default:
                    response = new DefaultFullBinaryMemcacheResponse(Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER)
                    break;
            }
            log.debug("request : op={},k={}", req.opcode(), k)
            log.debug("response : status={},content={}", response.status(), response.content())
            ctx.writeAndFlush(response)
        }
    }
}
