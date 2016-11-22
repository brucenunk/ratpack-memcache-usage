package jamesl.ratpack.usage;

import com.google.common.net.HostAndPort;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.pool.*;
import io.netty.handler.codec.memcache.binary.*;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.exec.Downstream;
import ratpack.exec.ExecController;
import ratpack.exec.Operation;
import ratpack.exec.Promise;
import ratpack.func.Action;
import ratpack.func.Function;
import ratpack.util.internal.ChannelImplDetector;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author jamesl
 */
public class DefaultMemcache implements Memcache {
    private static final Logger logger = LoggerFactory.getLogger(DefaultMemcache.class);
    private final ByteBufAllocator allocator;
    private final Bootstrap bootstrap;
    private final ChannelPoolMap<SocketAddress, ChannelPool> channelPools;
    private final Duration readTimeout;
    private final InetSocketAddress remoteHost;

    @Inject
    public DefaultMemcache(Configuration configuration, ExecController exec, ByteBufAllocator allocator) {
        this.allocator = allocator;
        this.bootstrap = new Bootstrap()
                .option(ChannelOption.ALLOCATOR, allocator)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) configuration.connectTimeout.toMillis())
                .channel(ChannelImplDetector.getSocketChannelImpl())
                .group(exec.getEventLoopGroup());

        this.channelPools = new AbstractChannelPoolMap<SocketAddress, ChannelPool>() {
            @Override
            protected ChannelPool newPool(SocketAddress remoteHost) {
                ChannelPoolHandler channelPoolHandler = new AbstractChannelPoolHandler() {
                    @Override
                    public void channelCreated(Channel ch) throws Exception {
                        ch.pipeline().addLast("codec", new BinaryMemcacheClientCodec());
                        ch.pipeline().addLast("aggregator", new BinaryMemcacheObjectAggregator(Integer.MAX_VALUE));
                        ch.pipeline().addLast("responseHandler", new ResponseHandler());
                        logger.trace("created channel={}", ch);
                    }

                    @Override
                    public void channelReleased(Channel ch) throws Exception {
                        ReadTimeoutHandler readTimeoutHandler = ch.pipeline().get(ReadTimeoutHandler.class);
                        if (readTimeoutHandler != null) {
                            ch.pipeline().remove(readTimeoutHandler);
                        }
                        logger.trace("released channel={}", ch);
                    }
                };

                return new FixedChannelPool(bootstrap.remoteAddress(remoteHost), channelPoolHandler, configuration.pool);
            }
        };

        HostAndPort parsed = HostAndPort.fromString(configuration.remoteHost);
        this.readTimeout = configuration.readTimeout;
        this.remoteHost = new InetSocketAddress(parsed.getHostText(), parsed.getPort());
    }

    /**
     * Attempts to perform a "add" operation".
     *
     * @param key    the memcache item's key.
     * @param ttl             the time-to-live for this value.
     * @param valueFactory    a function that will yield the item's "value".
     * @return
     */
    @Override
    public Promise<Boolean> add(String key, Duration ttl, Function<ByteBufAllocator, ByteBuf> valueFactory) {
        return request(key, spec -> {
            spec.extras(allocator.buffer(8).writeZero(4).writeInt(asTtl(ttl)));
            spec.op(BinaryMemcacheOpcodes.ADD);
            spec.value(valueFactory.apply(allocator));
        })
        .flatMap(response -> {
            return Promise.<Boolean>async(downstream -> {
                switch (response.status()) {
                    case BinaryMemcacheResponseStatus.SUCCESS:
                        downstream.success(true);
                        break;
                    case BinaryMemcacheResponseStatus.KEY_EEXISTS:
                        downstream.success(false);
                        break;
                    default:
                        downstream.error(new MemcacheException(response.content().toString(StandardCharsets.UTF_8)));
                }
            })
            .wiretap(r -> response.release());
        });
    }

    /**
     * Attempts to perform a "decrement" operation using {@code 1L} as the "delta".
     *
     * @param key    the memcache item's key.
     * @param ttl             the time-to-live for this value.
     * @param initial the initial value to use as a seed if the key is not found.
     * @return
     */
    @Override
    public Promise<Long> decrement(String key, Duration ttl, long initial) {
        return request(key, spec -> {
            long delta = 1L;
            spec.extras(allocator.buffer(20).writeLong(delta).writeLong(initial).writeInt(asTtl(ttl)));
            spec.op(BinaryMemcacheOpcodes.DECREMENT);
        })
        .flatMap(response -> {
            return Promise.<Long>async(downstream -> {
                if (response.status() == BinaryMemcacheResponseStatus.SUCCESS) {
                    downstream.success(response.content().readLong());
                } else {
                    downstream.error(new MemcacheException(response.content().toString(StandardCharsets.UTF_8)));
                }
            })
            .wiretap(r -> response.release());
        });
    }

    /**
     * Performs a "get" operation and returns whether the key was found.
     *
     * @param key
     * @return
     */
    @Override
    public Promise<Boolean> exists(String key) {
        return request(key, spec -> {
            spec.op(BinaryMemcacheOpcodes.GET);
        })
        .flatMap(response -> {
            return Promise.<Boolean>async(downstream -> {
                switch (response.status()) {
                    case BinaryMemcacheResponseStatus.SUCCESS:
                        downstream.success(true);
                        break;
                    case BinaryMemcacheResponseStatus.KEY_ENOENT:
                        downstream.success(false);
                        break;
                    default:
                        downstream.error(new MemcacheException(response.content().toString(StandardCharsets.UTF_8)));
                }
            })
            .wiretap(r -> response.release());
        });
    }

    /**
     * Attempts to perform a "get" operation and return the mapped response.
     *
     * @param key    the memcache item's key.
     * @param mapper a mapper for converting from {@link ByteBuf} to type {@code T}.
     * @param <T>
     * @return
     */
    @Override
    public <T> Promise<T> get(String key, Function<ByteBuf, T> mapper) {
        return request(key, spec -> {
            spec.op(BinaryMemcacheOpcodes.GET);
        })
        .flatMap(response -> {
            return Promise.<T>async(downstream -> {
                switch (response.status()) {
                    case BinaryMemcacheResponseStatus.SUCCESS:
                        downstream.success(mapper.apply(response.content()));
                        break;
                    case BinaryMemcacheResponseStatus.KEY_ENOENT:
                        downstream.success(null);
                        break;
                    default:
                        downstream.error(new MemcacheException(response.content().toString(StandardCharsets.UTF_8)));
                }
            })
            .wiretap(r -> response.release());
        });
    }

    /**
     * Attempts to perform a "increment" operation using {@code 1L} as the "delta".
     *
     * @param key    the memcache item's key.
     * @param ttl             the time-to-live for this value.
     * @param initial the initial value to use as a seed if the key is not found.
     * @return
     */
    @Override
    public Promise<Long> increment(String key, Duration ttl, long initial) {
        return request(key, spec -> {
            long delta = 1L;
            spec.extras(allocator.buffer(20).writeLong(delta).writeLong(initial).writeInt(asTtl(ttl)));
            spec.op(BinaryMemcacheOpcodes.INCREMENT);
        })
        .flatMap(response -> {
            return Promise.<Long>async(downstream -> {
                if (response.status() == BinaryMemcacheResponseStatus.SUCCESS) {
                    downstream.success(response.content().readLong());
                } else {
                    downstream.error(new MemcacheException(response.content().toString(StandardCharsets.UTF_8)));
                }
            })
            .wiretap(r -> response.release());
        });
    }

    /**
     * Attempts to perform a "set" operation".
     *
     * @param key    the memcache item's key.
     * @param ttl             the time-to-live for this value.
     * @param valueFactory    a function that will yield the item's "value".
     * @return
     */
    @Override
    public Operation set(String key, Duration ttl, Function<ByteBufAllocator, ByteBuf> valueFactory) {
        return request(key, spec -> {
            spec.extras(allocator.buffer(8).writeZero(4).writeInt(asTtl(ttl)));
            spec.op(BinaryMemcacheOpcodes.SET);
            spec.value(valueFactory.apply(allocator));
        })
        .operation(response -> response.release());
    }

    /**
     * Returns {@code ttl} as an {@code int} so that it can be used as a "ttl" for memcache values.
     *
     * @param ttl
     * @return
     */
    private int asTtl(Duration ttl) {
        return (int) (ttl.toMillis() / 1000);
    }

    /**
     * @param key
     * @param configurer
     * @return
     */
    private Promise<MemcacheResponse> request(String key, Action<RequestSpec> configurer) {
        ChannelPool channelPool = channelPools.get(remoteHost);

        return Promise.<Channel>async(downstream -> {
            channelPool.acquire().addListener(f -> {
                if (f.isSuccess()) {
                    downstream.success((Channel) f.getNow());
                } else {
                    downstream.error(f.cause());
                }
            });
        })
        .flatMap(ch -> {
            return Promise.<MemcacheResponse>async(downstream -> {
                AtomicBoolean complete = new AtomicBoolean(false);

                ch.attr(ResponseHandler.CHANNEL_POOL).set(channelPool);
                ch.attr(ResponseHandler.COMPLETE).set(complete);
                ch.attr(ResponseHandler.DOWNSTREAM).set(downstream);

                DefaultRequestSpec spec = new DefaultRequestSpec(allocator, key);
                configurer.execute(spec);
                FullBinaryMemcacheRequest req = spec.req();

                if (logger.isDebugEnabled()) {
                    logger.debug("request> op={}, k={}, v={}.", req.opcode(), req.key().toString(StandardCharsets.UTF_8),
                            req.content().toString(StandardCharsets.UTF_8));
                }

                ch.writeAndFlush(req).addListener(f -> {
                    if (f.isSuccess()) {
                        long ms = readTimeout.toMillis();
                        if (ms > 0) {
                            ch.pipeline().addBefore("responseHandler", "readTimeoutHandler", new ReadTimeoutHandler(ms, TimeUnit.MILLISECONDS));
                        }
                    } else {
                        if (complete.compareAndSet(false, true)) {
                            downstream.error(f.cause());
                        }
                    }
                });
            })
            .wiretap(r -> {
                if (r.isError()) {
                    ch.close();
                    channelPool.release(ch);
                }
            });
        });
    }

    /**
     *
     */
    private static class MemcacheResponse {
        private final ChannelPool channelPool;
        private final Channel channel;
        private final FullBinaryMemcacheResponse message;

        public MemcacheResponse(ChannelPool channelPool, Channel channel, FullBinaryMemcacheResponse message) {
            this.channelPool = channelPool;
            this.channel = channel;
            this.message = message;
        }

        public ByteBuf content() {
            return message.content();
        }

        public void release() {
            logger.trace("releasing message={},ref={}", message, message.refCnt());
            message.release();
            logger.trace("releasing channel={},channelPool={}", channel, channelPool);
            channelPool.release(channel);
        }

        public short status() {
            return message.status();
        }
    }

    /**
     *
     */
    private static class ResponseHandler extends SimpleChannelInboundHandler<FullBinaryMemcacheResponse> {
        static final AttributeKey<ChannelPool> CHANNEL_POOL = AttributeKey.valueOf(ResponseHandler.class, "channelPool");
        static final AttributeKey<AtomicBoolean> COMPLETE = AttributeKey.valueOf(AtomicBoolean.class, "complete");
        static final AttributeKey<Downstream<? super MemcacheResponse>> DOWNSTREAM = AttributeKey.valueOf(ResponseHandler.class, "downstream");

        ResponseHandler() {
            super(false);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullBinaryMemcacheResponse message) {
            Downstream<? super MemcacheResponse> downstream = downstream(ctx);

            if (logger.isDebugEnabled()) {
                logger.debug("response> status={}, content={}", message.status(), message.content().toString(StandardCharsets.UTF_8));
            }

            if (downstream != null) {
                if (canComplete(ctx)) {
                    ChannelPool channelPool = ctx.channel().attr(CHANNEL_POOL).get();
                    MemcacheResponse response = new MemcacheResponse(channelPool, ctx.channel(), message);
                    downstream.success(response);
                    return;
                }
            }

            message.release();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            Downstream<? super MemcacheResponse> downstream = downstream(ctx);

            if (downstream != null) {
                if (canComplete(ctx)) {
                    downstream.error(cause);
                }
            }

            ctx.close();
        }

        private boolean canComplete(ChannelHandlerContext ctx) {
            return ctx.channel().attr(COMPLETE).get().compareAndSet(false, true);
        }

        private Downstream<? super MemcacheResponse> downstream(ChannelHandlerContext ctx) {
            return ctx.channel().attr(DOWNSTREAM).get();
        }
    }

    /**
     *
     */
    private interface RequestSpec {
        void extras(ByteBuf x);
        void op(byte op);
        void value(ByteBuf v);
    }

    /**
     *
     */
    private static class DefaultRequestSpec implements RequestSpec {
        private final ByteBuf k;
        private byte op;
        private ByteBuf v;
        private ByteBuf x;

        public DefaultRequestSpec(ByteBufAllocator allocator, String key) {
            this.x = Unpooled.EMPTY_BUFFER;
            this.k = allocator.buffer(key.length()).writeBytes(key.getBytes());
            this.v = Unpooled.EMPTY_BUFFER;
        }

        @Override
        public void extras(ByteBuf x) {
            this.x = x;
        }

        @Override
        public void op(byte op) {
            this.op = op;
        }

        @Override
        public void value(ByteBuf v) {
            this.v = v;
        }

        FullBinaryMemcacheRequest req() {
            DefaultFullBinaryMemcacheRequest req = new DefaultFullBinaryMemcacheRequest(k, x, v);
            req.setOpcode(op);
            return req;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("DefaultRequestSpec{");
            sb.append("k=").append(k);
            sb.append(", op=").append(op);
            sb.append(", v=").append(v);
            sb.append(", x=").append(x);
            sb.append('}');
            return sb.toString();
        }
    }

    /**
     *
     */
    public static class Configuration {
        Duration connectTimeout;
        int pool;
        Duration readTimeout;
        String remoteHost;

        public void setConnectTimeout(String s) {
            this.connectTimeout = DurationParser.parse(s);
        }

        public void setPool(int pool) {
            this.pool = pool;
        }

        public void setReadTimeout(String s) {
            this.readTimeout = DurationParser.parse(s);
        }
        public void setRemoteHost(String remoteHost) {
            this.remoteHost = remoteHost;
        }
    }
}
