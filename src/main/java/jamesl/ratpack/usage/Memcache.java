package jamesl.ratpack.usage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import ratpack.exec.Operation;
import ratpack.exec.Promise;
import ratpack.func.Function;

import java.time.Duration;

/**
 * @author jamesl
 */
public interface Memcache {
    /**
     * Attempts to perform a "add" operation".
     *
     * @param key    the memcache item's key.
     * @param ttl             the time-to-live for this value.
     * @param valueFactory    a function that will yield the item's "value".
     * @return
     */
    Promise<Boolean> add(String key, Duration ttl, Function<ByteBufAllocator, ByteBuf> valueFactory);

    /**
     * Attempts to perform a "decrement" operation using {@code 1L} as the "delta".
     *
     * @param key    the memcache item's key.
     * @param ttl             the time-to-live for this value.
     * @param initial the initial value to use as a seed if the key is not found.
     * @return
     */
    Promise<Long> decrement(String key, Duration ttl, long initial);

    /**
     * Performs a "get" operation and returns whether the key was found.
     *
     * @param key
     * @return
     */
    Promise<Boolean> exists(String key);

    /**
     * Attempts to perform a "get" operation and return the mapped response.
     *
     * @param key    the memcache item's key.
     * @param mapper a mapper for converting from {@link ByteBuf} to type {@code T}.
     * @param <T>
     * @return
     */
    <T> Promise<T> get(String key, Function<ByteBuf, T> mapper);

    /**
     * Attempts to perform a "increment" operation using {@code 1L} as the "delta".
     *
     * @param key    the memcache item's key.
     * @param ttl             the time-to-live for this value.
     * @param initial the initial value to use as a seed if the key is not found.
     * @return
     */
    Promise<Long> increment(String key, Duration ttl, long initial);

    /**
     * Attempts to perform a "set" operation".
     *
     * @param key    the memcache item's key.
     * @param ttl             the time-to-live for this value.
     * @param valueFactory    a function that will yield the item's "value".
     * @return
     */
    Operation set(String key, Duration ttl, Function<ByteBufAllocator, ByteBuf> valueFactory);
}
