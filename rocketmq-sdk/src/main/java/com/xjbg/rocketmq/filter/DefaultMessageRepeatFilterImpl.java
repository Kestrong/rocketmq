package com.xjbg.rocketmq.filter;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.concurrent.TimeUnit;

/**
 * @author kesc
 * @since 2019/4/4
 */
public class DefaultMessageRepeatFilterImpl implements MessageRepeatFilter {
    private final LoadingCache<String, Integer> cache = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.HOURS)
            .build(new CacheLoader<String, Integer>() {
                @Override
                public Integer load(String key) throws Exception {
                    return null;
                }
            });

    @Override
    public boolean isRepeat(String msgKey) {
        synchronized (this) {
            if (cache.getIfPresent(msgKey) != null) {
                return true;
            }
            cache.put(msgKey, 1);
        }
        return false;
    }

    @Override
    public void reset(String msgKey) {
        cache.invalidate(msgKey);
    }
}
