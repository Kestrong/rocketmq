package com.xjbg.rocketmq.util;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author: kesc
 * @time: 2017-12-19 20:05
 */
public class ThreadPoolUtils {
    private static final ThreadPoolExecutor POOL = create();

    public static ThreadPoolExecutor create() {
        return new ThreadPoolExecutor(Math.max(1, Runtime.getRuntime().availableProcessors() / 2),
                Runtime.getRuntime().availableProcessors(),
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1000),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public static void execute(Runnable runnable) {
        POOL.execute(runnable);
    }
}
