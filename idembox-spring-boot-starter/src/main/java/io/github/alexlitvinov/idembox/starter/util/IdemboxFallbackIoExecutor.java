package io.github.alexlitvinov.idembox.starter.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Dedicated bounded fallback executor for blocking JDBC / I/O operations.
 * Isolates background offload tasks from {@link java.util.concurrent.ForkJoinPool#commonPool()}
 * when primary task executors are saturated or rejected.
 */
public final class IdemboxFallbackIoExecutor {

    private static final Logger log = LoggerFactory.getLogger(IdemboxFallbackIoExecutor.class);
    private static final AtomicInteger THREAD_COUNTER = new AtomicInteger(0);

    private static final ThreadPoolExecutor INSTANCE = new ThreadPoolExecutor(
            2,
            6,
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(200),
            r -> {
                Thread t = new Thread(r, "idembox-fallback-io-" + THREAD_COUNTER.incrementAndGet());
                t.setDaemon(true);
                return t;
            },
            (r, executor) -> log.warn("IdemboxFallbackIoExecutor queue saturated (200 tasks). Task dropped to preserve JVM stability.")
    );

    static {
        INSTANCE.allowCoreThreadTimeOut(true);
    }

    private IdemboxFallbackIoExecutor() {
    }

    public static void execute(Runnable task) {
        if (task != null) {
            INSTANCE.execute(task);
        }
    }
}
