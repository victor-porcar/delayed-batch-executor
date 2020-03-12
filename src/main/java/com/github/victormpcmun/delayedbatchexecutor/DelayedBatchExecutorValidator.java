package com.github.victormpcmun.delayedbatchexecutor;

import java.time.Duration;
import java.util.concurrent.ExecutorService;

public class DelayedBatchExecutorValidator {

    protected static final int MAX_SIZE = 1024;
    protected static final Duration MAX_TIME = Duration.ofSeconds(10);
    private static final Duration MIN_TIME=Duration.ofMillis(1);

    static void validateBoundaries(int size, Duration time, ExecutorService executorService) {

        if (executorService==null) {
            throw new IllegalArgumentException("executorService can not be null");
        }

        if (size < 1 || size > MAX_SIZE)  {
            throw new IllegalArgumentException("max elements parameter must be in range ["+ 1 + ","+ MAX_SIZE + "]");
        }

        if (MAX_TIME.compareTo(time) < 0 || time.compareTo(MIN_TIME) < 0) {
            throw new IllegalArgumentException("time window parameter must be in range ["+ 1 + ","+ MAX_TIME.toMillis() + "] ms");
        }
    }

}
