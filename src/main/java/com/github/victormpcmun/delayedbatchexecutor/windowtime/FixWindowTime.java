package com.github.victormpcmun.delayedbatchexecutor.windowtime;

import com.github.victormpcmun.delayedbatchexecutor.WindowTime;

import java.time.Duration;


public class FixWindowTime extends WindowTime {


    protected static final int MAX_SIZE = 1024;
    protected static final Duration MAX_TIME = Duration.ofSeconds(10);
    private static final Duration MIN_TIME=Duration.ofMillis(1);

    private Duration duration;
    private int size;

    private FixWindowTime(Duration duration, int size) {
        validateBoundaries(duration, size);
        this.duration = duration;
        this.size = size;
    }

    public static FixWindowTime create(Duration duration, int size) {
        return new FixWindowTime(duration, size);
    }

    @Override
    public void startup() {
        updateBufferedTimeoutFlux(duration, size);
    }

    @Override
    public void invocation() {
        // do nothing
    }


    private void validateBoundaries( Duration time, int size) {
        if (size < 1 || size > MAX_SIZE)  {
            throw new IllegalArgumentException("max elements parameter must be in range ["+ 1 + ","+ MAX_SIZE + "]");
        }

        if (MAX_TIME.compareTo(time) < 0 || time.compareTo(MIN_TIME) < 0) {
            throw new IllegalArgumentException("time window parameter must be in range ["+ 1 + ","+ MAX_TIME.toMillis() + "] ms");
        }
    }
}
