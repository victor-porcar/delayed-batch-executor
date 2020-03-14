package com.github.victormpcmun.delayedbatchexecutor;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class TupleFuture<T> extends Tuple<T> implements Future<T> {

    private final Instant initInstant;
    private Instant endInstant;

    public static <T> TupleFuture<T>  create(Object... argsAsArray) {
        TupleFuture<T> tupleFuture = new TupleFuture<T>(argsAsArray);
        return tupleFuture;
    }

    public Future<T> getFuture() {
        return this;
    }

    private TupleFuture(Object... argsAsArray) {
        super(argsAsArray);
        this.initInstant= Instant.now();
    }

    public void commitResult() {
        super.commitResult();
        this.endInstant= Instant.now();
    }


    @Override
    public boolean isDone() {
        return super.isDone();
    }


    @Override
    public void continueIfIsWaiting() {
        synchronized (this) {
            this.notify();
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        try {
            return get(0L);
        } catch (TimeoutException te) {
            throw new RuntimeException("This RuntimeException should never thrown at this point.", te);
        }
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        long milliseconds =  TimeUnit.MILLISECONDS.convert(timeout, unit);
        return get(milliseconds);
    }


    private T get(long millisecondsWait) throws InterruptedException, ExecutionException, TimeoutException {
        synchronized (this) {
            if (!done) {
                if (millisecondsWait==0L) {
                    this.wait();
                } else {
                    this.wait(millisecondsWait);
                    if (!done) {
                        throw new TimeoutException("can not get the result in " + millisecondsWait);
                    }
                }
            }
        }
        if (hasRuntimeException()) {
            throw new ExecutionException(getRuntimeException());
        }
        return result;
    }

    public Duration getDelayedTime() {
        return Duration.between(initInstant,endInstant);
    }
}
