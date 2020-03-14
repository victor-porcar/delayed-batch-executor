package com.github.victormpcmun.delayedbatchexecutor;

import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class TupleFuture<T> extends Tuple<T> implements Future<T> {

    public static <T> TupleFuture<T>  create(Object... argsAsArray) {
        TupleFuture tupleFuture = new TupleFuture(argsAsArray);
        return tupleFuture;
    }


    public Future<T> getFuture() {
        return this;
    }


    private TupleFuture(Object... argsAsArray) {
        super(argsAsArray);
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        try {
            return get(0l);
        } catch (TimeoutException te) {
            throw (InterruptedException) te.getCause();
        }
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
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException {
        long milliseconds =  (long) TimeUnit.MILLISECONDS.convert(timeout, unit);
        try {
            return get(milliseconds);
        } catch (TimeoutException te) {
            throw (InterruptedException) te.getCause();
        }
    }


    private T get(long millisecondsWait) throws InterruptedException, ExecutionException, TimeoutException {
        synchronized (this) {
            if (!done) {
                long initTime = Instant.now().toEpochMilli();
                try {
                    this.wait(millisecondsWait);
                } catch (InterruptedException ie)  {
                    long now = Instant.now().toEpochMilli();
                    long difference = now - initTime;
                    if (difference>=millisecondsWait) {
                        throw new TimeoutException("can not get the result in " + difference);
                    }
                    throw ie;
                }
            }
        }
        if (hasRuntimeException()) {
            throw new ExecutionException(getRuntimeException());
        }
        return result;
    }
}
