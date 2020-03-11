package com.github.victormpcmun.delayedbatchexecutor;

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
        synchronized (this) {
            if (!done) {
                this.wait();
            }
        }
        if (hasRuntimeException()) {
            throw new ExecutionException(getRuntimeException());
        }
        return result;
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
    public void commitResult() {
        synchronized (this) {
            this.done = true;
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
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        throw new UnsupportedOperationException();
    }
}
