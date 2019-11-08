package com.vp.delayedbatchexecutor;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class Tuple<T> implements Future<T> {

    private boolean done;
    private T result;
    private final Object[] argsAsArray;

    Tuple(Object... argsAsArray) {
        super();
        this.result = null;
        this.done = false;
        this.argsAsArray = argsAsArray;
    }

    int getArgsSize() {
        return argsAsArray.length;
    }


    @Override
    public T get() throws InterruptedException, ExecutionException {
        synchronized (this) {
            if (!done) {
                this.wait();
            }
        }
        return result;
    }

    @Override
    public boolean isDone() {
        return done;
    }

    Object getArgumentByPosition(int argPosition) {
        return argsAsArray[argPosition];
    }

    void continueIfIsWaiting() {
        synchronized (this) {
            this.notify();
        }
    }

    void setResult(T result) {
        this.result = result;
    }

    void commitResult() {
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
