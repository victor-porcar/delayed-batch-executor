package com.github.victormpcmun.delayedbatchexecutor;

import java.time.Duration;
import java.time.Instant;

abstract class Tuple<T>  {

    protected boolean done;
    protected T result;
    protected final Object[] argsAsArray;

    protected RuntimeException runtimeException;

    private Instant initInstant;
    private Instant endInstant;


    Tuple(Object... argsAsArray) {
        super();
        this.result = null;
        this.done = false;
        this.argsAsArray = argsAsArray;
        this.initInstant= Instant.now();
    }

    int getArgsSize() {
        return argsAsArray.length;
    }

    Object getArgumentByPosition(int argPosition) {
        return argsAsArray[argPosition];
    }

    public void setResult(T result) {
        this.result = result;
    }


    public void commitResult() {
        synchronized (this) {
            this.done = true;
            this.endInstant= Instant.now();
        }
    }

    public boolean isDone() {
        return done;
    }


    public abstract void continueIfIsWaiting();

    public RuntimeException getRuntimeException() {
        return runtimeException;
    }

    public void setRuntimeException(RuntimeException runtimeException) {
        this.runtimeException = runtimeException;
    }

    public boolean hasRuntimeException() {
        return runtimeException!=null;
    }

    public Duration getDelayedTime() {
        return Duration.between(initInstant,endInstant);
    }
}
