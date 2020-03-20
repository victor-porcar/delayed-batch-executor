package com.github.victormpcmun.delayedbatchexecutor;

abstract class Tuple<T>  {

    protected boolean done;
    protected T result;
    protected final Object[] argsAsArray;

    protected RuntimeException runtimeException;

    Tuple(Object... argsAsArray) {
        super();
        this.result = null;
        this.done = false;
        this.argsAsArray = argsAsArray;

    }

    int getArgsSize() {
        return argsAsArray.length;
    }

    Object getArgumentByPosition(int argPosition) {
        return argsAsArray[argPosition];
    }

    void setResult(T result) {
        this.result = result;
    }


    void commitResult() {
        synchronized (this) {
            this.done = true;
        }
    }

    boolean isDone() {
        return done;
    }

    abstract void continueIfIsWaiting();

    RuntimeException getRuntimeException() {
        return runtimeException;
    }

    void setRuntimeException(RuntimeException runtimeException) {
        this.runtimeException = runtimeException;
    }

    boolean hasRuntimeException() {
        return runtimeException!=null;
    }
}
