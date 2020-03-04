package com.github.victormpcmun.delayedbatchexecutor.tuple;

public abstract class Tuple<T>  {

    protected boolean done;
    protected T result;
    protected final Object[] argsAsArray;

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

    public void setResult(T result) {
        this.result = result;
    }


    public void commitResult() {
        synchronized (this) {
            this.done = true;
        }
    }

    public boolean isDone() {
        return done;
    }


    public abstract void continueIfIsWaiting();
}
