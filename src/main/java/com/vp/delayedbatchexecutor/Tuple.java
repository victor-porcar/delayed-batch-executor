package com.vp.delayedbatchexecutor;

class Tuple {

    private boolean resultCommitted;
    private Object result;
    private final Object[] argsAsArray;


    Tuple(Object... argsAsArray) {
        super();
        this.result = null;
        this.resultCommitted = false;
        this.argsAsArray = argsAsArray;
    }

    int getArgsSize() {
        return argsAsArray.length;
    }


    Object getResult() {
        return result;
    }

    Object getArgumentByPosition(int argPosition) {
        return argsAsArray[argPosition];
    }


    void waitIfResultHasNotCommitted() {
        synchronized (this) {
            if (!resultCommitted) {
                try {
                    this.wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException("Interrupted waiting.  it shouldn't happen ever", e);
                }
            }
        }
    }

    void continueIfIsWaiting() {
        synchronized (this) {
            this.notify();
        }
    }

    void setResult(Object result) {
        this.result = result;
    }

    void commitResult() {
        synchronized (this) {
            this.resultCommitted = true;
        }
    }
}
