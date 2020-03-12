package com.github.victormpcmun.delayedbatchexecutor;

import java.time.Duration;

public abstract class WindowTime {

    private DelayedBatchExecutor delayedBatchExecutor;

    void setDelayedBatchExecutor(DelayedBatchExecutor delayedBatchExecutor) {
        this.delayedBatchExecutor = delayedBatchExecutor;
    }

    public void updateBufferedTimeoutFlux(Duration duration, int size) {
        delayedBatchExecutor.updateBufferedTimeoutFlux(duration,size);
    }

    public abstract void startup();
    public abstract void invocation();
}
