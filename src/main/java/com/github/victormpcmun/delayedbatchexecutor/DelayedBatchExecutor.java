package com.github.victormpcmun.delayedbatchexecutor;

import reactor.core.publisher.Flux;
import reactor.core.publisher.UnicastProcessor;

import java.time.Duration;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

abstract class DelayedBatchExecutor {

    private static final int DEFAULT_FIXED_THREAD_POOL_COUNTER = 10;
    private static final int QUEUE_SIZE = 8192; // max elements queued

    private final AtomicLong invocationsCounter = new AtomicLong(0);
    private final AtomicLong callBackExecutionsCounter = new AtomicLong(0);

    private Duration duration;
    private Integer size;

	private  UnicastProcessor<Tuple> source;
    private ExecutorService executorService;

    protected DelayedBatchExecutor(Duration duration, int size, ExecutorService executorService) {
        super();
        updateConfig(duration,size, executorService);
    }

    /**
     * Update the configuration params of this Delayed Batch Executor
     * It could be invoked at anytime.
     * <br>
     * @param  duration  the new {@link Duration} for this Delayed Batch Executor
     * @param  size  the new size
     *
     */
    public  void updateConfig(Duration duration, int size) {
        updateConfig(duration, size, executorService);
    }

    /**
     * Update the configuration params of this Delayed Batch Executor
     * It could be invoked at anytime.
     * <br>
     * @param  duration  the new {@link Duration} for this Delayed Batch Executor
     * @param  size  the new size
     * @param  executorService  the new {@link ExecutorService}
     *
     */
    public synchronized void updateConfig(Duration duration, int size, ExecutorService executorService) {
        boolean sameDuration = this.duration!=null && this.duration.compareTo(duration)==0;
        boolean sameSize = this.size!=null && this.size.compareTo(size)==0;
        boolean sameExecutorService = this.executorService!=null && this.executorService==executorService;
        boolean sameParameters=sameDuration && sameSize && sameExecutorService;

        if (!sameParameters) {

            this.duration=duration;
            this.size=size;
            this.executorService=executorService;

            Queue<Tuple> blockingQueue =  new ArrayBlockingQueue<>(QUEUE_SIZE) ; // => https://github.com/reactor/reactor-core/issues/469
            this.source  = UnicastProcessor.create(blockingQueue);
            Flux<Tuple> flux = source.publish().autoConnect();
            flux.bufferTimeout(size, duration).subscribe(this::executeList);
        }

    }

    protected abstract List<Object> getResultListFromCallBack(List<List<Object>>  transposedTupleList);

    private CallBackExecutionResult getExecutionResultFromCallback(List<Tuple> tupleList) {
        List<Object> resultFromCallBack=null;
        RuntimeException runtimeException=null;

        List<List<Object>> transposedTupleList = TupleListTransposer.transpose(tupleList);
        try {
             resultFromCallBack = getResultListFromCallBack(transposedTupleList);
        } catch (RuntimeException re) {
            runtimeException=re;
        }
        return new CallBackExecutionResult(resultFromCallBack, runtimeException, tupleList.size());
    }

    private void executeList(List<Tuple> tupleList) {
        callBackExecutionsCounter.incrementAndGet();
        CompletableFuture.runAsync(() -> {
            callBackExecutionsCounter.incrementAndGet();
            CallBackExecutionResult callBackExecutionResult = getExecutionResultFromCallback(tupleList);

            for (int index=0; index<tupleList.size(); index++) {
                Tuple tuple = tupleList.get(index);
                tuple.setResult(callBackExecutionResult.getReturnedResultOrNull(index));
                tuple.setRuntimeException(callBackExecutionResult.getThrownRuntimeExceptionOrNull());
                tuple.commitResult();
                tuple.continueIfIsWaiting();
            }
         }, this.executorService);
    }

   <Z> void enlistTuple(Tuple<Z> param) {
       invocationsCounter.incrementAndGet();
       source.onNext(param);
    }



    /**
     * The count of invocations of all blocking, Future and Mono invocations since the creation of this Delayed Batch Executor
     * @return  the count of invocations of all blocking, Future and Mono invocations since the creation of this Delayed Batch Executor
     *
     */
    public Long getInvocationsCounter() {
        return invocationsCounter.get();
    }

    /**
     * The count of executions of the batchCallBack method since the creation of this Delayed Batch Executor
     * @return  the count of executions of the batchCallBack method since the creation of this Delayed Batch Executor
     *
     */
    public Long getCallBackExecutionsCounter() {
        return callBackExecutionsCounter.get();
    }

    /**
     * The current {@link Duration} of this Delayed Batch Executor
     * @return  the current {@link Duration} of this Delayed Batch Executor
     *
     */
    public Duration getDuration() {
        return duration;
    }

    /**
     * The current size of this Delayed Batch Executor
     * @return  the current size of this Delayed Batch Executor
     *
     */
    public Integer getSize() {
        return size;
    }

    protected static ExecutorService getNewDefaultExecutorService() {
        return Executors.newFixedThreadPool(DEFAULT_FIXED_THREAD_POOL_COUNTER);
    }

    @Override
    public String toString() {
        return "DelayedBatchExecutor {" +
                "invocationsCounter=" + invocationsCounter +
                ", callBackExecutionsCounter=" + callBackExecutionsCounter +
                ", duration=" + duration.toMillis() +
                ", size=" + size +
                '}';
    }
}