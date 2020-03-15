package com.github.victormpcmun.delayedbatchexecutor;

import reactor.core.publisher.UnicastProcessor;

import java.time.Duration;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

abstract class DelayedBatchExecutor {

    public static final int MIN_TIME_WINDOW_TIME_IN_MILLISECONDS=1;
    public static final int MAX_TIME_WINDOW_TIME_IN_MILLISECONDS=60*60*1000;

    private static final int DEFAULT_FIXED_THREAD_POOL_COUNTER = 10;
    private static final int QUEUE_SIZE = 8192; // max elements queued

    private final AtomicLong invocationsCounter = new AtomicLong(0);
    private final AtomicLong callBackExecutionsCounter = new AtomicLong(0);

    private Duration duration;
    private int maxSize;
    private ExecutorService executorService;
	private UnicastProcessor<Tuple> source;

    private static final String TO_STRING_FORMAT="DelayedBatchExecutor {invocationsCounter=%d, callBackExecutionsCounter=%d, duration=%d, size=%d}";

    protected DelayedBatchExecutor(Duration duration, int maxSize, ExecutorService executorService) {
        super();
        boolean configurationSuccessful = updateConfig(duration, maxSize, executorService);
        if (!configurationSuccessful) {
            throw new RuntimeException("Illegal configuration parameters");
        }
    }

    /**
     * Update the Duration and maxsize params of this Delayed Batch Executor
     * This method is thread safe
     * <br>
     * @param  duration  the new {@link Duration} for this Delayed Batch Executor
     * @param  maxSize  the new maxsize  for this Delayed Batch Executor
     * @return  true if the configuration was successful updated, false otherwise
     *
     */
    public  boolean updateConfig(Duration duration, int maxSize) {
        return updateConfig(duration, maxSize, executorService);
    }

    /**
     * Update the Duration, maxsize and ExecutorService params of this Delayed Batch Executor
     * This method is thread safe
     * <br>
     * @param  duration  the new {@link Duration} for this Delayed Batch Executor
     * @param  maxSize  the new maxsize  for this Delayed Batch Executor
     * @param  executorService the new {@link ExecutorService} for this Delayed Batch Executor
     * @return  true if the configuration was successful updated, false otherwise
     *
     */
    public synchronized boolean updateConfig(Duration duration, int maxSize, ExecutorService executorService) {
        boolean validateConfig = validateConfigurationParameters(duration, maxSize, executorService);
        if (validateConfig) {
            boolean parameterAreEqualToCurrentOnes = parameterAreEqualToCurrentOnes(duration,  maxSize, executorService);
            if (!parameterAreEqualToCurrentOnes) {
                this.maxSize =maxSize;
                this.duration=duration;
                this.executorService=executorService;
                this.source = createBufferedTimeoutUnicastProcessor(duration, maxSize);
            }
        }
        return validateConfig;
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
    public Integer getMaxSize() {
        return maxSize;
    }


    @Override
    public String toString() {
        return String.format(TO_STRING_FORMAT, invocationsCounter.get(), callBackExecutionsCounter.get(),  duration.toMillis(), maxSize);
    }

    <Z> void enlistTuple(Tuple<Z> param) {
        invocationsCounter.incrementAndGet();
        source.onNext(param);
    }

    protected abstract List<Object> getResultListFromBatchCallBack(List<List<Object>>  transposedTupleList);

    protected static ExecutorService getNewDefaultExecutorService() {
        return Executors.newFixedThreadPool(DEFAULT_FIXED_THREAD_POOL_COUNTER);
    }

    private BatchCallBackExecutionResult getExecutionResultFromBatchCallback(List<Tuple> tupleList) {
        List<Object> resultFromCallBack=null;
        RuntimeException runtimeException=null;

        List<List<Object>> transposedTupleList = TupleListTransposer.transpose(tupleList);
        try {
             resultFromCallBack = getResultListFromBatchCallBack(transposedTupleList);
        } catch (RuntimeException re) {
            runtimeException=re;
        }
        return new BatchCallBackExecutionResult(resultFromCallBack, runtimeException, tupleList.size());
    }

    private void executeBatchCallBack(List<Tuple> tupleList) {
        callBackExecutionsCounter.incrementAndGet();
        CompletableFuture.runAsync(() -> {
            BatchCallBackExecutionResult batchCallBackExecutionResult = getExecutionResultFromBatchCallback(tupleList);

            for (int index=0; index<tupleList.size(); index++) {
                Tuple tuple = tupleList.get(index);
                tuple.setResult(batchCallBackExecutionResult.getReturnedResultOrNull(index));
                tuple.setRuntimeException(batchCallBackExecutionResult.getThrownRuntimeExceptionOrNull());
                tuple.commitResult();
                tuple.continueIfIsWaiting();
            }
         }, this.executorService);
    }

    private  UnicastProcessor<Tuple> createBufferedTimeoutUnicastProcessor(Duration duration, int maxSize) {
        Queue<Tuple> blockingQueue =  new ArrayBlockingQueue<>(QUEUE_SIZE) ; // => https://github.com/reactor/reactor-core/issues/469
        UnicastProcessor<Tuple> newSource=UnicastProcessor.create(blockingQueue);
        newSource.publish().autoConnect().bufferTimeout(maxSize, duration).subscribe(this::executeBatchCallBack);
        return newSource;
    }

    private boolean validateConfigurationParameters(Duration duration, int maxSize, ExecutorService executorService) {
        boolean sizeValidation = maxSize >= 1 && maxSize < QUEUE_SIZE;
        boolean durationValidation = duration!=null && duration.toMillis()>=MIN_TIME_WINDOW_TIME_IN_MILLISECONDS && duration.toMillis()<=MAX_TIME_WINDOW_TIME_IN_MILLISECONDS;
        boolean executorServiceValidation = executorService!=null;
        return sizeValidation && durationValidation && executorServiceValidation;
    }

    private boolean parameterAreEqualToCurrentOnes(Duration duration, int size, ExecutorService executorService) {
        boolean sameDuration = this.duration != null && this.duration.compareTo(duration) == 0;
        boolean sameSize = (this.maxSize == size);
        boolean sameExecutorService = this.executorService != null && this.executorService == executorService; // same reference is enough
        return sameDuration && sameSize && sameExecutorService;
    }
}