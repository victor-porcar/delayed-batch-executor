package com.github.victormpcmun.delayedbatchexecutor;

import reactor.core.publisher.UnicastProcessor;

import java.time.Duration;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

abstract class DelayedBatchExecutor {

    private static final String TO_STRING_FORMAT="DelayedBatchExecutor {invocationsCounter=%d, callBackExecutionsCounter=%d, duration=%d, size=%d, bufferQueueSize=%d}";
    public static final int MIN_TIME_WINDOW_TIME_IN_MILLISECONDS=1;
    public static final int MAX_TIME_WINDOW_TIME_IN_MILLISECONDS=60*60*1000;

    public static final int DEFAULT_FIXED_THREAD_POOL_COUNTER = 4;
    public static final int DEFAULT_BUFFER_QUEUE_SIZE = 8192;

    private final AtomicLong invocationsCounter;
    private final AtomicLong callBackExecutionsCounter;

    private Duration duration;
    private int maxSize;
    private ExecutorService executorService;
    private int bufferQueueSize;
	private UnicastProcessor<Tuple> source;

    protected DelayedBatchExecutor(Duration duration, int maxSize, ExecutorService executorService, int bufferQueueSize) {
        super();
        boolean configurationSuccessful = updateConfig(duration, maxSize, executorService, bufferQueueSize);
        if (!configurationSuccessful) {
            throw new RuntimeException("Illegal configuration parameters");
        }
        invocationsCounter = new AtomicLong(0);
        callBackExecutionsCounter = new AtomicLong(0);
    }

    /**
     * Update the Duration and maxSize params of this Delayed Batch Executor, keeping the current existing value for executorService and bufferQueueSize
     * <br>
     * <br>
     * This method is thread safe
     * <br>
     * @param  duration  the new {@link Duration} for this Delayed Batch Executor
     * @param  maxSize  the new maxsize  for this Delayed Batch Executor
     * @return  true if the configuration was successful updated, false otherwise
     *
     */
    public boolean updateConfig(Duration duration, int maxSize) {
        return updateConfig(duration, maxSize, executorService, bufferQueueSize);
    }

    /**
     * Update the Duration, maxsize, ExecutorService  and bufferQueueSize params of this Delayed Batch Executor
     * <br>
     * <br>
     * This method is thread safe
     * <br>
     * @param  duration  the new {@link Duration} for this Delayed Batch Executor
     * @param  maxSize  the new maxsize  for this Delayed Batch Executor
     * @param  executorService the new {@link ExecutorService} for this Delayed Batch Executor
     * @param  bufferQueueSize max size of the internal queue to buffer values
     * @return  true if the configuration was successful updated, false otherwise
     *
     */
    public synchronized boolean updateConfig(Duration duration, int maxSize,  ExecutorService executorService, int bufferQueueSize) {
        boolean validateConfig = validateConfigurationParameters(duration, maxSize, executorService, bufferQueueSize);
        if (validateConfig) {
            boolean parameterAreEqualToCurrentOnes = parameterAreEqualToCurrentOnes(duration,  maxSize, executorService, bufferQueueSize);
            if (!parameterAreEqualToCurrentOnes) {
                this.maxSize =maxSize;
                this.duration=duration;
                this.executorService=executorService;
                this.bufferQueueSize=bufferQueueSize;
                this.source = createBufferedTimeoutUnicastProcessor(duration, maxSize, bufferQueueSize);

            }
        }
        return validateConfig;
    }

    /**
     * The count of invocations of all of the execute methods of this Delayed Batch Executor: execute(...), executeAsFuture(...) or executeAsMono(...) since the creation of this Delayed Batch Executor
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

    /**
     * The current max size of the internal queue to buffer values
     * @return  the current max size of the internal queue to buffer values
     *
     */
    public Integer getBufferQueueSize() {
        return bufferQueueSize;
    }

    /**
     * The current ExecutorService
     * @return  the current ExecutorService
     *
     */
    public ExecutorService getExecutorService() {
        return executorService;
    }

    @Override
    public String toString() {
        return String.format(TO_STRING_FORMAT, invocationsCounter.get(), callBackExecutionsCounter.get(),  duration.toMillis(), maxSize, bufferQueueSize);
    }

    protected <Z> void enlistTuple(Tuple<Z> param) {
        invocationsCounter.incrementAndGet();
        source.onNext(param);
    }

    protected abstract List<Object> getResultListFromBatchCallBack(List<List<Object>>  transposedTupleList);

    protected static ExecutorService getDefaultExecutorService() {
        return Executors.newFixedThreadPool(DEFAULT_FIXED_THREAD_POOL_COUNTER);
    }

    private BatchCallBackExecutionResult getExecutionResultFromBatchCallback(List<Tuple> tupleList) {
        List<Object> resultFromCallBack=null;
        RuntimeException runtimeException=null;

        List<List<Object>> transposedTupleList = TupleListTransposer.transposeValuesAsListOfList(tupleList);
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

            for (int indexTuple=0; indexTuple<tupleList.size(); indexTuple++) {
                Tuple tuple = tupleList.get(indexTuple);
                tuple.setResult(batchCallBackExecutionResult.getReturnedResultOrNull(indexTuple));
                tuple.setRuntimeException(batchCallBackExecutionResult.getThrownRuntimeExceptionOrNull());
                tuple.commitResult();
                tuple.continueIfIsWaiting();
            }
         }, this.executorService);
    }

    private  UnicastProcessor<Tuple> createBufferedTimeoutUnicastProcessor(Duration duration, int maxSize, int bufferQueueSize) {
        Queue<Tuple> blockingQueue =  new ArrayBlockingQueue<>(bufferQueueSize) ; // => https://github.com/reactor/reactor-core/issues/469
        UnicastProcessor<Tuple> newSource=UnicastProcessor.create(blockingQueue);
        newSource.publish().autoConnect().bufferTimeout(maxSize, duration).subscribe(this::executeBatchCallBack);
        return newSource;
    }

    private boolean validateConfigurationParameters(Duration duration, int maxSize, ExecutorService executorService, int bufferQueueSize) {
        boolean sizeValidation = (maxSize >= 1);
        boolean durationValidation = duration!=null && duration.toMillis()>=MIN_TIME_WINDOW_TIME_IN_MILLISECONDS && duration.toMillis()<=MAX_TIME_WINDOW_TIME_IN_MILLISECONDS;
        boolean executorServiceValidation = (executorService!=null);
        boolean bufferQueueSizeValidation = (bufferQueueSize>=1);
        return sizeValidation && durationValidation && executorServiceValidation && bufferQueueSizeValidation;
    }

    private boolean parameterAreEqualToCurrentOnes(Duration duration, int size, ExecutorService executorService, int bufferQueueSize) {
        boolean sameDuration = this.duration != null && this.duration.compareTo(duration) == 0;
        boolean sameSize = (this.maxSize == size);
        boolean sameExecutorService = this.executorService != null && this.executorService == executorService; // same reference is enough
        boolean sameBufferQueueSize = (this.bufferQueueSize==bufferQueueSize);
        return sameDuration && sameSize && sameExecutorService && sameBufferQueueSize;
    }
}