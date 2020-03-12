package com.github.victormpcmun.delayedbatchexecutor;

import reactor.core.publisher.Flux;
import reactor.core.publisher.UnicastProcessor;

import java.time.Duration;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;

abstract class DelayedBatchExecutor {

    private static final int DEFAULT_FIXED_THREAD_POOL_COUNTER = 10;
    private static final int QUEUE_SIZE = 4096; // max elements queued

    private final int size;
    private final Duration windowTime;

	private final UnicastProcessor<Tuple> source;
    private Queue<Tuple> blockingQueue;
    private ExecutorService executorService;


    protected DelayedBatchExecutor(Duration windowTime, int size) {
       this(windowTime, size, Executors.newFixedThreadPool(DEFAULT_FIXED_THREAD_POOL_COUNTER));
    }


    protected DelayedBatchExecutor(Duration windowTime, int size, ExecutorService executorService) {
        super();
        DelayedBatchExecutorValidator.validateBoundaries(size, windowTime, executorService);
        this.size = size;
        this.windowTime = windowTime;
        this.executorService = executorService;

        this.blockingQueue =  new ArrayBlockingQueue<>(QUEUE_SIZE) ; // => https://github.com/reactor/reactor-core/issues/469
        this.source = UnicastProcessor.create(blockingQueue);
        Flux<Tuple> flux = source.publish().autoConnect();
        flux.bufferTimeout(size, windowTime).subscribe(this::executeList);
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
        CompletableFuture.runAsync(() -> {

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
        source.onNext(param);
    }


    @Override
    public String toString() {
        return "DelayedBulkExecutor [size=" + size + ", windowTime=" + windowTime + "]";
    }

}