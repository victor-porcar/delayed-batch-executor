package com.github.victormpcmun.delayedbatchexecutor;

import com.github.victormpcmun.delayedbatchexecutor.windowtime.FixWindowTime;
import reactor.core.publisher.Flux;
import reactor.core.publisher.UnicastProcessor;

import java.time.Duration;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public abstract class DelayedBatchExecutor {


    private AtomicLong callBackExecutionsCounter = new AtomicLong(0);

    private Duration usingDuration;
    private Integer usingSize;


    private static final int DEFAULT_FIXED_THREAD_POOL_COUNTER = 10;
    private static final int QUEUE_SIZE = 4096; // max elements queued

    private WindowTime windowTime;

	private  UnicastProcessor<Tuple> source;
    private Queue<Tuple> blockingQueue;
    private ExecutorService executorService;

    protected DelayedBatchExecutor(WindowTime windowTime, ExecutorService executorService) {
        super();


        this.windowTime = windowTime;
        this.executorService = executorService;



        windowTime.setDelayedBatchExecutor(this);
        windowTime.startup();

    }


    public void updateBufferedTimeoutFlux(Duration duration, int size) {
        boolean sameDuration = this.usingDuration!=null && this.usingDuration.compareTo(duration)==0;
        boolean sameSize = this.usingSize!=null && this.usingSize.compareTo(size)==0;
        boolean sameParameters=sameDuration && sameSize;

        if (!sameParameters) {
            this.blockingQueue =  new ArrayBlockingQueue<>(QUEUE_SIZE) ; // => https://github.com/reactor/reactor-core/issues/469
            this.source  = UnicastProcessor.create(blockingQueue);
            Flux<Tuple> flux = source.publish().autoConnect();
            this.usingDuration=duration;
            this.usingSize=size;
            flux.bufferTimeout(size, duration).subscribe(this::executeList);
        }


    }

    protected static ExecutorService getNewDefaultExecutorService() {
        return Executors.newFixedThreadPool(DEFAULT_FIXED_THREAD_POOL_COUNTER);
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
        windowTime.invocation();
        source.onNext(param);
    }


    @Override
    public String toString() {
        return "DelayedBatchExecutor {" +
                " usingDuration=" + usingDuration.toMillis() +
                ", usingSize=" + usingSize +
                ", callBackExecutionsCounter=" + callBackExecutionsCounter +
                "}";
    }
}