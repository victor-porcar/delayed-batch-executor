package com.github.victormpcmun.delayedbatchexecutor;

import com.github.victormpcmun.delayedbatchexecutor.tuple.Tuple;
import com.github.victormpcmun.delayedbatchexecutor.tuple.TupleListTransposer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.UnicastProcessor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;

abstract class DelayedBatchExecutor {

    protected static final int MAX_SIZE = 1024;
	protected static final Duration MAX_TIME = Duration.ofSeconds(10);
	private static final Duration MIN_TIME=Duration.ofMillis(1);


    private static final int QUEUE_SIZE = 4096; // max elements queued

    private final int size;
    private final Duration windowTime;

	private final UnicastProcessor<Tuple> source;
    private Queue<Tuple> threadSafeQueue;
    private ExecutorService executorService;


    protected DelayedBatchExecutor(Duration windowTime, int size) {
       this(windowTime, size, null);
    }


    protected DelayedBatchExecutor(Duration windowTime, int size, ExecutorService executorService) {
        super();
        validateBoundaries(size, windowTime);
        this.size = size;
        this.windowTime = windowTime;
        this.executorService = executorService;

        this.threadSafeQueue =  new ArrayBlockingQueue<>(QUEUE_SIZE) ; // => https://github.com/reactor/reactor-core/issues/469
        this.source = UnicastProcessor.create(threadSafeQueue);
        Flux<Tuple> flux = source.publish().autoConnect();
        flux.bufferTimeout(size, windowTime).subscribe(this::executeList);
    }




    protected abstract List<Object> getResultFromTupleList(List<List<Object>>  transposedTupleList);

    private void executeList(List<Tuple> tupleList) {
        CompletableFuture.runAsync(() -> {
            List result = new ArrayList();
            List resizedList = new ArrayList();
            RuntimeException runtimeException=null;
            try {
                List<List<Object>> transposedTupleList = TupleListTransposer.transpose(tupleList);
                result = getResultFromTupleList(transposedTupleList);
            } catch (RuntimeException e) {
                    runtimeException=e;
            }
            if (runtimeException==null) {
                resizedList = ensureSizeFillingWithNullsIfNecessary(result, tupleList.size());
            }

            for (int index=0; index<tupleList.size(); index++) {
                Tuple tuple = tupleList.get(index);
                if (runtimeException==null) {
                    tuple.setResult(resizedList.get(index));
                } else {
                    tuple.setRuntimeException(runtimeException);
                }
                tuple.commitResult();
                tuple.continueIfIsWaiting();
            }
         }, this.executorService);
    }


   <Z> void enlistTuple(Tuple<Z> param) {
        source.onNext(param);
    }

    private void validateBoundaries(int size, Duration time) {
        if (size < 1 || size > MAX_SIZE)  {
            throw new IllegalArgumentException("max elements parameter must be in range ["+ 1 + ","+ MAX_SIZE + "]");
        }

        if (MAX_TIME.compareTo(time) < 0 || time.compareTo(MIN_TIME) < 0) {
            throw new IllegalArgumentException("time window parameter must be in range ["+ 1 + ","+ MAX_TIME.toMillis() + "] ms");
        }
    }

    @Override
    public String toString() {
        return "DelayedBulkExecutor [size=" + size + ", windowTime=" + windowTime + "]";
    }

    private <T> List<T> ensureSizeFillingWithNullsIfNecessary(List<T> list, int size) {
        if (list==null) {
            list=Collections.nCopies(size,  null);
        } else if (list.size()<size) {
            list = new ArrayList(list); // make it mutable in case it isn't
            list.addAll(Collections.nCopies(size-list.size(),null));
        }
        return list;
    }


}