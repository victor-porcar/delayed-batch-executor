package com.vp.delayedbatchexecutor;

import reactor.core.publisher.Flux;
import reactor.core.publisher.UnicastProcessor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;


public abstract class DelayedBatchExecutor {

    public static final int MAX_SIZE = 1024;
	public static final Duration MAX_TIME = Duration.ofSeconds(10);
	private static final Duration MIN_TIME=Duration.ofMillis(1);


    private static final int QUEUE_SIZE = 4096; // max elements queued

    private final int size;
    private final Duration windowTime;

	private final UnicastProcessor<Tuple> source;


    @FunctionalInterface
    public interface DelayedParameterFunction2<Z,A> {
          List<Z> apply(List<A> firstParam);
    }

    @FunctionalInterface
    public interface DelayedParameterFunction3<Z,A,B> {
        List<Z> apply(List<A> firstParam, List<B> secondParam);
    }

    @FunctionalInterface
    public interface DelayedParameterFunction4<Z,A,B,C> {
        List<Z> apply(List<A> firstParam, List<B> secondParam, List<C> thirdParam);
    }

    @FunctionalInterface
    public interface DelayedParameterFunction5<Z,A,B,C,D> {
        List<Z> apply(List<A> firstParam, List<B> secondParam, List<C> thirdParam, List<D> fourthParam);
    }

    @FunctionalInterface
    public interface DelayedParameterFunction6<Z,A,B,C,D,E> {
        List<Z> apply(List<A> firstParam, List<B> secondParam, List<C> thirdParam, List<D> fourthParam, List<E> fifthParam);
    }


    // 2 PARAM

    public static <Z,A> DelayedBatchExecutor2<Z,A> define(Duration windowTime, int size, DelayedParameterFunction2<Z,A> delayedParameterFunction) {
        return new DelayedBatchExecutor2<>(windowTime, size, delayedParameterFunction);
    }

    // 3 PARAM

    public static <Z,A,B> DelayedBatchExecutor3<Z,A,B> define(Duration windowTime, int size, DelayedParameterFunction3<Z,A,B> delayedParameterFunction) {
        return new DelayedBatchExecutor3<>(windowTime, size, delayedParameterFunction);
    }

    // 4 PARAM

    public static <Z,A,B,C> DelayedBatchExecutor4<Z,A,B,C> define(Duration windowTime, int size, DelayedParameterFunction4<Z,A,B,C> delayedParameterFunction) {
        return new DelayedBatchExecutor4<>(windowTime, size, delayedParameterFunction);
    }


    // 5 PARAM

    public static <Z,A,B,C,D> DelayedBatchExecutor5<Z,A,B,C,D> define(Duration windowTime, int size, DelayedParameterFunction5<Z,A,B,C,D> delayedParameterFunction) {
        return new DelayedBatchExecutor5<>(windowTime, size, delayedParameterFunction);
    }

    // 6 PARAM

    public static <Z,A,B,C,D,E> DelayedBatchExecutor6<Z,A,B,C,D,E> define(Duration windowTime, int size, DelayedParameterFunction6<Z,A,B,C,D,E> delayedParameterFunction) {
        return new DelayedBatchExecutor6<>(windowTime, size, delayedParameterFunction);
    }


    protected DelayedBatchExecutor(Duration windowTime, int size) {
        super();
        validateBoundaries(size, windowTime);
        this.size = size;
        this.windowTime = windowTime;
        this.source = UnicastProcessor.create( new ArrayBlockingQueue<>(QUEUE_SIZE));  // => https://github.com/reactor/reactor-core/issues/469
		Flux<Tuple> flux = source.publish().autoConnect();
        flux.bufferTimeout(size, windowTime).subscribe(this::executeList);
    }


    protected abstract List<Object> getResultFromTupleList(TupleListArgs tupleListArgs);

    private void executeList(List<Tuple> paramList) {
        CompletableFuture.runAsync(() -> {

            List result = getResultFromTupleList(new TupleListArgs(paramList));
            List resizedList = ensureSizeFillingWithNullsIfNecessary(result, paramList.size());

            for (int index=0; index<paramList.size(); index++) {
                Tuple tuple = paramList.get(index);
                tuple.setResult(resizedList.get(index));
                tuple.commitResult();
                tuple.continueIfIsWaiting();
            }

        });
    }


    Tuple executeWithArgs(Object ...args) {
        Tuple param = new Tuple(args);
        source.onNext(param);
        param.waitIfResultHasNotCommitted();
        return param;
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