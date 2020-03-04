package com.github.victormpcmun.delayedbatchexecutor;

import com.github.victormpcmun.delayedbatchexecutor.tuple.Tuple;
import com.github.victormpcmun.delayedbatchexecutor.tuple.TupleListArgs;
import com.github.victormpcmun.delayedbatchexecutor.tuple.TupleMono;
import reactor.core.publisher.Flux;
import reactor.core.publisher.MonoSink;
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
    public interface CallBack2<Z,A> {
          List<Z> apply(List<A> firstParam);
    }

    @FunctionalInterface
    public interface CallBack3<Z,A,B> {
        List<Z> apply(List<A> firstParam, List<B> secondParam);
    }

    @FunctionalInterface
    public interface CallBack4<Z,A,B,C> {
        List<Z> apply(List<A> firstParam, List<B> secondParam, List<C> thirdParam);
    }

    @FunctionalInterface
    public interface CallBack5<Z,A,B,C,D> {
        List<Z> apply(List<A> firstParam, List<B> secondParam, List<C> thirdParam, List<D> fourthParam);
    }

    @FunctionalInterface
    public interface CallBack6<Z,A,B,C,D,E> {
        List<Z> apply(List<A> firstParam, List<B> secondParam, List<C> thirdParam, List<D> fourthParam, List<E> fifthParam);
    }



    /**
     * Creates an instance of a DelayedBatchExecutor2&lt;Z,A&gt;,
     * which is a subclass of a DelayedBatchExecutor for one argument.
     * <p>
     * @param  <Z>  the return type
     * @param  <A>  the type of the argument
     * @param  windowTime  the time of the window time, defined as a java.time.Duration
     * @param  size the maximum number of parameters, whenever the number of parameters reaches this limit, the window time is close and the callback method is executed
     * @param  callBack the method that will receive a list of type A and returns a list of Type Z, which must have he size of the list received
     * @return  an instance of type Z
     *
     * @author Victor Porcar
     *
     */


    public static <Z,A> DelayedBatchExecutor2<Z,A> define(Duration windowTime, int size, CallBack2<Z,A> callBack) {
        return new DelayedBatchExecutor2<>(windowTime, size, callBack);
    }


    /**
     * Creates an instance of a DelayedBatchExecutor3&lt;Z,A,B&gt;,
     * which is a subclass of a DelayedBatchExecutor for two arguments.
     * <p>
     * @param  <Z>  the return type
     * @param  <A>  the type of the first argument
     * @param  <B>  the type of the second argument
     * @param  windowTime  the time of the window time, defined as a java.time.Duration
     * @param  size the maximum number of parameters, whenever the number of parameters reaches this limit, the window time is close and the callback method is executed
     * @param  callBack the method that will receive a list of type A and a list of type B, having both the same size and return a list Type Z, which must have he size of the lists received
     * @return  an instance of type Z
     *
     * @author Victor Porcar
     *
     */

    public static <Z,A,B> DelayedBatchExecutor3<Z,A,B> define(Duration windowTime, int size, CallBack3<Z,A,B> callBack) {
        return new DelayedBatchExecutor3<>(windowTime, size, callBack);
    }

    /**
     * Creates an instance of a DelayedBatchExecutor4&lt;Z,A,B,C&gt;,
     * which is a subclass of a DelayedBatchExecutor for three arguments.
     * <p>
     * @param  <Z>  the return type
     * @param  <A>  the type of the first argument
     * @param  <B>  the type of the second argument
     * @param  <C>  the type of the third argument
     * @param  windowTime  the time of the window time, defined as a java.time.Duration
     * @param  size the maximum number of parameters, whenever the number of parameters reaches this limit, the window time is close and the callback method is executed
     * @param  callBack the method that will receive a list of type A,  a list of type B and a list of type C having all of them the same size and return a list Type Z, which must have he size of the lists received
     * @return  an instance of type Z
     *
     * @author Victor Porcar
     *
     */

    public static <Z,A,B,C> DelayedBatchExecutor4<Z,A,B,C> define(Duration windowTime, int size, CallBack4<Z,A,B,C> callBack) {
        return new DelayedBatchExecutor4<>(windowTime, size, callBack);
    }


    /**
     * Creates an instance of a DelayedBatchExecutor5&lt;Z,A,B,C,D&gt;,
     * which is a subclass of a DelayedBatchExecutor for four arguments.
     * @param  <Z>  the return type
     * @param  <A>  the type of the first argument
     * @param  <B>  the type of the second argument
     * @param  <C>  the type of the third argument
     * @param  <D>  the type of the fourth argument
     * @param  windowTime  the time of the window time, defined as a java.time.Duration
     * @param  size the maximum number of parameters, whenever the number of parameters reaches this limit, the window time is close and the callback method is executed
     * @param  callBack the method that will receive a list of type A,  a list of type B, a list of type C and a list of type D having all of them the same size and return a list Type Z, which must have he size of the lists received
     * @return  an instance of type Z
     *
     * @author Victor Porcar
     *
     */
    public static <Z,A,B,C,D> DelayedBatchExecutor5<Z,A,B,C,D> define(Duration windowTime, int size, CallBack5<Z,A,B,C,D> callBack) {
        return new DelayedBatchExecutor5<>(windowTime, size, callBack);
    }

    /**
     * Creates an instance of a DelayedBatchExecutor6&lt;Z,A,B,C,D&gt;,
     * which is a subclass of a DelayedBatchExecutor for five arguments.
     * @param  <Z>  the return type
     * @param  <A>  the type of the first argument
     * @param  <B>  the type of the second argument
     * @param  <C>  the type of the third argument
     * @param  <D>  the type of the fourth argument
     * @param  <E>  the type of the fifth argument
     * @param  windowTime  the time of the window time, defined as a java.time.Duration
     * @param  size the maximum number of parameters, whenever the number of parameters reaches this limit, the window time is close and the callback method is executed
     * @param  callBack the method that will receive a list of type A,  a list of type B, a list of type C, a list of type D and a list of type E having all of them the same size and return a list Type Z, which must have he size of the lists received
     * @return  an instance of type Z
     *
     * @author Victor Porcar
     *
     */
    public static <Z,A,B,C,D,E> DelayedBatchExecutor6<Z,A,B,C,D,E> define(Duration windowTime, int size, CallBack6<Z,A,B,C,D,E> callBack) {
        return new DelayedBatchExecutor6<>(windowTime, size, callBack);
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


    static <Z> void doSetSink(TupleMono<Z> tuple, MonoSink<Z> sinker) {
        tuple.setMonoSink(sinker);
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


   <Z> void executeWithArgs(Tuple<Z> param) {
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