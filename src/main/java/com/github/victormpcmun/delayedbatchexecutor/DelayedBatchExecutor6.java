package com.github.victormpcmun.delayedbatchexecutor;



import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * A subclass of a DelayedBatchExecutor for five arguments.
 * @author Victor Porcar
 *
 */
public class DelayedBatchExecutor6<Z,A,B,C,D,E> extends DelayedBatchExecutor {

    private final CallBack6 handler;


    @FunctionalInterface
    public interface CallBack6<Z,A,B,C,D,E> {
        List<Z> apply(List<A> firstParam, List<B> secondParam, List<C> thirdParam, List<D> fourthParam, List<E> fifthParam);
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


   private DelayedBatchExecutor6(Duration windowTime, int size, CallBack6 handler) {
        super(windowTime, size);
        this.handler=handler;
    }


    /**
     * returns the result of execution of the callback method of the DelayedBatchExecutor for the given parameters.
     * It blocks the execution of the thread until the result is available
     * which means that it could take in the worst case the windowTime defined for the DelayedBatchExecutor.
     *
     * <p>
     *
     * @param  arg1 an instance of the first  argument defined for the DelayedBatchExecutor
     * @param  arg2 an instance of the second argument defined for the DelayedBatchExecutor
     * @param  arg3 an instance of the third argument defined for the DelayedBatchExecutor
     * @param  arg4 an instance of the fourth argument defined for the DelayedBatchExecutor
     * @param  arg5 an instance of the fifth argument defined for the DelayedBatchExecutor
     * @return  an instance of type Z
     *
     * @author Victor Porcar
     *
     */
    public Z execute(A arg1, B arg2, C arg3, D arg4, E arg5) {
        Future<Z> future = executeAsFuture(arg1,arg2,arg3,arg4,arg5);
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException ee) {
            throw new RuntimeException("Interrupted waiting.  it shouldn't happen ever", ee);
        }
    }


    /**
     * returns a Future of the result of execution of the callback method of the DelayedBatchExecutor for the given parameters.
     * It does NOT block the execution of the thread.
     *
     * <p>
     *
     * @param  arg1 an instance of the first  argument defined for the DelayedBatchExecutor
     * @param  arg2 an instance of the second argument defined for the DelayedBatchExecutor
     * @param  arg3 an instance of the third argument defined for the DelayedBatchExecutor
     * @param  arg4 an instance of the fourth argument defined for the DelayedBatchExecutor
     * @param  arg5  an instance of the fifth argument defined for the DelayedBatchExecutor
     * @return  a Future of of type Z
     *
     * @author Victor Porcar
     *
     */
    public Future<Z> executeAsFuture(A arg1, B arg2, C arg3, D arg4, E arg5) {
        TupleFuture<Z> tupleFuture = TupleFuture.create(arg1, arg2, arg3, arg4, arg5);
        enlistTuple(tupleFuture);
        Future<Z> future = tupleFuture.getFuture();
        return future;
    }

    public <Z> Mono<Z> executeAsMono(A arg1, B arg2,C arg3, D arg4, E arg5) {
        TupleMono<Z> tupleMono = TupleMono.create(arg1, arg2, arg3, arg4, arg5);
        enlistTuple(tupleMono);
        Mono<Z> mono = tupleMono.getMono();
        return mono;
    }


    @Override
    protected  List<Object> getResultFromTupleList( List<List<Object>> transposedTupleList) {
        List<Object> resultList = handler.apply(
                transposedTupleList.get(0),
                transposedTupleList.get(1),
                transposedTupleList.get(2),
                transposedTupleList.get(3),
                transposedTupleList.get(4)
        );
        return resultList;
    }
}