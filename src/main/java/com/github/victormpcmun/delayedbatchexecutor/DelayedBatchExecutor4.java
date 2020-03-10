package com.github.victormpcmun.delayedbatchexecutor;


import com.github.victormpcmun.delayedbatchexecutor.tuple.Tuple;
import com.github.victormpcmun.delayedbatchexecutor.tuple.TupleFuture;
import com.github.victormpcmun.delayedbatchexecutor.tuple.TupleListTransposer;
import com.github.victormpcmun.delayedbatchexecutor.tuple.TupleMono;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * A subclass of a DelayedBatchExecutor for three arguments.
 * @author Victor Porcar
 *
 */
public class DelayedBatchExecutor4<Z,A,B,C> extends DelayedBatchExecutor {

    private final CallBack4 handler;

    @FunctionalInterface
    public interface CallBack4<Z,A,B,C> {
        List<Z> apply(List<A> firstParam, List<B> secondParam, List<C> thirdParam);
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



    private DelayedBatchExecutor4(Duration windowTime, int size, CallBack4 handler) {
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
     * @return  an instance of type Z
     *
     * @author Victor Porcar
     *
     */
    public Z execute(A arg1, B arg2, C arg3) {
        Future<Z> future = executeAsFuture(arg1,arg2,arg3);
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Interrupted waiting.  it shouldn't happen ever", e);
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
     * @return  a Future of of type Z
     *
     * @author Victor Porcar
     *
     */
    public Future<Z> executeAsFuture(A arg1, B arg2, C arg3) {
        TupleFuture<Z> tupleFuture = TupleFuture.create(arg1, arg2, arg3);
        enlistTuple(tupleFuture);
        Future<Z> future = tupleFuture.getFuture();
        return future;
    }

    public <Z> Mono<Z> executeAsMono(A arg1, B arg2,C arg3) {
        TupleMono<Z> tupleMono = TupleMono.create(arg1, arg2, arg3);
        enlistTuple(tupleMono);
        Mono<Z> mono = tupleMono.getMono();
        return mono;
    }


    @Override
    protected  List<Object> getResultFromTupleList( List<List<Object>> transposedTupleList) {
        List<Object> resultList = handler.apply(
                transposedTupleList.get(0),
                transposedTupleList.get(1),
                transposedTupleList.get(2)
        );
        return resultList;
    }
}